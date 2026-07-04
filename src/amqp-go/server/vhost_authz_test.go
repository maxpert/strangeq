package server

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"os"
	"testing"

	"github.com/maxpert/amqp-go/auth"
	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"go.uber.org/zap"
)

// ============================================================================
// B3: Vhost access check + loopback restriction tests (spec-driven)
//
// AMQP 0.9.1 spec, connection.open "security" rule (SHOULD):
//   "The server SHOULD verify that the client has permission to access
//    the specified virtual host."
//
// AMQP 0.9.1 spec, connection.open "separation" rule (MUST):
//   "If the server supports multiple virtual hosts, it MUST enforce a
//    full separation of exchanges, queues, and all associated entities
//    per virtual host."
//
// AMQP 0.9.1 spec, invalid-path (402):
//   "The client tried to work with an unknown virtual host."
//
// RabbitMQ loopback restriction:
//   "guest" user can only connect via localhost. Remote connections
//   with the default user are refused.
// ============================================================================

// mockConn wraps a net.Pipe conn to override RemoteAddr for loopback tests.
type mockConn struct {
	net.Conn
	remote net.Addr
}

func (m *mockConn) RemoteAddr() net.Addr { return m.remote }

func newLoopbackPipeConn(t *testing.T) *protocol.Connection {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() { clientConn.Close(); serverConn.Close() })
	go io.Copy(io.Discard, clientConn)
	return protocol.NewConnection(&mockConn{
		Conn:   serverConn,
		remote: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345},
	})
}

func newNonLoopbackPipeConn(t *testing.T) *protocol.Connection {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() { clientConn.Close(); serverConn.Close() })
	go io.Copy(io.Discard, clientConn)
	return protocol.NewConnection(&mockConn{
		Conn:   serverConn,
		remote: &net.TCPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 12345},
	})
}

// encodeConnectionOpen builds a protocol.Frame containing a connection.open
// method (class 10, method 40) with the given virtual host.
func encodeConnectionOpen(t *testing.T, vhost string) *protocol.Frame {
	t.Helper()
	method := &protocol.ConnectionOpenMethod{VirtualHost: vhost}
	data, err := method.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize connection.open: %v", err)
	}
	payload := make([]byte, 4+len(data))
	binary.BigEndian.PutUint16(payload[0:2], 10) // classID
	binary.BigEndian.PutUint16(payload[2:4], 40) // methodID
	copy(payload[4:], data)
	return &protocol.Frame{Type: protocol.FrameMethod, Channel: 0, Payload: payload}
}

func newAuthzTestServer(t *testing.T, authEnabled bool, authenticator interfaces.Authenticator) *Server {
	t.Helper()
	logger := zap.NewNop()
	cfg := config.DefaultConfig()
	cfg.Storage.Path = t.TempDir()
	cfg.Security.AuthenticationEnabled = authEnabled
	cfg.Security.AuthorizationEnabled = authEnabled

	storageImpl, err := storage.NewDisruptorStorageWithDataDir(cfg.Storage.Path)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	storageBroker := broker.NewStorageBroker(storageImpl, cfg.GetEngine())
	unifiedBroker := NewStorageBrokerAdapter(storageBroker)

	srv := &Server{
		Addr:             ":0",
		Connections:      make(map[string]*protocol.Connection),
		Log:              logger,
		Broker:           unifiedBroker,
		Config:           cfg,
		MetricsCollector: &NoOpMetricsCollector{},
		Authenticator:    authenticator,
	}
	if authenticator != nil {
		registry := auth.NewRegistry()
		registry.Register(&auth.PlainMechanism{})
		srv.MechanismRegistry = NewMechanismRegistryAdapter(registry)
	}
	return srv
}

// writeAuthFileForTest creates a temp auth file and returns a FileAuthenticator.
func writeAuthFileForTest(t *testing.T, users []auth.UserEntry) *auth.FileAuthenticator {
	t.Helper()
	dir := t.TempDir()
	path := dir + "/auth.json"
	type authFile struct {
		Users []auth.UserEntry `json:"users"`
	}
	data, err := json.MarshalIndent(authFile{Users: users}, "", "  ")
	if err != nil {
		t.Fatalf("failed to marshal auth file: %v", err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("failed to write auth file: %v", err)
	}
	a, err := auth.NewFileAuthenticator(path)
	if err != nil {
		t.Fatalf("failed to create authenticator: %v", err)
	}
	return a
}

// --- Tests ---

func TestVHostAccessAllowed(t *testing.T) {
	authenticator := writeAuthFileForTest(t, []auth.UserEntry{
		{
			Username:     "testuser",
			PasswordHash: "$2a$10$placeholder",
			VHostPermissions: []interfaces.VHostPermission{
				{VHost: "/", Permission: interfaces.Permission{Configure: ".*", Write: ".*", Read: ".*"}},
			},
		},
	})

	srv := newAuthzTestServer(t, true, authenticator)
	conn := newLoopbackPipeConn(t)

	// Simulate post-authentication state
	user, err := authenticator.GetUser("testuser")
	if err != nil {
		t.Fatalf("GetUser failed: %v", err)
	}
	conn.User = user

	// Process connection.open for vhost "/"
	err = srv.processConnectionOpen(conn, encodeConnectionOpen(t, "/"))
	if err != nil {
		t.Errorf("user with permission for / should be allowed: %v", err)
	}
	if conn.Vhost != "/" {
		t.Errorf("conn.Vhost should be /, got %q", conn.Vhost)
	}
}

func TestVHostAccessRefused(t *testing.T) {
	authenticator := writeAuthFileForTest(t, []auth.UserEntry{
		{
			Username:     "restricted",
			PasswordHash: "$2a$10$placeholder",
			VHostPermissions: []interfaces.VHostPermission{
				{VHost: "/prod", Permission: interfaces.Permission{Configure: ".*", Write: ".*", Read: ".*"}},
			},
		},
	})

	srv := newAuthzTestServer(t, true, authenticator)
	conn := newLoopbackPipeConn(t)

	user, err := authenticator.GetUser("restricted")
	if err != nil {
		t.Fatalf("GetUser failed: %v", err)
	}
	conn.User = user

	// User has permission for /prod but not /dev
	err = srv.processConnectionOpen(conn, encodeConnectionOpen(t, "/dev"))
	if err == nil {
		t.Error("user without permission for /dev should be refused")
	}
}

func TestVHostAccessAuthDisabled(t *testing.T) {
	srv := newAuthzTestServer(t, false, nil)
	conn := newLoopbackPipeConn(t)

	// When auth is disabled, any vhost should be allowed (not just "/")
	err := srv.processConnectionOpen(conn, encodeConnectionOpen(t, "/custom-vhost"))
	if err != nil {
		t.Errorf("auth disabled: vhost /custom-vhost should be allowed: %v", err)
	}
	if conn.Vhost != "/custom-vhost" {
		t.Errorf("conn.Vhost should be /custom-vhost, got %q", conn.Vhost)
	}
}

func TestLoopbackGuestAllowed(t *testing.T) {
	authenticator := writeAuthFileForTest(t, []auth.UserEntry{
		{
			Username:     "guest",
			PasswordHash: "$2a$10$placeholder",
			VHostPermissions: []interfaces.VHostPermission{
				{VHost: "/", Permission: interfaces.Permission{Configure: ".*", Write: ".*", Read: ".*"}},
			},
			LoopbackOnly: true,
		},
	})

	srv := newAuthzTestServer(t, true, authenticator)
	conn := newLoopbackPipeConn(t) // localhost

	user, err := authenticator.GetUser("guest")
	if err != nil {
		t.Fatalf("GetUser failed: %v", err)
	}
	conn.User = user

	// Guest from localhost should be allowed
	err = srv.processConnectionOpen(conn, encodeConnectionOpen(t, "/"))
	if err != nil {
		t.Errorf("guest from localhost should be allowed: %v", err)
	}
}

func TestLoopbackGuestRefused(t *testing.T) {
	authenticator := writeAuthFileForTest(t, []auth.UserEntry{
		{
			Username:     "guest",
			PasswordHash: "$2a$10$placeholder",
			VHostPermissions: []interfaces.VHostPermission{
				{VHost: "/", Permission: interfaces.Permission{Configure: ".*", Write: ".*", Read: ".*"}},
			},
			LoopbackOnly: true,
		},
	})

	srv := newAuthzTestServer(t, true, authenticator)
	conn := newNonLoopbackPipeConn(t) // non-localhost

	user, err := authenticator.GetUser("guest")
	if err != nil {
		t.Fatalf("GetUser failed: %v", err)
	}
	conn.User = user

	// Guest from non-localhost should be refused
	err = srv.processConnectionOpen(conn, encodeConnectionOpen(t, "/"))
	if err == nil {
		t.Error("guest from non-localhost should be refused")
	}
}

func TestLoopbackNonGuestAllowed(t *testing.T) {
	authenticator := writeAuthFileForTest(t, []auth.UserEntry{
		{
			Username:     "admin",
			PasswordHash: "$2a$10$placeholder",
			VHostPermissions: []interfaces.VHostPermission{
				{VHost: "/", Permission: interfaces.Permission{Configure: ".*", Write: ".*", Read: ".*"}},
			},
			LoopbackOnly: false,
		},
	})

	srv := newAuthzTestServer(t, true, authenticator)
	conn := newNonLoopbackPipeConn(t) // non-localhost

	user, err := authenticator.GetUser("admin")
	if err != nil {
		t.Fatalf("GetUser failed: %v", err)
	}
	conn.User = user

	// Non-guest user from non-localhost should be allowed
	err = srv.processConnectionOpen(conn, encodeConnectionOpen(t, "/"))
	if err != nil {
		t.Errorf("non-guest user from non-localhost should be allowed: %v", err)
	}
}

func TestVHostAccessAuthEnabledNilUser(t *testing.T) {
	// MAJOR-2: When authorization is enabled but conn.User is nil,
	// the connection must be refused (fail-closed, not fail-open).
	srv := newAuthzTestServer(t, true, nil) // auth enabled, no authenticator
	conn := newLoopbackPipeConn(t)
	// conn.User is nil (no authenticator configured to set it)

	err := srv.processConnectionOpen(conn, encodeConnectionOpen(t, "/"))
	if err == nil {
		t.Error("authorization enabled with nil user should be refused (fail-closed)")
	}
}

func TestVHostAccessEmptyPermissions(t *testing.T) {
	// MINOR-5: User with no VHostPermissions at all should be refused
	authenticator := writeAuthFileForTest(t, []auth.UserEntry{
		{
			Username:         "noperm",
			PasswordHash:     "$2a$10$placeholder",
			VHostPermissions: nil, // no permissions at all
		},
	})

	srv := newAuthzTestServer(t, true, authenticator)
	conn := newLoopbackPipeConn(t)

	user, err := authenticator.GetUser("noperm")
	if err != nil {
		t.Fatalf("GetUser failed: %v", err)
	}
	conn.User = user

	err = srv.processConnectionOpen(conn, encodeConnectionOpen(t, "/"))
	if err == nil {
		t.Error("user with no VHostPermissions should be refused for any vhost")
	}
}
