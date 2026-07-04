package server

import (
	"encoding/json"
	"io"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/maxpert/amqp-go/auth"
	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"go.uber.org/zap"
)

// serializer is an interface for methods that can serialize themselves.
type serializer interface {
	Serialize() ([]byte, error)
}

// ============================================================================
// B4: Per-operation authorization checks in handlers (spec-driven)
//
// RabbitMQ permission table (AMQP 0.9.1 operation → permission mapping):
//
//   exchange.declare  → configure on exchange
//   exchange.delete   → configure on exchange
//   queue.declare     → configure on queue
//   queue.delete      → configure on queue
//   queue.bind        → write on queue + read on exchange
//   queue.unbind      → write on queue + read on exchange
//   basic.publish     → write on exchange
//   basic.consume     → read on queue
//   basic.get         → read on queue
//   queue.purge       → read on queue
//
// AMQP 0.9.1 spec "reserved" rule (on-failure: access-refused 403):
//   exchange.declare/queue.declare with name starting "amq." and passive=false
//   → access-refused (403)
// ============================================================================

// --- Test helpers ---

func newB4Server(t *testing.T, authEnabled bool, users []auth.UserEntry) *Server {
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

	dir := t.TempDir()
	authPath := dir + "/auth.json"
	type authFile struct {
		Users []auth.UserEntry `json:"users"`
	}
	data, _ := json.MarshalIndent(authFile{Users: users}, "", "  ")
	os.WriteFile(authPath, data, 0600)
	authenticator, err := auth.NewFileAuthenticator(authPath)
	if err != nil {
		t.Fatalf("failed to create authenticator: %v", err)
	}

	srv := &Server{
		Addr:             ":0",
		Connections:      make(map[string]*protocol.Connection),
		Log:              logger,
		Broker:           unifiedBroker,
		Config:           cfg,
		MetricsCollector: &NoOpMetricsCollector{},
		Authenticator:    authenticator,
	}
	return srv
}

func newB4Conn(t *testing.T, authenticator interfaces.Authenticator, username string) *protocol.Connection {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() { clientConn.Close(); serverConn.Close() })
	go io.Copy(io.Discard, clientConn)
	conn := protocol.NewConnection(serverConn)
	conn.Vhost = "/"

	if authenticator != nil && username != "" {
		user, err := authenticator.GetUser(username)
		if err != nil {
			t.Fatalf("GetUser(%q) failed: %v", username, err)
		}
		conn.User = user
	}
	return conn
}

// encodeMethod serializes a method for passing directly to handler functions.
// Handlers expect just the method data (no classID/methodID prefix).
func encodeMethod(t *testing.T, classID, methodID uint16, method serializer) []byte {
	t.Helper()
	data, err := method.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize method: %v", err)
	}
	return data
}

// fullAdminUser returns a UserEntry with full permissions (.* on all three).
func fullAdminUser(username string) auth.UserEntry {
	return auth.UserEntry{
		Username:     username,
		PasswordHash: "$2a$10$placeholder",
		VHostPermissions: []interfaces.VHostPermission{
			{VHost: "/", Permission: interfaces.Permission{Configure: ".*", Write: ".*", Read: ".*"}},
		},
	}
}

// readOnlyUser has only read permission (configure=^$, write=^$, read=.*).
func readOnlyUser(username string) auth.UserEntry {
	return auth.UserEntry{
		Username:     username,
		PasswordHash: "$2a$10$placeholder",
		VHostPermissions: []interfaces.VHostPermission{
			{VHost: "/", Permission: interfaces.Permission{Configure: "^$", Write: "^$", Read: ".*"}},
		},
	}
}

// writeOnlyUser has only write permission (configure=^$, write=.*, read=^$).
func writeOnlyUser(username string) auth.UserEntry {
	return auth.UserEntry{
		Username:     username,
		PasswordHash: "$2a$10$placeholder",
		VHostPermissions: []interfaces.VHostPermission{
			{VHost: "/", Permission: interfaces.Permission{Configure: "^$", Write: ".*", Read: "^$"}},
		},
	}
}

// --- Exchange declare/delete tests ---

func TestAuthzExchangeDeclareAllowed(t *testing.T) {
	srv := newB4Server(t, true, []auth.UserEntry{fullAdminUser("admin")})
	conn := newB4Conn(t, srv.Authenticator, "admin")

	method := &protocol.ExchangeDeclareMethod{Exchange: "test-exchange", Type: "direct"}
	err := srv.handleExchangeDeclare(conn, 1, encodeMethod(t, 40, 10, method))
	if err != nil {
		t.Errorf("admin should be allowed to declare exchange: %v", err)
	}
}

func TestAuthzExchangeDeclareRefused(t *testing.T) {
	srv := newB4Server(t, true, []auth.UserEntry{readOnlyUser("reader")})
	conn := newB4Conn(t, srv.Authenticator, "reader")

	method := &protocol.ExchangeDeclareMethod{Exchange: "test-exchange", Type: "direct"}
	err := srv.handleExchangeDeclare(conn, 1, encodeMethod(t, 40, 10, method))
	if err == nil {
		t.Error("reader with configure=^$ should be refused from declaring exchange")
	}
}

func TestAuthzExchangeDeleteAllowed(t *testing.T) {
	srv := newB4Server(t, true, []auth.UserEntry{fullAdminUser("admin")})
	conn := newB4Conn(t, srv.Authenticator, "admin")

	// First declare an exchange
	declMethod := &protocol.ExchangeDeclareMethod{Exchange: "to-delete", Type: "direct"}
	srv.handleExchangeDeclare(conn, 1, encodeMethod(t, 40, 10, declMethod))

	// Now delete it
	delMethod := &protocol.ExchangeDeleteMethod{Exchange: "to-delete"}
	err := srv.handleExchangeDelete(conn, 1, encodeMethod(t, 40, 20, delMethod))
	if err != nil {
		t.Errorf("admin should be allowed to delete exchange: %v", err)
	}
}

func TestAuthzExchangeDeleteRefused(t *testing.T) {
	srv := newB4Server(t, true, []auth.UserEntry{readOnlyUser("reader")})
	conn := newB4Conn(t, srv.Authenticator, "reader")

	delMethod := &protocol.ExchangeDeleteMethod{Exchange: "test-exchange"}
	err := srv.handleExchangeDelete(conn, 1, encodeMethod(t, 40, 20, delMethod))
	if err == nil {
		t.Error("reader with configure=^$ should be refused from deleting exchange")
	}
}

// --- Queue declare/delete tests ---

func TestAuthzQueueDeclareAllowed(t *testing.T) {
	srv := newB4Server(t, true, []auth.UserEntry{fullAdminUser("admin")})
	conn := newB4Conn(t, srv.Authenticator, "admin")

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	method := &protocol.QueueDeclareMethod{Queue: "test-queue"}
	err := srv.handleQueueDeclare(conn, 1, encodeMethod(t, 50, 10, method))
	if err != nil {
		t.Errorf("admin should be allowed to declare queue: %v", err)
	}
}

func TestAuthzQueueDeclareRefused(t *testing.T) {
	srv := newB4Server(t, true, []auth.UserEntry{readOnlyUser("reader")})
	conn := newB4Conn(t, srv.Authenticator, "reader")

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	method := &protocol.QueueDeclareMethod{Queue: "test-queue"}
	err := srv.handleQueueDeclare(conn, 1, encodeMethod(t, 50, 10, method))
	if err == nil {
		t.Error("reader with configure=^$ should be refused from declaring queue")
	}
}

func TestAuthzQueueDeleteAllowed(t *testing.T) {
	srv := newB4Server(t, true, []auth.UserEntry{fullAdminUser("admin")})
	conn := newB4Conn(t, srv.Authenticator, "admin")

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	// First declare
	declMethod := &protocol.QueueDeclareMethod{Queue: "to-delete"}
	srv.handleQueueDeclare(conn, 1, encodeMethod(t, 50, 10, declMethod))

	// Now delete
	delMethod := &protocol.QueueDeleteMethod{Queue: "to-delete"}
	err := srv.handleQueueDelete(conn, 1, encodeMethod(t, 50, 40, delMethod))
	if err != nil {
		t.Errorf("admin should be allowed to delete queue: %v", err)
	}
}

func TestAuthzQueueDeleteRefused(t *testing.T) {
	srv := newB4Server(t, true, []auth.UserEntry{readOnlyUser("reader")})
	conn := newB4Conn(t, srv.Authenticator, "reader")

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	delMethod := &protocol.QueueDeleteMethod{Queue: "test-queue"}
	err := srv.handleQueueDelete(conn, 1, encodeMethod(t, 50, 40, delMethod))
	if err == nil {
		t.Error("reader with configure=^$ should be refused from deleting queue")
	}
}

// --- Queue bind tests (write on queue + read on exchange) ---

func TestAuthzQueueBindAllowed(t *testing.T) {
	srv := newB4Server(t, true, []auth.UserEntry{fullAdminUser("admin")})
	conn := newB4Conn(t, srv.Authenticator, "admin")

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	// Declare exchange and queue first
	srv.handleExchangeDeclare(conn, 1, encodeMethod(t, 40, 10, &protocol.ExchangeDeclareMethod{Exchange: "test-ex", Type: "direct"}))
	srv.handleQueueDeclare(conn, 1, encodeMethod(t, 50, 10, &protocol.QueueDeclareMethod{Queue: "test-q"}))

	method := &protocol.QueueBindMethod{Queue: "test-q", Exchange: "test-ex", RoutingKey: "rk", Arguments: map[string]interface{}{}}
	err := srv.handleQueueBind(conn, 1, encodeMethod(t, 50, 20, method))
	if err != nil {
		t.Errorf("admin should be allowed to bind queue: %v", err)
	}
}

func TestAuthzQueueBindRefusedNoWriteOnQueue(t *testing.T) {
	srv := newB4Server(t, true, []auth.UserEntry{readOnlyUser("reader")})
	conn := newB4Conn(t, srv.Authenticator, "reader")

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	method := &protocol.QueueBindMethod{Queue: "test-q", Exchange: "test-ex", RoutingKey: "rk", Arguments: map[string]interface{}{}}
	err := srv.handleQueueBind(conn, 1, encodeMethod(t, 50, 20, method))
	if err == nil {
		t.Error("reader with write=^$ should be refused from binding queue")
	}
}

func TestAuthzQueueBindRefusedNoReadOnExchange(t *testing.T) {
	// User has write but not read
	srv := newB4Server(t, true, []auth.UserEntry{writeOnlyUser("writer")})
	conn := newB4Conn(t, srv.Authenticator, "writer")

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	method := &protocol.QueueBindMethod{Queue: "test-q", Exchange: "test-ex", RoutingKey: "rk", Arguments: map[string]interface{}{}}
	err := srv.handleQueueBind(conn, 1, encodeMethod(t, 50, 20, method))
	if err == nil {
		t.Error("writer with read=^$ should be refused from binding to exchange")
	}
}

// --- Basic publish tests (write on exchange) ---

func TestAuthzBasicPublishAllowed(t *testing.T) {
	srv := newB4Server(t, true, []auth.UserEntry{writeOnlyUser("writer")})
	conn := newB4Conn(t, srv.Authenticator, "writer")

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	// Declare exchange first (auth disabled for setup)
	srv.Config.Security.AuthorizationEnabled = false
	srv.handleExchangeDeclare(conn, 1, encodeMethod(t, 40, 10, &protocol.ExchangeDeclareMethod{Exchange: "test-ex", Type: "direct"}))
	srv.Config.Security.AuthorizationEnabled = true

	method := &protocol.BasicPublishMethod{Exchange: "test-ex", RoutingKey: "rk"}
	err := srv.handleBasicPublish(conn, 1, encodeMethod(t, 60, 40, method))
	if err != nil {
		t.Errorf("writer with write=.* should be allowed to publish: %v", err)
	}
}

func TestAuthzBasicPublishRefused(t *testing.T) {
	srv := newB4Server(t, true, []auth.UserEntry{readOnlyUser("reader")})
	conn := newB4Conn(t, srv.Authenticator, "reader")

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	method := &protocol.BasicPublishMethod{Exchange: "test-ex", RoutingKey: "rk"}
	err := srv.handleBasicPublish(conn, 1, encodeMethod(t, 60, 40, method))
	if err == nil {
		t.Error("reader with write=^$ should be refused from publishing")
	}
}

// --- Basic consume tests (read on queue) ---

func TestAuthzBasicConsumeAllowed(t *testing.T) {
	srv := newB4Server(t, true, []auth.UserEntry{readOnlyUser("reader")})
	conn := newB4Conn(t, srv.Authenticator, "reader")

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	// Declare queue first (auth disabled for setup)
	srv.Config.Security.AuthorizationEnabled = false
	srv.handleQueueDeclare(conn, 1, encodeMethod(t, 50, 10, &protocol.QueueDeclareMethod{Queue: "test-q"}))
	srv.Config.Security.AuthorizationEnabled = true

	method := &protocol.BasicConsumeMethod{Queue: "test-q", ConsumerTag: "ctag-test"}
	err := srv.handleBasicConsume(conn, 1, encodeMethod(t, 60, 20, method))
	if err != nil {
		t.Errorf("reader with read=.* should be allowed to consume: %v", err)
	}
}

func TestAuthzBasicConsumeRefused(t *testing.T) {
	srv := newB4Server(t, true, []auth.UserEntry{writeOnlyUser("writer")})
	conn := newB4Conn(t, srv.Authenticator, "writer")

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	method := &protocol.BasicConsumeMethod{Queue: "test-q", ConsumerTag: "ctag-test"}
	err := srv.handleBasicConsume(conn, 1, encodeMethod(t, 60, 20, method))
	if err == nil {
		t.Error("writer with read=^$ should be refused from consuming")
	}
}

// --- Basic get tests (read on queue) ---

func TestAuthzBasicGetAllowed(t *testing.T) {
	srv := newB4Server(t, true, []auth.UserEntry{readOnlyUser("reader")})
	conn := newB4Conn(t, srv.Authenticator, "reader")

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	srv.Config.Security.AuthorizationEnabled = false
	srv.handleQueueDeclare(conn, 1, encodeMethod(t, 50, 10, &protocol.QueueDeclareMethod{Queue: "test-q"}))
	srv.Config.Security.AuthorizationEnabled = true

	method := &protocol.BasicGetMethod{Queue: "test-q"}
	err := srv.handleBasicGet(conn, 1, encodeMethod(t, 60, 70, method))
	if err != nil {
		t.Errorf("reader with read=.* should be allowed to basic.get: %v", err)
	}
}

func TestAuthzBasicGetRefused(t *testing.T) {
	srv := newB4Server(t, true, []auth.UserEntry{writeOnlyUser("writer")})
	conn := newB4Conn(t, srv.Authenticator, "writer")

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	method := &protocol.BasicGetMethod{Queue: "test-q"}
	err := srv.handleBasicGet(conn, 1, encodeMethod(t, 60, 70, method))
	if err == nil {
		t.Error("writer with read=^$ should be refused from basic.get")
	}
}

// --- Reserved amq. name enforcement (spec "reserved" rule) ---

func TestAuthzReservedExchangeNameRefused(t *testing.T) {
	srv := newB4Server(t, true, []auth.UserEntry{fullAdminUser("admin")})
	conn := newB4Conn(t, srv.Authenticator, "admin")

	// Non-passive declare of amq.custom → access-refused
	method := &protocol.ExchangeDeclareMethod{Exchange: "amq.custom", Type: "direct", Passive: false}
	err := srv.handleExchangeDeclare(conn, 1, encodeMethod(t, 40, 10, method))
	if err == nil {
		t.Error("non-passive declare of amq.custom should be refused (spec: reserved rule)")
	}
}

func TestAuthzReservedExchangeNamePassiveAllowed(t *testing.T) {
	srv := newB4Server(t, true, []auth.UserEntry{fullAdminUser("admin")})
	conn := newB4Conn(t, srv.Authenticator, "admin")

	// Passive declare of amq.direct (pre-declared) → should be allowed
	method := &protocol.ExchangeDeclareMethod{Exchange: "amq.direct", Type: "direct", Passive: true}
	err := srv.handleExchangeDeclare(conn, 1, encodeMethod(t, 40, 10, method))
	if err != nil {
		t.Errorf("passive declare of amq.direct should be allowed: %v", err)
	}
}

func TestAuthzReservedQueueNameRefused(t *testing.T) {
	srv := newB4Server(t, true, []auth.UserEntry{fullAdminUser("admin")})
	conn := newB4Conn(t, srv.Authenticator, "admin")

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	// Non-passive declare of amq.custom → access-refused
	method := &protocol.QueueDeclareMethod{Queue: "amq.custom", Passive: false}
	err := srv.handleQueueDeclare(conn, 1, encodeMethod(t, 50, 10, method))
	if err == nil {
		t.Error("non-passive declare of amq.custom should be refused (spec: reserved rule)")
	}
}

func TestAuthzReservedQueueNamePassiveAllowed(t *testing.T) {
	srv := newB4Server(t, true, []auth.UserEntry{fullAdminUser("admin")})
	conn := newB4Conn(t, srv.Authenticator, "admin")

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	// Passive declare of a reserved name → should NOT be refused by reserved check
	method := &protocol.QueueDeclareMethod{Queue: "amq.direct", Passive: true}
	err := srv.handleQueueDeclare(conn, 1, encodeMethod(t, 50, 10, method))
	if err != nil && strings.Contains(err.Error(), "reserved") {
		t.Errorf("passive declare of amq.direct should not be refused by reserved check: %v", err)
	}
}

func TestAuthzAuthDisabledAllowsAll(t *testing.T) {
	srv := newB4Server(t, false, nil)
	conn := newB4Conn(t, nil, "")

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	// With auth disabled, all operations should be allowed
	method := &protocol.ExchangeDeclareMethod{Exchange: "test-ex", Type: "direct"}
	err := srv.handleExchangeDeclare(conn, 1, encodeMethod(t, 40, 10, method))
	if err != nil {
		t.Errorf("auth disabled: exchange declare should be allowed: %v", err)
	}
}
