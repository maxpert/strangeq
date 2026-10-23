package server

import (
	"encoding/binary"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/auth"
	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"golang.org/x/crypto/bcrypt"
)

// pipeConnFrom returns a server-side connection whose RemoteAddr is remote,
// plus a channel fed by a goroutine reading frames the server writes.
func pipeConnFrom(t *testing.T, remote net.Addr) (*protocol.Connection, <-chan *protocol.Frame) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})

	frames := make(chan *protocol.Frame, 8)
	go func() {
		defer close(frames)
		for {
			frame, err := protocol.ReadFrame(clientConn)
			if err != nil {
				return
			}
			frames <- frame
		}
	}()

	conn := protocol.NewConnection(&mockConn{Conn: serverConn, remote: remote})
	conn.ID = "test-conn"
	return conn, frames
}

func tcpAddr(host string, port int) net.Addr {
	return &net.TCPAddr{IP: net.ParseIP(host), Port: port}
}

func startOKPayload(t *testing.T, mechanism string, response []byte) []byte {
	t.Helper()
	method := &protocol.ConnectionStartOKMethod{
		ClientProperties: map[string]interface{}{"product": "test"},
		Mechanism:        mechanism,
		Response:         response,
		Locale:           "en_US",
	}
	data, err := method.Serialize()
	require.NoError(t, err)
	return data
}

// closeFrame decodes a method frame as connection.close (10.50).
func closeFrame(t *testing.T, frame *protocol.Frame) (code uint16, text string) {
	t.Helper()
	require.NotNil(t, frame)
	require.Equal(t, uint8(protocol.FrameMethod), uint8(frame.Type))
	require.GreaterOrEqual(t, len(frame.Payload), 6)
	classID := binary.BigEndian.Uint16(frame.Payload[0:2])
	methodID := binary.BigEndian.Uint16(frame.Payload[2:4])
	require.Equal(t, uint16(10), classID)
	require.Equal(t, uint16(50), methodID)
	code = binary.BigEndian.Uint16(frame.Payload[4:6])
	textLen := int(frame.Payload[6])
	require.GreaterOrEqual(t, len(frame.Payload), 7+textLen)
	text = string(frame.Payload[7 : 7+textLen])
	return code, text
}

func firstFrame(t *testing.T, frames <-chan *protocol.Frame) *protocol.Frame {
	t.Helper()
	select {
	case frame, ok := <-frames:
		require.True(t, ok, "frame channel closed without a frame")
		return frame
	case <-time.After(5 * time.Second):
		t.Fatal("server wrote no frame")
		return nil
	}
}

// countingAuth counts Authenticate calls while delegating.
type countingAuth struct {
	inner interfaces.Authenticator
	calls atomic.Int32
}

func (c *countingAuth) Authenticate(username, password string) (*interfaces.User, error) {
	c.calls.Add(1)
	return c.inner.Authenticate(username, password)
}

func (c *countingAuth) Authorize(user *interfaces.User, operation interfaces.Operation) error {
	return c.inner.Authorize(user, operation)
}

func (c *countingAuth) GetUser(username string) (*interfaces.User, error) {
	return c.inner.GetUser(username)
}

func (c *countingAuth) RefreshUser(user *interfaces.User) error {
	return c.inner.RefreshUser(user)
}

// fakeAuthMetrics records auth-failure reasons while embedding the no-op set.
type fakeAuthMetrics struct {
	*NoOpMetricsCollector
	reasons []string
}

func (f *fakeAuthMetrics) RecordAuthFailure(reason string) {
	f.reasons = append(f.reasons, reason)
}

func setSecurity(t *testing.T, srv *Server, mutate func(*config.AMQPConfig)) {
	t.Helper()
	require.NotNil(t, srv.Config)
	mutate(srv.Config)
}

func TestHostAllowBlock(t *testing.T) {
	plainAlice := []byte("\x00alice\x00pw")

	t.Run("allowed peer reaches SASL", func(t *testing.T) {
		authn := &countingAuth{inner: writeAuthFileForTest(t, []auth.UserEntry{
			{Username: "alice", PasswordHash: mustHash(t, "pw")},
		})}
		srv := newAuthzTestServer(t, true, authn)
		setSecurity(t, srv, func(c *config.AMQPConfig) {
			c.Security.AllowedHosts = []string{"10.0.0.0/8"}
		})
		conn, _ := pipeConnFrom(t, tcpAddr("10.1.2.3", 12345))

		require.NoError(t, srv.handleConnectionStartOK(conn, startOKPayload(t, "PLAIN", plainAlice)))
		assert.Equal(t, int32(1), authn.calls.Load())
	})

	t.Run("peer outside allow list refused before SASL", func(t *testing.T) {
		authn := &countingAuth{inner: writeAuthFileForTest(t, []auth.UserEntry{
			{Username: "alice", PasswordHash: mustHash(t, "pw")},
		})}
		srv := newAuthzTestServer(t, true, authn)
		setSecurity(t, srv, func(c *config.AMQPConfig) {
			c.Security.AllowedHosts = []string{"10.0.0.0/8"}
		})
		conn, frames := pipeConnFrom(t, tcpAddr("192.168.1.5", 12345))

		err := srv.handleConnectionStartOK(conn, startOKPayload(t, "PLAIN", plainAlice))
		require.Error(t, err)
		code, _ := closeFrame(t, firstFrame(t, frames))
		assert.Equal(t, uint16(403), code)
		assert.Equal(t, int32(0), authn.calls.Load(), "host deny must precede SASL")
	})

	t.Run("blocked beats allowed", func(t *testing.T) {
		srv := newAuthzTestServer(t, false, nil)
		setSecurity(t, srv, func(c *config.AMQPConfig) {
			c.Security.AllowedHosts = []string{"10.0.0.0/8"}
			c.Security.BlockedHosts = []string{"10.1.2.3"}
		})
		conn, frames := pipeConnFrom(t, tcpAddr("10.1.2.3", 12345))

		err := srv.handleConnectionStartOK(conn, startOKPayload(t, "PLAIN", nil))
		require.Error(t, err)
		assert.Equal(t, uint16(403), mustCode(t, frames))
	})

	t.Run("IPv6 CIDR", func(t *testing.T) {
		srv := newAuthzTestServer(t, false, nil)
		setSecurity(t, srv, func(c *config.AMQPConfig) {
			c.Security.AllowedHosts = []string{"2001:db8::/32"}
		})

		conn, _ := pipeConnFrom(t, tcpAddr("2001:db8::1", 12345))
		require.NoError(t, srv.handleConnectionStartOK(conn, startOKPayload(t, "PLAIN", nil)))

		conn, frames := pipeConnFrom(t, tcpAddr("2001:db9::1", 12345))
		require.Error(t, srv.handleConnectionStartOK(conn, startOKPayload(t, "PLAIN", nil)))
		assert.Equal(t, uint16(403), mustCode(t, frames))
	})

	t.Run("IPv4-mapped peer matches v4 CIDR", func(t *testing.T) {
		srv := newAuthzTestServer(t, false, nil)
		setSecurity(t, srv, func(c *config.AMQPConfig) {
			c.Security.AllowedHosts = []string{"10.0.0.0/8"}
		})
		conn, _ := pipeConnFrom(t, tcpAddr("::ffff:10.1.2.3", 12345))
		require.NoError(t, srv.handleConnectionStartOK(conn, startOKPayload(t, "PLAIN", nil)))
	})

	t.Run("host lists apply with authentication disabled", func(t *testing.T) {
		srv := newAuthzTestServer(t, false, nil)
		setSecurity(t, srv, func(c *config.AMQPConfig) {
			c.Security.AllowedHosts = []string{"10.0.0.0/8"}
		})
		conn, frames := pipeConnFrom(t, tcpAddr("192.168.1.5", 12345))
		require.Error(t, srv.handleConnectionStartOK(conn, startOKPayload(t, "PLAIN", nil)))
		assert.Equal(t, uint16(403), mustCode(t, frames))
	})

	t.Run("non-TCP peer with empty lists passes", func(t *testing.T) {
		srv := newAuthzTestServer(t, false, nil)
		clientConn, serverConn := net.Pipe()
		t.Cleanup(func() { clientConn.Close(); serverConn.Close() })
		go func() {
			for {
				if _, err := protocol.ReadFrame(clientConn); err != nil {
					return
				}
			}
		}()
		conn := protocol.NewConnection(serverConn)
		require.NoError(t, srv.handleConnectionStartOK(conn, startOKPayload(t, "PLAIN", nil)))
	})

	t.Run("non-TCP peer with a list is refused", func(t *testing.T) {
		srv := newAuthzTestServer(t, false, nil)
		setSecurity(t, srv, func(c *config.AMQPConfig) {
			c.Security.AllowedHosts = []string{"10.0.0.0/8"}
		})
		clientConn, serverConn := net.Pipe()
		t.Cleanup(func() { clientConn.Close(); serverConn.Close() })
		frames := make(chan *protocol.Frame, 1)
		go func() {
			frame, err := protocol.ReadFrame(clientConn)
			if err == nil {
				frames <- frame
			}
		}()
		conn := protocol.NewConnection(serverConn)
		require.Error(t, srv.handleConnectionStartOK(conn, startOKPayload(t, "PLAIN", nil)))
		assert.Equal(t, uint16(403), mustCode(t, frames))
	})
}

func mustHash(t *testing.T, password string) string {
	t.Helper()
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	require.NoError(t, err)
	return string(hash)
}

func mustCode(t *testing.T, frames <-chan *protocol.Frame) uint16 {
	t.Helper()
	code, _ := closeFrame(t, firstFrame(t, frames))
	return code
}

func TestUserBlockList(t *testing.T) {
	newServer := func(t *testing.T) (*Server, *countingAuth) {
		authn := &countingAuth{inner: writeAuthFileForTest(t, []auth.UserEntry{
			{Username: "alice", PasswordHash: mustHash(t, "pw")},
			{Username: "mallory", PasswordHash: mustHash(t, "pw")},
			{Username: "bob", PasswordHash: mustHash(t, "pw")},
		})}
		return newAuthzTestServer(t, true, authn), authn
	}

	t.Run("blocked user gets Authentication failed after identity", func(t *testing.T) {
		srv, authn := newServer(t)
		setSecurity(t, srv, func(c *config.AMQPConfig) {
			c.Security.BlockedUsers = []string{"mallory"}
		})
		conn, frames := pipeConnFrom(t, tcpAddr("10.0.0.9", 12345))

		err := srv.handleConnectionStartOK(conn, startOKPayload(t, "PLAIN", []byte("\x00mallory\x00pw")))
		require.Error(t, err)
		code, text := closeFrame(t, firstFrame(t, frames))
		assert.Equal(t, uint16(403), code)
		assert.Equal(t, "Authentication failed", text)
		assert.Nil(t, conn.User)
		assert.Equal(t, int32(1), authn.calls.Load(), "identity is checked before the user list")
	})

	t.Run("allowed user passes", func(t *testing.T) {
		srv, _ := newServer(t)
		setSecurity(t, srv, func(c *config.AMQPConfig) {
			c.Security.BlockedUsers = []string{"mallory"}
		})
		conn, _ := pipeConnFrom(t, tcpAddr("10.0.0.9", 12345))
		require.NoError(t, srv.handleConnectionStartOK(conn, startOKPayload(t, "PLAIN", []byte("\x00alice\x00pw"))))
		require.NotNil(t, conn.User)
	})

	t.Run("user outside allow list refused", func(t *testing.T) {
		srv, _ := newServer(t)
		setSecurity(t, srv, func(c *config.AMQPConfig) {
			c.Security.AllowedUsers = []string{"alice"}
		})
		conn, frames := pipeConnFrom(t, tcpAddr("10.0.0.9", 12345))
		err := srv.handleConnectionStartOK(conn, startOKPayload(t, "PLAIN", []byte("\x00bob\x00pw")))
		require.Error(t, err)
		code, text := closeFrame(t, firstFrame(t, frames))
		assert.Equal(t, uint16(403), code)
		assert.Equal(t, "Authentication failed", text, "user deny must not leak an allow-list oracle")
	})
}

func TestAuthRateLimit(t *testing.T) {
	window := 60 * time.Second
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	newLimitedServer := func(t *testing.T) (*Server, *countingAuth) {
		authn := &countingAuth{inner: writeAuthFileForTest(t, []auth.UserEntry{
			{Username: "alice", PasswordHash: mustHash(t, "pw")},
		})}
		srv := newAuthzTestServer(t, true, authn)
		srv.MetricsCollector = &fakeAuthMetrics{NoOpMetricsCollector: &NoOpMetricsCollector{}}
		setSecurity(t, srv, func(c *config.AMQPConfig) {
			c.Security.AuthFailureLimit = 2
			c.Security.AuthFailureWindowMS = window.Milliseconds()
		})
		srv.gate().limiter.now = func() time.Time { return now }
		return srv, authn
	}

	t.Run("correct credentials refused once the budget is spent", func(t *testing.T) {
		srv, authn := newLimitedServer(t)
		bad := startOKPayload(t, "PLAIN", []byte("\x00alice\x00wrong"))

		for i := 0; i < 2; i++ {
			conn, frames := pipeConnFrom(t, tcpAddr("10.0.0.9", 12345))
			require.Error(t, srv.handleConnectionStartOK(conn, bad))
			assert.Equal(t, uint16(403), mustCode(t, frames))
			assert.Equal(t, int32(i+1), authn.calls.Load())
		}

		conn, frames := pipeConnFrom(t, tcpAddr("10.0.0.9", 12345))
		err := srv.handleConnectionStartOK(conn, startOKPayload(t, "PLAIN", []byte("\x00alice\x00pw")))
		require.Error(t, err)
		assert.Equal(t, uint16(403), mustCode(t, frames))
		assert.Equal(t, int32(2), authn.calls.Load(), "rate-limited attempts must not run bcrypt")

		metrics := srv.MetricsCollector.(*fakeAuthMetrics)
		assert.Contains(t, metrics.reasons, "rate_limited")

		// One window later the bucket has refilled.
		now = now.Add(window)
		conn, _ = pipeConnFrom(t, tcpAddr("10.0.0.9", 12345))
		require.NoError(t, srv.handleConnectionStartOK(conn, startOKPayload(t, "PLAIN", []byte("\x00alice\x00pw"))))
		assert.Equal(t, int32(3), authn.calls.Load())
	})

	t.Run("IPv6 peers share a /64 bucket but not across /64s", func(t *testing.T) {
		now = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		srv, _ := newLimitedServer(t)

		conn, frames := pipeConnFrom(t, tcpAddr("2001:db8::1", 1000))
		require.Error(t, srv.handleConnectionStartOK(conn, startOKPayload(t, "PLAIN", []byte("\x00alice\x00wrong"))))
		assert.Equal(t, uint16(403), mustCode(t, frames))

		// Limit is 2, so this consumes the second token.
		conn, frames = pipeConnFrom(t, tcpAddr("2001:db8::ffff", 1000))
		require.Error(t, srv.handleConnectionStartOK(conn, startOKPayload(t, "PLAIN", []byte("\x00alice\x00wrong"))))
		assert.Equal(t, uint16(403), mustCode(t, frames))

		// Same /64, correct credentials: refused before bcrypt.
		conn, frames = pipeConnFrom(t, tcpAddr("2001:db8::2", 1000))
		require.Error(t, srv.handleConnectionStartOK(conn, startOKPayload(t, "PLAIN", []byte("\x00alice\x00pw"))))
		assert.Equal(t, uint16(403), mustCode(t, frames))

		// Different /64 still has its own bucket.
		conn, _ = pipeConnFrom(t, tcpAddr("2001:db9::1", 1000))
		require.NoError(t, srv.handleConnectionStartOK(conn, startOKPayload(t, "PLAIN", []byte("\x00alice\x00pw"))))
	})
}

func TestAuthFailureLogIncludesRemoteAddr(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	srv := newAuthzTestServer(t, true, writeAuthFileForTest(t, []auth.UserEntry{
		{Username: "alice", PasswordHash: mustHash(t, "pw")},
	}))
	srv.Log = zap.New(core)

	conn, frames := pipeConnFrom(t, tcpAddr("10.0.0.9", 12345))
	require.Error(t, srv.handleConnectionStartOK(conn, startOKPayload(t, "PLAIN", []byte("\x00alice\x00bad"))))
	_ = mustCode(t, frames)

	entries := logs.FilterMessage("Authentication failed").All()
	require.Len(t, entries, 1)
	assert.Equal(t, "10.0.0.9:12345", entries[0].ContextMap()["remote_addr"])
}
