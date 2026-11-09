package management

import (
	"bufio"
	"crypto/tls"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
)

// This file pins item ②'s reporting half: the management /api/connections DTO
// gains RabbitMQ's `secure`/`ssl` transport flags and the negotiated
// `auth_mechanism`.

// injectConn installs (or replaces) a registry entry with an explicit net.Conn
// so the snapshot's transport derivation can be exercised with a concrete
// *tls.Conn and a plain pipe.
func (e *env) injectConn(t *testing.T, id, user string, c net.Conn) {
	t.Helper()
	if c != nil {
		t.Cleanup(func() { _ = c.Close() })
	}
	e.srv.Mutex.Lock()
	e.srv.Connections[id] = &protocol.Connection{ID: id, Conn: c, Username: user, Vhost: "/"}
	e.srv.Mutex.Unlock()
}

// startEnvListener starts the env's AMQP listener and returns its address.
func startEnvListener(t *testing.T, e *env) string {
	t.Helper()
	go func() { _ = e.srv.Start() }()
	deadline := time.Now().Add(2 * time.Second)
	for !e.srv.IsListening() {
		if time.Now().After(deadline) {
			t.Fatal("AMQP listener not ready within 2s")
		}
		time.Sleep(5 * time.Millisecond)
	}
	e.srv.Mutex.RLock()
	defer e.srv.Mutex.RUnlock()
	return e.srv.Listener.Addr().String()
}

// TestConnections_SecureFromTransport: `secure`/`ssl` come from the transport.
// A concrete *tls.Conn is secure; a non-TLS (net.Pipe) conn is conservatively
// false, mirroring isLoopbackConn's deny-by-default shape.
func TestConnections_SecureFromTransport(t *testing.T) {
	e := basicEnv(t, true, nil)

	// A concrete *tls.Conn over a net.Pipe pair; no handshake is needed to
	// type-assert the transport.
	clientTLS, serverTLS := net.Pipe()
	t.Cleanup(func() { _ = clientTLS.Close() })
	e.injectConn(t, "tls-conn", "alice", tls.Server(serverTLS, &tls.Config{}))

	// A plain pipe conn (test transport): not secure.
	clientPlain, serverPlain := net.Pipe()
	t.Cleanup(func() { _ = clientPlain.Close() })
	e.injectConn(t, "plain-conn", "bob", serverPlain)

	rec := e.do(http.MethodGet, "/api/connections", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	conns := decode[[]connectionInfo](t, rec)

	byName := map[string]connectionInfo{}
	for _, c := range conns {
		byName[c.Name] = c
	}
	tlsConn, ok := byName["tls-conn"]
	if !ok {
		t.Fatalf("tls-conn missing from snapshot: %+v", conns)
	}
	if !tlsConn.Secure || !tlsConn.SSL {
		t.Fatalf("a *tls.Conn must report secure=true ssl=true, got %+v", tlsConn)
	}
	if tlsConn.AuthMechanism != "" {
		t.Fatalf("an injected connection has not authenticated: auth_mechanism = %q", tlsConn.AuthMechanism)
	}
	plainConn, ok := byName["plain-conn"]
	if !ok {
		t.Fatalf("plain-conn missing from snapshot: %+v", conns)
	}
	if plainConn.Secure || plainConn.SSL {
		t.Fatalf("a non-TLS conn must report secure=false ssl=false, got %+v", plainConn)
	}

	// The wire keys are RabbitMQ's names, and a secure connection serialises
	// `secure:true` / `ssl:true`.
	body := rec.Body.String()
	if !strings.Contains(body, `"secure":true`) || !strings.Contains(body, `"ssl":true`) {
		t.Fatalf("secure/ssl keys not emitted with the TLS conn: %s", truncate(body))
	}
}

// TestConnections_AuthMechanismFromHandshake drives a real PLAIN handshake
// against an auth-enabled server and reads the mechanism back over the API.
func TestConnections_AuthMechanismFromHandshake(t *testing.T) {
	authJSON := `{"users":[{"username":"admin","password_hash":"` + secretHash(t) + `","tags":["administrator"],"vhost_permissions":[{"vhost":"/","permission":{"configure":".*","write":".*","read":".*"}}]}]}`
	e := inheritEnv(t, authJSON, func(cfg *config.AMQPConfig) {
		cfg.Network.Address = "127.0.0.1:0"
	})
	addr := startEnvListener(t, e)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	raw := &b2RawConn{t: t, c: conn, r: bufio.NewReader(conn)}
	raw.handshakeAs("admin", "secret")

	headers := map[string]string{"Authorization": "Basic " + basicToken("admin", "secret")}
	rec := e.do(http.MethodGet, "/api/connections", nil, headers)
	wantStatus(t, rec, http.StatusOK)
	conns := decode[[]connectionInfo](t, rec)
	if len(conns) != 1 {
		t.Fatalf("connections = %+v, want the one just-authenticated connection", conns)
	}
	c := conns[0]
	if c.AuthMechanism != "PLAIN" {
		t.Fatalf("auth_mechanism = %q, want PLAIN for a validated PLAIN start-ok", c.AuthMechanism)
	}
	if c.Secure || c.SSL {
		t.Fatalf("a plaintext handshake must not be reported secure: %+v", c)
	}
}

// TestConnections_AuthMechanismEmptyWhenAuthDisabled: with authentication off,
// no SASL verification happens, so the offered mechanism must not be reported.
func TestConnections_AuthMechanismEmptyWhenAuthDisabled(t *testing.T) {
	e := basicEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Network.Address = "127.0.0.1:0"
	})
	addr := startEnvListener(t, e)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	raw := &b2RawConn{t: t, c: conn, r: bufio.NewReader(conn)}
	raw.handshakeAs("guest", "guest") // PLAIN offered, but the server ignores it

	rec := e.do(http.MethodGet, "/api/connections", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	conns := decode[[]connectionInfo](t, rec)
	if len(conns) != 1 {
		t.Fatalf("connections = %+v, want the one handshaken connection", conns)
	}
	if conns[0].AuthMechanism != "" {
		t.Fatalf("authentication disabled must not report the offered mechanism: %+v", conns[0])
	}
}
