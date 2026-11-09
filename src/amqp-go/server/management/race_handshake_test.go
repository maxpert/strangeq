package management

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
)

// handshakePaused runs the happy-path AMQP handshake but pauses after sending
// connection.open, leaving the connection published in Server.Connections with
// conn.Username and conn.Vhost written and no more synchronization until the
// client reads connection.open-ok. On the pre-fix code those two writes are not
// under Server.Mutex, so a concurrent management read is a data race; the pause
// makes that window wide enough for -race to observe it deterministically.
func (r *b2RawConn) handshakePaused(pause time.Duration) {
	r.t.Helper()
	r.handshakeAsPaused("guest", "guest", pause)
}

// handshakeAsPaused is handshakePaused with explicit SASL PLAIN credentials, so
// the same post-open window can be exercised against an auth-enabled server
// (where the guest credentials handshakePaused sends would be refused).
func (r *b2RawConn) handshakeAsPaused(user, pass string, pause time.Duration) {
	r.t.Helper()
	if _, err := r.c.Write([]byte("AMQP\x00\x00\x09\x01")); err != nil {
		r.t.Fatalf("write protocol header: %v", err)
	}
	_, cls, mth, _ := r.readMethod() // connection.start
	if cls != 10 || mth != 10 {
		r.t.Fatalf("expected connection.start, got %d.%d", cls, mth)
	}
	startOK := &protocol.ConnectionStartOKMethod{
		ClientProperties: map[string]interface{}{},
		Mechanism:        "PLAIN",
		Response:         []byte("\x00" + user + "\x00" + pass),
		Locale:           "en_US",
	}
	data, err := startOK.Serialize()
	if err != nil {
		r.t.Fatalf("serialize start-ok: %v", err)
	}
	r.send(0, 10, 11, data)

	_, cls, mth, _ = r.readMethod() // connection.tune
	if cls != 10 || mth != 30 {
		r.t.Fatalf("expected connection.tune, got %d.%d", cls, mth)
	}
	tuneOK := make([]byte, 8)
	binary.BigEndian.PutUint16(tuneOK[0:2], 16)
	binary.BigEndian.PutUint32(tuneOK[2:6], 131072)
	binary.BigEndian.PutUint16(tuneOK[6:8], 0)
	r.send(0, 10, 31, tuneOK)

	open := &protocol.ConnectionOpenMethod{VirtualHost: "/"}
	odata, err := open.Serialize()
	if err != nil {
		r.t.Fatalf("serialize open: %v", err)
	}
	r.send(0, 10, 40, odata)

	// The server has written conn.Username and conn.Vhost (no lock, pre-fix)
	// and is about to send open-ok. Hold here so the poller reads them.
	time.Sleep(pause)

	_, cls, mth, _ = r.readMethod() // connection.open-ok
	if cls != 10 || mth != 41 {
		r.t.Fatalf("expected connection.open-ok, got %d.%d", cls, mth)
	}
}

// TestRace_HandshakeUsernameVhost pins the F2 finding: the AMQP handshake
// writes conn.Username / conn.Vhost AFTER the connection is published into
// Server.Connections, while the management read path reads them under
// Server.Mutex.RLock. This test opens real handshakes while a bounded goroutine
// polls GET /api/connections and GET /api/channels; run under -race it fails if
// the two are unsynchronised.
func TestRace_HandshakeUsernameVhost(t *testing.T) {
	e := basicEnv(t, true, func(cfg *config.AMQPConfig) {
		cfg.Network.Address = "127.0.0.1:0"
	})
	go func() { _ = e.srv.Start() }()
	deadline := time.Now().Add(2 * time.Second)
	for !e.srv.IsListening() {
		if time.Now().After(deadline) {
			t.Fatal("AMQP listener not ready within 2s")
		}
		time.Sleep(5 * time.Millisecond)
	}
	e.srv.Mutex.RLock()
	addr := e.srv.Listener.Addr().String()
	e.srv.Mutex.RUnlock()

	stop := make(chan struct{})
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = e.do(http.MethodGet, "/api/connections", nil, basicAuthHeader())
			_ = e.do(http.MethodGet, "/api/channels", nil, basicAuthHeader())
		}
	}()

	// A bounded number of paused handshakes: each holds the connection in the
	// registry for `pause` so the poller is guaranteed to read it.
	const conns = 20
	const pause = 15 * time.Millisecond
	for i := 0; i < conns; i++ {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("dial %s: %v", addr, err)
		}
		raw := &b2RawConn{t: t, c: conn, r: bufio.NewReader(conn)}
		raw.handshakePaused(pause)
		_ = conn.Close()
	}
	close(stop)
	<-pollDone
}

// TestRace_HandshakeAuthMechanism pins the auth-enabled half of the F2 fix.
// TestRace_HandshakeUsernameVhost runs with authentication disabled, so it only
// exercises the conn.Username write at connection_handlers.go:288 and never the
// auth-enabled conn.AuthMechanism writes at :346/:355. This case drives
// concurrent PLAIN handshakes against an auth-enabled server while a bounded
// poller reads GET /api/connections, and asserts every snapshot reports either
// the pre-auth empty mechanism or PLAIN. Run under -race it fails if the
// AuthMechanism write and the management read are unsynchronised.
func TestRace_HandshakeAuthMechanism(t *testing.T) {
	authJSON := `{"users":[{"username":"admin","password_hash":"` + secretHash(t) + `","tags":["administrator"],"vhost_permissions":[{"vhost":"/","permission":{"configure":".*","write":".*","read":".*"}}]}]}`
	e := inheritEnv(t, authJSON, func(cfg *config.AMQPConfig) {
		cfg.Network.Address = "127.0.0.1:0"
	})
	go func() { _ = e.srv.Start() }()
	deadline := time.Now().Add(2 * time.Second)
	for !e.srv.IsListening() {
		if time.Now().After(deadline) {
			t.Fatal("AMQP listener not ready within 2s")
		}
		time.Sleep(5 * time.Millisecond)
	}
	e.srv.Mutex.RLock()
	addr := e.srv.Listener.Addr().String()
	e.srv.Mutex.RUnlock()

	stop := make(chan struct{})
	pollDone := make(chan struct{})
	// firstBad records the first per-snapshot invariant violation the poller
	// observes (a mechanism other than "" or PLAIN). It is a correctness
	// assertion, not the race signal: -race reports the unsynchronised access
	// itself; this names a torn or unexpected value if one ever appears.
	var firstBad atomic.Value
	go func() {
		defer close(pollDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			rec := e.do(http.MethodGet, "/api/connections", nil, basicAuthHeader())
			if rec.Code != http.StatusOK {
				firstBad.Store(fmt.Sprintf("GET /api/connections = %d, want 200", rec.Code))
				return
			}
			var conns []connectionInfo
			if err := json.Unmarshal(rec.Body.Bytes(), &conns); err != nil {
				firstBad.Store(fmt.Sprintf("decode /api/connections: %v", err))
				return
			}
			for _, c := range conns {
				if c.AuthMechanism != "" && c.AuthMechanism != "PLAIN" {
					firstBad.Store(fmt.Sprintf("auth_mechanism = %q, want empty or PLAIN", c.AuthMechanism))
					return
				}
			}
		}
	}()

	// A bounded number of paused PLAIN handshakes: each holds the connection in
	// the registry for `pause` after connection.open so the poller is guaranteed
	// to read its auth_mechanism.
	const conns = 20
	const pause = 15 * time.Millisecond
	for i := 0; i < conns; i++ {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("dial %s: %v", addr, err)
		}
		raw := &b2RawConn{t: t, c: conn, r: bufio.NewReader(conn)}
		raw.handshakeAsPaused("admin", "secret", pause)
		_ = conn.Close()
	}
	close(stop)
	select {
	case <-pollDone:
	case <-time.After(2 * time.Second):
		t.Fatal("poller did not stop within 2s")
	}
	if v := firstBad.Load(); v != nil {
		t.Fatalf("%s", v)
	}
}
