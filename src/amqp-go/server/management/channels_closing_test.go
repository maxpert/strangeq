package management

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
)

// This file pins WS4 review finding 2: the DATA-006 `state=="closing"` IS
// observable over HTTP. A bogus basic.ack (60.80) is diverted to the ack
// processor, whose handleFrameError(..., onProcessor=false) classification
// marks the channel closing WITHOUT tearing it down; the channel stays in
// conn.Channels until the client's channel.close-ok, so GET /api/channels must
// report it as `closing` in that window.

// b2RawConn is a minimal raw AMQP 0-9-1 client (the management package has no
// raw helper of its own; the server package's is test-local to that package).
type b2RawConn struct {
	t *testing.T
	c net.Conn
	r *bufio.Reader
}

func (r *b2RawConn) send(ch, class, method uint16, args []byte) {
	r.t.Helper()
	frame := protocol.EncodeMethodFrameForChannel(ch, class, method, args)
	if err := protocol.WriteFrame(r.c, frame); err != nil {
		r.t.Fatalf("write %d.%d: %v", class, method, err)
	}
}

// readMethod reads frames (skipping heartbeats) until a method frame arrives.
func (r *b2RawConn) readMethod() (ch, class, method uint16, payload []byte) {
	r.t.Helper()
	if err := r.c.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		r.t.Fatalf("set deadline: %v", err)
	}
	for {
		f, err := protocol.ReadFrame(r.r)
		if err != nil {
			r.t.Fatalf("read frame: %v", err)
		}
		if f.Type == byte(protocol.FrameHeartbeat) {
			continue
		}
		if f.Type != byte(protocol.FrameMethod) || len(f.Payload) < 4 {
			r.t.Fatalf("unexpected frame type %d (payload %d)", f.Type, len(f.Payload))
		}
		return f.Channel, binary.BigEndian.Uint16(f.Payload[0:2]), binary.BigEndian.Uint16(f.Payload[2:4]), f.Payload[4:]
	}
}

func (r *b2RawConn) handshake() {
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
		Response:         []byte("\x00guest\x00guest"),
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
	_, cls, mth, _ = r.readMethod() // connection.open-ok
	if cls != 10 || mth != 41 {
		r.t.Fatalf("expected connection.open-ok, got %d.%d", cls, mth)
	}
}

func (r *b2RawConn) openChannel(ch uint16) {
	r.t.Helper()
	r.send(ch, 20, 10, []byte{})
	gotCh, cls, mth, _ := r.readMethod()
	if gotCh != ch || cls != 20 || mth != 11 {
		r.t.Fatalf("expected channel.open-ok on %d, got %d.%d on %d", ch, cls, mth, gotCh)
	}
}

// TestChannels_StateClosingOverHTTP drives the real frame path and observes the
// closing state through the management API before replying close-ok.
func TestChannels_StateClosingOverHTTP(t *testing.T) {
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

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	raw := &b2RawConn{t: t, c: conn, r: bufio.NewReader(conn)}
	raw.handshake()
	raw.openChannel(1)

	// A basic.ack for an unknown delivery tag: handled by the ack processor, so
	// the channel is marked closing but NOT torn down. The server answers
	// channel.close(406) and leaves the channel registered until close-ok.
	args := make([]byte, 9)
	binary.BigEndian.PutUint64(args[:8], 999)
	raw.send(1, 60, 80, args)

	closeCh, cls, mth, payload := raw.readMethod()
	if closeCh != 1 || cls != 20 || mth != 40 {
		t.Fatalf("expected channel.close(406) on channel 1, got %d.%d on %d", cls, mth, closeCh)
	}
	if len(payload) < 2 || binary.BigEndian.Uint16(payload[:2]) != 406 {
		t.Fatalf("channel.close payload = %v, want code 406", payload)
	}

	// HTTP-level observation, BEFORE the client replies close-ok.
	rec := e.do(http.MethodGet, "/api/channels", nil, basicAuthHeader())
	wantStatus(t, rec, http.StatusOK)
	var items []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &items); err != nil {
		t.Fatalf("decode /api/channels: %v (body=%s)", err, truncate(rec.Body.String()))
	}
	var found map[string]any
	for _, it := range items {
		if it["number"] == float64(1) {
			found = it
			break
		}
	}
	if found == nil {
		t.Fatalf("channel 1 is not listed while awaiting close-ok: %v", items)
	}
	if found["state"] != "closing" {
		t.Fatalf("state = %v, want \"closing\" before close-ok", found["state"])
	}

	// Complete the handshake so the channel is torn down cleanly.
	raw.send(1, 20, 41, []byte{})
}
