package server

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dialSilentSocket opens a TCP connection that sends nothing.
func dialSilentSocket(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestWSB_PreAuth_BadVersion_GetsSupportedHeader: AMQP 0-9-1 §4.2.2 requires
// the server to reply with the supported protocol header on a mismatch.
func TestWSB_PreAuth_BadVersion_GetsSupportedHeader(t *testing.T) {
	_, addr := startWSBServer(t, nil, nil)

	c := dialSilentSocket(t, addr)
	_, err := c.Write([]byte("AMQP\x01\x01\x00\x09"))
	require.NoError(t, err)

	buf := make([]byte, 8)
	require.NoError(t, c.SetReadDeadline(time.Now().Add(2*time.Second)))
	_, err = io.ReadFull(c, buf)
	require.NoError(t, err)
	assert.Equal(t, []byte("AMQP\x00\x00\x09\x01"), buf, "server must reply with the supported header")

	_, err = c.Read(buf)
	assert.Error(t, err, "socket must close after the supported-header reply")
}

// TestWSB_PreAuth_GarbageHeader_GetsSupportedHeader: same for non-AMQP bytes.
func TestWSB_PreAuth_GarbageHeader_GetsSupportedHeader(t *testing.T) {
	_, addr := startWSBServer(t, nil, nil)

	c := dialSilentSocket(t, addr)
	_, err := c.Write([]byte("GET / HT"))
	require.NoError(t, err)

	buf := make([]byte, 8)
	require.NoError(t, c.SetReadDeadline(time.Now().Add(2*time.Second)))
	_, err = io.ReadFull(c, buf)
	require.NoError(t, err)
	assert.Equal(t, []byte("AMQP\x00\x00\x09\x01"), buf)
}

// TestWSB_PreAuth_ShortHeaderTimesOut: a peer that sends a partial header and
// stops is reaped by the header deadline, and its handshake slot is released.
func TestWSB_PreAuth_ShortHeaderTimesOut(t *testing.T) {
	srv, addr := startWSBServer(t, nil, func(s *Server) {
		s.headerTimeoutOverride = 150 * time.Millisecond
	})

	c := dialSilentSocket(t, addr)
	_, err := c.Write([]byte("AMQP"))
	require.NoError(t, err)

	require.NoError(t, c.SetReadDeadline(time.Now().Add(2*time.Second)))
	_, err = c.Read(make([]byte, 1))
	assert.Error(t, err, "short header must be closed by the header deadline")

	require.Eventually(t, func() bool {
		return srv.pendingHandshakes.Load() == 0
	}, 2*time.Second, 10*time.Millisecond, "handshake slot must be released")
}

// TestWSB_PreAuth_SilentSocketsDoNotBlockRealClient: silent pre-auth sockets
// are capped by the pending-handshake limit, far below MaxConnections, so they
// can never starve authenticated clients.
func TestWSB_PreAuth_SilentSocketsDoNotBlockRealClient(t *testing.T) {
	srv, addr := startWSBServer(t, func(cfg *config.AMQPConfig) {
		cfg.Network.MaxConnections = 1000 // pending limit = 250
	}, nil)

	silent := make([]net.Conn, 0, 50)
	for i := 0; i < 50; i++ {
		silent = append(silent, dialSilentSocket(t, addr))
	}
	defer func() {
		for _, c := range silent {
			_ = c.Close()
		}
	}()

	require.Eventually(t, func() bool {
		return srv.pendingHandshakes.Load() >= 50
	}, 2*time.Second, 10*time.Millisecond)

	conn, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err, "a real client must connect behind 50 silent sockets")
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	_, err = ch.QueueDeclare("pa-q", false, true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.Publish("", "pa-q", false, false, amqp.Publishing{Body: []byte("m")}))
	msg, ok, err := ch.Get("pa-q", true)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "m", string(msg.Body))
}

// TestWSB_PreAuth_PendingLimitRefusesExcess: sockets beyond the
// pending-handshake limit are refused immediately.
func TestWSB_PreAuth_PendingLimitRefusesExcess(t *testing.T) {
	srv, addr := startWSBServer(t, nil, func(s *Server) {
		s.pendingLimitOverride = 4
	})

	silent := make([]net.Conn, 0, 4)
	for i := 0; i < 4; i++ {
		silent = append(silent, dialSilentSocket(t, addr))
	}

	require.Eventually(t, func() bool {
		return srv.pendingHandshakes.Load() == 4
	}, 2*time.Second, 10*time.Millisecond)

	fifth := dialSilentSocket(t, addr)
	require.NoError(t, fifth.SetReadDeadline(time.Now().Add(1*time.Second)))
	_, err := fifth.Read(make([]byte, 1))
	assert.Error(t, err, "the 5th pending socket must be refused")

	for _, c := range silent {
		_ = c.Close()
	}
	require.Eventually(t, func() bool {
		return srv.pendingHandshakes.Load() == 0
	}, 2*time.Second, 10*time.Millisecond, "slots must be released on close")

	conn, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn.Close()
}

// TestWSB_PreAuth_SlotHeldUntilOpenOK: the slot is held for the WHOLE
// handshake (not released at the protocol header), so a peer that sends 8
// valid bytes and idles still occupies a bounded slot.
func TestWSB_PreAuth_SlotHeldUntilOpenOK(t *testing.T) {
	_, addr := startWSBServer(t, nil, func(s *Server) {
		s.pendingLimitOverride = 1
	})

	// First client: complete everything EXCEPT connection.open.
	r := dialRaw(t, addr)
	r.sendHeader()
	r.expectMethod(10, 10)
	resp := append([]byte{0}, []byte("guest")...)
	resp = append(resp, 0)
	resp = append(resp, []byte("guest")...)
	r.startOK("PLAIN", resp)
	r.expectMethod(10, 30)
	tuneOK := make([]byte, 8)
	r.send(0, 10, 31, tuneOK)

	// Slot held: a second socket is refused.
	second := dialSilentSocket(t, addr)
	require.NoError(t, second.SetReadDeadline(time.Now().Add(1*time.Second)))
	_, err := second.Read(make([]byte, 1))
	assert.Error(t, err, "second socket must be refused while the slot is held")

	// First client completes the handshake: slot released.
	open := &protocol.ConnectionOpenMethod{VirtualHost: "/"}
	data, err := open.Serialize()
	require.NoError(t, err)
	r.send(0, 10, 40, data)
	r.expectMethod(10, 41) // open-ok

	conn, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err, "slot must be released after open-ok")
	defer conn.Close()
}
