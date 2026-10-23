package server

import (
	"errors"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/auth"
	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startWSBAuthServer starts a server with file auth (alice/secret) and
// authorization DISABLED (the worst case in the report).
func startWSBAuthServer(t *testing.T, tweak func(*Server)) (*Server, string) {
	t.Helper()
	return startWSBServer(t, func(cfg *config.AMQPConfig) {
		cfg.Security.AuthorizationEnabled = false
		withFileAuth(t, cfg, []auth.UserEntry{wsbUser(t, "alice", "secret", ".*", ".*", ".*")})
	}, tweak)
}

// TestWSB_Handshake_TuneOKFirst_Gets503NoTune: sending tune-ok before start-ok
// is a hard 503 (and specifically NOT answered with connection.tune). Today
// the server sends tune and the connection proceeds unauthenticated (bypass #1).
func TestWSB_Handshake_TuneOKFirst_Gets503NoTune(t *testing.T) {
	_, addr := startWSBAuthServer(t, nil)

	r := dialRaw(t, addr)
	r.sendHeader()
	r.expectMethod(10, 10) // connection.start

	tuneOK := make([]byte, 8)
	r.send(0, 10, 31, tuneOK)

	closeM := r.expectConnectionClose(503)
	assert.Equal(t, uint16(10), closeM.ClassID)
	assert.Equal(t, uint16(31), closeM.MethodID)
	r.send(0, 10, 51, []byte{}) // close-ok
	r.expectEOF()
}

// TestWSB_Handshake_OpenFirst_Gets503: connection.open before start-ok → 503.
func TestWSB_Handshake_OpenFirst_Gets503(t *testing.T) {
	_, addr := startWSBAuthServer(t, nil)

	r := dialRaw(t, addr)
	r.sendHeader()
	r.expectMethod(10, 10)

	open := &protocol.ConnectionOpenMethod{VirtualHost: "/"}
	data, err := open.Serialize()
	require.NoError(t, err)
	r.send(0, 10, 40, data)

	closeM := r.expectConnectionClose(503)
	assert.Equal(t, uint16(10), closeM.ClassID)
	assert.Equal(t, uint16(40), closeM.MethodID)
	r.send(0, 10, 51, []byte{})
	r.expectEOF()
}

// TestWSB_Handshake_HeaderFrameFirst_Gets505: a non-method frame during the
// handshake → 505 UNEXPECTED_FRAME.
func TestWSB_Handshake_HeaderFrameFirst_Gets505(t *testing.T) {
	_, addr := startWSBAuthServer(t, nil)

	r := dialRaw(t, addr)
	r.sendHeader()
	r.expectMethod(10, 10)

	hdr := &protocol.ContentHeader{ClassID: 60, BodySize: 0, PropertyFlags: 0}
	hp, err := hdr.Serialize()
	require.NoError(t, err)
	r.sendFrame(&protocol.Frame{Type: protocol.FrameHeader, Channel: 0, Size: uint32(len(hp)), Payload: hp})

	r.expectConnectionClose(505)
	r.send(0, 10, 51, []byte{})
	r.expectEOF()
}

// TestWSB_Handshake_UnsupportedMechanism_Gets403NoTune: a start-ok with an
// unsupported mechanism must be answered 403 and MUST NOT be followed by
// connection.tune (today: close, then tune — bypass #2).
func TestWSB_Handshake_UnsupportedMechanism_Gets403NoTune(t *testing.T) {
	_, addr := startWSBAuthServer(t, func(s *Server) {
		s.closeOKTimeoutOverride = 50 * time.Millisecond // EOF promptly without close-ok
	})

	r := dialRaw(t, addr)
	r.sendHeader()
	r.expectMethod(10, 10)
	r.startOK("BOGUS", []byte{0})

	r.expectConnectionClose(403)
	// EOF without a 10.30 ever arriving.
	r.expectEOF()
}

// TestWSB_Handshake_MalformedStartOK_Gets503NoTune: an unparseable start-ok is
// a hard 503 with no tune after (bypass #2).
func TestWSB_Handshake_MalformedStartOK_Gets503NoTune(t *testing.T) {
	_, addr := startWSBAuthServer(t, func(s *Server) {
		s.closeOKTimeoutOverride = 50 * time.Millisecond
	})

	r := dialRaw(t, addr)
	r.sendHeader()
	r.expectMethod(10, 10)
	r.send(0, 10, 11, []byte{0x01})

	r.expectConnectionClose(503)
	r.expectEOF()
}

// TestWSB_Handshake_BadPassword_Gets403: valid start-ok, wrong password → 403.
func TestWSB_Handshake_BadPassword_Gets403(t *testing.T) {
	_, addr := startWSBAuthServer(t, func(s *Server) {
		s.closeOKTimeoutOverride = 50 * time.Millisecond
	})

	r := dialRaw(t, addr)
	r.sendHeader()
	r.expectMethod(10, 10)
	r.startOK("PLAIN", []byte{0, 'a', 'l', 'i', 'c', 'e', 0, 'w', 'r', 'o', 'n', 'g'})

	r.expectConnectionClose(403)
	r.expectEOF()
}

// TestWSB_Handshake_OpenInsteadOfTuneOK_Gets503: after a valid start-ok and
// the server's tune, connection.open (instead of tune-ok) → 503.
func TestWSB_Handshake_OpenInsteadOfTuneOK_Gets503(t *testing.T) {
	_, addr := startWSBAuthServer(t, nil)

	r := dialRaw(t, addr)
	r.sendHeader()
	r.expectMethod(10, 10)
	resp := append([]byte{0}, []byte("alice")...)
	resp = append(resp, 0)
	resp = append(resp, []byte("secret")...)
	r.startOK("PLAIN", resp)
	r.expectMethod(10, 30) // connection.tune

	open := &protocol.ConnectionOpenMethod{VirtualHost: "/"}
	data, err := open.Serialize()
	require.NoError(t, err)
	r.send(0, 10, 40, data)

	closeM := r.expectConnectionClose(503)
	assert.Equal(t, uint16(10), closeM.ClassID)
	assert.Equal(t, uint16(40), closeM.MethodID)
	r.send(0, 10, 51, []byte{})
	r.expectEOF()
}

// TestWSB_Handshake_ClientCloseMidHandshake_GetsCloseOK: a client
// connection.close mid-handshake is answered close-ok, then the socket closes.
func TestWSB_Handshake_ClientCloseMidHandshake_GetsCloseOK(t *testing.T) {
	_, addr := startWSBAuthServer(t, nil)

	r := dialRaw(t, addr)
	r.sendHeader()
	r.expectMethod(10, 10)

	closeM := &protocol.ConnectionCloseMethod{ReplyCode: 200, ReplyText: "goodbye", ClassID: 0, MethodID: 0}
	data, err := closeM.Serialize()
	require.NoError(t, err)
	r.send(0, 10, 50, data)

	r.expectMethod(10, 51) // connection.close-ok
	r.expectEOF()
}

// TestWSB_Handshake_PostOpenStartOK_Gets503: a start-ok AFTER the handshake
// must not re-authenticate (mid-connection identity change) — it is a hard 503.
func TestWSB_Handshake_PostOpenStartOK_Gets503(t *testing.T) {
	_, addr := startWSBAuthServer(t, nil)

	r := dialRaw(t, addr)
	r.handshake("alice", "secret", 0, 0)

	resp := append([]byte{0}, []byte("alice")...)
	resp = append(resp, 0)
	resp = append(resp, []byte("secret")...)
	r.startOK("PLAIN", resp)

	closeM := r.expectConnectionClose(503)
	assert.Equal(t, uint16(10), closeM.ClassID)
	assert.Equal(t, uint16(11), closeM.MethodID)
	r.send(0, 10, 51, []byte{})
	r.expectEOF()
}

// TestWSB_Handshake_ValidClientStillConnects: the happy path keeps working.
func TestWSB_Handshake_ValidClientStillConnects(t *testing.T) {
	_, addr := startWSBAuthServer(t, nil)

	conn, err := amqp.Dial("amqp://alice:secret@" + addr + "/")
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	_, err = ch.QueueDeclare("hs-q", false, true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.Publish("", "hs-q", false, false, amqp.Publishing{Body: []byte("m")}))
	msg, ok, err := ch.Get("hs-q", true)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "m", string(msg.Body))
}

// TestWSB_Handshake_AbsoluteDeadline: one absolute deadline covers the whole
// handshake — a peer that sends the header and then idles is reaped.
func TestWSB_Handshake_AbsoluteDeadline(t *testing.T) {
	_, addr := startWSBAuthServer(t, func(s *Server) {
		s.handshakeTimeoutOverride = 300 * time.Millisecond
	})

	r := dialRaw(t, addr)
	r.sendHeader()
	r.expectMethod(10, 10)
	// Send nothing more: the absolute handshake deadline must reap us.
	r.expectEOF()
}

// TestWSB_ConnectionOpen_RequiresUsernameWhenAuthEnabled (unit): with
// authentication enabled, connection.open on a connection whose start-ok
// produced no identity is refused 403 (defence in depth, §1.3 step 6).
func TestWSB_ConnectionOpen_RequiresUsernameWhenAuthEnabled(t *testing.T) {
	srv := newB4Server(t, true, []auth.UserEntry{fullAdminUser("admin")})
	conn, frameCh := newPipeConnWithFrames(t)
	require.Equal(t, "", conn.Username)

	open := &protocol.ConnectionOpenMethod{VirtualHost: "/"}
	payload, err := open.Serialize()
	require.NoError(t, err)

	err = srv.processConnectionOpen(conn, payload)
	require.Error(t, err)
	var cne *ConnectionError
	require.True(t, errors.As(err, &cne), "want *ConnectionError, got %T", err)
	assert.Equal(t, uint16(403), cne.Code)

	classID, methodID, payload2 := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(10), classID)
	assert.Equal(t, uint16(50), methodID)
	cm := &protocol.ConnectionCloseMethod{}
	require.NoError(t, cm.Deserialize(payload2))
	assert.Equal(t, uint16(403), cm.ReplyCode)
}
