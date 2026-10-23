package server

// S4 seam test (WS-E access control × WS-B handshake): an access-policy deny
// during the handshake must produce 403 ACCESS_REFUSED through WS-B's typed
// ConnectionError path — EXACTLY ONE connection.close on the wire, then the
// socket closes, and the pending-handshake slot is released. These tests drive
// the real handleConnection path (not handleConnectionStartOK on a pipe), so
// slot accounting and the close handshake are the shipped ones.

import (
	"testing"
	"time"

	"github.com/maxpert/amqp-go/auth"
	"github.com/maxpert/amqp-go/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Host deny: admitPeer runs before SASL, so the refusal follows start-ok
// directly (no tune), with one 403 close and a released slot.
func TestS4_HostDeny_OneCloseSocketClosesSlotReleased(t *testing.T) {
	srv, addr := startWSBServer(t, func(cfg *config.AMQPConfig) {
		// Loopback is not in 10.0.0.0/8 → every test peer is refused.
		cfg.Security.AllowedHosts = []string{"10.0.0.0/8"}
	}, nil)

	r := dialRaw(t, addr)
	r.sendHeader()
	r.expectMethod(10, 10) // connection.start
	resp := append([]byte{0}, []byte("guest")...)
	resp = append(resp, 0)
	resp = append(resp, []byte("guest")...)
	r.startOK("PLAIN", resp)

	// Exactly one connection.close, 403 ACCESS_REFUSED, echoing 10.11.
	m := r.expectConnectionClose(403)
	assert.Contains(t, m.ReplyText, "ACCESS_REFUSED")
	assert.Equal(t, uint16(10), m.ClassID)
	assert.Equal(t, uint16(11), m.MethodID)

	// Answer close-ok; the very next thing on the socket must be EOF — no
	// second close frame, no tune.
	r.send(0, 10, 51, []byte{})
	r.expectEOF()

	require.Eventually(t, func() bool {
		return srv.pendingHandshakes.Load() == 0
	}, 2*time.Second, 10*time.Millisecond, "pending-handshake slot must be released after a deny")
}

// User deny: checkUserAccess runs once identity is known (the *interfaces.User
// branch), so the refusal also produces exactly one 403 close through WS-B's
// path and releases the slot.
func TestS4_BlockedUser_OneCloseSocketClosesSlotReleased(t *testing.T) {
	srv, addr := startWSBServer(t, func(cfg *config.AMQPConfig) {
		withFileAuth(t, cfg, []auth.UserEntry{wsbUser(t, "alice", "pw", ".*", ".*", ".*")})
		cfg.Security.BlockedUsers = []string{"alice"}
	}, nil)

	r := dialRaw(t, addr)
	r.sendHeader()
	r.expectMethod(10, 10) // connection.start
	resp := append([]byte{0}, []byte("alice")...)
	resp = append(resp, 0)
	resp = append(resp, []byte("pw")...)
	r.startOK("PLAIN", resp)

	m := r.expectConnectionClose(403)
	assert.Equal(t, uint16(10), m.ClassID)
	assert.Equal(t, uint16(11), m.MethodID)

	r.send(0, 10, 51, []byte{})
	r.expectEOF()

	require.Eventually(t, func() bool {
		return srv.pendingHandshakes.Load() == 0
	}, 2*time.Second, 10*time.Millisecond, "pending-handshake slot must be released after a deny")
}

// Control: a peer the policy ADMITS still completes the full handshake, so the
// deny paths above are policy-driven, not a broken handshake.
func TestS4_AdmittedPeer_HandshakeStillCompletes(t *testing.T) {
	_, addr := startWSBServer(t, func(cfg *config.AMQPConfig) {
		cfg.Security.AllowedHosts = []string{"127.0.0.0/8"}
	}, nil)

	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 0, 0)
	r.openChannel(1)
	r.declareQueue(1, "s4-admit-q")
}
