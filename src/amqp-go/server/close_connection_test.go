package server

import (
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

// TestCloseConnection_Graceful320 proves a live peer receives connection.close
// with reply-code 320 and the operator reason, and that the server removes the
// connection after the peer replies close-ok (FR-API-021, spec §5.4 test 34).
func TestCloseConnection_Graceful320(t *testing.T) {
	srv, addr := startWSBServer(t, nil, nil)

	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 0, 0)

	conn := wsbServerConn(srv)
	require.NotNil(t, conn, "the handshake must have registered a server-side connection")

	require.NoError(t, srv.CloseConnection(conn.ID, "operator requested close"))

	m := r.expectConnectionClose(320)
	require.Equal(t, "operator requested close", m.ReplyText)

	// The peer answers close-ok; the connection is then torn down.
	r.send(0, 10, 51, []byte{})

	deadline := time.Now().Add(2 * time.Second)
	for wsbConnCount(srv) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("connection was not removed after close-ok within 2s (count=%d)", wsbConnCount(srv))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestCloseConnection_ReasonSanitised proves a reason with CR/LF and >255 bytes
// is delivered as a single well-formed shortstr frame with the control bytes
// stripped and the length capped (SEC-085, spec §5.4 test 35).
func TestCloseConnection_ReasonSanitised(t *testing.T) {
	srv, addr := startWSBServer(t, nil, nil)
	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 0, 0)
	conn := wsbServerConn(srv)
	require.NotNil(t, conn)

	reason := "bad\r\nreason\x00" + strings.Repeat("x", 400)
	require.NoError(t, srv.CloseConnection(conn.ID, reason))

	// A successful parse of the close frame proves the reply-text did not split
	// the frame.
	m := r.expectConnectionClose(320)
	require.LessOrEqual(t, len(m.ReplyText), 255, "the reason must be capped at 255 bytes")
	require.NotContains(t, m.ReplyText, "\r")
	require.NotContains(t, m.ReplyText, "\n")
	require.NotContains(t, m.ReplyText, "\x00")
	require.True(t, utf8.ValidString(m.ReplyText), "the reason must stay valid UTF-8")
	require.True(t, strings.HasPrefix(m.ReplyText, "badreason"), "reason=%q", m.ReplyText)
}

// TestCloseConnection_StuckForcesBounded proves a peer that never replies
// close-ok is force-closed within closeOKTimeout (spec §5.4 test 36).
func TestCloseConnection_StuckForcesBounded(t *testing.T) {
	srv, addr := startWSBServer(t, nil, func(s *Server) {
		s.closeOKTimeoutOverride = 50 * time.Millisecond
	})
	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 0, 0)
	conn := wsbServerConn(srv)
	require.NotNil(t, conn)

	require.NoError(t, srv.CloseConnection(conn.ID, "stuck peer"))
	r.expectConnectionClose(320)
	// Never reply close-ok; the bounded fallback must close the socket.
	r.expectEOF()
}

// TestCloseConnection_NotFound proves an unknown id yields the exported
// sentinel (spec §5.4 test 37).
func TestCloseConnection_NotFound(t *testing.T) {
	srv, _ := newEntityTestServer(t)
	err := srv.CloseConnection("no-such-connection", "x")
	require.ErrorIs(t, err, ErrConnectionNotFound)
}

// TestCloseConnection_IdempotentExactlyOnce proves failConnection's CAS makes
// the handshake exactly-once: a second CloseConnection sends no second frame.
func TestCloseConnection_IdempotentExactlyOnce(t *testing.T) {
	srv, addr := startWSBServer(t, nil, nil)
	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 0, 0)
	conn := wsbServerConn(srv)
	require.NotNil(t, conn)

	require.NoError(t, srv.CloseConnection(conn.ID, "first"))
	r.expectConnectionClose(320)

	require.NoError(t, srv.CloseConnection(conn.ID, "second"), "an already-closing connection is a no-op returning nil")
	r.expectSilence(150 * time.Millisecond)
}

// TestCloseConnection_ConcurrentExactlyOnce is the -race assertion that two
// concurrent CloseConnection calls emit exactly one connection.close frame.
func TestCloseConnection_ConcurrentExactlyOnce(t *testing.T) {
	srv, addr := startWSBServer(t, nil, nil)
	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 0, 0)
	conn := wsbServerConn(srv)
	require.NotNil(t, conn)

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			require.NoError(t, srv.CloseConnection(conn.ID, "close"))
		}(i)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("concurrent CloseConnection calls did not return within 2s")
	}

	r.expectConnectionClose(320)
	r.expectSilence(150 * time.Millisecond)
}

// TestCloseConnection_RaceWithTeardown closes a connection while its reader is
// parked mid-frame and its teardown runs; -race clean, exactly one close frame
// (spec §5.3 test 20).
func TestCloseConnection_RaceWithTeardown(t *testing.T) {
	srv, addr := startWSBServer(t, nil, func(s *Server) {
		// Shorten the bounded force-close so the test does not wait 3 s.
		s.closeOKTimeoutOverride = 200 * time.Millisecond
	})
	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 0, 0)
	r.openChannel(1)
	r.declareQueue(1, "race-q")

	conn := wsbServerConn(srv)
	require.NotNil(t, conn)

	// Park the server reader mid-frame: three bytes of a seven-byte frame
	// header. After the handshake there is no read deadline, so the reader
	// blocks here until the socket closes.
	_, err := r.c.Write([]byte{0x01, 0x00, 0x00})
	require.NoError(t, err)

	require.NoError(t, srv.CloseConnection(conn.ID, "teardown race"))
	m := r.expectConnectionClose(320)
	require.Equal(t, uint16(320), m.ReplyCode)

	// The peer never sends close-ok (its reader is stalled); the bounded
	// fallback force-closes the socket. Then the server-side teardown runs.
	r.expectEOF()

	deadline := time.Now().Add(3 * time.Second)
	for wsbConnCount(srv) != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("connection not torn down within 3s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
