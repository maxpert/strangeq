package server

import (
	"errors"
	"strings"
	"unicode/utf8"

	amqperrors "github.com/maxpert/amqp-go/errors"
	"go.uber.org/zap"
)

// ErrConnectionNotFound is returned by CloseConnection when no live connection
// has the given id.
var ErrConnectionNotFound = errors.New("connection not found")

// CloseConnection closes the connection with id gracefully: it sends an AMQP
// connection.close with reply-code 320 and reason (the operator-supplied text),
// then arms the server's bounded force-close timer. It returns once the close
// frame has been written (or the write failed), not once the peer has replied
// close-ok. It is idempotent: a connection already closing is a no-op returning
// nil. It takes only Server.Mutex.RLock to look the connection up and never a
// forbidden lock. Unknown id => ErrConnectionNotFound.
//
// It reuses the existing failConnection handshake (spec mgmt-04 §2.1): the
// frame write is bounded by writeTimeout, and the close-ok wait by
// closeOKTimeout (3 s), after which the socket is force-closed. No goroutine is
// spawned.
func (s *Server) CloseConnection(id, reason string) error {
	reason = sanitizeCloseReason(reason)

	s.Mutex.RLock()
	conn, ok := s.Connections[id]
	s.Mutex.RUnlock()
	if !ok || conn == nil {
		return ErrConnectionNotFound
	}

	// Prefer the graceful handshake whenever a connection runtime is
	// registered (production connections always are). Confirm flushing inside
	// failConnection is gated on handshakeDone, so this is safe before the
	// handshake completes too.
	if rt, ok := s.runtimes.Load(conn); ok {
		s.failConnection(rt.(*connRuntime), amqperrors.ConnectionForced, reason, 0, 0)
		return nil
	}

	// No runtime (a bare/test connection that never reached handleConnection):
	// fall back to the force path, preserving the pre-BOB-5 behaviour. failConnection's
	// CAS does not exist here, so guard on the connection's own Closed atomic.
	if conn.Closed.CompareAndSwap(false, true) {
		if conn.Conn != nil {
			_ = conn.Conn.Close()
		}
	}
	if s.Log != nil {
		s.Log.Debug("force-closing connection without a runtime",
			zap.String("connection_id", id))
	}
	return nil
}

// sanitizeCloseReason strips ASCII control characters (\x00-\x1f, \x7f,
// including CR/LF) and truncates to at most 255 bytes on a UTF-8 boundary. The
// connection.close reply-text is a shortstr (protocol/methods.go), so an
// unsanitised operator reason could otherwise split a frame or inject a header
// (SEC-085). An empty reason is legal and returns "".
func sanitizeCloseReason(reason string) string {
	// 255 is the maximum shortstr payload AMQP can carry in one length byte.
	const max = 255
	if reason == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(reason))
	for i := 0; i < len(reason); i++ {
		c := reason[i]
		if c < 0x20 || c == 0x7f {
			continue
		}
		b.WriteByte(c)
	}
	out := b.String()
	if len(out) <= max {
		return out
	}
	// Truncate at or below max bytes on a valid rune boundary so the shortstr
	// never carries a partial multi-byte sequence.
	cut := max
	for cut > 0 && !utf8.RuneStart(out[cut]) {
		cut--
	}
	return out[:cut]
}

// ensure the compile-time contract the management layer type-asserts.
var _ interface {
	CloseConnection(id, reason string) error
} = (*Server)(nil)
