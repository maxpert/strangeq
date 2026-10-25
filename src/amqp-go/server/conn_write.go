package server

import (
	"net"
	"time"

	"github.com/maxpert/amqp-go/protocol"
)

// defaultWriteTimeout bounds every server→client socket write (P0-10). Without
// it a peer that stops reading blocks the writer holding conn.WriteMutex, and
// every other writer on the connection queues behind it forever. Follow-up for
// WS-E: optional Network.WriteTimeoutMS config knob (spec D2).
const defaultWriteTimeout = 30 * time.Second

func (s *Server) writeTimeout() time.Duration {
	if s.writeTimeoutOverride > 0 {
		return s.writeTimeoutOverride
	}
	return defaultWriteTimeout
}

// writeLocked is the ONLY way server code writes to a client socket (except
// alarm_*.go and sendChannelFlow, which use
// protocol.WriteFrameToConnectionWithDeadline — compatible: both set and clear
// the deadline under the same WriteMutex).
//
// On any error the stream may hold a partial frame, so the connection is
// unusable: mark it closed and close the socket (unblocks the reader →
// teardown). The deadline is armed before and cleared after every write, all
// under WriteMutex, so a deadline can never leak between writers.
func (s *Server) writeLocked(conn *protocol.Connection, fn func(w net.Conn) error) error {
	conn.WriteMutex.Lock()
	defer conn.WriteMutex.Unlock()
	_ = conn.Conn.SetWriteDeadline(time.Now().Add(s.writeTimeout()))
	err := fn(conn.Conn)
	_ = conn.Conn.SetWriteDeadline(time.Time{})
	if err != nil {
		conn.Closed.Store(true)
		_ = conn.Conn.Close()
		// WS-I: unblock a reader parked in a publish-lease Reserve — the
		// connection is dead, so its wait must not linger until WAL progress
		// or shutdown (cold path: the runtimes lookup is fine here).
		if rt, ok := s.runtimes.Load(conn); ok {
			rt.(*connRuntime).leaseCancel()
		}
	}
	return err
}

func (s *Server) writeFrame(conn *protocol.Connection, f *protocol.Frame) error {
	return s.writeLocked(conn, func(w net.Conn) error { return protocol.WriteFrame(w, f) })
}

func (s *Server) writeBytes(conn *protocol.Connection, b []byte) error {
	return s.writeLocked(conn, func(w net.Conn) error { _, err := w.Write(b); return err })
}

// writeBuffers: net.Buffers.WriteTo has no deadline parameter, but the deadline
// lives on the fd (poll.FD), so the writev(2) it issues on *net.TCPConn — and
// the sequential per-iovec Writes it falls back to on *tls.Conn / net.Pipe —
// all honour the SetWriteDeadline set by writeLocked.
func (s *Server) writeBuffers(conn *protocol.Connection, bufs *net.Buffers) error {
	return s.writeLocked(conn, func(w net.Conn) error { _, err := bufs.WriteTo(w); return err })
}
