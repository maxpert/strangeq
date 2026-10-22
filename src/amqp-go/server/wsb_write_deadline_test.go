package server

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// fakeDeadlineConn is a net.Conn that records the order of SetWriteDeadline,
// Write and Close calls so tests can assert the writeLocked discipline.
type fakeDeadlineConn struct {
	mu        sync.Mutex
	calls     []string
	writeErr  error
	closed    bool
	deadlines []time.Time
}

func (f *fakeDeadlineConn) record(call string) {
	f.mu.Lock()
	f.calls = append(f.calls, call)
	f.mu.Unlock()
}

func (f *fakeDeadlineConn) Read(b []byte) (int, error) { return 0, errors.New("no reads") }
func (f *fakeDeadlineConn) Write(b []byte) (int, error) {
	f.record("write")
	return len(b), f.writeErr
}
func (f *fakeDeadlineConn) Close() error {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
	f.record("close")
	return nil
}
func (f *fakeDeadlineConn) LocalAddr() net.Addr  { return fakeAddr{} }
func (f *fakeDeadlineConn) RemoteAddr() net.Addr { return fakeAddr{} }
func (f *fakeDeadlineConn) SetDeadline(t time.Time) error {
	return nil
}
func (f *fakeDeadlineConn) SetReadDeadline(t time.Time) error { return nil }
func (f *fakeDeadlineConn) SetWriteDeadline(t time.Time) error {
	f.mu.Lock()
	f.deadlines = append(f.deadlines, t)
	f.mu.Unlock()
	if t.IsZero() {
		f.record("clear-deadline")
	} else {
		f.record("set-deadline")
	}
	return nil
}

type fakeAddr struct{}

func (fakeAddr) Network() string { return "fake" }
func (fakeAddr) String() string  { return "fake" }

func (f *fakeDeadlineConn) getCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.calls))
	copy(out, f.calls)
	return out
}

func (f *fakeDeadlineConn) getDeadlines() []time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]time.Time, len(f.deadlines))
	copy(out, f.deadlines)
	return out
}

func (f *fakeDeadlineConn) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

func newWriteDeadlineServer() *Server {
	return &Server{
		Log:              zap.NewNop(),
		Config:           config.DefaultConfig(),
		MetricsCollector: &NoOpMetricsCollector{},
	}
}

// TestWSB_WriteLocked_SetsAndClearsDeadline verifies the ordering discipline of
// writeLocked: arm a non-zero write deadline, write, then clear the deadline.
func TestWSB_WriteLocked_SetsAndClearsDeadline(t *testing.T) {
	srv := newWriteDeadlineServer()
	fc := &fakeDeadlineConn{}
	conn := protocol.NewConnection(fc)

	err := srv.writeLocked(conn, func(w net.Conn) error {
		_, werr := w.Write([]byte("x"))
		return werr
	})
	require.NoError(t, err)

	calls := fc.getCalls()
	require.Equal(t, []string{"set-deadline", "write", "clear-deadline"}, calls,
		"writeLocked must arm the deadline, write, then clear it, in that order")

	deadlines := fc.getDeadlines()
	require.Len(t, deadlines, 2)
	assert.False(t, deadlines[0].IsZero(), "first deadline must be non-zero (armed)")
	assert.True(t, deadlines[1].IsZero(), "second deadline must be zero (cleared)")
	assert.False(t, conn.Closed.Load(), "successful write must not close the connection")
	assert.False(t, fc.isClosed())
}

// TestWSB_WriteLocked_WriteErrorClosesConnection verifies that a failed write
// marks the connection closed and closes the socket (the stream may hold a
// partial frame, so the connection is unusable).
func TestWSB_WriteLocked_WriteErrorClosesConnection(t *testing.T) {
	srv := newWriteDeadlineServer()
	fc := &fakeDeadlineConn{writeErr: errors.New("boom")}
	conn := protocol.NewConnection(fc)

	err := srv.writeLocked(conn, func(w net.Conn) error {
		_, werr := w.Write([]byte("x"))
		return werr
	})
	require.Error(t, err)

	assert.True(t, conn.Closed.Load(), "failed write must mark the connection closed")
	assert.True(t, fc.isClosed(), "failed write must close the socket")

	calls := fc.getCalls()
	require.Equal(t, []string{"set-deadline", "write", "clear-deadline", "close"}, calls)
}

// TestWSB_WriteBuffers_HonoursDeadline verifies that a vectored write to a peer
// that never reads fails with a timeout error within the configured write
// timeout instead of blocking forever.
func TestWSB_WriteBuffers_HonoursDeadline(t *testing.T) {
	srv := newWriteDeadlineServer()
	srv.writeTimeoutOverride = 50 * time.Millisecond

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	conn := protocol.NewConnection(serverConn)
	// Nobody reads clientConn: every write blocks until the deadline fires.

	bufs := net.Buffers{make([]byte, 64<<10)}
	start := time.Now()
	err := srv.writeBuffers(conn, &bufs)
	elapsed := time.Since(start)

	require.Error(t, err, "write to a non-reading peer must fail")
	assert.Less(t, elapsed, 1*time.Second, "write must be bounded by the deadline")
	var ne net.Error
	require.True(t, errors.As(err, &ne) && ne.Timeout(),
		"error must be a network timeout, got %v", err)
	assert.True(t, conn.Closed.Load(), "timed-out write must mark the connection closed")
}

// TestWSB_WriteFrame_UsesDeadlineHelper verifies writeFrame round-trips a frame
// through the deadline-armed writer.
func TestWSB_WriteFrame_UsesDeadlineHelper(t *testing.T) {
	srv := newWriteDeadlineServer()
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	conn := protocol.NewConnection(serverConn)

	go func() {
		buf := make([]byte, 64)
		_, _ = clientConn.Read(buf)
	}()

	frame := protocol.EncodeMethodFrame(10, 51, []byte{})
	require.NoError(t, srv.writeFrame(conn, frame))
	assert.False(t, conn.Closed.Load())
}
