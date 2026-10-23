package server

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// stopReader tears down a rig reader the way production teardown does:
// close the socket FIRST (unblocks a blocked read), then signal Done.
func stopReader(conn *protocol.Connection, done chan struct{}) {
	_ = conn.Conn.Close()
	close(conn.Done)
	<-done
}

// newMemboundRig builds a net.Pipe connection and a Server configured per
// §14.10: 256KiB frame budget, no probe reads (time.Hour), huge overflow cap.
func newMemboundRig(t *testing.T, budgetOverride int64) (*Server, *protocol.Connection, net.Conn) {
	t.Helper()
	srv := &Server{
		Log:              zap.NewNop(),
		Config:           config.DefaultConfig(),
		MetricsCollector: &NoOpMetricsCollector{},
	}
	if budgetOverride > 0 {
		srv.frameQueueByteBudgetOverride = budgetOverride
	}
	srv.readerBackpressureProbe = time.Hour // no probe reads
	srv.Config.Network.ReaderOverflowHardCapBytes = 1 << 30

	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() { _ = clientConn.Close() })
	t.Cleanup(func() { _ = serverConn.Close() })
	conn := protocol.NewConnection(serverConn)
	return srv, conn, clientConn
}

// startBodyFrameWriter launches a goroutine writing size-byte body frames on
// ch 1, incrementing sent after each completed WriteFrame.
func startBodyFrameWriter(t *testing.T, c net.Conn, size int, sent *atomic.Int64) {
	t.Helper()
	go func() {
		payload := make([]byte, size)
		for {
			f := &protocol.Frame{Type: protocol.FrameBody, Channel: 1, Size: uint32(size), Payload: payload}
			if err := protocol.WriteFrame(c, f); err != nil {
				return
			}
			sent.Add(1)
		}
	}()
}

// TestWSB_FrameBudget_BlocksReader: once the byte budget is exhausted the
// reader parks (TCP backpressure) instead of growing memory.
func TestWSB_FrameBudget_BlocksReader(t *testing.T) {
	srv, conn, clientConn := newMemboundRig(t, 256<<10)
	rt := srv.runtimeFor(conn)
	done := make(chan struct{})
	defer stopReader(conn, done)
	go srv.readFrames(rt, done)

	var sent atomic.Int64
	startBodyFrameWriter(t, clientConn, 64<<10, &sent)

	budget := srv.frameQueueByteBudget()
	maxFrame := int64(64<<10) + 64

	// The reader must stop admitting: sent stabilizes, bounded by budget + slack.
	var last int64
	require.Eventually(t, func() bool {
		cur := sent.Load()
		stalled := cur == last && cur > 0
		last = cur
		return stalled
	}, 2*time.Second, 100*time.Millisecond)

	final := sent.Load()
	assert.Less(t, final, int64(20), "reader must block well before unbounded growth")
	assert.LessOrEqual(t, conn.FrameQueueBytes.Load(), budget+maxFrame,
		"queued bytes must never exceed budget + one max frame")
}

// TestWSB_FrameBudget_ReleaseWakesReader: releasing budget wakes the parked
// reader and the writer advances again.
func TestWSB_FrameBudget_ReleaseWakesReader(t *testing.T) {
	srv, conn, clientConn := newMemboundRig(t, 256<<10)
	rt := srv.runtimeFor(conn)
	done := make(chan struct{})
	defer stopReader(conn, done)
	go srv.readFrames(rt, done)

	var sent atomic.Int64
	startBodyFrameWriter(t, clientConn, 64<<10, &sent)

	// Wait for the reader to park.
	var last int64
	require.Eventually(t, func() bool {
		cur := sent.Load()
		stalled := cur == last && cur > 0
		last = cur
		return stalled
	}, 2*time.Second, 100*time.Millisecond)

	// Drain one frame from the FrameQueue and release its budget.
	select {
	case f := <-conn.FrameQueue:
		srv.releaseFrameBudget(conn, frameCost(f))
		protocol.PutFrame(f)
	case <-time.After(time.Second):
		t.Fatal("no frame queued")
	}

	before := sent.Load()
	require.Eventually(t, func() bool {
		return sent.Load() > before
	}, 2*time.Second, 20*time.Millisecond, "releasing budget must wake the reader")
}

// TestWSB_FrameBudget_AcksBypass: acks never count against the budget and are
// never blocked by it — one gets through while the budget is exhausted.
func TestWSB_FrameBudget_AcksBypass(t *testing.T) {
	srv, conn, clientConn := newMemboundRig(t, 256<<10)
	srv.readerBackpressureProbe = 5 * time.Millisecond // probe reads keep acks flowing
	rt := srv.runtimeFor(conn)
	done := make(chan struct{})
	defer stopReader(conn, done)
	go srv.readFrames(rt, done)

	var sent atomic.Int64
	startBodyFrameWriter(t, clientConn, 64<<10, &sent)

	// Wait for the budget to be exhausted (FrameQueueBytes plateaus): with the
	// 5ms probe the reader keeps draining the pipe into its own pending buffer,
	// so the writer never stalls — the queued-bytes plateau is the signal.
	var lastQ int64
	require.Eventually(t, func() bool {
		q := conn.FrameQueueBytes.Load()
		stalled := q == lastQ && q > 0
		lastQ = q
		return stalled
	}, 2*time.Second, 20*time.Millisecond)
	budgetBytes := conn.FrameQueueBytes.Load()

	// A 13-byte basic.ack frame must still be read and diverted.
	ack := &protocol.BasicAckMethod{DeliveryTag: 1, Multiple: false}
	data, err := ack.Serialize()
	require.NoError(t, err)
	require.NoError(t, protocol.WriteFrame(clientConn, protocol.EncodeMethodFrameForChannel(1, 60, protocol.BasicAck, data)))

	select {
	case f := <-conn.AckQueue:
		protocol.PutFrame(f)
	case <-time.After(2 * time.Second):
		t.Fatal("ack must bypass the frame budget and reach AckQueue")
	}
	assert.Equal(t, budgetBytes, conn.FrameQueueBytes.Load(), "acks must not touch FrameQueueBytes")
}

// TestWSB_OversizedAckFrame_ClosesConnection: an ack-classified frame over the
// payload bound hard-closes the connection (no close frame — the reader must
// not write).
func TestWSB_OversizedAckFrame_ClosesConnection(t *testing.T) {
	srv, conn, clientConn := newMemboundRig(t, 256<<10)
	rt := srv.runtimeFor(conn)
	done := make(chan struct{})
	go srv.readFrames(rt, done)

	bigPayload := make([]byte, 1024)
	// class 60, method 80 (basic.ack) + 1016 bytes of junk
	bigPayload[1] = 60
	bigPayload[3] = 80
	require.NoError(t, protocol.WriteFrame(clientConn, protocol.EncodeMethodFrameForChannel(1, 60, protocol.BasicAck, bigPayload)))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reader must exit on an oversized ack frame")
	}
	assert.True(t, conn.Closed.Load())

	clientConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err := clientConn.Read(make([]byte, 1))
	assert.Error(t, err, "client side must see EOF")
}

// TestWSB_FrameBudget_SmallFramesUnaffected: the default budget (32MiB) never
// binds on small frames — thousands of slots fill with no block.
func TestWSB_FrameBudget_SmallFramesUnaffected(t *testing.T) {
	srv, conn, clientConn := newMemboundRig(t, 0) // default 32MiB budget
	rt := srv.runtimeFor(conn)
	done := make(chan struct{})
	defer stopReader(conn, done)
	go srv.readFrames(rt, done)

	const n = 5000
	payload := make([]byte, 100)
	var written atomic.Int64
	go func() {
		for i := 0; i < n; i++ {
			f := &protocol.Frame{Type: protocol.FrameBody, Channel: 1, Size: uint32(len(payload)), Payload: payload}
			if err := protocol.WriteFrame(clientConn, f); err != nil {
				return
			}
			written.Add(1)
		}
	}()

	deadline := time.Now().Add(5 * time.Second)
	for len(conn.FrameQueue) != n && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("queued=%d written=%d budgetBytes=%d", len(conn.FrameQueue), written.Load(), conn.FrameQueueBytes.Load())
	require.Equal(t, n, len(conn.FrameQueue), "all small frames must be handed off without blocking")
	assert.Equal(t, int64(n)*frameCost(&protocol.Frame{Size: 100}), conn.FrameQueueBytes.Load())
}
