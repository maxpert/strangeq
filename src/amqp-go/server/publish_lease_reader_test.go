package server

import (
	"net"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SL-21 (Opus): while a header waits on the lease (owed == 0, no immediate
// grant), the reader must STILL hand buffered (pending) frames to the
// processor when the frame budget allows — the lease wait must not starve the
// SQ-13 hand-off — and frame/lease FIFO ordering must be preserved.
func TestLease_PendingHandoffDuringLeaseWait(t *testing.T) {
	srv := newTransactionTestServer(t)
	srv.Config.Network.PublishInflightBudgetBytes = 2000
	// m1+h1+b1 ≈ 1016 bytes of frame cost; +m2 (≈74) exceeds 1050 → m2 waits
	// in the reader's pending buffer while h2 waits on the lease.
	srv.frameQueueByteBudgetOverride = 1050

	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() { _ = clientConn.Close() })
	conn := protocol.NewConnection(serverConn)
	rt := srv.runtimeFor(conn)
	done := make(chan struct{})
	defer stopReader(conn, done)
	go srv.readFrames(rt, done)

	w := func(f *protocol.Frame) { require.NoError(t, protocol.WriteFrame(clientConn, f)) }
	method := func() *protocol.Frame {
		pm := &protocol.BasicPublishMethod{Exchange: "", RoutingKey: "q"}
		data, _ := pm.Serialize()
		return protocol.EncodeMethodFrameForChannel(1, 60, 40, data)
	}
	header := func(body uint64) *protocol.Frame {
		h := &protocol.ContentHeader{ClassID: 60, BodySize: body}
		hp, _ := h.Serialize()
		return &protocol.Frame{Type: protocol.FrameHeader, Channel: 1, Size: uint32(len(hp)), Payload: hp}
	}

	// msg1: method + header(800) + body(800): lease 1384 of 2000 granted.
	w(method())
	w(header(800))
	w(&protocol.Frame{Type: protocol.FrameBody, Channel: 1, Size: 800, Payload: make([]byte, 800)})
	require.Eventually(t, func() bool { return leaseUsed(srv) == 1384 }, 30*time.Second, time.Millisecond)

	// msg2: the method frame no longer fits the frame budget (goes to the
	// reader's pending buffer); the header's Begin waits (1384+1384 > 2000).
	w(method())
	w(header(800))
	require.Eventually(t, func() bool { return leaseWaiters(srv) == 1 }, 30*time.Second, time.Millisecond)

	// The queue holds [m1, h1, b1]; m2 sits in the reader's pending buffer.
	// Drain m1: the WAITING reader must hand m2 off (the wedge the fix
	// removes: a Reserve-blocked reader could never do this).
	drain := func() *protocol.Frame {
		t.Helper()
		select {
		case f := <-conn.FrameQueue:
			srv.releaseFrameBudget(conn, frameCost(f))
			return f
		case <-time.After(30 * time.Second):
			t.Fatal("frame missing from the queue")
			return nil
		}
	}

	m1 := drain() // m1
	drain()       // h1
	drain()       // b1
	// m2 was handed off DURING the lease wait (still waiting: not granted).
	m2 := drain()
	assert.Equal(t, byte(protocol.FrameMethod), m2.Type, "m2 must be handed off during the lease wait")
	protocol.PutFrame(m2)
	protocol.PutFrame(m1)
	assert.Equal(t, 1, leaseWaiters(srv), "the lease wait continues through the hand-off")

	// Release msg1's lease → the wait grants → the header is enqueued LAST
	// (ordering preserved).
	tok := conn.LeaseFIFO.Pop()
	require.True(t, tok.Valid())
	tok.Abort()
	h2 := drain()
	assert.Equal(t, byte(protocol.FrameHeader), h2.Type, "h2 enqueued after m2 (order preserved)")
	protocol.PutFrame(h2)
	tok2 := conn.LeaseFIFO.Pop()
	require.True(t, tok2.Valid())
	tok2.Abort()
}

// Item 6 (Sol): a zero-length body frame is NOT progress: it neither
// decrements nor refreshes the owed deadline — a client streaming zero-length
// body frames is still closed at the owed deadline.
func TestLease_ZeroLengthBodyFrameIsNotProgress(t *testing.T) {
	srv, addr, created, released, probe := leaseServer(t, 0, nil, func(srv *Server) {
		srv.writeTimeoutOverride = 200 * time.Millisecond
	})

	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 4, 1)
	r.openChannel(1)
	r.declareQueue(1, "sl6z-q")
	leaseConfirmSelect(t, r, 1)

	leaseSendMethod(t, r, 1, "", "sl6z-q", false)
	leaseSendHeader(t, r, 1, 100<<10, true, "")
	require.Eventually(t, func() bool { return created.Load() == 1 }, 30*time.Second, time.Millisecond)

	// Stream zero-length body frames: never progress.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		f := &protocol.Frame{Type: protocol.FrameBody, Channel: 1, Size: 0, Payload: []byte{}}
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := protocol.WriteFrame(r.c, f); err != nil {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	r.expectEOF() // closed at the owed deadline (wait on the event, not on time)
	require.Eventually(t, func() bool { return probe.bodyStallCloses.Load() == 1 }, 30*time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		return released.Load() == created.Load() && leaseUsed(srv) == 0
	}, 30*time.Second, time.Millisecond)
}

// Item 8: channel.close mid-body clears the owed entry — the connection must
// NOT be body-stall-closed afterwards.
func TestLease_ChannelCloseMidBodyClearsOwed(t *testing.T) {
	srv, addr, created, released, probe := leaseServer(t, 0, nil, func(srv *Server) {
		srv.writeTimeoutOverride = 200 * time.Millisecond
	})

	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 4, 1)
	r.openChannel(1)
	r.declareQueue(1, "sl8c-q")

	leaseSendMethod(t, r, 1, "", "sl8c-q", false)
	leaseSendHeader(t, r, 1, 100<<10, true, "")
	leaseSendBody(t, r, 1, leaseBody(100)) // partial: 100 of 100 KiB
	require.Eventually(t, func() bool { return created.Load() == 1 }, 30*time.Second, time.Millisecond)

	// Client closes the channel mid-body.
	r.send(1, 20, 40, mustSerialize(&protocol.ChannelCloseMethod{ReplyCode: 200, ReplyText: "bye", ClassID: 60, MethodID: 40}))
	r.expectMethodCh(1, 20, 41) // channel.close-ok

	// The connection must survive well beyond the owed deadline.
	assert.Never(t, func() bool { return probe.bodyStallCloses.Load() > 0 }, 500*time.Millisecond, 50*time.Millisecond,
		"a channel-closed body is no longer owed — no body-stall close")

	// And the connection is still fully usable.
	r.openChannel(2)
	r.declareQueue(2, "sl8c-q2")
	require.Eventually(t, func() bool {
		return released.Load() == created.Load() && leaseUsed(srv) == 0
	}, 30*time.Second, time.Millisecond)
}

// Item 8: a DISABLED limiter (budget −1) skips owed tracking entirely — the
// pre-WS-I baseline: a header-only stall is never closed by the lease layer.
func TestLease_DisabledLimiterSkipsOwedTracking(t *testing.T) {
	srv, addr, created, _, probe := leaseServer(t, -1, nil, func(srv *Server) {
		srv.writeTimeoutOverride = 200 * time.Millisecond
	})

	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 4, 1)
	r.openChannel(1)
	r.declareQueue(1, "sl8d-q")

	leaseSendMethod(t, r, 1, "", "sl8d-q", false)
	leaseSendHeader(t, r, 1, 100<<10, true, "")

	// With the limiter disabled, no lease is tracked and nothing is owed: the
	// connection stays open (no body-stall close) and nothing waits.
	assert.Never(t, func() bool {
		return probe.bodyStallCloses.Load() > 0 || leaseWaiters(srv) > 0
	}, 500*time.Millisecond, 50*time.Millisecond)
	assert.Equal(t, int64(0), created.Load(), "a disabled limiter never creates leases")
}
