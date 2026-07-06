package server

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// SQ-5: publisher-confirm batching.
//
// When a pipelining publisher keeps the connection's FrameQueue non-empty, the
// per-publish durability completion must NOT write one basic.ack per publish.
// Instead it advances a lock-free per-channel watermark and pokes the
// per-connection flusher, which coalesces everything pending into a single
// basic.ack with multiple=true. When the processor is idle (FrameQueue empty)
// the confirm is flushed inline so a lone publish is confirmed promptly.
// ============================================================================

// sq5Setup declares a queue and puts channel 1 into confirm mode.
func sq5Setup(t *testing.T, srv *Server, conn *protocol.Connection, frameCh chan *protocol.Frame, queue string) *protocol.Channel {
	t.Helper()
	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	declPayload, err := (&protocol.QueueDeclareMethod{Queue: queue}).Serialize()
	require.NoError(t, err)
	require.NoError(t, srv.handleQueueDeclare(conn, 1, declPayload))
	drainFrame(t, frameCh) // queue.declare-ok

	confirmPayload, err := (&protocol.ConfirmSelectMethod{NoWait: false}).Serialize()
	require.NoError(t, err)
	require.NoError(t, srv.handleConfirmSelect(conn, 1, confirmPayload))
	drainFrame(t, frameCh) // confirm.select-ok

	return ch
}

// expectNoFrame asserts no frame is waiting on frameCh.
func expectNoFrame(t *testing.T, frameCh chan *protocol.Frame, msg string) {
	t.Helper()
	select {
	case frame := <-frameCh:
		t.Fatalf("%s: unexpected frame type=%d payload=%v", msg, frame.Type, frame.Payload)
	default:
	}
}

// decodeAck asserts the frame stream's next method frame is basic.ack and
// returns (deliveryTag, multiple).
func decodeAck(t *testing.T, frameCh chan *protocol.Frame) (uint64, bool) {
	t.Helper()
	classID, methodID, payload := nextMethodFrame(t, frameCh)
	require.Equal(t, uint16(60), classID, "expected class basic")
	require.Equal(t, uint16(80), methodID, "expected method ack")
	ack := &protocol.BasicAckMethod{}
	require.NoError(t, ack.Deserialize(payload))
	return ack.DeliveryTag, ack.Multiple
}

// occupyFrameQueue parks a heartbeat frame in the connection's FrameQueue to
// simulate a pipelining publisher (more inbound work already queued for the
// processor). Returns a drain func.
func occupyFrameQueue(t *testing.T, conn *protocol.Connection) func() {
	t.Helper()
	conn.FrameQueue <- &protocol.Frame{Type: protocol.FrameHeartbeat}
	return func() { <-conn.FrameQueue }
}

func TestSQ5PipelinedPublishesCoalesceIntoSingleMultipleAck(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	sq5Setup(t, srv, conn, frameCh, "sq5-batch-q")

	// Simulate a pipelining publisher: FrameQueue non-empty while publishes
	// complete, so no inline per-publish acks may be written.
	drain := occupyFrameQueue(t, conn)

	const n = 5
	for i := 0; i < n; i++ {
		require.NoError(t, srv.processCompleteMessage(conn, 1, makePendingMessage("", "sq5-batch-q", "msg")))
	}

	expectNoFrame(t, frameCh, "no ack may be written per publish while pipelined")
	drain()

	// Start the flusher; the coalesced poke from the publishes above must make
	// it emit exactly ONE basic.ack covering all five tags.
	done := make(chan struct{})
	go srv.confirmFlusher(conn, done)
	defer func() { <-done }()
	defer close(conn.Done)

	tag, multiple := decodeAck(t, frameCh)
	assert.Equal(t, uint64(n), tag, "single ack must carry the highest contiguous tag")
	assert.True(t, multiple, "batched ack must set multiple=true")

	// Nothing else pending.
	time.Sleep(50 * time.Millisecond)
	expectNoFrame(t, frameCh, "exactly one ack for the whole batch")
}

func TestSQ5IdleProcessorFlushesConfirmInlinePromptly(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	sq5Setup(t, srv, conn, frameCh, "sq5-idle-q")

	// No flusher goroutine running at all: with an empty FrameQueue a lone
	// publish must still be confirmed, inline, without waiting on any timer.
	require.NoError(t, srv.processCompleteMessage(conn, 1, makePendingMessage("", "sq5-idle-q", "solo")))

	tag, multiple := decodeAck(t, frameCh)
	assert.Equal(t, uint64(1), tag)
	assert.False(t, multiple, "a single confirm must not claim multiple")
}

func TestSQ5MandatoryUnroutableReturnPrecedesBatchedAck(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	sq5Setup(t, srv, conn, frameCh, "sq5-return-q")

	drain := occupyFrameQueue(t, conn)

	// Tag 1: routable. Tag 2: mandatory unroutable (basic.return). Tag 3: routable.
	require.NoError(t, srv.processCompleteMessage(conn, 1, makePendingMessage("", "sq5-return-q", "one")))
	unroutable := &protocol.PendingMessage{
		Method: &protocol.BasicPublishMethod{
			Exchange:   "",
			RoutingKey: "sq5-no-such-queue",
			Mandatory:  true,
		},
		Header:   &protocol.ContentHeader{ClassID: 60, BodySize: 4},
		Body:     []byte("body"),
		Received: 4,
	}
	require.NoError(t, srv.processCompleteMessage(conn, 1, unroutable))
	require.NoError(t, srv.processCompleteMessage(conn, 1, makePendingMessage("", "sq5-return-q", "three")))

	// The basic.return for tag 2 must already be on the wire (sent inline by
	// the processor), while NO ack has been written yet.
	frame := nextFrame(t, frameCh)
	require.Equal(t, byte(protocol.FrameMethod), frame.Type)
	classID := binary.BigEndian.Uint16(frame.Payload[0:2])
	methodID := binary.BigEndian.Uint16(frame.Payload[2:4])
	assert.Equal(t, uint16(60), classID)
	assert.Equal(t, uint16(50), methodID, "expected basic.return before any ack")
	require.Equal(t, byte(protocol.FrameHeader), nextFrame(t, frameCh).Type)
	require.Equal(t, byte(protocol.FrameBody), nextFrame(t, frameCh).Type)
	expectNoFrame(t, frameCh, "no ack may precede or interleave the basic.return while pipelined")

	drain()

	done := make(chan struct{})
	go srv.confirmFlusher(conn, done)
	defer func() { <-done }()
	defer close(conn.Done)

	tag, multiple := decodeAck(t, frameCh)
	assert.Equal(t, uint64(3), tag, "batched ack covers routable and returned tags")
	assert.True(t, multiple)
}

func TestSQ5ChannelCloseFlushesPendingConfirmsBeforeCloseOK(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	ch := sq5Setup(t, srv, conn, frameCh, "sq5-close-q")

	drain := occupyFrameQueue(t, conn)
	const n = 3
	for i := 0; i < n; i++ {
		require.NoError(t, srv.processCompleteMessage(conn, 1, makePendingMessage("", "sq5-close-q", "msg")))
	}
	expectNoFrame(t, frameCh, "no ack before channel.close while pipelined")
	drain()

	// channel.close arrives before any flusher pass ran: the close handler must
	// flush the outstanding confirms, THEN send channel.close-ok.
	require.NoError(t, srv.processChannelSpecificMethod(conn, 1, protocol.ChannelClose, []byte{}))

	tag, multiple := decodeAck(t, frameCh)
	assert.Equal(t, uint64(n), tag)
	assert.True(t, multiple)

	classID, methodID, _ := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(20), classID)
	assert.Equal(t, uint16(41), methodID, "channel.close-ok must follow the confirm flush")

	// After close, the confirm state is sealed: further flush attempts are no-ops.
	ch.AdvanceConfirmDurable(uint64(n + 1))
	require.NoError(t, ch.FlushConfirms(func(uint64, bool) error {
		t.Fatal("flush after close must not emit an ack")
		return nil
	}))
}

// ============================================================================
// Watermark primitive semantics (protocol.Channel).
// ============================================================================

func TestSQ5WatermarkPrimitives(t *testing.T) {
	ch := protocol.NewChannel(1, nil)

	assert.False(t, ch.HasUnflushedConfirms())

	ch.AdvanceConfirmDurable(3)
	ch.AdvanceConfirmDurable(2) // must never regress
	assert.True(t, ch.HasUnflushedConfirms())

	var gotTag uint64
	var gotMultiple bool
	require.NoError(t, ch.FlushConfirms(func(tag uint64, multiple bool) error {
		gotTag, gotMultiple = tag, multiple
		return nil
	}))
	assert.Equal(t, uint64(3), gotTag)
	assert.True(t, gotMultiple, "3 unacked tags flush as multiple")
	assert.False(t, ch.HasUnflushedConfirms())

	// Single pending tag flushes with multiple=false.
	ch.AdvanceConfirmDurable(4)
	require.NoError(t, ch.FlushConfirms(func(tag uint64, multiple bool) error {
		gotTag, gotMultiple = tag, multiple
		return nil
	}))
	assert.Equal(t, uint64(4), gotTag)
	assert.False(t, gotMultiple)

	// Nothing pending: send must not be called.
	require.NoError(t, ch.FlushConfirms(func(uint64, bool) error {
		t.Fatal("send called with nothing pending")
		return nil
	}))

	// A failed send leaves the tags pending (retryable).
	ch.AdvanceConfirmDurable(6)
	sendErr := assert.AnError
	require.Error(t, ch.FlushConfirms(func(uint64, bool) error { return sendErr }))
	assert.True(t, ch.HasUnflushedConfirms())
}
