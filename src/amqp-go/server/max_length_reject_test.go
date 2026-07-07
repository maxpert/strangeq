package server

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// assertNoFrame fails if any frame arrives within a short window. net.Pipe is
// synchronous, so a frame written by the handler is already read by the pipe
// reader by the time the handler returns; the small wait closes the reader
// goroutine's ReadFrame->channel-send gap so an erroneous frame is caught
// reliably rather than flakily.
func assertNoFrame(t *testing.T, frameCh chan *protocol.Frame) {
	t.Helper()
	select {
	case frame := <-frameCh:
		t.Fatalf("expected no frame, got type %d payload %x", frame.Type, frame.Payload)
	case <-time.After(100 * time.Millisecond):
	}
}

// SQ-11 (W5) server-layer wiring: when the broker refuses a publish because a
// target queue is at/over its x-max-length / x-max-length-bytes limit under
// x-overflow=reject-publish (broker.ErrMaxLengthExceeded), processCompleteMessage
// must signal the publisher correctly — confirm-nack, mandatory basic.return, or
// a silent drop. The contiguous-watermark interlock proper is unit-tested in
// protocol/confirm_nack_test.go; these tests verify the end-to-end frame wiring
// through the real publish handler.

// declareRejectPublishQueue declares a non-durable queue whose policy rejects
// publishes once it holds x-max-length messages. Declared straight on the broker
// (setting the QueueState policy) so the field-table round-trip is out of scope;
// the publish path under test is unchanged.
func declareRejectPublishQueue(t *testing.T, srv *Server, name string, maxLen int64) {
	t.Helper()
	_, err := srv.Broker.DeclareQueue(name, false, false, false, map[string]interface{}{
		"x-max-length": maxLen,
		"x-overflow":   "reject-publish",
	})
	require.NoError(t, err)
}

// confirmSelectOnChannel puts channel 1 into confirm mode, draining the
// confirm.select-ok response.
func confirmSelectOnChannel(t *testing.T, srv *Server, conn *protocol.Connection, frameCh chan *protocol.Frame) {
	t.Helper()
	payload, err := (&protocol.ConfirmSelectMethod{NoWait: false}).Serialize()
	require.NoError(t, err)
	require.NoError(t, srv.handleConfirmSelect(conn, 1, payload))
	drainFrame(t, frameCh)
}

// mandatoryPending builds a mandatory basic.publish PendingMessage to the
// default exchange (routing key == queue).
func mandatoryPending(queue, body string) *protocol.PendingMessage {
	return &protocol.PendingMessage{
		Method: &protocol.BasicPublishMethod{
			Exchange:   "",
			RoutingKey: queue,
			Mandatory:  true,
		},
		Header:   &protocol.ContentHeader{ClassID: 60, BodySize: uint64(len(body))},
		Body:     []byte(body),
		Received: uint64(len(body)),
	}
}

// TestRejectPublishConfirmModeSendsNack verifies that on a confirm-mode channel
// a reject-publish refusal emits basic.nack(tag, multiple=false, requeue=false)
// for the refused tag — and that the prior accepted publish was acked normally.
func TestRejectPublishConfirmModeSendsNack(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)
	declareRejectPublishQueue(t, srv, "rp-confirm-q", 1)
	confirmSelectOnChannel(t, srv, conn, frameCh)

	// Publish 1: accepted (empty queue) -> ack(1).
	require.NoError(t, srv.processCompleteMessage(conn, 1, makePendingMessage("", "rp-confirm-q", "m0")))
	classID, methodID, ackPayload := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(60), classID)
	assert.Equal(t, uint16(80), methodID, "first accepted publish must be acked")
	ack := &protocol.BasicAckMethod{}
	require.NoError(t, ack.Deserialize(ackPayload))
	assert.Equal(t, uint64(1), ack.DeliveryTag)

	// Publish 2: queue at the count limit -> reject-publish -> nack(2).
	require.NoError(t, srv.processCompleteMessage(conn, 1, makePendingMessage("", "rp-confirm-q", "m1")))
	classID, methodID, nackPayload := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(60), classID)
	assert.Equal(t, uint16(120), methodID, "rejected publish must be nacked (basic.nack)")
	nack := &protocol.BasicNackMethod{}
	require.NoError(t, nack.Deserialize(nackPayload))
	assert.Equal(t, uint64(2), nack.DeliveryTag)
	assert.False(t, nack.Multiple, "reject-publish nack settles only its own tag")
	assert.False(t, nack.Requeue, "the refused message was never enqueued")
}

// TestRejectPublishConfirmInterlockNoAckCoversNackedTag is the end-to-end
// interlock assertion through the real server: after a reject-publish nack for
// tag N on a confirm channel, a later successful publish's ack must never
// re-confirm N. Two queues share the channel — one full (reject-publish), one
// open — so a post-nack accepted publish produces an ack whose tag is > N.
func TestRejectPublishConfirmInterlockNoAckCoversNackedTag(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)
	declareRejectPublishQueue(t, srv, "rp-full-q", 1)
	_, err := srv.Broker.DeclareQueue("rp-open-q", false, false, false, nil)
	require.NoError(t, err)
	confirmSelectOnChannel(t, srv, conn, frameCh)

	// tag 1 -> open queue (ack), tag 2 -> full queue empty (ack),
	// tag 3 -> full queue at limit (NACK), tag 4 -> open queue (ack).
	require.NoError(t, srv.processCompleteMessage(conn, 1, makePendingMessage("", "rp-open-q", "a")))
	require.NoError(t, srv.processCompleteMessage(conn, 1, makePendingMessage("", "rp-full-q", "b")))
	require.NoError(t, srv.processCompleteMessage(conn, 1, makePendingMessage("", "rp-full-q", "c")))
	require.NoError(t, srv.processCompleteMessage(conn, 1, makePendingMessage("", "rp-open-q", "d")))

	// Collect the four settlement frames and assert the interlock invariant.
	nackTags := map[uint64]int{}
	var ackTags []uint64
	for i := 0; i < 4; i++ {
		classID, methodID, payload := nextMethodFrame(t, frameCh)
		require.Equal(t, uint16(60), classID)
		switch methodID {
		case 80: // basic.ack
			m := &protocol.BasicAckMethod{}
			require.NoError(t, m.Deserialize(payload))
			ackTags = append(ackTags, m.DeliveryTag)
		case 120: // basic.nack
			m := &protocol.BasicNackMethod{}
			require.NoError(t, m.Deserialize(payload))
			nackTags[m.DeliveryTag]++
		default:
			t.Fatalf("unexpected method %d among confirm settlements", methodID)
		}
	}

	require.Equal(t, map[uint64]int{3: 1}, nackTags, "exactly one nack, for the refused tag 3")
	for _, tag := range ackTags {
		require.NotEqual(t, uint64(3), tag, "no ack may re-confirm the nacked tag 3")
	}
	require.Contains(t, ackTags, uint64(4), "the post-nack accepted publish must be acked at its own tag")
}

// TestRejectPublishMandatoryNonConfirmSendsReturn verifies that on a
// non-confirm channel a mandatory publish refused by reject-publish is returned
// to the publisher via basic.return (StrangeQ extension for publisher feedback
// without confirms).
func TestRejectPublishMandatoryNonConfirmSendsReturn(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)
	declareRejectPublishQueue(t, srv, "rp-mand-q", 1)

	// Fill the queue (accepted, no frame on a non-confirm channel).
	require.NoError(t, srv.processCompleteMessage(conn, 1, makePendingMessage("", "rp-mand-q", "keep")))
	assertNoFrame(t, frameCh)

	// Mandatory publish over the limit -> basic.return (60,50) + header + body.
	require.NoError(t, srv.processCompleteMessage(conn, 1, mandatoryPending("rp-mand-q", "returned")))

	frame := nextFrame(t, frameCh)
	require.Equal(t, byte(protocol.FrameMethod), frame.Type)
	assert.Equal(t, uint16(60), binary.BigEndian.Uint16(frame.Payload[0:2]))
	assert.Equal(t, uint16(50), binary.BigEndian.Uint16(frame.Payload[2:4]), "reject-publish + mandatory must basic.return")

	frame = nextFrame(t, frameCh)
	assert.Equal(t, byte(protocol.FrameHeader), frame.Type)
	frame = nextFrame(t, frameCh)
	assert.Equal(t, byte(protocol.FrameBody), frame.Type)
	assert.Equal(t, []byte("returned"), frame.Payload)
}

// TestRejectPublishNonConfirmNonMandatorySilentlyDrops verifies the RabbitMQ
// parity case: a reject-publish refusal on a plain channel (no confirms, not
// mandatory) drops silently — no frame is emitted and the handler returns nil.
func TestRejectPublishNonConfirmNonMandatorySilentlyDrops(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)
	declareRejectPublishQueue(t, srv, "rp-drop-q", 1)

	require.NoError(t, srv.processCompleteMessage(conn, 1, makePendingMessage("", "rp-drop-q", "keep")))
	assertNoFrame(t, frameCh)

	require.NoError(t, srv.processCompleteMessage(conn, 1, makePendingMessage("", "rp-drop-q", "dropped")))
	assertNoFrame(t, frameCh)
}

// TestRejectPublishConfirmSupersedesMandatory documents the composition
// decision: on a channel that is BOTH in confirm mode AND publishing mandatory,
// a reject-publish refusal emits the basic.nack and NOT a basic.return (the
// nack is the definitive, RabbitMQ-faithful signal).
func TestRejectPublishConfirmSupersedesMandatory(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)
	declareRejectPublishQueue(t, srv, "rp-both-q", 1)
	confirmSelectOnChannel(t, srv, conn, frameCh)

	// tag 1 accepted -> ack(1).
	require.NoError(t, srv.processCompleteMessage(conn, 1, makePendingMessage("", "rp-both-q", "keep")))
	_, methodID, _ := nextMethodFrame(t, frameCh)
	require.Equal(t, uint16(80), methodID)

	// tag 2 mandatory + over limit -> nack(2), never a basic.return.
	require.NoError(t, srv.processCompleteMessage(conn, 1, mandatoryPending("rp-both-q", "rejected")))
	classID, methodID, payload := nextMethodFrame(t, frameCh)
	require.Equal(t, uint16(60), classID)
	require.Equal(t, uint16(120), methodID, "confirm nack supersedes mandatory basic.return")
	nack := &protocol.BasicNackMethod{}
	require.NoError(t, nack.Deserialize(payload))
	assert.Equal(t, uint64(2), nack.DeliveryTag)
	assertNoFrame(t, frameCh)
}
