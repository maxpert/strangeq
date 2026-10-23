package server

import (
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTx_Rollback_AckNotApplied (T5.3, §6): a tx ack takes the wire tag at
// BUFFER time, and rollback RESTORES it (undo) — the delivery stays unacked
// and the SAME wire tag can be acked again and committed. Pre-fix mechanism:
// Rollback dropped the op after TakeWireTag, so the re-ack was "unknown"
// (no-op) and the delivery stayed unacked until channel close.
func TestTx_Rollback_AckNotApplied(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	setupTxChannelAndQueue(t, srv, conn, frameCh, "tx-rollback-ack")

	publishMessageToQueue(t, srv, "tx-rollback-ack", "m1")
	wireTag := getDeliveryTag(t, srv, conn, frameCh, "tx-rollback-ack")
	msgID := resolveWireTagToMsgID(t, srv, conn, 1, wireTag)

	txSelect(t, srv, conn, frameCh, 1)
	require.NoError(t, srv.handleBasicAck(conn, 1, makeAckPayload(wireTag, false)))

	// One op buffered; the wire tag was TAKEN at buffer time (R14).
	ops, err := srv.txm(conn).GetPendingOperations(1)
	require.NoError(t, err)
	require.Len(t, ops, 1)
	ch := srv.wireChannel(conn, 1)
	_, ok := ch.ResolveWireTag(wireTag)
	assert.False(t, ok, "wire tag taken at buffer time (RabbitMQ removes from the unacked map at ack time)")

	txRollback(t, srv, conn, frameCh, 1)

	// m1 still unacked AND the wire tag is RESOLVED again (undo ran).
	_, tracked := srv.Broker.GetConsumerForDelivery(msgID)
	assert.True(t, tracked, "rolled-back ack never settles the delivery")
	ref, ok := ch.ResolveWireTag(wireTag)
	require.True(t, ok, "rollback restores the wire-tag entry (undo)")
	assert.Equal(t, msgID, ref.MsgID)

	// Ack the SAME wire tag again and commit: settles cleanly.
	require.NoError(t, srv.handleBasicAck(conn, 1, makeAckPayload(wireTag, false)))
	txCommit(t, srv, conn, frameCh, 1)
	_, tracked = srv.Broker.GetConsumerForDelivery(msgID)
	assert.False(t, tracked, "the re-acked delivery settles at commit")
}

// TestTx_Commit_AckAppliedOnce (T5.4): ack(1)+commit, then ack(1) again in a
// second tx → the second ack is unknown (the tag was consumed by the first
// tx), 406 PRECONDITION_FAILED, no op is buffered, and the broker settles
// EXACTLY once (WS-D §6, R14).
func TestTx_Commit_AckAppliedOnce(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	setupTxChannelAndQueue(t, srv, conn, frameCh, "tx-ack-once")

	publishMessageToQueue(t, srv, "tx-ack-once", "m1")
	wireTag := getDeliveryTag(t, srv, conn, frameCh, "tx-ack-once")
	msgID := resolveWireTagToMsgID(t, srv, conn, 1, wireTag)

	txSelect(t, srv, conn, frameCh, 1)
	require.NoError(t, srv.handleBasicAck(conn, 1, makeAckPayload(wireTag, false)))
	txCommit(t, srv, conn, frameCh, 1)
	_, tracked := srv.Broker.GetConsumerForDelivery(msgID)
	require.False(t, tracked, "first commit settles")

	// Second tx: ack the same wire tag — unknown → 406 PRECONDITION_FAILED,
	// nothing buffered.
	err := srv.handleBasicAck(conn, 1, makeAckPayload(wireTag, false))
	require.Error(t, err)
	var chErr *ChannelError
	require.ErrorAs(t, err, &chErr)
	assert.Equal(t, uint16(406), chErr.Code)

	// No op was buffered (the channel is closing, but the txm state is
	// queryable before the frame loop drives teardown).
	ops, _ := srv.txm(conn).GetPendingOperations(1)
	assert.Empty(t, ops, "a duplicate ack of a consumed wire tag buffers nothing (R14 parity)")

	// Exactly once: inflight is 0 (not negative / not re-settled).
	sb := srv.Broker.(*StorageBrokerAdapter)
	ready, inflight, _ := sb.broker.QueueCounters("tx-ack-once")
	assert.Equal(t, int64(0), ready)
	assert.Equal(t, int64(0), inflight, "the delivery settled exactly once")
}

// TestTx_DuplicateAckInSameTx_SecondUnknown (T5.5): two acks of the same tag
// in ONE tx — the second ack is unknown and raises 406 (WS-D §6, R14).
func TestTx_DuplicateAckInSameTx_SecondUnknown(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	setupTxChannelAndQueue(t, srv, conn, frameCh, "tx-dup-ack")

	publishMessageToQueue(t, srv, "tx-dup-ack", "m1")
	wireTag := getDeliveryTag(t, srv, conn, frameCh, "tx-dup-ack")

	txSelect(t, srv, conn, frameCh, 1)
	require.NoError(t, srv.handleBasicAck(conn, 1, makeAckPayload(wireTag, false)))

	// Second ack of the same wire tag → 406 PRECONDITION_FAILED.
	err := srv.handleBasicAck(conn, 1, makeAckPayload(wireTag, false))
	require.Error(t, err)
	var chErr *ChannelError
	require.ErrorAs(t, err, &chErr)
	assert.Equal(t, uint16(406), chErr.Code)

	// Exactly one settlement op was buffered (the first ack).
	ops, _ := srv.txm(conn).GetPendingOperations(1)
	assert.Len(t, ops, 1, "exactly one settlement op buffered (duplicate is unknown, R14 parity)")
}

// TestTx_CumulativeAck_MultipleConsumers (T6.1): a cumulative tx ack
// (multiple=true) covers ALL outstanding deliveries on the channel — including
// basic.get deliveries from DIFFERENT queues — one single-tag settlement each.
// Pre-fix mechanism: only the highest wire tag resolved and multiple=true was
// passed to one consumer's broker multi-ack; the rest were never settled.
func TestTx_CumulativeAck_MultipleConsumers(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	// Three queues, one get-delivery each on channel 1 (wire tags 1,2,3).
	for _, q := range []string{"cumA", "cumB", "cumC"} {
		setupTxChannelAndQueue(t, srv, conn, frameCh, q)
		publishMessageToQueue(t, srv, q, "m-"+q)
	}
	wt1 := getDeliveryTag(t, srv, conn, frameCh, "cumA")
	mid1 := resolveWireTagToMsgID(t, srv, conn, 1, wt1)
	wt2 := getDeliveryTag(t, srv, conn, frameCh, "cumB")
	mid2 := resolveWireTagToMsgID(t, srv, conn, 1, wt2)
	wt3 := getDeliveryTag(t, srv, conn, frameCh, "cumC")
	mid3 := resolveWireTagToMsgID(t, srv, conn, 1, wt3)

	txSelect(t, srv, conn, frameCh, 1)
	// Cumulative ack up to the highest wire tag.
	require.NoError(t, srv.handleBasicAck(conn, 1, makeAckPayload(wt3, true)))

	// Three single-tag settlement ops buffered (one per taken entry).
	ops, err := srv.txm(conn).GetPendingOperations(1)
	require.NoError(t, err)
	require.Len(t, ops, 3, "cumulative tx ack settles every outstanding delivery, one op each")

	txCommit(t, srv, conn, frameCh, 1)
	for _, mid := range []uint64{mid1, mid2, mid3} {
		_, tracked := srv.Broker.GetConsumerForDelivery(mid)
		assert.False(t, tracked, "msgID %d settled by the cumulative commit", mid)
	}
}

// TestTx_CumulativeAck_UnknownTopTag_406 (T5.6, §6 R14): a tx cumulative ack
// with multiple=true and an unknown top wire tag (N not outstanding) raises
// 406 PRECONDITION_FAILED and settles NOTHING — the lower outstanding
// deliveries stay untouched. tag=0 ("ack all") is the only exception and
// never errors.
func TestTx_CumulativeAck_UnknownTopTag_406(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	setupTxChannelAndQueue(t, srv, conn, frameCh, "tx-unk-cum")
	setupTxChannelAndQueue(t, srv, conn, frameCh, "tx-unk-cum-b")

	publishMessageToQueue(t, srv, "tx-unk-cum", "m1")
	publishMessageToQueue(t, srv, "tx-unk-cum-b", "m2")
	wt1 := getDeliveryTag(t, srv, conn, frameCh, "tx-unk-cum")
	mid1 := resolveWireTagToMsgID(t, srv, conn, 1, wt1)
	wt2 := getDeliveryTag(t, srv, conn, frameCh, "tx-unk-cum-b")
	mid2 := resolveWireTagToMsgID(t, srv, conn, 1, wt2)

	txSelect(t, srv, conn, frameCh, 1)

	// Cumulative ack with an unknown top tag (higher than any outstanding).
	unknownTag := wt2 + 100
	err := srv.handleBasicAck(conn, 1, makeAckPayload(unknownTag, true))
	require.Error(t, err)
	var chErr *ChannelError
	require.ErrorAs(t, err, &chErr)
	assert.Equal(t, uint16(406), chErr.Code)

	// Nothing was settled: both deliveries are still tracked by the broker.
	ch := srv.wireChannel(conn, 1)
	_, tracked1 := srv.Broker.GetConsumerForDelivery(mid1)
	assert.True(t, tracked1, "delivery 1 must still be tracked after 406 cumulative ack")
	_, tracked2 := srv.Broker.GetConsumerForDelivery(mid2)
	assert.True(t, tracked2, "delivery 2 must still be tracked after 406 cumulative ack")

	// Wire tags are still resolvable (never taken).
	_, ok1 := ch.ResolveWireTag(wt1)
	assert.True(t, ok1, "wire tag %d must still be resolvable", wt1)
	_, ok2 := ch.ResolveWireTag(wt2)
	assert.True(t, ok2, "wire tag %d must still be resolvable", wt2)

	// No op was buffered.
	ops, _ := srv.txm(conn).GetPendingOperations(1)
	assert.Empty(t, ops, "no settlement ops buffered after 406 cumulative ack")
}

// TestTx_CumulativeNack_Rollback_RestoresAllTags (T6.2): nack(multiple,
// requeue) then rollback → every taken wire tag is re-resolvable and NOTHING
// was requeued (the settlements never ran).
func TestTx_CumulativeNack_Rollback_RestoresAllTags(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	for _, q := range []string{"cnA", "cnB", "cnC"} {
		setupTxChannelAndQueue(t, srv, conn, frameCh, q)
		publishMessageToQueue(t, srv, q, "m-"+q)
	}
	wt1 := getDeliveryTag(t, srv, conn, frameCh, "cnA")
	wt2 := getDeliveryTag(t, srv, conn, frameCh, "cnB")
	wt3 := getDeliveryTag(t, srv, conn, frameCh, "cnC")

	txSelect(t, srv, conn, frameCh, 1)
	require.NoError(t, srv.handleBasicNack(conn, 1, makeNackPayload(wt3, true, true)))
	txRollback(t, srv, conn, frameCh, 1)

	ch := srv.wireChannel(conn, 1)
	for _, wt := range []uint64{wt1, wt2, wt3} {
		_, ok := ch.ResolveWireTag(wt)
		assert.True(t, ok, "wire tag %d restored after rollback", wt)
	}

	// Nothing was requeued: each queue still holds its message READY (the
	// delivery tracking was restored, not settled; requeue happens at commit).
	sb := srv.Broker.(*StorageBrokerAdapter)
	for _, q := range []string{"cnA", "cnB", "cnC"} {
		ready, inflight, _ := sb.broker.QueueCounters(q)
		assert.Equal(t, int64(0), ready, "%s: requeue never ran", q)
		assert.Equal(t, int64(1), inflight, "%s: deliveries still tracked", q)
	}
}

// TestTx_CommitFailure_RestoresWireTags (T6.3, §5.4): a FAILED commit runs the
// undos (wire tags restored) before the server closes the channel.
func TestTx_CommitFailure_RestoresWireTags(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	setupTxChannelAndQueue(t, srv, conn, frameCh, "cf-q")

	publishMessageToQueue(t, srv, "cf-q", "m1")
	wireTag := getDeliveryTag(t, srv, conn, frameCh, "cf-q")
	msgID := resolveWireTagToMsgID(t, srv, conn, 1, wireTag)

	// A publish target that exists NOW but dies before commit.
	declareX := &protocol.ExchangeDeclareMethod{Exchange: "doomed-x", Type: "direct"}
	xpayload, err := declareX.Serialize()
	require.NoError(t, err)
	require.NoError(t, srv.handleExchangeDeclare(conn, 1, xpayload))
	drainFrame(t, frameCh)

	txSelect(t, srv, conn, frameCh, 1)
	// Buffer the settlement (takes the wire tag).
	require.NoError(t, srv.handleBasicAck(conn, 1, makeAckPayload(wireTag, false)))
	// Buffer a publish to the doomed exchange (valid at buffer time).
	require.NoError(t, srv.processCompleteMessage(conn, 1, makePendingMessage("doomed-x", "k", "x")))

	// Kill the publish target: the commit's publish leg fails (errNoExchange).
	require.NoError(t, srv.Broker.DeleteExchange("doomed-x", false))

	// Commit fails → handleTxCommit reports a channel.close (see i22) and the
	// undos ran. WS-B contract: the handler RETURNS the typed *ChannelError
	// (close already on the wire, sent=true) instead of nil.
	err = srv.handleTxCommit(conn, 1, []byte{})
	require.Error(t, err, "handleTxCommit returns the commit-failure ChannelError (WS-B contract)")
	var chErr *ChannelError
	require.ErrorAs(t, err, &chErr)
	assert.Equal(t, uint16(541), chErr.Code)
	classID, methodID, _ := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(20), classID)
	assert.Equal(t, uint16(40), methodID, "commit failure closes the channel")

	ch := srv.wireChannel(conn, 1)
	ref, ok := ch.ResolveWireTag(wireTag)
	require.True(t, ok, "the failed commit restored the wire tag (undo ran)")
	assert.Equal(t, msgID, ref.MsgID)

	// The settlement never reached the broker.
	_, tracked := srv.Broker.GetConsumerForDelivery(msgID)
	assert.True(t, tracked, "settlement not applied on a failed commit")
}
