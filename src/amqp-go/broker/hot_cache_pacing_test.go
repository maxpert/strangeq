package broker

// TASK 2 — the broker paces the publisher to the hot cache's BYTE budget
// (design D-B), so the delivered head stays resident instead of being evicted
// thousands of messages before the FIFO consumer reaches it.
//
// The defect these tests fail on (before the fix): PublishMessageAsyncConfirm
// returns a DepthGate only when the queue reaches its message-count depth HWM
// (52,428 at shipped defaults), so a producer is allowed to run ~51,000
// messages ahead of the consumer and nearly every delivery is a cold WAL read.

import (
	"bytes"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/require"
)

// publishDurableAwait publishes one durable message and waits for its confirm,
// returning the backpressure gate the broker reported and the tag it minted.
func publishDurableAwait(t *testing.T, b *StorageBroker, queue string, body []byte) (protocol.DepthGate, uint64) {
	t.Helper()
	done := make(chan error, 1)
	msg := &protocol.Message{RoutingKey: queue, Body: body, DeliveryMode: 2}
	_, gate, err := b.PublishMessageAsyncConfirm("", queue, msg, func(e error) { done <- e }, protocol.PublishLeaseToken{})
	require.NoError(t, err)
	select {
	case e := <-done:
		require.NoError(t, e, "durable publish must complete")
	case <-time.After(10 * time.Second):
		t.Fatal("durable publish never completed")
	}
	return gate, msg.DeliveryTag
}

// TestPublisherPacedToHotCacheCapacity pins that the broker arms backpressure
// when the queue's hot cache is full, LONG before the message-count depth HWM.
//
// RED BEFORE THE FIX: the only gate was the depth HWM (set to 1,000,000 here),
// so the returned gate was always nil and `armed` was never set.
func TestPublisherPacedToHotCacheCapacity(t *testing.T) {
	const queue = "pace-q"
	body := bytes.Repeat([]byte("p"), 256)
	ec := interfaces.EngineConfig{
		// Room for a handful of 256-byte bodies — the point is that the gate
		// binds on BYTES, not on the 1,000,000-message depth HWM below.
		HotCacheQueueBytes: 3200,
		MaxQueueBacklog:    1_000_000,
		// fsync:false always wires the hot-cache pace gate. (Under the durable
		// crash models it is wired only for an ordering-only fence; see
		// TestHotCachePaceGateWiringByCrashModel.)
		WALSyncDisabled: true,
	}
	store, err := storage.NewStorageEngineWithConfig(t.TempDir(), ec)
	require.NoError(t, err)
	b := NewStorageBroker(store, ec)
	t.Cleanup(func() { b.Close(); _ = store.Close() })
	_, err = b.DeclareQueue(queue, true, false, false, nil)
	require.NoError(t, err)

	qs := b.getOrCreateQueueState(queue)
	require.Equal(t, uint64(1_000_000), qs.MaxQueueBacklog(),
		"fixture premise: the depth HWM is far above the hot-cache byte budget")

	tags := make([]uint64, 0, 8)
	var armed protocol.DepthGate
	for i := 0; i < 8; i++ {
		gate, tag := publishDurableAwait(t, b, queue, body)
		tags = append(tags, tag)
		if gate != nil && gate.AtHighWaterMark() {
			armed = gate
			break
		}
	}
	require.NotNil(t, armed,
		"the publisher must be paced by the hot-cache byte budget, not only by the depth HWM")
	require.Less(t, qs.Depth(), qs.MaxQueueBacklog(),
		"hot-cache pacing must bind well before the message-count depth HWM")
	require.LessOrEqual(t, len(tags), 8)

	// An acknowledgement frees exactly the room the next message needs, so the
	// gate clears and the paced producer resumes.
	require.NoError(t, store.DeleteMessage(queue, tags[0]))
	require.False(t, armed.AtHighWaterMark(),
		"settling a delivered message must clear the pacing gate")
}

// TestDeliveryHeadStaysHotUnderSyntheticBacklog builds a backlog deeper than the
// hot budget and runs a paced producer/consumer: publish until the gate arms,
// drain half, repeat. Every delivery of the drain must be served from memory.
//
// This is an integration-style test (real WAL, real hot cache, real broker
// publish path); it is deterministic — no sleeps, no timing assertions.
func TestDeliveryHeadStaysHotUnderSyntheticBacklog(t *testing.T) {
	const queue = "backlog-q"
	body := bytes.Repeat([]byte("b"), 256)
	ec := interfaces.EngineConfig{
		HotCacheQueueBytes: 50000, // ~63 bodies of ~786 bytes
		MaxQueueBacklog:    1_000_000,
		// fsync:false is the ONLY mode in which the hot-cache pace gate is
		// wired (durable mode leaves the publisher path unpaced).
		WALSyncDisabled: true,
	}
	store, err := storage.NewStorageEngineWithConfig(t.TempDir(), ec)
	require.NoError(t, err)
	b := NewStorageBroker(store, ec)
	t.Cleanup(func() { b.Close(); _ = store.Close() })
	_, err = b.DeclareQueue(queue, true, false, false, nil)
	require.NoError(t, err)
	qs := b.getOrCreateQueueState(queue)

	var pending []uint64
	for cycle := 0; cycle < 12; cycle++ {
		// Publish until the broker says the hot cache is full. The queue is
		// then AT LEAST the hot budget deep — the precondition for the defect.
		for {
			gate, tag := publishDurableAwait(t, b, queue, body)
			pending = append(pending, tag)
			if gate != nil && gate.AtHighWaterMark() {
				break
			}
			require.Less(t, len(pending), 256, "the gate must arm within the hot budget")
		}
		// Drain half of what is queued. Each delivery must be a hot-cache hit.
		for j := 0; j < len(pending)/2; j++ {
			tag := pending[0]
			pending = pending[1:]
			got, gerr := store.GetMessageForDelivery(queue, tag)
			require.NoError(t, gerr, "the delivered head must be readable")
			require.Equal(t, body, got.Body)
			require.NoError(t, store.DeleteMessage(queue, tag))
		}
	}

	hits, cold, _ := store.DeliveryCacheStats()
	require.Greater(t, hits+cold, uint64(0), "the fixture must have delivered messages")
	ratio := float64(hits) / float64(hits+cold)
	require.GreaterOrEqual(t, ratio, 0.90,
		"with the publisher paced to the hot-cache budget, at least 90%% of deliveries must be hot (got %.3f)",
		ratio)
	require.Less(t, qs.MaxBacklogSeen(), qs.MaxQueueBacklog(),
		"the backlog high-water must stay far below the depth HWM")
}
