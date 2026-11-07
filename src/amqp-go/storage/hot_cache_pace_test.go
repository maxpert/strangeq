package storage

// TASK 2 — the publisher is paced to the hot cache's BYTE budget.
//
// The defect: the per-queue hot cache holds 64 MiB (~1,015 64 KiB bodies) while
// the queue's message-count depth high-water mark is RingBufferSize ×
// SpillThresholdPercent = 52,428 messages. The publisher is therefore allowed
// to run ~51,000 messages ahead of the FIFO consumer; the delivered head is
// evicted from RAM long before it is read, and ~96 % of deliveries take a cold
// WAL pread.
//
// The fix repurposes the existing reader backpressure gate: the storage layer
// answers "would the queue's hot cache refuse one more message this size?" and
// the broker turns that answer into reader backpressure. These tests pin the
// storage half — the gate must arm EXACTLY at the byte budget, must clear the
// instant an acknowledgement frees room, and must never arm for a message it
// could never admit.

import (
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// newPaceStorage builds a storage with a per-queue hot budget of exactly
// budgetBytes. The WAL and segment tiers are real (the storage is fully
// constructed) but these tests never touch them.
func newPaceStorage(t *testing.T, budgetBytes int64) *DisruptorStorage {
	t.Helper()
	ds, err := NewDisruptorStorageWithEngineConfig(t.TempDir(), interfaces.EngineConfig{
		HotCacheQueueBytes: budgetBytes,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })
	return ds
}

func paceMsg(tag uint64) *protocol.Message {
	return &protocol.Message{
		Exchange:     "amq.direct",
		RoutingKey:   "pace-q",
		Body:         []byte("0123456789abcdef"),
		DeliveryMode: 2,
		DeliveryTag:  tag,
	}
}

// TestHotCachePaceGate_ArmsExactlyAtTheByteBudget is the core D-B assertion:
// while the queue has room for one more message the gate is nil; the moment the
// budget is spent the gate arms; and an acknowledgement that frees one entry
// clears it again.
func TestHotCachePaceGate_ArmsExactlyAtTheByteBudget(t *testing.T) {
	const queue = "pace-q"
	charge := cacheCharge(queue, paceMsg(1))
	require.Greater(t, charge, int64(0))
	const capacity = 3

	ds := newPaceStorage(t, capacity*charge)
	ds.hot.queueState(queue) // create the queue's state, as a real publish would

	// Room for all three: the gate must not arm.
	for tag := uint64(1); tag <= capacity; tag++ {
		require.Nil(t, ds.HotCachePaceGate(queue, paceMsg(tag)),
			"the gate must not arm while the queue has byte room (before admitting tag %d)", tag)
		require.True(t, ds.hot.Put(queue, paceMsg(tag), true), "admit tag %d", tag)
	}
	require.Equal(t, int64(capacity*charge), ds.HotCacheResidentBytes(queue),
		"fixture premise: the queue must be exactly full")

	// Full: the NEXT message of the same size does not fit.
	gate := ds.HotCachePaceGate(queue, paceMsg(99))
	require.NotNil(t, gate, "a full queue must arm the pacing gate")
	require.True(t, gate.AtHighWaterMark())

	// An acknowledgement frees one entry: the gate must clear.
	require.True(t, ds.hot.Delete(queue, 1))
	require.Nil(t, ds.HotCachePaceGate(queue, paceMsg(99)),
		"an acknowledgement that frees the room the message needs must clear the gate")

	// And filling the freed slot re-arms it.
	require.True(t, ds.hot.Put(queue, paceMsg(100), true))
	require.NotNil(t, ds.HotCachePaceGate(queue, paceMsg(101)),
		"refilling the freed slot must re-arm the gate")
}

// TestHotCachePaceGate_NeverArmsForAnOversizedMessage pins the documented
// "oversized durable publish succeeds disk-only" path: a message larger than
// the WHOLE per-queue budget can never be admitted, so arming a gate on it
// would park the producer forever. The gate must stay nil.
func TestHotCachePaceGate_NeverArmsForAnOversizedMessage(t *testing.T) {
	const queue = "pace-q"
	huge := &protocol.Message{Body: make([]byte, 4096), DeliveryTag: 1}
	charge := cacheCharge(queue, huge)

	ds := newPaceStorage(t, charge-1) // smaller than one message
	ds.hot.queueState(queue)

	require.Nil(t, ds.HotCachePaceGate(queue, huge),
		"a message that can never fit must not arm a gate that can never clear")
}

// TestHotCachePaceGate_ReadsLiveAccounting proves the gate is not a stale
// snapshot: it reflects the queue's accounting mirror at every call, so it
// clears the instant the consumer settles a message and re-arms when another is
// admitted.
func TestHotCachePaceGate_ReadsLiveAccounting(t *testing.T) {
	const queue = "pace-q"
	const capacity = 2
	charge := cacheCharge(queue, paceMsg(1))
	ds := newPaceStorage(t, capacity*charge)
	ds.hot.queueState(queue)

	require.True(t, ds.hot.Put(queue, paceMsg(1), true))
	require.Nil(t, ds.HotCachePaceGate(queue, paceMsg(2)), "one of two slots used: room")

	require.True(t, ds.hot.Put(queue, paceMsg(2), true))
	require.NotNil(t, ds.HotCachePaceGate(queue, paceMsg(3)), "both slots used: no room")

	require.True(t, ds.hot.Delete(queue, 2))
	require.Nil(t, ds.HotCachePaceGate(queue, paceMsg(3)), "one settled: room again")
}
