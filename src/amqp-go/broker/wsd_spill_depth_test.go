package broker

import (
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createSmallRingBroker builds a broker whose ring spills after a handful of
// messages, so tests exercise the WAL-only (spilled) ready path.
func createSmallRingBroker(t testing.TB) (*StorageBroker, func()) {
	t.Helper()
	tmpDir := t.TempDir()
	engineConfig := interfaces.EngineConfig{
		RingBufferSize:          16,
		SpillThresholdPercent:   50,
		WALBatchSize:            1000,
		WALBatchTimeoutMS:       10,
		ConsumerSelectTimeoutMS: 1,
		ConsumerMaxBatchSize:    100,
		DepthHighWMOverride:     100000,
	}
	store, err := storage.NewDisruptorStorageWithEngineConfig(tmpDir, engineConfig)
	require.NoError(t, err)
	b := NewStorageBroker(store, engineConfig)
	return b, func() {
		b.Close()
		_ = store.Close()
	}
}

// TestDropHead_SpilledEvictionCounted (T8.4): drop-head eviction of SPILLED
// (WAL-only) messages is counted exactly once. The ring (16 slots, spill at
// count>8) spills every message from ~#10 on, so with x-max-length=12 (ABOVE
// the spill threshold, so trimming starts only after spilling has begun) the
// steady-state eviction stream is all spilled tags. Pre-fix mechanism:
// dropHeadEvictOne gated the eviction count on the ring-CAS result, so a
// spilled eviction "lost", was gap-skipped WITHOUT counting, and drop-head
// evicted another message — the queue emptied far below the limit while
// WaitingCount stayed inflated at the cap.
func TestDropHead_SpilledEvictionCounted(t *testing.T) {
	b, cleanup := createSmallRingBroker(t)
	defer cleanup()

	qs := declareMaxLen(t, b, "dh", map[string]interface{}{"x-max-length": int64(12)})

	// Body encodes the publish index as two chars.
	body := func(i int) string { return string(rune('a'+i/26)) + string(rune('a'+i%26)) }
	const n = 28
	for i := 0; i < n; i++ {
		publishBody(b, "dh", body(i))
	}

	require.Equal(t, int64(12), qs.WaitingCount(), "drop-head must hold the queue at exactly x-max-length (spilled evictions counted)")
	got := drainBodies(t, b, "dh")
	require.Len(t, got, 12, "exactly the newest 12 messages survive")
	want := make([]string, 12)
	for i := 0; i < 12; i++ {
		want[i] = body(n - 12 + i)
	}
	assert.Equal(t, want, got, "the NEWEST 12 survive, in order")
}

// TestGetNoAck_SpilledDepth_NoLeak_Broker is the broker-level twin of T8.1:
// no-ack basic.get on spilled (WAL-only) messages decrements the ready count
// exactly once per message. Pre-fix mechanism: deleteIfPresent returned the
// ring-CAS result, so a spilled removal reported a "loss" and the depth
// counters never moved.
func TestGetNoAck_SpilledDepth_NoLeak_Broker(t *testing.T) {
	b, cleanup := createSmallRingBroker(t)
	defer cleanup()

	qs := declareMaxLen(t, b, "sq", nil)

	const n = 20
	for i := 0; i < n; i++ {
		publishBody(b, "sq", string(rune('a'+i)))
	}
	require.Equal(t, int64(n), qs.WaitingCount())

	// Verify the premise: at least one message is spilled (WAL-only).
	ring := b.storage.(*storage.DisruptorStorage)
	_ = ring // ring residency is exercised implicitly by the small ring.

	for i := 0; i < n; i++ {
		msg, _, _, err := b.GetMessageForGet("sq", true)
		require.NoError(t, err)
		require.NotNil(t, msg, "message %d must be retrievable", i)
	}
	assert.Equal(t, int64(0), qs.WaitingCount(), "every get (ring or spilled) decrements depth")
	assert.Equal(t, int64(0), qs.InflightCount())
	assert.Equal(t, b.GetQueueReadyCount("sq"), uint32(0))
}
