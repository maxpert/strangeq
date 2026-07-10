package broker

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createSharedBodyBroker builds a broker over a real DisruptorStorage and returns
// its data directory (for WAL record introspection) and the store handle.
func createSharedBodyBroker(t testing.TB) (*StorageBroker, *storage.DisruptorStorage, string, func()) {
	tmpDir := t.TempDir()
	store, err := storage.NewDisruptorStorageWithDataDir(tmpDir)
	require.NoError(t, err)

	engineConfig := interfaces.EngineConfig{
		RingBufferSize:          65536,
		SpillThresholdPercent:   80,
		WALBatchSize:            1000,
		WALBatchTimeoutMS:       10,
		ConsumerSelectTimeoutMS: 1,
		ConsumerMaxBatchSize:    100,
		SharedBodyThreshold:     4096,
	}
	broker := NewStorageBroker(store, engineConfig)
	cleanup := func() {
		broker.Close()
		_ = store.Close()
	}
	return broker, store, tmpDir, cleanup
}

func makeSharedFanoutBody(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + (i % 26))
	}
	return b
}

// declareFanout declares a durable fanout exchange bound to K durable queues.
func declareFanout(t testing.TB, b *StorageBroker, exch string, queues []string) {
	t.Helper()
	require.NoError(t, b.DeclareExchange(exch, "fanout", true, false, false, nil))
	for _, q := range queues {
		_, err := b.DeclareQueue(q, true, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, b.BindQueue(q, exch, "", nil))
	}
}

// TestSharedBody_FanoutWritesBodyOnce (spec test #1): a durable fan-out to K
// queues via PublishMessageAsyncConfirm writes exactly ONE BodyBlock + K
// reference records (one body copy on disk), fires the confirm once, and makes
// every queue's copy visible with the correct body.
func TestSharedBody_FanoutWritesBodyOnce(t *testing.T) {
	b, store, dataDir, cleanup := createSharedBodyBroker(t)
	defer cleanup()

	const K = 8
	queues := make([]string, K)
	for i := 0; i < K; i++ {
		queues[i] = "fq_" + string(rune('a'+i))
	}
	declareFanout(t, b, "fx", queues)

	body := makeSharedFanoutBody(8192)
	var confirms int32
	msg := &protocol.Message{Exchange: "fx", Body: body, DeliveryMode: 2}
	di, _, err := b.PublishMessageAsyncConfirm("fx", "", msg, func(e error) {
		if e == nil {
			atomic.AddInt32(&confirms, 1)
		}
	})
	require.NoError(t, err)
	require.True(t, di, "a routed durable fan-out reports durableInflight")

	require.Eventually(t, func() bool { return atomic.LoadInt32(&confirms) == 1 }, 5*time.Second, 5*time.Millisecond,
		"the fan-out fires exactly one publisher confirm")

	// Exactly one body copy on disk: 1 BodyBlock + K references, no inline copies.
	counts, err := storage.WALRecordCountsForTest(dataDir)
	require.NoError(t, err)
	assert.Equal(t, 1, counts.BodyBlocks, "the shared body is written ONCE")
	assert.Equal(t, K, counts.RefBodies, "one reference record per fan-out target")
	assert.Equal(t, 0, counts.InlineBodies, "no per-queue inline body copies")

	// Every queue is visible with the correct body.
	for _, q := range queues {
		assert.Equal(t, uint32(1), b.GetQueueReadyCount(q), "queue %s must have the message visible", q)
		msgs, gerr := store.GetQueueMessages(q)
		require.NoError(t, gerr)
		require.Len(t, msgs, 1, "queue %s", q)
		assert.Equal(t, body, msgs[0].Body, "queue %s body", q)
	}
}

// TestSharedBody_FanoutBelowThresholdInlined (spec test #8, broker layer): a
// small-body fan-out stays per-queue inline (size gate) — no BodyBlock.
func TestSharedBody_FanoutBelowThresholdInlined(t *testing.T) {
	b, _, dataDir, cleanup := createSharedBodyBroker(t)
	defer cleanup()

	const K = 4
	queues := make([]string, K)
	for i := 0; i < K; i++ {
		queues[i] = "sq_" + string(rune('a'+i))
	}
	declareFanout(t, b, "sx", queues)

	var confirms int32
	msg := &protocol.Message{Exchange: "sx", Body: makeSharedFanoutBody(100), DeliveryMode: 2}
	di, _, err := b.PublishMessageAsyncConfirm("sx", "", msg, func(e error) {
		if e == nil {
			atomic.AddInt32(&confirms, 1)
		}
	})
	require.NoError(t, err)
	require.True(t, di)
	require.Eventually(t, func() bool { return atomic.LoadInt32(&confirms) == 1 }, 5*time.Second, 5*time.Millisecond)

	counts, err := storage.WALRecordCountsForTest(dataDir)
	require.NoError(t, err)
	assert.Equal(t, 0, counts.BodyBlocks, "below-threshold fan-out must NOT share")
	assert.Equal(t, K, counts.InlineBodies, "each small copy stays inline")
	for _, q := range queues {
		assert.Equal(t, uint32(1), b.GetQueueReadyCount(q))
	}
}

// TestSharedBody_SingleQueueDurableNotShared (spec test #7, broker layer): a
// single-queue durable publish (N==1) never takes the shared path — the WAL
// record is inline and NO BodyBlock is emitted (the winning durable cells).
func TestSharedBody_SingleQueueDurableNotShared(t *testing.T) {
	b, _, dataDir, cleanup := createSharedBodyBroker(t)
	defer cleanup()

	_, err := b.DeclareQueue("solo", true, false, false, nil)
	require.NoError(t, err)

	var confirms int32
	msg := &protocol.Message{Body: makeSharedFanoutBody(8192), DeliveryMode: 2, RoutingKey: "solo"}
	di, _, err := b.PublishMessageAsyncConfirm("", "solo", msg, func(e error) {
		if e == nil {
			atomic.AddInt32(&confirms, 1)
		}
	})
	require.NoError(t, err)
	require.True(t, di)
	require.Eventually(t, func() bool { return atomic.LoadInt32(&confirms) == 1 }, 5*time.Second, 5*time.Millisecond)

	counts, err := storage.WALRecordCountsForTest(dataDir)
	require.NoError(t, err)
	assert.Equal(t, 0, counts.BodyBlocks, "single-queue durable publish must NOT share (N==1 hot path)")
	assert.Equal(t, 1, counts.InlineBodies, "single-queue durable is inline")
}

// TestSharedBody_SyncPublishFanoutShares (PublishMessage path): the synchronous
// durable fan-out also shares the body (feature is consistent across entry
// points) and returns only after the message is durable + visible.
func TestSharedBody_SyncPublishFanoutShares(t *testing.T) {
	b, store, dataDir, cleanup := createSharedBodyBroker(t)
	defer cleanup()

	const K = 5
	queues := make([]string, K)
	for i := 0; i < K; i++ {
		queues[i] = "syncq_" + string(rune('a'+i))
	}
	declareFanout(t, b, "syncfx", queues)

	body := makeSharedFanoutBody(8192)
	require.NoError(t, b.PublishMessage("syncfx", "", &protocol.Message{Exchange: "syncfx", Body: body, DeliveryMode: 2}))

	// Synchronous contract: durable + visible on return.
	counts, err := storage.WALRecordCountsForTest(dataDir)
	require.NoError(t, err)
	assert.Equal(t, 1, counts.BodyBlocks)
	assert.Equal(t, K, counts.RefBodies)
	for _, q := range queues {
		assert.Equal(t, uint32(1), b.GetQueueReadyCount(q), "queue %s visible on return", q)
		msgs, gerr := store.GetQueueMessages(q)
		require.NoError(t, gerr)
		require.Len(t, msgs, 1)
		assert.Equal(t, body, msgs[0].Body)
	}
}
