package broker

import (
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/require"
)

// TestRecovery_DurableQueueTTL_ExpiredWhileDown is the SQ-9 recovery-replay
// acceptance case (unlocked by W2's durable EnqueueUnixMilli): a durable message
// whose effective TTL elapsed WHILE THE BROKER WAS DOWN must be dead-lettered
// (reason=expired) on boot from the persisted deadline — never delivered as if
// live. Boot 1 publishes a durable TTL message and stamps its enqueue anchor;
// boot 2 recovers it with the wall clock advanced past the deadline, and the
// reaper re-evaluates the persisted anchor and dead-letters it.
func TestRecovery_DurableQueueTTL_ExpiredWhileDown(t *testing.T) {
	dataDir := t.TempDir()
	ec := interfaces.EngineConfig{
		RingBufferSize:          65536,
		SpillThresholdPercent:   80,
		WALBatchSize:            1000,
		WALBatchTimeoutMS:       10,
		ConsumerSelectTimeoutMS: 1,
		ConsumerMaxBatchSize:    100,
	}

	// --- Boot 1: declare durable topology and publish a durable TTL message. ---
	store1, err := storage.NewDisruptorStorageWithDataDir(dataDir)
	require.NoError(t, err)
	b1 := NewStorageBroker(store1, ec)
	// A large TTL so boot 1's own reaper never expires it before shutdown; the
	// expiry must happen only because time passes while DOWN.
	b1.SetTTLClock(func() int64 { return 1_000_000 })

	require.NoError(t, b1.DeclareExchange("dlx", "direct", true, false, false, nil))
	_, err = b1.DeclareQueue("dlq", true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, b1.BindQueue("dlq", "dlx", "src", nil))
	_, err = b1.DeclareQueue("src", true, false, false, map[string]interface{}{
		"x-dead-letter-exchange": "dlx",
		"x-message-ttl":          5000,
	})
	require.NoError(t, err)

	durable := &protocol.Message{
		Exchange: "", RoutingKey: "src", Body: []byte("was-live-at-shutdown"),
		DeliveryMode: 2,
	}
	require.NoError(t, b1.PublishMessage("", "src", durable))
	require.NotEqual(t, int64(0), durable.EnqueueUnixMilli, "durable TTL message must be stamped at publish")

	b1.Close()                         // stop boot-1 reaper cleanly
	require.NoError(t, store1.Close()) // WAL fsync completes before close

	// --- Boot 2: reopen, advance the clock past the deadline, recover. ---
	store2, err := storage.NewDisruptorStorageWithDataDir(dataDir)
	require.NoError(t, err)
	b2 := NewStorageBroker(store2, ec)
	defer func() { b2.Close(); _ = store2.Close() }()
	// Wall clock is now well past the persisted deadline (1_000_000 + 5000): the
	// message expired while the broker was down.
	b2.SetTTLClock(func() int64 { return 1 << 50 })

	require.NoError(t, b2.DeclareExchange("dlx", "direct", true, false, false, nil))
	_, err = b2.DeclareQueue("dlq", true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, b2.BindQueue("dlq", "dlx", "src", nil))
	// Redeclaring src (as the recovery manager does from persisted metadata)
	// re-attaches the TTL/DLX policy and starts the reaper.
	_, err = b2.DeclareQueue("src", true, false, false, map[string]interface{}{
		"x-dead-letter-exchange": "dlx",
		"x-message-ttl":          5000,
	})
	require.NoError(t, err)

	recoverPersistentIntoQueues(t, store2, b2)

	// The expired-while-down message is dead-lettered on boot, not delivered.
	dlq := b2.getOrCreateQueueState("dlq")
	require.Eventually(t, func() bool { return dlq.Depth() == 1 }, 2*time.Second, 5*time.Millisecond,
		"a message expired while down must be dead-lettered on recovery")

	src := b2.getOrCreateQueueState("src")
	require.Eventually(t, func() bool { return src.Depth() == 0 }, time.Second, 5*time.Millisecond,
		"the recovered-and-expired message must not remain live on the source")

	got := drainOne(t, b2, "dlq")
	require.NotNil(t, got)
	require.Equal(t, []byte("was-live-at-shutdown"), got.Body)
	assertEntry(t, xDeathEntries(t, got.Headers)[0], "src", DeadLetterExpired, 1)
}
