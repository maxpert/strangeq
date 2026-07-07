package broker

import (
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/require"
)

// recoverPersistentIntoQueues replays the persistent (DeliveryMode==2) messages
// from storage into the ring buffers and initializes each queue's dispatch
// cursor, mirroring what server.RecoveryManager.recoverPersistentMessages does.
// It lets a broker-package test exercise the full durable dead-letter round-trip
// without pulling in the server package.
func recoverPersistentIntoQueues(t *testing.T, store interfaces.Storage, b *StorageBroker) {
	t.Helper()
	recovered, err := store.GetRecoverableMessages()
	require.NoError(t, err)

	var maxDeliveryTag uint64
	for queueName, msgs := range recovered {
		var minTag, maxTag, count uint64
		has := false
		for _, m := range msgs {
			if m.DeliveryTag > maxDeliveryTag {
				maxDeliveryTag = m.DeliveryTag
			}
			if m.DeliveryMode != 2 {
				continue
			}
			require.NoError(t, store.LoadMessageFromRecovery(queueName, m))
			if !has || m.DeliveryTag < minTag {
				minTag = m.DeliveryTag
			}
			if m.DeliveryTag > maxTag {
				maxTag = m.DeliveryTag
			}
			has = true
			count++
		}
		if has {
			b.RecoverQueue(queueName, minTag, maxTag, count)
		}
	}
	if maxDeliveryTag > 0 {
		b.AdvanceDeliveryTag(maxDeliveryTag)
	}
}

// TestRecovery_DurableDeadLetter_RetainsXDeath is the W3 durability acceptance
// case (unlocked by W2): a persistent message dead-lettered into a durable DLX
// target retains its x-death / x-first-death-* headers — and the expired-death
// Expiration strip + original-expiration record — across a broker restart.
func TestRecovery_DurableDeadLetter_RetainsXDeath(t *testing.T) {
	dataDir := t.TempDir()
	ec := interfaces.EngineConfig{
		RingBufferSize:          65536,
		SpillThresholdPercent:   80,
		WALBatchSize:            1000,
		WALBatchTimeoutMS:       10,
		ConsumerSelectTimeoutMS: 1,
		ConsumerMaxBatchSize:    100,
	}

	// --- Boot 1: declare durable topology and dead-letter a durable message. ---
	store1, err := storage.NewDisruptorStorageWithDataDir(dataDir)
	require.NoError(t, err)
	b1 := NewStorageBroker(store1, ec)

	require.NoError(t, b1.DeclareExchange("dlx", "direct", true, false, false, nil))
	_, err = b1.DeclareQueue("dlq", true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, b1.BindQueue("dlq", "dlx", "src", nil))
	_, err = b1.DeclareQueue("src", true, false, false, map[string]interface{}{
		"x-dead-letter-exchange": "dlx",
	})
	require.NoError(t, err)
	p := b1.GetQueuePolicy("src")
	require.NotNil(t, p)

	durable := &protocol.Message{
		Exchange: "orders-ex", RoutingKey: "src", Body: []byte("persistent"),
		DeliveryMode: 2, Expiration: "30000",
	}
	require.NoError(t, b1.deadLetter(p, "src", durable, DeadLetterExpired))

	// wal.Write blocks until fsynced, so the clone is durable before Close.
	require.NoError(t, store1.Close())

	// --- Boot 2: reopen the same data dir, recover, and verify x-death. ---
	store2, err := storage.NewDisruptorStorageWithDataDir(dataDir)
	require.NoError(t, err)
	defer store2.Close()
	b2 := NewStorageBroker(store2, ec)

	// Re-declare durable entities (the recovery manager does this from persisted
	// metadata) so the queue exists and has a dispatch cursor to recover into.
	require.NoError(t, b2.DeclareExchange("dlx", "direct", true, false, false, nil))
	_, err = b2.DeclareQueue("dlq", true, false, false, nil)
	require.NoError(t, err)
	recoverPersistentIntoQueues(t, store2, b2)

	got := drainOne(t, b2, "dlq")
	require.NotNil(t, got, "durable dead-lettered message must survive a broker restart")
	require.Equal(t, []byte("persistent"), got.Body)

	entries := xDeathEntries(t, got.Headers)
	require.Len(t, entries, 1)
	assertEntry(t, entries[0], "src", DeadLetterExpired, 1)
	require.Equal(t, "orders-ex", entries[0]["exchange"])
	require.Equal(t, "30000", entries[0]["original-expiration"],
		"original-expiration must survive recovery")
	require.Equal(t, "expired", got.Headers["x-first-death-reason"])
	require.Equal(t, "src", got.Headers["x-first-death-queue"])
	require.Equal(t, "orders-ex", got.Headers["x-first-death-exchange"])
	require.Equal(t, "", got.Expiration,
		"expired-death Expiration strip must persist across recovery")
}
