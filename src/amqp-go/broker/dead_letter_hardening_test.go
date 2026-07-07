package broker

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/require"
)

// TestNackMultiple_LargeBatch_BoundedFanout exercises a multi-nack whose covered
// set is larger than the dead-letter fan-out cap, proving the bounded worker
// pool neither deadlocks nor loses messages: every covered message must still be
// dead-lettered. Run under -race to stress the semaphore acquire/release.
func TestNackMultiple_LargeBatch_BoundedFanout(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	setupDLX(t, b, dlxOpts{
		exchange: "dlx", exchangeKind: "direct", targets: []string{"dlq"}, bindKey: "src",
		sourceArgs: map[string]interface{}{"x-dead-letter-exchange": "dlx"},
	})

	// n well above deadLetterFanoutLimit so the semaphore blocks and drains.
	const n = deadLetterFanoutLimit*2 + 17
	c := registerManualConsumer(t, b, "src", "c1", uint16(n), n)
	for i := 0; i < n; i++ {
		publishToSource(t, b, "src", "m")
	}
	ds := recvDeliveries(t, c, n)
	var maxTag uint64
	for _, d := range ds {
		if d.DeliveryTag > maxTag {
			maxTag = d.DeliveryTag
		}
	}
	require.NoError(t, b.NacknowledgeMessage("c1", maxTag, true, false))

	require.Equal(t, n, countInQueue(t, b, "dlq"),
		"a multi-nack larger than the fan-out cap must still dead-letter every message")
}

// storeFailStorage wraps a real storage and injects a StoreMessage failure for
// one target queue, so tests can drive the republish store-error path.
type storeFailStorage struct {
	interfaces.Storage
	failQueue string
	fails     atomic.Int64
}

func (s *storeFailStorage) StoreMessage(queueName string, message *protocol.Message) error {
	if queueName == s.failQueue {
		s.fails.Add(1)
		return errors.New("injected store failure")
	}
	return s.Storage.StoreMessage(queueName, message)
}

// dropCountingCollector implements the broker MetricsCollector plus the optional
// dead-letter-drop hook so we can assert the store-failure drop is counted.
type dropCountingCollector struct {
	redelivered atomic.Int64
	dropped     atomic.Int64
}

func (c *dropCountingCollector) RecordMessageRedelivered() { c.redelivered.Add(1) }
func (c *dropCountingCollector) RecordDeadLetterDropped()  { c.dropped.Add(1) }

// TestDeadLetter_StoreFailure_IsObservableAndDrops verifies that when the
// republish StoreMessage fails, the message is dropped (the reject still
// completes and the source is removed) AND the drop is recorded via the optional
// metrics hook — a genuine loss must be observable, not silent.
func TestDeadLetter_StoreFailure_IsObservableAndDrops(t *testing.T) {
	tmpDir := t.TempDir()
	real, err := storage.NewDisruptorStorageWithDataDir(tmpDir)
	require.NoError(t, err)
	defer real.Close()

	fs := &storeFailStorage{Storage: real, failQueue: "dlq"}
	ec := interfaces.EngineConfig{
		RingBufferSize:          65536,
		SpillThresholdPercent:   80,
		WALBatchSize:            1000,
		WALBatchTimeoutMS:       10,
		ConsumerSelectTimeoutMS: 1,
		ConsumerMaxBatchSize:    100,
	}
	b := NewStorageBroker(fs, ec)
	coll := &dropCountingCollector{}
	b.SetMetricsCollector(coll)

	setupDLX(t, b, dlxOpts{
		exchange: "dlx", exchangeKind: "direct", targets: []string{"dlq"}, bindKey: "src",
		sourceArgs: map[string]interface{}{"x-dead-letter-exchange": "dlx"},
	})
	publishToSource(t, b, "src", "victim")

	_, tag, _, err := b.GetMessageForGet("src", false)
	require.NoError(t, err)

	// Reject must still complete (drop-on-store-failure), not error out.
	require.NoError(t, b.RejectGetDelivery(tag, false))

	// The republish store failed at least once and was counted.
	require.GreaterOrEqual(t, fs.fails.Load(), int64(1), "store failure must have been exercised")
	require.Equal(t, int64(1), coll.dropped.Load(),
		"the store-failure drop must be recorded via the metrics hook")

	// Message was dropped from the DLX target (store failed) but still removed
	// from the source (caller deletes it).
	require.Nil(t, drainOne(t, b, "dlq"), "nothing lands in the DLX target on store failure")
	srcMsg, _ := b.storage.GetMessage("src", tag)
	require.Nil(t, srcMsg, "source message is still discarded by the caller")
}
