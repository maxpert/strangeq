package storage

import (
	"errors"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// atomicTestStorage builds a DisruptorStorage with a small, fast WAL for the
// atomicity tests.
func atomicTestStorage(t *testing.T) *DisruptorStorage {
	t.Helper()
	engineCfg := interfaces.EngineConfig{
		WALBatchSize:      10,
		WALBatchTimeoutMS: 5,
		WALFileSize:       1 << 20,
		WALChannelBuffer:  1000,
	}
	ds, err := NewDisruptorStorageWithEngineConfig(t.TempDir(), 5*time.Second, engineCfg)
	require.NoError(t, err)
	t.Cleanup(func() { ds.Close() })
	return ds
}

// durableWALCount returns how many durable records for queueName are currently
// recoverable from the WAL (i.e. committed and not yet acked).
func durableWALCount(t *testing.T, ds *DisruptorStorage, queueName string) int {
	t.Helper()
	recovered, err := ds.wal.RecoverFromWAL()
	require.NoError(t, err)
	n := 0
	for _, rm := range recovered {
		if rm.QueueName == queueName {
			n++
		}
	}
	return n
}

// TestExecuteAtomic_RollsBackAllOpsOnMidBatchFailure is the SQ-8 runtime
// atomicity test. A transaction stages two publishes and then a later operation
// in the same transaction fails (modelled by the callback returning an error
// after the stores). NOTHING from the transaction may become visible: no
// partial publishes in the queue ring and nothing durable in the WAL.
//
// Pre-fix (ExecuteAtomic was `return operations(ds)`), the callback's
// StoreMessage calls hit live storage immediately, so both messages were in the
// ring AND fsynced to the WAL by the time the callback returned its error —
// this test would observe count==2 and durableWALCount==2 and FAIL. The staging
// ExecuteAtomic buffers the stores and drops the buffer on error, so nothing is
// applied.
func TestExecuteAtomic_RollsBackAllOpsOnMidBatchFailure(t *testing.T) {
	ds := atomicTestStorage(t)
	queueName := "atomic-rollback-q"
	_ = ds.getOrCreateQueueRing(queueName)

	injected := errors.New("injected mid-transaction operation failure")

	err := ds.ExecuteAtomic(func(txn interfaces.Storage) error {
		require.NoError(t, txn.StoreMessage(queueName, &protocol.Message{
			DeliveryTag: 1, DeliveryMode: 2, Body: []byte("first"),
		}))
		require.NoError(t, txn.StoreMessage(queueName, &protocol.Message{
			DeliveryTag: 2, DeliveryMode: 2, Body: []byte("second"),
		}))
		// A subsequent operation in the transaction fails.
		return injected
	})

	require.ErrorIs(t, err, injected)

	count, err := ds.GetQueueMessageCount(queueName)
	require.NoError(t, err)
	assert.Equal(t, 0, count,
		"failed atomic commit must leave NO messages visible in the queue (clean rollback)")

	assert.Equal(t, 0, durableWALCount(t, ds, queueName),
		"failed atomic commit must write NOTHING durable to the WAL")
}

// TestExecuteAtomic_CommitAppliesAllOps is the positive counterpart: when the
// transaction callback succeeds, every staged publish is applied atomically —
// visible in the ring and durable (bracketed) in the WAL.
func TestExecuteAtomic_CommitAppliesAllOps(t *testing.T) {
	ds := atomicTestStorage(t)
	queueName := "atomic-commit-q"
	_ = ds.getOrCreateQueueRing(queueName)

	err := ds.ExecuteAtomic(func(txn interfaces.Storage) error {
		require.NoError(t, txn.StoreMessage(queueName, &protocol.Message{
			DeliveryTag: 1, DeliveryMode: 2, Body: []byte("first"),
		}))
		require.NoError(t, txn.StoreMessage(queueName, &protocol.Message{
			DeliveryTag: 2, DeliveryMode: 2, Body: []byte("second"),
		}))
		return nil
	})
	require.NoError(t, err)

	count, err := ds.GetQueueMessageCount(queueName)
	require.NoError(t, err)
	assert.Equal(t, 2, count, "successful atomic commit must apply every publish")

	assert.Equal(t, 2, durableWALCount(t, ds, queueName),
		"successful atomic commit must durably persist every publish (committed tx unit)")

	// Both messages must be individually readable by delivery tag.
	for _, tag := range []uint64{1, 2} {
		msg, err := ds.GetMessage(queueName, tag)
		require.NoError(t, err, "message with tag %d must be retrievable", tag)
		require.NotNil(t, msg)
	}
}

// TestExecuteAtomic_SettlementsRolledBackOnFailure proves the rollback also
// covers settlements (acks): a transaction that deletes a previously-stored
// durable message but then fails must leave that message intact.
func TestExecuteAtomic_SettlementsRolledBackOnFailure(t *testing.T) {
	ds := atomicTestStorage(t)
	queueName := "atomic-settle-q"
	_ = ds.getOrCreateQueueRing(queueName)

	// Seed one durable message (outside any transaction).
	require.NoError(t, ds.StoreMessage(queueName, &protocol.Message{
		DeliveryTag: 10, DeliveryMode: 2, Body: []byte("keep-me"),
	}))
	require.Equal(t, 1, durableWALCount(t, ds, queueName))

	injected := errors.New("injected failure after settlement")
	err := ds.ExecuteAtomic(func(txn interfaces.Storage) error {
		require.NoError(t, txn.DeleteMessage(queueName, 10))
		return injected
	})
	require.ErrorIs(t, err, injected)

	// The settlement must NOT have taken effect: the message is still there.
	count, err := ds.GetQueueMessageCount(queueName)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "a rolled-back transaction must not settle (ack) any delivery")
	assert.Equal(t, 1, durableWALCount(t, ds, queueName),
		"rolled-back settlement must not remove the durable message from the WAL")
}
