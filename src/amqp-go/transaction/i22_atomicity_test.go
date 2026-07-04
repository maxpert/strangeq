package transaction

import (
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactionManagerCommitFailureClearsOperations(t *testing.T) {
	tm := NewTransactionManager()
	executor := &MockExecutor{fail: true, failOn: "publish"}
	tm.SetExecutor(executor)

	channelID := uint16(1)

	err := tm.Select(channelID)
	require.NoError(t, err)

	publishOp := NewPublishOperation("test.exchange", "test.key", &protocol.Message{})
	err = tm.AddOperation(channelID, publishOp)
	require.NoError(t, err)

	err = tm.Commit(channelID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to commit transaction")

	operations, err := tm.GetPendingOperations(channelID)
	assert.NoError(t, err)
	assert.Empty(t, operations, "after failed Commit, pending operations must be cleared (0 ops)")
}

func TestTransactionManagerCommitFailureDoesNotDuplicateOnRetry(t *testing.T) {
	tm := NewTransactionManager()
	executor := &MockExecutor{failOnNthPublish: 2}
	tm.SetExecutor(executor)

	channelID := uint16(1)

	err := tm.Select(channelID)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		publishOp := NewPublishOperation("ex", "key", &protocol.Message{Body: []byte("msg")})
		err = tm.AddOperation(channelID, publishOp)
		require.NoError(t, err)
	}

	err = tm.Commit(channelID)
	assert.Error(t, err)

	assert.Len(t, executor.publishes, 1, "only 1 publish should have executed before failure on op 2")

	err = tm.Commit(channelID)
	assert.Error(t, err)

	assert.Len(t, executor.publishes, 1, "retrying Commit must not re-execute already-committed publishes")
}

func TestTransactionManagerCommitFailureTerminatesTransaction(t *testing.T) {
	tm := NewTransactionManager()
	executor := &MockExecutor{fail: true, failOn: "publish"}
	tm.SetExecutor(executor)

	channelID := uint16(1)

	err := tm.Select(channelID)
	require.NoError(t, err)

	publishOp := NewPublishOperation("ex", "key", &protocol.Message{})
	err = tm.AddOperation(channelID, publishOp)
	require.NoError(t, err)

	err = tm.Commit(channelID)
	assert.Error(t, err)

	assert.Equal(t, interfaces.TransactionStateNone, tm.GetState(channelID),
		"after failed Commit, transaction state must be None (terminated)")
}
