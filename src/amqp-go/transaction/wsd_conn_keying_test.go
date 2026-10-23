package transaction

import (
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTx_CrossConnectionIsolation_Manager (D-NEW-1, manager twin of the §20
// client test): connection A's tx.select on channel 1 must NOT make every
// connection's channel 1 transactional. Pre-fix mechanism: channels were keyed
// by channelID alone, so conn B's channel-1 publish was silently buffered
// into A's tx.
func TestTx_CrossConnectionIsolation_Manager(t *testing.T) {
	tm := NewTransactionManager()
	tm.SetExecutor(&MockExecutor{})

	connA := tm.ForConnection("conn-A")
	connB := tm.ForConnection("conn-B")

	require.NoError(t, connA.Select(1))
	assert.True(t, connA.IsTransactional(1))
	assert.False(t, connB.IsTransactional(1), "conn B's channel 1 is NOT transactional")

	// B publishes on channel 1: buffered nowhere — AddOperation refuses because
	// B has no tx, which is exactly the non-tx fast path's signal.
	err := connB.AddOperation(1, NewPublishOperation("ex", "k", &protocol.Message{}))
	require.Error(t, err, "B has no transaction to buffer into")

	// B's operations never leak into A's tx.
	ops, err := connA.GetPendingOperations(1)
	require.NoError(t, err)
	assert.Empty(t, ops)

	// The legacy ("" scope) view sees neither — full backward compatibility.
	assert.False(t, tm.IsTransactional(1), "legacy scope untouched by conn-scoped selects")
	require.Error(t, tm.AddOperation(1, NewPublishOperation("ex", "k", &protocol.Message{})),
		"legacy-scope channel 1 is not transactional")
}

// TestTx_ConfirmSelectNotBlockedByOtherConnectionsTx_Manager (manager twin):
// the confirm.select guard checks IsTransactional(channelID); with conn
// keying, B's confirm path is unaffected by A's tx.
func TestTx_ConfirmSelectNotBlockedByOtherConnectionsTx_Manager(t *testing.T) {
	tm := NewTransactionManager()
	tm.SetExecutor(&MockExecutor{})

	require.NoError(t, tm.ForConnection("A").Select(1))
	assert.False(t, tm.ForConnection("B").IsTransactional(1),
		"B's channel 1 stays non-transactional (its confirm.select is not refused)")
}

// TestTx_ChannelCloseDoesNotDropOtherConnectionsTx_Manager (manager twin):
// A closing channel 1 deletes only A's tx; B's commit still applies B's
// buffered publish.
func TestTx_ChannelCloseDoesNotDropOtherConnectionsTx_Manager(t *testing.T) {
	tm := NewTransactionManager()
	exec := &MockExecutor{}
	tm.SetExecutor(exec)

	connA := tm.ForConnection("A")
	connB := tm.ForConnection("B")
	require.NoError(t, connA.Select(1))
	require.NoError(t, connB.Select(1))
	require.NoError(t, connB.AddOperation(1, NewPublishOperation("ex", "k", &protocol.Message{Body: []byte("b-msg")})))

	require.NoError(t, connA.Close(1), "A's channel close")
	assert.False(t, connA.IsTransactional(1))
	assert.True(t, connB.IsTransactional(1), "B's tx survives A's channel close")

	require.NoError(t, connB.Commit(1), "B's commit still applies")
	require.Len(t, exec.publishes, 1, "B's buffered publish executed")
	assert.Equal(t, "b-msg", string(exec.publishes[0].Message.Body))
}

// TestTx_ForConnection_CommitRollbackUndoScoped: undo/rollback/commit through
// the conn-scoped view behave identically to the legacy path.
func TestTx_ForConnection_CommitRollbackUndoScoped(t *testing.T) {
	tm := NewTransactionManager()
	tm.SetExecutor(&MockExecutor{})

	connA := tm.ForConnection("A")
	require.NoError(t, connA.Select(1))

	u, ok := connA.(interface {
		AddOperationWithUndo(uint16, *interfaces.TransactionOperation, func()) error
	})
	require.True(t, ok, "the ForConnection view carries AddOperationWithUndo (WS-D §6 needs it)")

	ran := false
	require.NoError(t, u.AddOperationWithUndo(1, NewPublishOperation("ex", "k", &protocol.Message{}), func() { ran = true }))
	require.NoError(t, connA.Rollback(1))
	assert.True(t, ran, "rollback through the view runs undos")
}

// TestTx_GetTransactionStats_AggregatesAcrossConnections: stats aggregate all
// conn-scoped keys.
func TestTx_GetTransactionStats_AggregatesAcrossConnections(t *testing.T) {
	tm := NewTransactionManager()
	tm.SetExecutor(&MockExecutor{})

	require.NoError(t, tm.ForConnection("A").Select(1))
	require.NoError(t, tm.ForConnection("B").Select(1))
	require.NoError(t, tm.ForConnection("B").AddOperation(1, NewPublishOperation("ex", "k", &protocol.Message{})))

	stats := tm.GetTransactionStats()
	assert.Equal(t, 2, stats.ActiveTransactions, "both connections' txs are active")
	assert.Equal(t, 1, stats.PendingOperations)
}
