package transaction

// WS-D LOW: CommitForConnection's nil-executor early return dropped the
// pending operations WITHOUT running their undos — the wire-tag entries taken
// at buffer time (WS-D §6) leaked: neither settled nor restored, so a later
// ack 406'd as unknown and a rollback had nothing to restore. The early
// return must restore channel-side state exactly like a failed commit does:
// undos in REVERSE order.

import (
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTx_CommitWithNilExecutor_RunsUndosInReverse(t *testing.T) {
	tm := NewTransactionManager()
	// Deliberately NO SetExecutor: the commit path's nil-executor branch.

	const conn, ch = "c1", uint16(1)
	require.NoError(t, tm.SelectForConnection(conn, ch))

	var ran []int
	for i := 1; i <= 3; i++ {
		i := i
		err := tm.AddOperationWithUndoForConnection(conn, ch,
			NewPublishOperation("ex", "k", &protocol.Message{}),
			func() { ran = append(ran, i) })
		require.NoError(t, err)
	}

	err := tm.CommitForConnection(conn, ch)
	require.Error(t, err, "commit without an executor fails")
	assert.Equal(t, []int{3, 2, 1}, ran,
		"nil-executor commit restores buffered state, undos in reverse")

	// Undos are consumed: a later rollback does not re-run them.
	ran = nil
	_ = tm.RollbackForConnection(conn, ch)
	assert.Empty(t, ran)

	// Nothing remains buffered.
	ops, gerr := tm.GetPendingOperationsForConnection(conn, ch)
	require.NoError(t, gerr)
	assert.Empty(t, ops)
}

// Same contract through the legacy zero-conn scope (Commit).
func TestTx_CommitWithNilExecutor_LegacyScopeRunsUndos(t *testing.T) {
	tm := NewTransactionManager()

	const ch = uint16(1)
	require.NoError(t, tm.Select(ch))

	ran := false
	require.NoError(t, tm.AddOperationWithUndo(ch,
		NewPublishOperation("ex", "k", &protocol.Message{}),
		func() { ran = true }))

	require.Error(t, tm.Commit(ch))
	assert.True(t, ran, "legacy-scope nil-executor commit runs the undo")
}
