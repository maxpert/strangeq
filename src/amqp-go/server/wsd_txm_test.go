package server

import (
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTxm_ConnectionScopedView (D-NEW-1 seam): txm returns a per-connection
// scoped view when the manager supports ForConnection, so WS-B's call-site
// switch is one mechanical replacement. It also proves the legacy manager
// path is untouched: a select through the scoped view does NOT leak into the
// shared "" scope the current call sites use.
func TestTxm_ConnectionScopedView(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, _ := newPipeConnWithFrames(t)

	m := srv.txm(conn)
	require.NotNil(t, m)

	require.NoError(t, m.Select(1))
	assert.True(t, m.IsTransactional(1), "scoped view sees its own tx")
	assert.False(t, srv.TransactionManager.IsTransactional(1),
		"the shared (legacy) scope is unaffected — today's call sites keep working until WS-B switches them")

	// A different connection's view is isolated.
	conn2, _ := newPipeConnWithFrames(t)
	m2 := srv.txm(conn2)
	assert.False(t, m2.IsTransactional(1), "conn2's channel 1 is not transactional")

	// Close through the view only drops its own scope.
	require.NoError(t, m.Close(1))
	assert.False(t, m.IsTransactional(1))
}

// TestTxm_NilManager: txm returns nil when there is no manager.
func TestTxm_NilManager(t *testing.T) {
	srv := &Server{}
	conn, _ := newPipeConnWithFrames(t)
	assert.Nil(t, srv.txm(conn))
}

// TestTxm_NonScopingManager: a foreign TransactionManager that cannot scope
// falls back to the shared manager unchanged.
func TestTxm_NonScopingManager(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, _ := newPipeConnWithFrames(t)
	srv.TransactionManager = &nonScopingTM{inner: srv.TransactionManager}

	m := srv.txm(conn)
	require.NotNil(t, m)
	assert.Same(t, srv.TransactionManager, m, "non-scoping manager passes through unchanged")
}

// nonScopingTM wraps a TransactionManager WITHOUT ForConnection.
type nonScopingTM struct {
	inner interfaces.TransactionManager
}

func (n *nonScopingTM) Select(channelID uint16) error { return n.inner.Select(channelID) }
func (n *nonScopingTM) IsTransactional(channelID uint16) bool {
	return n.inner.IsTransactional(channelID)
}
func (n *nonScopingTM) GetState(channelID uint16) interfaces.TransactionState {
	return n.inner.GetState(channelID)
}
func (n *nonScopingTM) AddOperation(channelID uint16, op *interfaces.TransactionOperation) error {
	return n.inner.AddOperation(channelID, op)
}
func (n *nonScopingTM) Commit(channelID uint16) error   { return n.inner.Commit(channelID) }
func (n *nonScopingTM) Rollback(channelID uint16) error { return n.inner.Rollback(channelID) }
func (n *nonScopingTM) Close(channelID uint16) error    { return n.inner.Close(channelID) }
func (n *nonScopingTM) GetPendingOperations(channelID uint16) ([]*interfaces.TransactionOperation, error) {
	return n.inner.GetPendingOperations(channelID)
}
func (n *nonScopingTM) SetExecutor(e interfaces.TransactionExecutor) { n.inner.SetExecutor(e) }
func (n *nonScopingTM) ExecuteAtomic(channelID uint16, ops func() error) error {
	return n.inner.ExecuteAtomic(channelID, ops)
}
func (n *nonScopingTM) GetTransactionStats() *interfaces.TransactionStats {
	return n.inner.GetTransactionStats()
}
