package server

import (
	"testing"

	amqperrors "github.com/maxpert/amqp-go/errors"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/transaction"
	"github.com/stretchr/testify/require"
)

// viewByNumber returns the ChannelView for the given channel number, or false.
func viewByNumber(srv *Server, num uint16) (ChannelView, bool) {
	for _, v := range srv.ChannelViews() {
		if v.Number == num {
			return v, true
		}
	}
	return ChannelView{}, false
}

// TestChannelView_PrefetchAtomic proves ChannelView reports the atomic
// basic.qos prefetch count (BOB-3, spec §5.2 test 13).
func TestChannelView_PrefetchAtomic(t *testing.T) {
	srv, _ := newEntityTestServer(t)
	conn := protocol.NewConnection(nil)
	conn.ID = "pf-conn"
	ch := protocol.NewChannel(1, conn)
	ch.PrefetchCount.Store(7)
	attachConnection(srv, "pf-conn", "u", "/", ch)
	srv.InitEntityCounters()

	v, ok := viewByNumber(srv, 1)
	require.True(t, ok)
	require.EqualValues(t, 7, v.PrefetchCount)

	ch.PrefetchCount.Store(0)
	v, ok = viewByNumber(srv, 1)
	require.True(t, ok)
	require.EqualValues(t, 0, v.PrefetchCount, "0 means unlimited")
}

// TestChannelView_TransactionalMirror proves tx.select flips the lock-free
// mirror true and teardown clears it (BOB-3, spec §5.2 test 14).
func TestChannelView_TransactionalMirror(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, _ := newPipeConnWithFrames(t)
	srv.Mutex.Lock()
	srv.Connections[conn.ID] = conn
	srv.Mutex.Unlock()
	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	require.NoError(t, srv.handleTxSelect(conn, 1, nil))
	v, ok := viewByNumber(srv, 1)
	require.True(t, ok)
	require.True(t, v.Transactional, "tx.select must set the mirror")

	srv.teardownChannel(conn, 1)
	require.False(t, ch.Transactional.Load(), "teardown must clear the mirror")
}

// TestChannelView_TransactionalMatchesManager cross-checks the mirror against
// the transaction manager after each transition (BOB-3, spec §5.2 test 15).
func TestChannelView_TransactionalMatchesManager(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, _ := newPipeConnWithFrames(t)
	srv.Mutex.Lock()
	srv.Connections[conn.ID] = conn
	srv.Mutex.Unlock()
	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	assertMirror := func(stage string) {
		t.Helper()
		view, ok := viewByNumber(srv, 1)
		require.True(t, ok, stage)
		require.Equal(t, srv.txm(conn).IsTransactional(1), view.Transactional,
			"the atomic mirror must agree with the manager at %s", stage)
	}

	require.NoError(t, srv.handleTxSelect(conn, 1, nil))
	assertMirror("after select")

	require.NoError(t, srv.handleTxCommit(conn, 1, nil))
	assertMirror("after commit")

	require.NoError(t, srv.handleTxRollback(conn, 1, nil))
	assertMirror("after rollback")

	// Failed-commit transition (WS4 review, tx_handlers.go:146-148): with a
	// buffered publish and no executor, the commit fails, the manager drops the
	// channel out of tx mode, and the handler must clear the atomic mirror on
	// the same path it closes the channel.
	require.NoError(t, srv.txm(conn).AddOperation(1, transaction.NewPublishOperation("", "", &protocol.Message{})))
	srv.TransactionManager.SetExecutor(nil)
	err := srv.handleTxCommit(conn, 1, nil)
	require.Error(t, err, "a commit with no executor must fail")
	require.False(t, ch.Transactional.Load(), "the failed commit must clear the channel mirror")
	require.False(t, srv.txm(conn).IsTransactional(1), "the failed commit must drop the manager's tx state")
	assertMirror("after failed commit")

	srv.teardownChannel(conn, 1)
	require.False(t, srv.txm(conn).IsTransactional(1))
	require.False(t, ch.Transactional.Load())
}

// TestChannelView_ClosingAfterServerChannelError proves a channel the server is
// closing reports Closing==true before close-ok (BOB-3, spec §5.2 test 16).
func TestChannelView_ClosingAfterServerChannelError(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, _ := newPipeConnWithFrames(t)
	srv.Mutex.Lock()
	srv.Connections[conn.ID] = conn
	srv.Mutex.Unlock()
	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)
	// Register the runtime the Closing signal is derived from.
	srv.runtimeFor(conn)

	srv.channelError(conn, 1, amqperrors.NotFound, "NOT_FOUND - no queue 'q' in vhost '/'", 50, 10)

	v, ok := viewByNumber(srv, 1)
	require.True(t, ok)
	require.True(t, v.Closing, "a server-initiated channel close must be observable as closing")
}

// TestChannelView_ClosingFalseForOpenChannel proves a healthy channel is not
// closing (BOB-3, spec §5.2 test 17).
func TestChannelView_ClosingFalseForOpenChannel(t *testing.T) {
	srv, _ := newEntityTestServer(t)
	conn := protocol.NewConnection(nil)
	conn.ID = "healthy-conn"
	ch := protocol.NewChannel(1, conn)
	attachConnection(srv, "healthy-conn", "u", "/", ch)
	srv.InitEntityCounters()

	v, ok := viewByNumber(srv, 1)
	require.True(t, ok)
	require.False(t, v.Closing)
	require.True(t, v.FlowActive)
}

// TestChannelViews_ClosingFromRuntimeOnlyWithoutRegistration proves Closing
// stays false when no connRuntime is registered (a bare/test connection).
func TestChannelViews_ClosingFromRuntimeOnlyWithoutRegistration(t *testing.T) {
	srv, _ := newEntityTestServer(t)
	conn := protocol.NewConnection(nil)
	conn.ID = "bare-conn"
	ch := protocol.NewChannel(1, conn)
	attachConnection(srv, "bare-conn", "u", "/", ch)
	srv.InitEntityCounters()

	v, ok := viewByNumber(srv, 1)
	require.True(t, ok)
	require.False(t, v.Closing, "no runtime => no closing signal")
}
