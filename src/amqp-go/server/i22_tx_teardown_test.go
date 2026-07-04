package server

import (
	"net"
	"testing"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/transaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTransactionStateCleanedOnConnectionTeardown(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, _ := newPipeConnWithFrames(t)

	ch1 := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch1)

	require.NotNil(t, srv.TransactionManager)
	err := srv.TransactionManager.Select(uint16(1))
	require.NoError(t, err)
	assert.True(t, srv.TransactionManager.IsTransactional(uint16(1)))

	srv.Mutex.Lock()
	srv.Connections[conn.ID] = conn
	srv.Mutex.Unlock()

	srv.cleanupConnection(conn)

	assert.False(t, srv.TransactionManager.IsTransactional(uint16(1)),
		"transaction state must be cleaned up on connection teardown")

	srv.Mutex.RLock()
	_, exists := srv.Connections[conn.ID]
	srv.Mutex.RUnlock()
	assert.False(t, exists, "connection should be removed from server map")
}

func TestTransactionStateCleanedOnConnectionTeardownMultipleChannels(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, _ := newPipeConnWithFrames(t)

	for _, id := range []uint16{1, 2, 3} {
		ch := protocol.NewChannel(id, conn)
		conn.Channels.Store(id, ch)
		err := srv.TransactionManager.Select(id)
		require.NoError(t, err)
		assert.True(t, srv.TransactionManager.IsTransactional(id))
	}

	srv.Mutex.Lock()
	srv.Connections[conn.ID] = conn
	srv.Mutex.Unlock()

	srv.cleanupConnection(conn)

	for _, id := range []uint16{1, 2, 3} {
		assert.False(t, srv.TransactionManager.IsTransactional(id),
			"transaction state for channel %d must be cleaned on teardown", id)
	}
}

func TestHandleTxCommitFailureClosesChannelNotConnection(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	ch1 := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch1)

	setupTxChannelAndQueue(t, srv, conn, frameCh, "tx-commit-fail")
	txSelect(t, srv, conn, frameCh, 1)

	pendingMsg := makePendingMessage("", "tx-commit-fail", "hello")
	err := srv.processCompleteMessage(conn, 1, pendingMsg)
	require.NoError(t, err)

	srv.TransactionManager.SetExecutor(nil)

	err = srv.handleTxCommit(conn, 1, []byte{})
	assert.NoError(t, err, "handleTxCommit should return nil on commit failure (channel.close sent instead of connection kill)")

	classID, methodID, _ := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(20), classID, "expected channel class (20)")
	assert.Equal(t, uint16(40), methodID, "expected channel.close (40)")

	ch, exists := conn.Channels.Load(uint16(1))
	if exists {
		channel := ch.(*protocol.Channel)
		channel.Mutex.RLock()
		closed := channel.Closed
		channel.Mutex.RUnlock()
		assert.True(t, closed, "channel should be marked closed after commit failure")
	}
}

func TestCleanupConnectionNilTransactionManager(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Storage.Path = t.TempDir()
	srv := &Server{
		Addr:             ":0",
		Connections:      make(map[string]*protocol.Connection),
		Log:              zap.NewNop(),
		Config:           cfg,
		MetricsCollector: &NoOpMetricsCollector{},
	}

	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() { clientConn.Close(); serverConn.Close() })
	conn := protocol.NewConnection(serverConn)
	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	srv.Mutex.Lock()
	srv.Connections[conn.ID] = conn
	srv.Mutex.Unlock()

	assert.NotPanics(t, func() {
		srv.cleanupConnection(conn)
	})
}

func TestTransactionManagerCloseIdempotent(t *testing.T) {
	tm := transaction.NewTransactionManager()
	err := tm.Select(1)
	require.NoError(t, err)

	err = tm.Close(1)
	assert.NoError(t, err)

	err = tm.Close(1)
	assert.NoError(t, err, "Close should be idempotent")
}
