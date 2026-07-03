package server

import (
	"encoding/binary"
	"testing"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/maxpert/amqp-go/transaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// ---------------------------------------------------------------------------
// Test infrastructure
// ---------------------------------------------------------------------------

func newTransactionTestServer(t *testing.T) *Server {
	t.Helper()
	logger := zap.NewNop()
	cfg := config.DefaultConfig()
	cfg.Storage.Path = t.TempDir()

	storageImpl := storage.NewDisruptorStorageWithDataDir(cfg.Storage.Path)
	storageBroker := broker.NewStorageBroker(storageImpl, cfg.GetEngine())
	unifiedBroker := NewStorageBrokerAdapter(storageBroker)

	tm := transaction.NewTransactionManager()
	executor := transaction.NewUnifiedBrokerExecutor(unifiedBroker)
	tm.SetExecutor(executor)

	return &Server{
		Addr:               ":0",
		Connections:        make(map[string]*protocol.Connection),
		Log:                logger,
		Broker:             unifiedBroker,
		Config:             cfg,
		MetricsCollector:   &NoOpMetricsCollector{},
		TransactionManager: tm,
	}
}

func makePendingMessage(exchange, routingKey, body string) *protocol.PendingMessage {
	return &protocol.PendingMessage{
		Method: &protocol.BasicPublishMethod{
			Exchange:   exchange,
			RoutingKey: routingKey,
		},
		Header: &protocol.ContentHeader{
			BodySize: uint64(len(body)),
		},
		Body:     []byte(body),
		Received: uint64(len(body)),
	}
}

func makeAckPayload(deliveryTag uint64, multiple bool) []byte {
	payload := make([]byte, 10)
	binary.BigEndian.PutUint64(payload[:8], deliveryTag)
	if multiple {
		binary.BigEndian.PutUint16(payload[8:10], 1)
	}
	return payload
}

func makeRejectPayload(deliveryTag uint64, requeue bool) []byte {
	payload := make([]byte, 10)
	binary.BigEndian.PutUint64(payload[:8], deliveryTag)
	if requeue {
		binary.BigEndian.PutUint16(payload[8:10], 1)
	}
	return payload
}

func makeNackPayload(deliveryTag uint64, multiple, requeue bool) []byte {
	payload := make([]byte, 10)
	binary.BigEndian.PutUint64(payload[:8], deliveryTag)
	var flags uint16
	if multiple {
		flags |= 1
	}
	if requeue {
		flags |= 2
	}
	binary.BigEndian.PutUint16(payload[8:10], flags)
	return payload
}

func setupTxChannelAndQueue(t *testing.T, srv *Server, conn *protocol.Connection, frameCh chan *protocol.Frame, queueName string) {
	t.Helper()
	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	declareMethod := &protocol.QueueDeclareMethod{Queue: queueName}
	payload, err := declareMethod.Serialize()
	require.NoError(t, err)
	err = srv.handleQueueDeclare(conn, 1, payload)
	require.NoError(t, err)
	drainFrame(t, frameCh)
}

func txSelect(t *testing.T, srv *Server, conn *protocol.Connection, frameCh chan *protocol.Frame, channelID uint16) {
	t.Helper()
	err := srv.handleTxSelect(conn, channelID, []byte{})
	require.NoError(t, err)
	drainFrame(t, frameCh)
}

func txCommit(t *testing.T, srv *Server, conn *protocol.Connection, frameCh chan *protocol.Frame, channelID uint16) {
	t.Helper()
	err := srv.handleTxCommit(conn, channelID, []byte{})
	require.NoError(t, err)
	drainFrame(t, frameCh)
}

func txRollback(t *testing.T, srv *Server, conn *protocol.Connection, frameCh chan *protocol.Frame, channelID uint16) {
	t.Helper()
	err := srv.handleTxRollback(conn, channelID, []byte{})
	require.NoError(t, err)
	drainFrame(t, frameCh)
}

func getDeliveryTag(t *testing.T, srv *Server, conn *protocol.Connection, frameCh chan *protocol.Frame, queueName string) uint64 {
	t.Helper()
	err := srv.handleBasicGet(conn, 1, encodeBasicGet(t, queueName, false))
	require.NoError(t, err)

	_, _, methodPayload := nextMethodFrame(t, frameCh)
	getOK := &protocol.BasicGetOKMethod{}
	require.NoError(t, getOK.Deserialize(methodPayload))
	deliveryTag := getOK.DeliveryTag

	drainFrame(t, frameCh) // header
	drainFrame(t, frameCh) // body
	return deliveryTag
}

// ---------------------------------------------------------------------------
// Publish transaction tests
// ---------------------------------------------------------------------------

func TestTxPublishBuffers(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	setupTxChannelAndQueue(t, srv, conn, frameCh, "tx-pub-buffer")

	txSelect(t, srv, conn, frameCh, 1)

	err := srv.processCompleteMessage(conn, 1, makePendingMessage("", "tx-pub-buffer", "hello"))
	require.NoError(t, err)

	// Message should NOT be in the queue — it was buffered
	msg, _, _, err := srv.Broker.GetMessageForGet("tx-pub-buffer", true)
	require.NoError(t, err)
	assert.Nil(t, msg, "message should be buffered, not published")

	// Verify operation was buffered
	ops, err := srv.TransactionManager.GetPendingOperations(1)
	require.NoError(t, err)
	require.Len(t, ops, 1)
	assert.Equal(t, interfaces.OpPublish, ops[0].Type)
}

func TestTxPublishCommitExecutes(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	setupTxChannelAndQueue(t, srv, conn, frameCh, "tx-pub-commit")

	txSelect(t, srv, conn, frameCh, 1)

	err := srv.processCompleteMessage(conn, 1, makePendingMessage("", "tx-pub-commit", "committed"))
	require.NoError(t, err)

	// Not published yet
	msg, _, _, _ := srv.Broker.GetMessageForGet("tx-pub-commit", true)
	assert.Nil(t, msg)

	txCommit(t, srv, conn, frameCh, 1)

	// Now message should be available
	msg, _, _, err = srv.Broker.GetMessageForGet("tx-pub-commit", true)
	require.NoError(t, err)
	require.NotNil(t, msg)
	assert.Equal(t, "committed", string(msg.Body))
}

func TestTxPublishRollbackDiscards(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	setupTxChannelAndQueue(t, srv, conn, frameCh, "tx-pub-rollback")

	txSelect(t, srv, conn, frameCh, 1)

	err := srv.processCompleteMessage(conn, 1, makePendingMessage("", "tx-pub-rollback", "rolled-back"))
	require.NoError(t, err)

	txRollback(t, srv, conn, frameCh, 1)

	// Message should NOT be in the queue
	msg, _, _, err := srv.Broker.GetMessageForGet("tx-pub-rollback", true)
	require.NoError(t, err)
	assert.Nil(t, msg, "message should be discarded by rollback")

	// No pending operations
	ops, err := srv.TransactionManager.GetPendingOperations(1)
	require.NoError(t, err)
	assert.Empty(t, ops)
}

func TestTxNonTransactionalPublishWorks(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	setupTxChannelAndQueue(t, srv, conn, frameCh, "tx-non-tx-pub")

	// No tx.select — publish should go through immediately
	err := srv.processCompleteMessage(conn, 1, makePendingMessage("", "tx-non-tx-pub", "direct"))
	require.NoError(t, err)

	msg, _, _, err := srv.Broker.GetMessageForGet("tx-non-tx-pub", true)
	require.NoError(t, err)
	require.NotNil(t, msg)
	assert.Equal(t, "direct", string(msg.Body))
}

// ---------------------------------------------------------------------------
// Ack transaction tests
// ---------------------------------------------------------------------------

func TestTxAckBuffers(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	setupTxChannelAndQueue(t, srv, conn, frameCh, "tx-ack-buffer")

	// Publish a message and retrieve it via basic.get (no_ack=false)
	publishMessageToQueue(t, srv, "tx-ack-buffer", "to-ack")
	deliveryTag := getDeliveryTag(t, srv, conn, frameCh, "tx-ack-buffer")

	// Verify delivery is tracked
	_, ok := srv.Broker.GetConsumerForDelivery(deliveryTag)
	assert.True(t, ok)

	txSelect(t, srv, conn, frameCh, 1)

	err := srv.handleBasicAck(conn, 1, makeAckPayload(deliveryTag, false))
	require.NoError(t, err)

	// Delivery should still be tracked — ack was buffered, not executed
	_, ok = srv.Broker.GetConsumerForDelivery(deliveryTag)
	assert.True(t, ok, "delivery should still be tracked after buffered ack")

	// Verify operation was buffered
	ops, err := srv.TransactionManager.GetPendingOperations(1)
	require.NoError(t, err)
	require.Len(t, ops, 1)
	assert.Equal(t, interfaces.OpAck, ops[0].Type)
}

func TestTxAckCommitExecutes(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	setupTxChannelAndQueue(t, srv, conn, frameCh, "tx-ack-commit")

	publishMessageToQueue(t, srv, "tx-ack-commit", "to-ack-commit")
	deliveryTag := getDeliveryTag(t, srv, conn, frameCh, "tx-ack-commit")

	txSelect(t, srv, conn, frameCh, 1)

	err := srv.handleBasicAck(conn, 1, makeAckPayload(deliveryTag, false))
	require.NoError(t, err)

	// Still tracked before commit
	_, ok := srv.Broker.GetConsumerForDelivery(deliveryTag)
	assert.True(t, ok)

	txCommit(t, srv, conn, frameCh, 1)

	// Delivery should be removed — ack was executed
	_, ok = srv.Broker.GetConsumerForDelivery(deliveryTag)
	assert.False(t, ok, "delivery should be removed after committed ack")
}

func TestTxAckRollbackDiscards(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	setupTxChannelAndQueue(t, srv, conn, frameCh, "tx-ack-rollback")

	publishMessageToQueue(t, srv, "tx-ack-rollback", "to-ack-rollback")
	deliveryTag := getDeliveryTag(t, srv, conn, frameCh, "tx-ack-rollback")

	txSelect(t, srv, conn, frameCh, 1)

	err := srv.handleBasicAck(conn, 1, makeAckPayload(deliveryTag, false))
	require.NoError(t, err)

	txRollback(t, srv, conn, frameCh, 1)

	// Delivery should still be tracked — ack was discarded
	_, ok := srv.Broker.GetConsumerForDelivery(deliveryTag)
	assert.True(t, ok, "delivery should still be tracked after rolled-back ack")

	// No pending operations
	ops, err := srv.TransactionManager.GetPendingOperations(1)
	require.NoError(t, err)
	assert.Empty(t, ops)
}

func TestTxRejectBuffers(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	setupTxChannelAndQueue(t, srv, conn, frameCh, "tx-reject-buffer")

	publishMessageToQueue(t, srv, "tx-reject-buffer", "to-reject")
	deliveryTag := getDeliveryTag(t, srv, conn, frameCh, "tx-reject-buffer")

	txSelect(t, srv, conn, frameCh, 1)

	err := srv.handleBasicReject(conn, 1, makeRejectPayload(deliveryTag, false))
	require.NoError(t, err)

	// Delivery should still be tracked
	_, ok := srv.Broker.GetConsumerForDelivery(deliveryTag)
	assert.True(t, ok, "delivery should still be tracked after buffered reject")

	ops, err := srv.TransactionManager.GetPendingOperations(1)
	require.NoError(t, err)
	require.Len(t, ops, 1)
	assert.Equal(t, interfaces.OpReject, ops[0].Type)
}

func TestTxNackBuffers(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	setupTxChannelAndQueue(t, srv, conn, frameCh, "tx-nack-buffer")

	publishMessageToQueue(t, srv, "tx-nack-buffer", "to-nack")
	deliveryTag := getDeliveryTag(t, srv, conn, frameCh, "tx-nack-buffer")

	txSelect(t, srv, conn, frameCh, 1)

	err := srv.handleBasicNack(conn, 1, makeNackPayload(deliveryTag, false, false))
	require.NoError(t, err)

	// Delivery should still be tracked
	_, ok := srv.Broker.GetConsumerForDelivery(deliveryTag)
	assert.True(t, ok, "delivery should still be tracked after buffered nack")

	ops, err := srv.TransactionManager.GetPendingOperations(1)
	require.NoError(t, err)
	require.Len(t, ops, 1)
	assert.Equal(t, interfaces.OpNack, ops[0].Type)
}

// ---------------------------------------------------------------------------
// Multiple operations and state persistence
// ---------------------------------------------------------------------------

func TestTxMultipleOperationsCommit(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	setupTxChannelAndQueue(t, srv, conn, frameCh, "tx-multi")

	// Publish a message to consume, so we can buffer an ack
	publishMessageToQueue(t, srv, "tx-multi", "to-consume")
	deliveryTag := getDeliveryTag(t, srv, conn, frameCh, "tx-multi")

	txSelect(t, srv, conn, frameCh, 1)

	// Buffer a publish
	err := srv.processCompleteMessage(conn, 1, makePendingMessage("", "tx-multi", "published-in-tx"))
	require.NoError(t, err)

	// Buffer an ack
	err = srv.handleBasicAck(conn, 1, makeAckPayload(deliveryTag, false))
	require.NoError(t, err)

	// Verify 2 pending operations
	ops, err := srv.TransactionManager.GetPendingOperations(1)
	require.NoError(t, err)
	require.Len(t, ops, 2)
	assert.Equal(t, interfaces.OpPublish, ops[0].Type)
	assert.Equal(t, interfaces.OpAck, ops[1].Type)

	// Queue should be empty (publish was buffered)
	msg, _, _, _ := srv.Broker.GetMessageForGet("tx-multi", true)
	assert.Nil(t, msg)

	// Delivery should still be tracked (ack was buffered)
	_, ok := srv.Broker.GetConsumerForDelivery(deliveryTag)
	assert.True(t, ok)

	txCommit(t, srv, conn, frameCh, 1)

	// Publish should have executed — message available in queue
	msg, _, _, err = srv.Broker.GetMessageForGet("tx-multi", true)
	require.NoError(t, err)
	require.NotNil(t, msg)
	assert.Equal(t, "published-in-tx", string(msg.Body))

	// Ack should have executed — delivery removed
	_, ok = srv.Broker.GetConsumerForDelivery(deliveryTag)
	assert.False(t, ok)

	// No pending operations
	ops, err = srv.TransactionManager.GetPendingOperations(1)
	require.NoError(t, err)
	assert.Empty(t, ops)
}

func TestTxChannelRemainsTransactionalAfterCommit(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	setupTxChannelAndQueue(t, srv, conn, frameCh, "tx-remains")

	txSelect(t, srv, conn, frameCh, 1)

	// First publish + commit
	err := srv.processCompleteMessage(conn, 1, makePendingMessage("", "tx-remains", "first"))
	require.NoError(t, err)
	txCommit(t, srv, conn, frameCh, 1)

	// Verify first message published
	msg, _, _, err := srv.Broker.GetMessageForGet("tx-remains", true)
	require.NoError(t, err)
	require.NotNil(t, msg)
	assert.Equal(t, "first", string(msg.Body))

	// Channel should still be transactional
	assert.True(t, srv.TransactionManager.IsTransactional(1))

	// Second publish should also buffer (not publish immediately)
	err = srv.processCompleteMessage(conn, 1, makePendingMessage("", "tx-remains", "second"))
	require.NoError(t, err)

	// Second message should NOT be in queue yet
	msg, _, _, _ = srv.Broker.GetMessageForGet("tx-remains", true)
	assert.Nil(t, msg)

	// Second commit
	txCommit(t, srv, conn, frameCh, 1)

	// Now second message should be available
	msg, _, _, err = srv.Broker.GetMessageForGet("tx-remains", true)
	require.NoError(t, err)
	require.NotNil(t, msg)
	assert.Equal(t, "second", string(msg.Body))
}

func TestTxChannelRemainsTransactionalAfterRollback(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	setupTxChannelAndQueue(t, srv, conn, frameCh, "tx-remains-rb")

	txSelect(t, srv, conn, frameCh, 1)

	err := srv.processCompleteMessage(conn, 1, makePendingMessage("", "tx-remains-rb", "discarded"))
	require.NoError(t, err)
	txRollback(t, srv, conn, frameCh, 1)

	// Channel should still be transactional
	assert.True(t, srv.TransactionManager.IsTransactional(1))

	// New publish should buffer
	err = srv.processCompleteMessage(conn, 1, makePendingMessage("", "tx-remains-rb", "kept"))
	require.NoError(t, err)

	// Not in queue
	msg, _, _, _ := srv.Broker.GetMessageForGet("tx-remains-rb", true)
	assert.Nil(t, msg)

	// Commit should publish
	txCommit(t, srv, conn, frameCh, 1)
	msg, _, _, err = srv.Broker.GetMessageForGet("tx-remains-rb", true)
	require.NoError(t, err)
	require.NotNil(t, msg)
	assert.Equal(t, "kept", string(msg.Body))
}

// ---------------------------------------------------------------------------
// Get delivery (empty consumer tag) ack in transaction
// ---------------------------------------------------------------------------

func TestTxGetDeliveryAckCommitExecutes(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	setupTxChannelAndQueue(t, srv, conn, frameCh, "tx-get-ack")

	publishMessageToQueue(t, srv, "tx-get-ack", "get-delivery")
	deliveryTag := getDeliveryTag(t, srv, conn, frameCh, "tx-get-ack")

	// Verify this is a basic.get delivery (empty consumer tag)
	consumerTag, ok := srv.Broker.GetConsumerForDelivery(deliveryTag)
	require.True(t, ok)
	assert.Equal(t, "", consumerTag)

	txSelect(t, srv, conn, frameCh, 1)

	err := srv.handleBasicAck(conn, 1, makeAckPayload(deliveryTag, false))
	require.NoError(t, err)

	// Still tracked
	_, ok = srv.Broker.GetConsumerForDelivery(deliveryTag)
	assert.True(t, ok)

	txCommit(t, srv, conn, frameCh, 1)

	// Should be removed after commit
	_, ok = srv.Broker.GetConsumerForDelivery(deliveryTag)
	assert.False(t, ok, "basic.get delivery ack should be executed on commit")
}
