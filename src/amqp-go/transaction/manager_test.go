package transaction

import (
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockExecutor implements TransactionExecutor for testing
type MockExecutor struct {
	publishes []PublishCall
	acks      []AckCall
	nacks     []NackCall
	rejects   []RejectCall
	fail      bool
	failOn    string
}

type PublishCall struct {
	Exchange   string
	RoutingKey string
	Message    *protocol.Message
}

type AckCall struct {
	QueueName   string
	DeliveryTag uint64
	Multiple    bool
}

type NackCall struct {
	QueueName   string
	DeliveryTag uint64
	Multiple    bool
	Requeue     bool
}

type RejectCall struct {
	QueueName   string
	DeliveryTag uint64
	Requeue     bool
}

func (m *MockExecutor) ExecutePublish(exchange, routingKey string, message *protocol.Message) error {
	if m.fail && m.failOn == "publish" {
		return assert.AnError
	}
	m.publishes = append(m.publishes, PublishCall{
		Exchange:   exchange,
		RoutingKey: routingKey,
		Message:    message,
	})
	return nil
}

func (m *MockExecutor) ExecuteAck(queueName string, deliveryTag uint64, multiple bool) error {
	if m.fail && m.failOn == "ack" {
		return assert.AnError
	}
	m.acks = append(m.acks, AckCall{
		QueueName:   queueName,
		DeliveryTag: deliveryTag,
		Multiple:    multiple,
	})
	return nil
}

func (m *MockExecutor) ExecuteNack(queueName string, deliveryTag uint64, multiple, requeue bool) error {
	if m.fail && m.failOn == "nack" {
		return assert.AnError
	}
	m.nacks = append(m.nacks, NackCall{
		QueueName:   queueName,
		DeliveryTag: deliveryTag,
		Multiple:    multiple,
		Requeue:     requeue,
	})
	return nil
}

func (m *MockExecutor) ExecuteReject(queueName string, deliveryTag uint64, requeue bool) error {
	if m.fail && m.failOn == "reject" {
		return assert.AnError
	}
	m.rejects = append(m.rejects, RejectCall{
		QueueName:   queueName,
		DeliveryTag: deliveryTag,
		Requeue:     requeue,
	})
	return nil
}

func (m *MockExecutor) Reset() {
	m.publishes = nil
	m.acks = nil
	m.nacks = nil
	m.rejects = nil
	m.fail = false
	m.failOn = ""
}

func TestNewTransactionManager(t *testing.T) {
	tm := NewTransactionManager()
	assert.NotNil(t, tm)

	stats := tm.GetTransactionStats()
	assert.Equal(t, 0, stats.ActiveTransactions)
	assert.Equal(t, int64(0), stats.TotalCommits)
	assert.Equal(t, int64(0), stats.TotalRollbacks)
}

func TestTransactionManagerSelect(t *testing.T) {
	tm := NewTransactionManager()
	channelID := uint16(1)

	// Initially not transactional
	assert.False(t, tm.IsTransactional(channelID))
	assert.Equal(t, interfaces.TransactionStateNone, tm.GetState(channelID))

	// Select transaction mode
	err := tm.Select(channelID)
	assert.NoError(t, err)

	// Now should be transactional
	assert.True(t, tm.IsTransactional(channelID))
	assert.Equal(t, interfaces.TransactionStateActive, tm.GetState(channelID))

	// Selecting again should fail
	err = tm.Select(channelID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already has an active transaction")
}

func TestTransactionManagerOperations(t *testing.T) {
	tm := NewTransactionManager()
	executor := &MockExecutor{}
	tm.SetExecutor(executor)

	channelID := uint16(1)

	// Start transaction
	err := tm.Select(channelID)
	require.NoError(t, err)

	// Add various operations
	message := &protocol.Message{
		Exchange:   "test.exchange",
		RoutingKey: "test.key",
		Body:       []byte("test message"),
	}

	// Add publish operation
	publishOp := NewPublishOperation("test.exchange", "test.key", message)
	err = tm.AddOperation(channelID, publishOp)
	assert.NoError(t, err)
	assert.Equal(t, interfaces.TransactionStatePending, tm.GetState(channelID))

	// Add ack operation
	ackOp := NewAckOperation("test.queue", 123, false)
	err = tm.AddOperation(channelID, ackOp)
	assert.NoError(t, err)

	// Add nack operation
	nackOp := NewNackOperation("test.queue", 124, false, true)
	err = tm.AddOperation(channelID, nackOp)
	assert.NoError(t, err)

	// Add reject operation
	rejectOp := NewRejectOperation("test.queue", 125, false)
	err = tm.AddOperation(channelID, rejectOp)
	assert.NoError(t, err)

	// Check pending operations
	operations, err := tm.GetPendingOperations(channelID)
	assert.NoError(t, err)
	assert.Len(t, operations, 4)

	// Operations should not have been executed yet
	assert.Empty(t, executor.publishes)
	assert.Empty(t, executor.acks)
	assert.Empty(t, executor.nacks)
	assert.Empty(t, executor.rejects)
}

func TestTransactionManagerCommit(t *testing.T) {
	tm := NewTransactionManager()
	executor := &MockExecutor{}
	tm.SetExecutor(executor)

	channelID := uint16(1)

	// Start transaction and add operations
	err := tm.Select(channelID)
	require.NoError(t, err)

	message := &protocol.Message{
		Exchange:   "test.exchange",
		RoutingKey: "test.key",
		Body:       []byte("test message"),
	}

	publishOp := NewPublishOperation("test.exchange", "test.key", message)
	ackOp := NewAckOperation("test.queue", 123, false)

	err = tm.AddOperation(channelID, publishOp)
	require.NoError(t, err)
	err = tm.AddOperation(channelID, ackOp)
	require.NoError(t, err)

	// Commit transaction
	err = tm.Commit(channelID)
	assert.NoError(t, err)

	// All operations should have been executed
	assert.Len(t, executor.publishes, 1)
	assert.Equal(t, "test.exchange", executor.publishes[0].Exchange)
	assert.Equal(t, "test.key", executor.publishes[0].RoutingKey)

	assert.Len(t, executor.acks, 1)
	assert.Equal(t, "test.queue", executor.acks[0].QueueName)
	assert.Equal(t, uint64(123), executor.acks[0].DeliveryTag)

	// Transaction should be back to active state (still in transactional mode)
	assert.Equal(t, interfaces.TransactionStateActive, tm.GetState(channelID))

	// No pending operations
	operations, err := tm.GetPendingOperations(channelID)
	assert.NoError(t, err)
	assert.Empty(t, operations)

	// Statistics should be updated
	stats := tm.GetTransactionStats()
	assert.Equal(t, int64(1), stats.TotalCommits)
	assert.Equal(t, int64(0), stats.TotalRollbacks)
}

func TestTransactionManagerRollback(t *testing.T) {
	tm := NewTransactionManager()
	executor := &MockExecutor{}
	tm.SetExecutor(executor)

	channelID := uint16(1)

	// Start transaction and add operations
	err := tm.Select(channelID)
	require.NoError(t, err)

	message := &protocol.Message{
		Exchange:   "test.exchange",
		RoutingKey: "test.key",
		Body:       []byte("test message"),
	}

	publishOp := NewPublishOperation("test.exchange", "test.key", message)
	ackOp := NewAckOperation("test.queue", 123, false)

	err = tm.AddOperation(channelID, publishOp)
	require.NoError(t, err)
	err = tm.AddOperation(channelID, ackOp)
	require.NoError(t, err)

	// Rollback transaction
	err = tm.Rollback(channelID)
	assert.NoError(t, err)

	// No operations should have been executed
	assert.Empty(t, executor.publishes)
	assert.Empty(t, executor.acks)

	// Transaction should be back to active state (still in transactional mode)
	assert.Equal(t, interfaces.TransactionStateActive, tm.GetState(channelID))

	// No pending operations
	operations, err := tm.GetPendingOperations(channelID)
	assert.NoError(t, err)
	assert.Empty(t, operations)

	// Statistics should be updated
	stats := tm.GetTransactionStats()
	assert.Equal(t, int64(0), stats.TotalCommits)
	assert.Equal(t, int64(1), stats.TotalRollbacks)
}

func TestTransactionManagerCommitFailure(t *testing.T) {
	tm := NewTransactionManager()
	executor := &MockExecutor{fail: true, failOn: "publish"}
	tm.SetExecutor(executor)

	channelID := uint16(1)

	// Start transaction and add operation
	err := tm.Select(channelID)
	require.NoError(t, err)

	message := &protocol.Message{
		Exchange:   "test.exchange",
		RoutingKey: "test.key",
		Body:       []byte("test message"),
	}

	publishOp := NewPublishOperation("test.exchange", "test.key", message)
	err = tm.AddOperation(channelID, publishOp)
	require.NoError(t, err)

	// Commit should fail
	err = tm.Commit(channelID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to commit transaction")

	// Operations should still be pending
	operations, err := tm.GetPendingOperations(channelID)
	assert.NoError(t, err)
	assert.Len(t, operations, 1)
}

func TestTransactionManagerClose(t *testing.T) {
	tm := NewTransactionManager()
	channelID := uint16(1)

	// Start transaction
	err := tm.Select(channelID)
	require.NoError(t, err)

	// Close channel
	err = tm.Close(channelID)
	assert.NoError(t, err)

	// Channel should no longer be transactional
	assert.False(t, tm.IsTransactional(channelID))
	assert.Equal(t, interfaces.TransactionStateNone, tm.GetState(channelID))
}

func TestTransactionManagerMultipleChannels(t *testing.T) {
	tm := NewTransactionManager()
	executor := &MockExecutor{}
	tm.SetExecutor(executor)

	channel1 := uint16(1)
	channel2 := uint16(2)

	// Start transactions on both channels
	err := tm.Select(channel1)
	require.NoError(t, err)
	err = tm.Select(channel2)
	require.NoError(t, err)

	// Add different operations to each channel
	message1 := &protocol.Message{Exchange: "ex1", RoutingKey: "key1", Body: []byte("msg1")}
	message2 := &protocol.Message{Exchange: "ex2", RoutingKey: "key2", Body: []byte("msg2")}

	publishOp1 := NewPublishOperation("ex1", "key1", message1)
	publishOp2 := NewPublishOperation("ex2", "key2", message2)

	err = tm.AddOperation(channel1, publishOp1)
	require.NoError(t, err)
	err = tm.AddOperation(channel2, publishOp2)
	require.NoError(t, err)

	// Commit channel 1, rollback channel 2
	err = tm.Commit(channel1)
	assert.NoError(t, err)
	err = tm.Rollback(channel2)
	assert.NoError(t, err)

	// Only channel 1's operation should have been executed
	assert.Len(t, executor.publishes, 1)
	assert.Equal(t, "ex1", executor.publishes[0].Exchange)

	// Statistics
	stats := tm.GetTransactionStats()
	assert.Equal(t, 2, stats.ActiveTransactions) // Both channels still in transactional mode
	assert.Equal(t, int64(1), stats.TotalCommits)
	assert.Equal(t, int64(1), stats.TotalRollbacks)
}

func TestTransactionManagerOperationWithoutSelect(t *testing.T) {
	tm := NewTransactionManager()
	channelID := uint16(1)

	// Try to add operation without selecting transaction mode
	publishOp := NewPublishOperation("test.exchange", "test.key", &protocol.Message{})
	err := tm.AddOperation(channelID, publishOp)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not in transactional mode")
}

func TestTransactionManagerCommitWithoutSelect(t *testing.T) {
	tm := NewTransactionManager()
	channelID := uint16(1)

	// Try to commit without selecting transaction mode
	err := tm.Commit(channelID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not in transactional mode")
}

func TestTransactionManagerRollbackWithoutSelect(t *testing.T) {
	tm := NewTransactionManager()
	channelID := uint16(1)

	// Try to rollback without selecting transaction mode
	err := tm.Rollback(channelID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not in transactional mode")
}

func TestTransactionManagerNoExecutor(t *testing.T) {
	tm := NewTransactionManager()
	channelID := uint16(1)

	// Start transaction and add operation
	err := tm.Select(channelID)
	require.NoError(t, err)

	publishOp := NewPublishOperation("test.exchange", "test.key", &protocol.Message{})
	err = tm.AddOperation(channelID, publishOp)
	require.NoError(t, err)

	// Try to commit without executor
	err = tm.Commit(channelID)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no transaction executor available")
}

func TestTransactionOperationHelpers(t *testing.T) {
	// Test operation helper functions
	message := &protocol.Message{Exchange: "test", Body: []byte("test")}

	publishOp := NewPublishOperation("exchange", "key", message)
	assert.Equal(t, interfaces.OpPublish, publishOp.Type)
	assert.Equal(t, "exchange", publishOp.Exchange)
	assert.Equal(t, "key", publishOp.RoutingKey)
	assert.Equal(t, message, publishOp.Message)

	ackOp := NewAckOperation("queue", 123, true)
	assert.Equal(t, interfaces.OpAck, ackOp.Type)
	assert.Equal(t, "queue", ackOp.QueueName)
	assert.Equal(t, uint64(123), ackOp.DeliveryTag)
	assert.True(t, ackOp.Multiple)

	nackOp := NewNackOperation("queue", 124, true, false)
	assert.Equal(t, interfaces.OpNack, nackOp.Type)
	assert.Equal(t, "queue", nackOp.QueueName)
	assert.Equal(t, uint64(124), nackOp.DeliveryTag)
	assert.True(t, nackOp.Multiple)
	assert.False(t, nackOp.Requeue)

	rejectOp := NewRejectOperation("queue", 125, true)
	assert.Equal(t, interfaces.OpReject, rejectOp.Type)
	assert.Equal(t, "queue", rejectOp.QueueName)
	assert.Equal(t, uint64(125), rejectOp.DeliveryTag)
	assert.True(t, rejectOp.Requeue)
}

func TestTransactionManagerStatistics(t *testing.T) {
	tm := NewTransactionManager()
	executor := &MockExecutor{}
	tm.SetExecutor(executor)

	channel1 := uint16(1)
	channel2 := uint16(2)

	// Initial stats
	stats := tm.GetTransactionStats()
	assert.Equal(t, 0, stats.ActiveTransactions)
	assert.Equal(t, 0, stats.PendingOperations)

	// Start transactions
	tm.Select(channel1)
	tm.Select(channel2)

	// Add operations
	publishOp := NewPublishOperation("ex", "key", &protocol.Message{})
	ackOp := NewAckOperation("queue", 1, false)

	tm.AddOperation(channel1, publishOp)
	tm.AddOperation(channel1, ackOp)
	tm.AddOperation(channel2, publishOp)

	// Check stats
	stats = tm.GetTransactionStats()
	assert.Equal(t, 2, stats.ActiveTransactions)
	assert.Equal(t, 3, stats.PendingOperations)
	assert.Equal(t, interfaces.TransactionStatePending, stats.ChannelTransactions[channel1])
	assert.Equal(t, interfaces.TransactionStatePending, stats.ChannelTransactions[channel2])
	assert.Equal(t, int64(2), stats.OperationCounts[interfaces.OpPublish])
	assert.Equal(t, int64(1), stats.OperationCounts[interfaces.OpAck])

	// Commit and rollback
	tm.Commit(channel1)
	tm.Rollback(channel2)

	stats = tm.GetTransactionStats()
	assert.Equal(t, int64(1), stats.TotalCommits)
	assert.Equal(t, int64(1), stats.TotalRollbacks)
	assert.Equal(t, 0, stats.PendingOperations) // All operations cleared
}
