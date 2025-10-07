package transaction

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

// DefaultTransactionManager provides the default implementation of TransactionManager
type DefaultTransactionManager struct {
	mutex         sync.RWMutex
	channels      map[uint16]*ChannelTransaction
	executor      interfaces.TransactionExecutor
	stats         *TransactionStats
	atomicStorage interfaces.AtomicStorage // For atomic operations when available
}

// ChannelTransaction holds the transaction state for a single channel
type ChannelTransaction struct {
	channelID  uint16
	state      interfaces.TransactionState
	operations []*interfaces.TransactionOperation
	mutex      sync.RWMutex
}

// TransactionStats holds statistics for the transaction manager
type TransactionStats struct {
	totalCommits    int64
	totalRollbacks  int64
	operationCounts map[interfaces.TransactionOperationType]int64
	operationMutex  sync.RWMutex
}

// NewTransactionManager creates a new default transaction manager
func NewTransactionManager() *DefaultTransactionManager {
	return &DefaultTransactionManager{
		channels: make(map[uint16]*ChannelTransaction),
		stats: &TransactionStats{
			operationCounts: make(map[interfaces.TransactionOperationType]int64),
		},
	}
}

// NewTransactionManagerWithStorage creates a transaction manager with atomic storage support
func NewTransactionManagerWithStorage(atomicStorage interfaces.AtomicStorage) *DefaultTransactionManager {
	tm := NewTransactionManager()
	tm.atomicStorage = atomicStorage
	return tm
}

// SetExecutor sets the executor for performing actual operations
func (tm *DefaultTransactionManager) SetExecutor(executor interfaces.TransactionExecutor) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	tm.executor = executor
}

// Select puts the channel into transactional mode
func (tm *DefaultTransactionManager) Select(channelID uint16) error {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	// Check if channel already has a transaction
	if existing, exists := tm.channels[channelID]; exists {
		if existing.state != interfaces.TransactionStateNone {
			return fmt.Errorf("channel %d already has an active transaction", channelID)
		}
	}

	// Create new transaction for the channel
	tm.channels[channelID] = &ChannelTransaction{
		channelID:  channelID,
		state:      interfaces.TransactionStateActive,
		operations: make([]*interfaces.TransactionOperation, 0),
	}

	return nil
}

// IsTransactional returns true if the channel is in transactional mode
func (tm *DefaultTransactionManager) IsTransactional(channelID uint16) bool {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	if tx, exists := tm.channels[channelID]; exists {
		return tx.state != interfaces.TransactionStateNone
	}
	return false
}

// GetState returns the current transaction state for a channel
func (tm *DefaultTransactionManager) GetState(channelID uint16) interfaces.TransactionState {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	if tx, exists := tm.channels[channelID]; exists {
		tx.mutex.RLock()
		defer tx.mutex.RUnlock()
		return tx.state
	}
	return interfaces.TransactionStateNone
}

// AddOperation adds an operation to the current transaction
func (tm *DefaultTransactionManager) AddOperation(channelID uint16, operation *interfaces.TransactionOperation) error {
	tm.mutex.RLock()
	tx, exists := tm.channels[channelID]
	tm.mutex.RUnlock()

	if !exists {
		return fmt.Errorf("channel %d is not in transactional mode", channelID)
	}

	tx.mutex.Lock()
	defer tx.mutex.Unlock()

	if tx.state != interfaces.TransactionStateActive && tx.state != interfaces.TransactionStatePending {
		return fmt.Errorf("channel %d transaction is not active", channelID)
	}

	// Add operation to pending list
	tx.operations = append(tx.operations, operation)
	tx.state = interfaces.TransactionStatePending

	// Update operation statistics
	tm.stats.operationMutex.Lock()
	tm.stats.operationCounts[operation.Type]++
	tm.stats.operationMutex.Unlock()

	return nil
}

// Commit commits all pending operations in the transaction
func (tm *DefaultTransactionManager) Commit(channelID uint16) error {
	tm.mutex.RLock()
	tx, exists := tm.channels[channelID]
	tm.mutex.RUnlock()

	if !exists {
		return fmt.Errorf("channel %d is not in transactional mode", channelID)
	}

	tx.mutex.Lock()
	defer tx.mutex.Unlock()

	if tx.state == interfaces.TransactionStateNone {
		return fmt.Errorf("channel %d is not in transactional mode", channelID)
	}

	if tm.executor == nil {
		return fmt.Errorf("no transaction executor available")
	}

	// Execute operations atomically if possible
	var err error
	if tm.atomicStorage != nil {
		err = tm.executeAtomically(tx.operations)
	} else {
		err = tm.executeSequentially(tx.operations)
	}

	if err != nil {
		return fmt.Errorf("failed to commit transaction for channel %d: %w", channelID, err)
	}

	// Clear operations and reset state to active (still in transactional mode)
	tx.operations = tx.operations[:0]
	tx.state = interfaces.TransactionStateActive

	// Update statistics
	atomic.AddInt64(&tm.stats.totalCommits, 1)

	return nil
}

// Rollback discards all pending operations in the transaction
func (tm *DefaultTransactionManager) Rollback(channelID uint16) error {
	tm.mutex.RLock()
	tx, exists := tm.channels[channelID]
	tm.mutex.RUnlock()

	if !exists {
		return fmt.Errorf("channel %d is not in transactional mode", channelID)
	}

	tx.mutex.Lock()
	defer tx.mutex.Unlock()

	if tx.state == interfaces.TransactionStateNone {
		return fmt.Errorf("channel %d is not in transactional mode", channelID)
	}

	// Simply discard all pending operations
	tx.operations = tx.operations[:0]
	tx.state = interfaces.TransactionStateActive

	// Update statistics
	atomic.AddInt64(&tm.stats.totalRollbacks, 1)

	return nil
}

// Close cleans up transaction state for a channel
func (tm *DefaultTransactionManager) Close(channelID uint16) error {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	delete(tm.channels, channelID)
	return nil
}

// GetPendingOperations returns all pending operations for debugging/introspection
func (tm *DefaultTransactionManager) GetPendingOperations(channelID uint16) ([]*interfaces.TransactionOperation, error) {
	tm.mutex.RLock()
	tx, exists := tm.channels[channelID]
	tm.mutex.RUnlock()

	if !exists {
		return nil, fmt.Errorf("channel %d is not in transactional mode", channelID)
	}

	tx.mutex.RLock()
	defer tx.mutex.RUnlock()

	// Return a copy to prevent external modification
	operations := make([]*interfaces.TransactionOperation, len(tx.operations))
	copy(operations, tx.operations)
	return operations, nil
}

// ExecuteAtomic executes operations atomically using the underlying storage
func (tm *DefaultTransactionManager) ExecuteAtomic(channelID uint16, operations func() error) error {
	if tm.atomicStorage == nil {
		return operations()
	}

	return tm.atomicStorage.ExecuteAtomic(func(txnStorage interfaces.Storage) error {
		return operations()
	})
}

// executeAtomically executes all operations within a single atomic transaction
func (tm *DefaultTransactionManager) executeAtomically(operations []*interfaces.TransactionOperation) error {
	return tm.atomicStorage.ExecuteAtomic(func(txnStorage interfaces.Storage) error {
		return tm.executeSequentially(operations)
	})
}

// executeSequentially executes operations one by one
func (tm *DefaultTransactionManager) executeSequentially(operations []*interfaces.TransactionOperation) error {
	for _, op := range operations {
		switch op.Type {
		case interfaces.OpPublish:
			err := tm.executor.ExecutePublish(op.Exchange, op.RoutingKey, op.Message)
			if err != nil {
				return fmt.Errorf("failed to execute publish: %w", err)
			}

		case interfaces.OpAck:
			err := tm.executor.ExecuteAck(op.QueueName, op.DeliveryTag, op.Multiple)
			if err != nil {
				return fmt.Errorf("failed to execute ack: %w", err)
			}

		case interfaces.OpNack:
			err := tm.executor.ExecuteNack(op.QueueName, op.DeliveryTag, op.Multiple, op.Requeue)
			if err != nil {
				return fmt.Errorf("failed to execute nack: %w", err)
			}

		case interfaces.OpReject:
			err := tm.executor.ExecuteReject(op.QueueName, op.DeliveryTag, op.Requeue)
			if err != nil {
				return fmt.Errorf("failed to execute reject: %w", err)
			}

		default:
			return fmt.Errorf("unknown transaction operation type: %d", op.Type)
		}
	}

	return nil
}

// GetTransactionStats returns current transaction statistics
func (tm *DefaultTransactionManager) GetTransactionStats() *interfaces.TransactionStats {
	tm.mutex.RLock()
	defer tm.mutex.RUnlock()

	// Count active transactions and build channel state map
	activeCount := 0
	channelStates := make(map[uint16]interfaces.TransactionState)
	pendingOpsCount := 0

	for channelID, tx := range tm.channels {
		tx.mutex.RLock()
		state := tx.state
		opsCount := len(tx.operations)
		tx.mutex.RUnlock()

		channelStates[channelID] = state
		if state != interfaces.TransactionStateNone {
			activeCount++
		}
		pendingOpsCount += opsCount
	}

	// Copy operation counts
	tm.stats.operationMutex.RLock()
	opCounts := make(map[interfaces.TransactionOperationType]int64)
	for opType, count := range tm.stats.operationCounts {
		opCounts[opType] = count
	}
	tm.stats.operationMutex.RUnlock()

	return &interfaces.TransactionStats{
		ActiveTransactions:  activeCount,
		TotalCommits:        atomic.LoadInt64(&tm.stats.totalCommits),
		TotalRollbacks:      atomic.LoadInt64(&tm.stats.totalRollbacks),
		PendingOperations:   pendingOpsCount,
		ChannelTransactions: channelStates,
		OperationCounts:     opCounts,
	}
}

// Helper function to create transaction operations
func NewPublishOperation(exchange, routingKey string, message *protocol.Message) *interfaces.TransactionOperation {
	return &interfaces.TransactionOperation{
		Type:       interfaces.OpPublish,
		Exchange:   exchange,
		RoutingKey: routingKey,
		Message:    message,
	}
}

func NewAckOperation(queueName string, deliveryTag uint64, multiple bool) *interfaces.TransactionOperation {
	return &interfaces.TransactionOperation{
		Type:        interfaces.OpAck,
		QueueName:   queueName,
		DeliveryTag: deliveryTag,
		Multiple:    multiple,
	}
}

func NewNackOperation(queueName string, deliveryTag uint64, multiple, requeue bool) *interfaces.TransactionOperation {
	return &interfaces.TransactionOperation{
		Type:        interfaces.OpNack,
		QueueName:   queueName,
		DeliveryTag: deliveryTag,
		Multiple:    multiple,
		Requeue:     requeue,
	}
}

func NewRejectOperation(queueName string, deliveryTag uint64, requeue bool) *interfaces.TransactionOperation {
	return &interfaces.TransactionOperation{
		Type:        interfaces.OpReject,
		QueueName:   queueName,
		DeliveryTag: deliveryTag,
		Requeue:     requeue,
	}
}
