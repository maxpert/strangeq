package transaction

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/maxpert/amqp-go/broker"
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
	// undos restore the channel-side state consumed when an operation was
	// buffered (WS-D §5.4/§6: wire-tag table entries taken at buffer time).
	// They run in REVERSE order on rollback and on a failed commit; a
	// successful commit clears them without running; Close drops them.
	undos []func()
	mutex sync.RWMutex
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

// AddOperationWithUndo buffers op and registers undo, which restores the
// channel-side state consumed when op was buffered (wire-tag table entry,
// WS-D §6). It behaves like AddOperation; on error it runs undo itself and
// returns.
func (tm *DefaultTransactionManager) AddOperationWithUndo(channelID uint16, op *interfaces.TransactionOperation, undo func()) error {
	tm.mutex.RLock()
	tx, exists := tm.channels[channelID]
	tm.mutex.RUnlock()

	if !exists {
		undo()
		return fmt.Errorf("channel %d is not in transactional mode", channelID)
	}

	tx.mutex.Lock()
	defer tx.mutex.Unlock()

	if tx.state != interfaces.TransactionStateActive && tx.state != interfaces.TransactionStatePending {
		undo()
		return fmt.Errorf("channel %d transaction is not active", channelID)
	}

	tx.operations = append(tx.operations, op)
	tx.undos = append(tx.undos, undo)
	tx.state = interfaces.TransactionStatePending

	tm.stats.operationMutex.Lock()
	tm.stats.operationCounts[op.Type]++
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
		tx.operations = tx.operations[:0]
		tx.state = interfaces.TransactionStateNone
		return fmt.Errorf("no transaction executor available")
	}

	var err error
	if tm.atomicStorage != nil {
		err = tm.executeAtomically(tx.operations)
	} else {
		err = tm.executeSequentially(tx.operations)
	}

	tx.operations = tx.operations[:0]
	undos := tx.undos
	tx.undos = nil

	if err != nil {
		// WS-D §5.4: a FAILED commit restores the channel-side state consumed
		// at buffer time (wire-tag entries), in reverse order; the server then
		// closes the channel.
		for i := len(undos) - 1; i >= 0; i-- {
			undos[i]()
		}
		tx.state = interfaces.TransactionStateNone
		return fmt.Errorf("failed to commit transaction for channel %d: %w", channelID, err)
	}

	// Success: undos are dropped WITHOUT running (the buffered state was
	// genuinely consumed by the commit).
	tx.state = interfaces.TransactionStateActive

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

	// Discard all pending operations, restoring the channel-side state
	// consumed at buffer time (WS-D §5.4: undos run in REVERSE order).
	undos := tx.undos
	tx.operations = tx.operations[:0]
	tx.undos = nil
	for i := len(undos) - 1; i >= 0; i-- {
		undos[i]()
	}
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

// txStagingExecutor is implemented by executors that can route a publish through
// a transactional storage staging view (SQ-8).
type txStagingExecutor interface {
	ExecutePublishStaged(txnStore interfaces.Storage, exchange, routingKey string, message *protocol.Message) (deferred []func(bool), err error)
}

// executeAtomically executes all operations within a single all-or-nothing
// transaction (SQ-8). Publishes are routed through the storage staging view so
// their durable writes and ring inserts are committed atomically (bracketed by
// WAL transaction-boundary markers); their consumer-visibility side effects are
// deferred. On a successful commit, the deferred closures run with commit=true
// (FrontierComplete(tag, true) makes each message visible). If any operation
// fails, ExecuteAtomic rolls back the whole buffered unit and the deferred
// closures run with commit=false (FrontierComplete(tag, false) releases each
// reserved frontier slot so the tag is gap-skipped, never stranded) — nothing
// published in the transaction becomes durable or visible.
//
// WS-D §5: settlements (acks/nacks/rejects) are NOT applied inside the atomic
// scope — an acknowledgement is never a durable record, so it lands nowhere in
// the WAL unit; applying it against the LIVE broker inside the scope meant a
// later op's failure left the settlement applied while the publishes aborted
// (and txStagingStorage.DeleteMessage was dead code). Settlements are COLLECTED
// during staging and applied AFTER the unit commits, in RabbitMQ's commit order
// (R14): publishes visible first (deferred(true)), then settlements in buffered
// order. A crash between the WAL commit and the settlement application is
// exactly as durable as any non-tx ack today (at-least-once redelivery).
func (tm *DefaultTransactionManager) executeAtomically(operations []*interfaces.TransactionOperation) error {
	var deferred []func(bool)
	var settles []*interfaces.TransactionOperation

	err := tm.atomicStorage.ExecuteAtomic(func(txnStorage interfaces.Storage) error {
		return tm.executeStaged(txnStorage, operations, &deferred, &settles)
	})
	if err != nil {
		for _, apply := range deferred {
			apply(false)
		}
		return err // settlements never touched the broker
	}

	for _, apply := range deferred {
		apply(true) // publishes visible first (R14 order)
	}
	return tm.applySettlements(settles)
}

// applySettlements applies the transaction's settlement operations against the
// LIVE broker, in buffered order. Every op is attempted; the first error is
// returned wrapped — the server treats it as a commit failure (channel closed;
// teardown requeues anything still unacked). In practice the broker methods
// return nil, and the broker's LoadAndDelete settle paths make a stale
// settlement (e.g. a tag already requeued by a consumer cancel) a no-op.
func (tm *DefaultTransactionManager) applySettlements(settles []*interfaces.TransactionOperation) error {
	for _, op := range settles {
		var err error
		switch op.Type {
		case interfaces.OpAck:
			err = tm.executor.ExecuteAck(op.ConsumerTag, op.DeliveryTag, op.Multiple)
		case interfaces.OpNack:
			err = tm.executor.ExecuteNack(op.ConsumerTag, op.DeliveryTag, op.Multiple, op.Requeue)
		case interfaces.OpReject:
			err = tm.executor.ExecuteReject(op.ConsumerTag, op.DeliveryTag, op.Requeue)
		}
		if err != nil {
			return fmt.Errorf("transaction committed but settlement failed: %w", err)
		}
	}
	return nil
}

// executeStaged runs the transaction's operations against the staging storage.
// Publishes use the staged path (deferred visibility) when the executor
// supports it; if not, they fall back to immediate execution. Settlements are
// COLLECTED (never executed here — see executeAtomically's doc).
func (tm *DefaultTransactionManager) executeStaged(txnStorage interfaces.Storage, operations []*interfaces.TransactionOperation, deferred *[]func(bool), settles *[]*interfaces.TransactionOperation) error {
	stager, canStage := tm.executor.(txStagingExecutor)

	for _, op := range operations {
		switch op.Type {
		case interfaces.OpPublish:
			if canStage {
				d, err := stager.ExecutePublishStaged(txnStorage, op.Exchange, op.RoutingKey, op.Message)
				if err != nil {
					if errors.Is(err, errTxStagingUnsupported) {
						// Executor advertises no staging for this broker: fall
						// back to immediate publish for the rest of this op.
						canStage = false
						if perr := tm.executePublishInline(op); perr != nil {
							return perr
						}
						continue
					}
					if !errors.Is(err, broker.ErrNoRoute) {
						return fmt.Errorf("failed to execute publish: %w", err)
					}
					continue
				}
				*deferred = append(*deferred, d...)
				continue
			}
			if err := tm.executePublishInline(op); err != nil {
				return err
			}

		case interfaces.OpAck, interfaces.OpNack, interfaces.OpReject:
			*settles = append(*settles, op)

		default:
			return fmt.Errorf("unknown transaction operation type: %d", op.Type)
		}
	}
	return nil
}

// executePublishInline publishes immediately (non-staged fallback), tolerating
// an unroutable message exactly as executeSequentially does.
func (tm *DefaultTransactionManager) executePublishInline(op *interfaces.TransactionOperation) error {
	err := tm.executor.ExecutePublish(op.Exchange, op.RoutingKey, op.Message)
	if err != nil && !errors.Is(err, broker.ErrNoRoute) {
		return fmt.Errorf("failed to execute publish: %w", err)
	}
	return nil
}

// executeSequentially executes operations one by one (non-atomic manager).
// WS-D §5.3: pass 1 executes all OpPublish; pass 2 applies the settlement ops
// in buffered order via applySettlements — a publish failure in pass 1 means
// NO settlement is applied (previously a settlement buffered before a failing
// publish was applied anyway).
func (tm *DefaultTransactionManager) executeSequentially(operations []*interfaces.TransactionOperation) error {
	var settles []*interfaces.TransactionOperation
	for _, op := range operations {
		switch op.Type {
		case interfaces.OpPublish:
			err := tm.executor.ExecutePublish(op.Exchange, op.RoutingKey, op.Message)
			if err != nil && !errors.Is(err, broker.ErrNoRoute) {
				return fmt.Errorf("failed to execute publish: %w", err)
			}

		case interfaces.OpAck, interfaces.OpNack, interfaces.OpReject:
			settles = append(settles, op)

		default:
			return fmt.Errorf("unknown transaction operation type: %d", op.Type)
		}
	}

	return tm.applySettlements(settles)
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

func NewAckOperation(consumerID string, deliveryTag uint64, multiple bool) *interfaces.TransactionOperation {
	return &interfaces.TransactionOperation{
		Type:        interfaces.OpAck,
		ConsumerTag: consumerID,
		DeliveryTag: deliveryTag,
		Multiple:    multiple,
	}
}

func NewNackOperation(consumerID string, deliveryTag uint64, multiple, requeue bool) *interfaces.TransactionOperation {
	return &interfaces.TransactionOperation{
		Type:        interfaces.OpNack,
		ConsumerTag: consumerID,
		DeliveryTag: deliveryTag,
		Multiple:    multiple,
		Requeue:     requeue,
	}
}

func NewRejectOperation(consumerID string, deliveryTag uint64, requeue bool) *interfaces.TransactionOperation {
	return &interfaces.TransactionOperation{
		Type:        interfaces.OpReject,
		ConsumerTag: consumerID,
		DeliveryTag: deliveryTag,
		Requeue:     requeue,
	}
}
