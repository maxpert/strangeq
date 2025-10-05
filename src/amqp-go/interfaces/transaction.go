package interfaces

import "github.com/maxpert/amqp-go/protocol"

// TransactionState represents the current state of a transaction
type TransactionState int

const (
	// TransactionStateNone indicates no active transaction
	TransactionStateNone TransactionState = iota
	// TransactionStateActive indicates an active transaction that can accept operations
	TransactionStateActive
	// TransactionStatePending indicates operations are queued but not yet committed
	TransactionStatePending
)

// TransactionOperation represents a single operation within a transaction
type TransactionOperation struct {
	Type       TransactionOperationType
	Exchange   string
	RoutingKey string
	Message    *protocol.Message
	QueueName  string
	DeliveryTag uint64
	Multiple   bool
	Requeue    bool
}

// TransactionOperationType defines the types of operations that can be transactional
type TransactionOperationType int

const (
	// OpPublish represents a message publish operation
	OpPublish TransactionOperationType = iota
	// OpAck represents a message acknowledgment operation
	OpAck
	// OpNack represents a negative acknowledgment operation
	OpNack
	// OpReject represents a message rejection operation
	OpReject
)

// TransactionHandler defines the interface for managing channel-level transactions
type TransactionHandler interface {
	// Select puts the channel into transactional mode
	Select(channelID uint16) error
	
	// IsTransactional returns true if the channel is in transactional mode
	IsTransactional(channelID uint16) bool
	
	// GetState returns the current transaction state for a channel
	GetState(channelID uint16) TransactionState
	
	// AddOperation adds an operation to the current transaction
	AddOperation(channelID uint16, operation *TransactionOperation) error
	
	// Commit commits all pending operations in the transaction
	Commit(channelID uint16) error
	
	// Rollback discards all pending operations in the transaction
	Rollback(channelID uint16) error
	
	// Close cleans up transaction state for a channel
	Close(channelID uint16) error
	
	// GetPendingOperations returns all pending operations for debugging/introspection
	GetPendingOperations(channelID uint16) ([]*TransactionOperation, error)
}

// TransactionExecutor defines the interface for executing transactional operations
type TransactionExecutor interface {
	// ExecutePublish executes a message publish operation
	ExecutePublish(exchange, routingKey string, message *protocol.Message) error
	
	// ExecuteAck executes a message acknowledgment operation
	ExecuteAck(queueName string, deliveryTag uint64, multiple bool) error
	
	// ExecuteNack executes a negative acknowledgment operation
	ExecuteNack(queueName string, deliveryTag uint64, multiple, requeue bool) error
	
	// ExecuteReject executes a message rejection operation
	ExecuteReject(queueName string, deliveryTag uint64, requeue bool) error
}

// TransactionManager coordinates transaction operations with the broker
type TransactionManager interface {
	TransactionHandler
	
	// SetExecutor sets the executor for performing actual operations
	SetExecutor(executor TransactionExecutor)
	
	// ExecuteAtomic executes operations atomically using the underlying storage
	ExecuteAtomic(channelID uint16, operations func() error) error
	
	// GetTransactionStats returns current transaction statistics
	GetTransactionStats() *TransactionStats
}

// TransactionStats provides statistics about transaction usage
type TransactionStats struct {
	ActiveTransactions   int                            `json:"active_transactions"`
	TotalCommits        int64                          `json:"total_commits"`
	TotalRollbacks      int64                          `json:"total_rollbacks"`
	PendingOperations   int                            `json:"pending_operations"`
	ChannelTransactions map[uint16]TransactionState    `json:"channel_transactions"`
	OperationCounts     map[TransactionOperationType]int64 `json:"operation_counts"`
}

// Transactional interface marks broker implementations that support transactions
type Transactional interface {
	// GetTransactionManager returns the transaction manager for this broker
	GetTransactionManager() TransactionManager
	
	// GetTransactionStats returns current transaction statistics
	GetTransactionStats() *TransactionStats
}