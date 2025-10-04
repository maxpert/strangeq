package interfaces

import (
	"errors"
	"time"
	
	"github.com/maxpert/amqp-go/protocol"
)

// AtomicStorage interface for atomic operations
type AtomicStorage interface {
	// ExecuteAtomic runs multiple operations atomically
	ExecuteAtomic(operations func(txnStorage Storage) error) error
}

// Storage defines the interface for message and metadata persistence
type Storage interface {
	MessageStore
	MetadataStore
	TransactionStore
	AcknowledgmentStore
	DurabilityStore
	AtomicStorage
}

// MessageStore defines the interface for message durability operations
type MessageStore interface {
	// StoreMessage persists a message
	StoreMessage(queueName string, message *protocol.Message) error
	
	// GetMessage retrieves a specific message by delivery tag
	GetMessage(queueName string, deliveryTag uint64) (*protocol.Message, error)
	
	// DeleteMessage removes a message from storage after acknowledgment
	DeleteMessage(queueName string, deliveryTag uint64) error
	
	// GetQueueMessages retrieves all messages for a queue
	GetQueueMessages(queueName string) ([]*protocol.Message, error)
	
	// GetQueueMessageCount returns the number of messages in a queue
	GetQueueMessageCount(queueName string) (int, error)
	
	// PurgeQueue removes all messages from a queue
	PurgeQueue(queueName string) (int, error)
}

// MetadataStore defines the interface for exchanges, queues, and bindings persistence
type MetadataStore interface {
	// Exchange operations
	StoreExchange(exchange *protocol.Exchange) error
	GetExchange(name string) (*protocol.Exchange, error)
	DeleteExchange(name string) error
	ListExchanges() ([]*protocol.Exchange, error)
	
	// Queue operations
	StoreQueue(queue *protocol.Queue) error
	GetQueue(name string) (*protocol.Queue, error)
	DeleteQueue(name string) error
	ListQueues() ([]*protocol.Queue, error)
	
	// Binding operations
	StoreBinding(queueName, exchangeName, routingKey string, arguments map[string]interface{}) error
	GetBinding(queueName, exchangeName, routingKey string) (*QueueBinding, error)
	DeleteBinding(queueName, exchangeName, routingKey string) error
	GetQueueBindings(queueName string) ([]*QueueBinding, error)
	GetExchangeBindings(exchangeName string) ([]*QueueBinding, error)
	
	// Consumer operations
	StoreConsumer(queueName, consumerTag string, consumer *protocol.Consumer) error
	GetConsumer(queueName, consumerTag string) (*protocol.Consumer, error)
	DeleteConsumer(queueName, consumerTag string) error
	GetQueueConsumers(queueName string) ([]*protocol.Consumer, error)
}

// TransactionStore defines the interface for transactional operations
type TransactionStore interface {
	// BeginTransaction starts a new transaction
	BeginTransaction(txID string) (*Transaction, error)
	
	// GetTransaction retrieves a transaction
	GetTransaction(txID string) (*Transaction, error)
	
	// AddAction adds an action to a transaction
	AddAction(txID string, action *TransactionAction) error
	
	// CommitTransaction commits all operations in a transaction
	CommitTransaction(txID string) error
	
	// RollbackTransaction rolls back all operations in a transaction
	RollbackTransaction(txID string) error
	
	// DeleteTransaction removes a transaction from storage
	DeleteTransaction(txID string) error
	
	// ListActiveTransactions returns all active transactions
	ListActiveTransactions() ([]*Transaction, error)
}

// QueueBinding represents a queue binding
type QueueBinding struct {
	QueueName    string                 `json:"queue_name"`
	ExchangeName string                 `json:"exchange_name"`
	RoutingKey   string                 `json:"routing_key"`
	Arguments    map[string]interface{} `json:"arguments"`
}

// Transaction represents an AMQP transaction
type Transaction struct {
	ID        string               `json:"id"`
	Status    TransactionStatus    `json:"status"`
	StartTime time.Time           `json:"start_time"`
	EndTime   time.Time           `json:"end_time,omitempty"`
	Actions   []*TransactionAction `json:"actions"`
}

// TransactionStatus represents the status of a transaction
type TransactionStatus string

const (
	TxStatusActive     TransactionStatus = "active"
	TxStatusCommitted  TransactionStatus = "committed"
	TxStatusRolledBack TransactionStatus = "rolled_back"
)

// TransactionAction represents an action within a transaction
type TransactionAction struct {
	Type      string                 `json:"type"`       // "publish", "ack", "nack", etc.
	QueueName string                 `json:"queue_name"` 
	MessageID string                 `json:"message_id"` 
	Data      map[string]interface{} `json:"data"`       // Operation-specific data
}

// StorageStats represents storage statistics
type StorageStats struct {
	TotalMessages    int   `json:"total_messages"`
	TotalSize        int64 `json:"total_size"`
	LSMSize          int64 `json:"lsm_size"`
	ValueLogSize     int64 `json:"value_log_size"`
	PendingWrites    int   `json:"pending_writes"`
	ExchangeCount    int   `json:"exchange_count"`
	QueueCount       int   `json:"queue_count"`
	BindingCount     int   `json:"binding_count"`
	ConsumerCount    int   `json:"consumer_count"`
}

// MetadataStats represents metadata storage statistics
type MetadataStats struct {
	ExchangeCount int `json:"exchange_count"`
	QueueCount    int `json:"queue_count"`
	BindingCount  int `json:"binding_count"`
	ConsumerCount int `json:"consumer_count"`
}

// Storage error types
var (
	ErrMessageNotFound     = errors.New("message not found")
	ErrExchangeNotFound    = errors.New("exchange not found")
	ErrQueueNotFound       = errors.New("queue not found")
	ErrBindingNotFound     = errors.New("binding not found")
	ErrConsumerNotFound    = errors.New("consumer not found")
	ErrTransactionNotFound = errors.New("transaction not found")
	ErrTransactionExists   = errors.New("transaction already exists")
	ErrTransactionNotActive = errors.New("transaction is not active")
	ErrPendingAckNotFound  = errors.New("pending acknowledgment not found")
	ErrCorruptedStorage    = errors.New("storage corruption detected")
	ErrRecoveryInProgress  = errors.New("recovery operation in progress")
)

// AcknowledgmentStore interface for handling message acknowledgment persistence
type AcknowledgmentStore interface {
	// Pending acknowledgment management
	StorePendingAck(pendingAck *protocol.PendingAck) error
	GetPendingAck(queueName string, deliveryTag uint64) (*protocol.PendingAck, error)
	DeletePendingAck(queueName string, deliveryTag uint64) error
	
	// Queue-level acknowledgment operations
	GetQueuePendingAcks(queueName string) ([]*protocol.PendingAck, error)
	GetConsumerPendingAcks(consumerTag string) ([]*protocol.PendingAck, error)
	
	// Cleanup operations
	CleanupExpiredAcks(maxAge time.Duration) error
	GetAllPendingAcks() ([]*protocol.PendingAck, error)
}

// DurabilityStore interface for handling durability and recovery operations
type DurabilityStore interface {
	// Durable entity recovery
	StoreDurableEntityMetadata(metadata *protocol.DurableEntityMetadata) error
	GetDurableEntityMetadata() (*protocol.DurableEntityMetadata, error)
	
	// Storage validation and repair
	ValidateStorageIntegrity() (*protocol.RecoveryStats, error)
	RepairCorruption(autoRepair bool) (*protocol.RecoveryStats, error)
	
	// Recovery operations
	GetRecoverableMessages() (map[string][]*protocol.Message, error) // queueName -> messages
	MarkRecoveryComplete(stats *protocol.RecoveryStats) error
}