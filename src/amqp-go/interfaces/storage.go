package interfaces

import "github.com/maxpert/amqp-go/protocol"

// Storage defines the interface for message and metadata persistence
type Storage interface {
	MessageStore
	MetadataStore
	TransactionStore
}

// MessageStore defines the interface for message durability operations
type MessageStore interface {
	// StoreMessage persists a message with optional durability settings
	StoreMessage(queueName string, message *protocol.Message, durable bool) error
	
	// GetMessage retrieves the next message from a queue
	GetMessage(queueName string) (*protocol.Message, error)
	
	// DeleteMessage removes a message from storage after acknowledgment
	DeleteMessage(queueName string, messageID string) error
	
	// GetMessageCount returns the number of messages in a queue
	GetMessageCount(queueName string) (int, error)
	
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
	DeleteBinding(queueName, exchangeName, routingKey string) error
	GetBindings(queueName string) ([]protocol.Binding, error)
	GetExchangeBindings(exchangeName string) ([]protocol.Binding, error)
}

// TransactionStore defines the interface for transactional operations
type TransactionStore interface {
	// Begin starts a new transaction
	BeginTransaction(connectionID, channelID string) (TransactionID, error)
	
	// Commit commits all operations in a transaction
	CommitTransaction(txID TransactionID) error
	
	// Rollback rolls back all operations in a transaction
	RollbackTransaction(txID TransactionID) error
	
	// AddToTransaction adds an operation to a transaction
	AddToTransaction(txID TransactionID, op TransactionOperation) error
}

// TransactionID represents a unique transaction identifier
type TransactionID string

// TransactionOperation represents an operation within a transaction
type TransactionOperation struct {
	Type      string      // "publish", "ack", "nack", etc.
	QueueName string      
	MessageID string      
	Data      interface{} // Operation-specific data
}