package interfaces

import "github.com/maxpert/amqp-go/protocol"

// Broker defines the interface for message routing and queue management
type Broker interface {
	// Exchange operations
	DeclareExchange(name, exchangeType string, durable, autoDelete, internal bool, arguments map[string]interface{}) error
	DeleteExchange(name string, ifUnused bool) error

	// Queue operations
	DeclareQueue(name string, durable, autoDelete, exclusive bool, arguments map[string]interface{}) (*protocol.Queue, error)
	DeleteQueue(name string, ifUnused, ifEmpty bool) error
	PurgeQueue(name string) (int, error)

	// Binding operations
	BindQueue(queueName, exchangeName, routingKey string, arguments map[string]interface{}) error
	UnbindQueue(queueName, exchangeName, routingKey string) error

	// Message operations
	PublishMessage(exchangeName, routingKey string, message *protocol.Message) error
	GetMessage(queueName string) (*protocol.Message, error)
	AckMessage(queueName, messageID string) error
	NackMessage(queueName, messageID string, requeue bool) error

	// Consumer operations
	RegisterConsumer(queueName, consumerTag string, consumer *protocol.Consumer) error
	UnregisterConsumer(consumerTag string) error
	GetConsumers(queueName string) []*protocol.Consumer

	// Statistics and monitoring
	GetQueueInfo(queueName string) (*QueueInfo, error)
	GetExchangeInfo(exchangeName string) (*ExchangeInfo, error)
	GetBrokerStats() (*BrokerStats, error)
}

// QueueInfo provides information about a queue
type QueueInfo struct {
	Name          string
	MessageCount  int
	ConsumerCount int
	Durable       bool
	AutoDelete    bool
	Exclusive     bool
}

// ExchangeInfo provides information about an exchange
type ExchangeInfo struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	Internal   bool
}

// BrokerStats provides overall broker statistics
type BrokerStats struct {
	ExchangeCount      int
	QueueCount         int
	ConnectionCount    int
	ConsumerCount      int
	TotalMessages      int64
	TotalMessagesReady int64
}
