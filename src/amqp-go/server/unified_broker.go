package server

import "github.com/maxpert/amqp-go/protocol"

// UnifiedBroker defines the interface that both broker implementations must satisfy
// This allows the Server to work with either the original in-memory broker or the storage-backed broker
type UnifiedBroker interface {
	// Exchange operations
	DeclareExchange(name, exchangeType string, durable, autoDelete, internal bool, arguments map[string]interface{}) error
	DeleteExchange(name string, ifUnused bool) error

	// Queue operations
	DeclareQueue(name string, durable, autoDelete, exclusive bool, arguments map[string]interface{}) (*protocol.Queue, error)
	DeleteQueue(name string, ifUnused, ifEmpty bool) error

	// Binding operations
	BindQueue(queueName, exchangeName, routingKey string, arguments map[string]interface{}) error
	UnbindQueue(queueName, exchangeName, routingKey string) error

	// Message operations
	PublishMessage(exchangeName, routingKey string, message *protocol.Message) error

	// Consumer operations
	RegisterConsumer(queueName, consumerTag string, consumer *protocol.Consumer) error
	UnregisterConsumer(consumerTag string) error

	// Acknowledgment operations
	AcknowledgeMessage(consumerTag string, deliveryTag uint64, multiple bool) error
	RejectMessage(consumerTag string, deliveryTag uint64, requeue bool) error
	NacknowledgeMessage(consumerTag string, deliveryTag uint64, multiple, requeue bool) error

	// Management operations (optional for some implementations)
	GetQueues() map[string]*protocol.Queue
	GetExchanges() map[string]*protocol.Exchange
	GetConsumers() map[string]*protocol.Consumer
}
