package server

import (
	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/protocol"
)

// OriginalBrokerAdapter wraps the original in-memory broker to implement UnifiedBroker
type OriginalBrokerAdapter struct {
	broker *broker.Broker
}

// NewOriginalBrokerAdapter creates an adapter for the original broker
func NewOriginalBrokerAdapter(b *broker.Broker) UnifiedBroker {
	return &OriginalBrokerAdapter{broker: b}
}

func (a *OriginalBrokerAdapter) DeclareExchange(name, exchangeType string, durable, autoDelete, internal bool, arguments map[string]interface{}) error {
	return a.broker.DeclareExchange(name, exchangeType, durable, autoDelete, internal, arguments)
}

func (a *OriginalBrokerAdapter) DeleteExchange(name string, ifUnused bool) error {
	return a.broker.DeleteExchange(name, ifUnused)
}

func (a *OriginalBrokerAdapter) DeclareQueue(name string, durable, autoDelete, exclusive bool, arguments map[string]interface{}) (*protocol.Queue, error) {
	return a.broker.DeclareQueue(name, durable, autoDelete, exclusive, arguments)
}

func (a *OriginalBrokerAdapter) DeleteQueue(name string, ifUnused, ifEmpty bool) error {
	return a.broker.DeleteQueue(name, ifUnused, ifEmpty)
}

func (a *OriginalBrokerAdapter) BindQueue(queueName, exchangeName, routingKey string, arguments map[string]interface{}) error {
	return a.broker.BindQueue(queueName, exchangeName, routingKey, arguments)
}

func (a *OriginalBrokerAdapter) UnbindQueue(queueName, exchangeName, routingKey string) error {
	return a.broker.UnbindQueue(queueName, exchangeName, routingKey)
}

func (a *OriginalBrokerAdapter) PublishMessage(exchangeName, routingKey string, message *protocol.Message) error {
	return a.broker.PublishMessage(exchangeName, routingKey, message)
}

func (a *OriginalBrokerAdapter) RegisterConsumer(queueName, consumerTag string, consumer *protocol.Consumer) error {
	return a.broker.RegisterConsumer(queueName, consumerTag, consumer)
}

func (a *OriginalBrokerAdapter) UnregisterConsumer(consumerTag string) error {
	return a.broker.UnregisterConsumer(consumerTag)
}

func (a *OriginalBrokerAdapter) AcknowledgeMessage(consumerTag string, deliveryTag uint64, multiple bool) error {
	return a.broker.AcknowledgeMessage(consumerTag, deliveryTag, multiple)
}

func (a *OriginalBrokerAdapter) RejectMessage(consumerTag string, deliveryTag uint64, requeue bool) error {
	return a.broker.RejectMessage(consumerTag, deliveryTag, requeue)
}

func (a *OriginalBrokerAdapter) NacknowledgeMessage(consumerTag string, deliveryTag uint64, multiple, requeue bool) error {
	return a.broker.NacknowledgeMessage(consumerTag, deliveryTag, multiple, requeue)
}

func (a *OriginalBrokerAdapter) GetQueues() map[string]*protocol.Queue {
	return a.broker.Queues
}

func (a *OriginalBrokerAdapter) GetExchanges() map[string]*protocol.Exchange {
	return a.broker.Exchanges
}

func (a *OriginalBrokerAdapter) GetConsumers() map[string]*protocol.Consumer {
	return a.broker.Consumers
}

// StorageBrokerAdapter wraps the storage-backed broker to implement UnifiedBroker
type StorageBrokerAdapter struct {
	broker *broker.StorageBroker
}

// NewStorageBrokerAdapter creates an adapter for the storage broker
func NewStorageBrokerAdapter(b *broker.StorageBroker) UnifiedBroker {
	return &StorageBrokerAdapter{broker: b}
}

func (a *StorageBrokerAdapter) DeclareExchange(name, exchangeType string, durable, autoDelete, internal bool, arguments map[string]interface{}) error {
	return a.broker.DeclareExchange(name, exchangeType, durable, autoDelete, internal, arguments)
}

func (a *StorageBrokerAdapter) DeleteExchange(name string, ifUnused bool) error {
	return a.broker.DeleteExchange(name, ifUnused)
}

func (a *StorageBrokerAdapter) DeclareQueue(name string, durable, autoDelete, exclusive bool, arguments map[string]interface{}) (*protocol.Queue, error) {
	return a.broker.DeclareQueue(name, durable, autoDelete, exclusive, arguments)
}

func (a *StorageBrokerAdapter) DeleteQueue(name string, ifUnused, ifEmpty bool) error {
	return a.broker.DeleteQueue(name, ifUnused, ifEmpty)
}

func (a *StorageBrokerAdapter) BindQueue(queueName, exchangeName, routingKey string, arguments map[string]interface{}) error {
	return a.broker.BindQueue(queueName, exchangeName, routingKey, arguments)
}

func (a *StorageBrokerAdapter) UnbindQueue(queueName, exchangeName, routingKey string) error {
	return a.broker.UnbindQueue(queueName, exchangeName, routingKey)
}

func (a *StorageBrokerAdapter) PublishMessage(exchangeName, routingKey string, message *protocol.Message) error {
	return a.broker.PublishMessage(exchangeName, routingKey, message)
}

func (a *StorageBrokerAdapter) RegisterConsumer(queueName, consumerTag string, consumer *protocol.Consumer) error {
	return a.broker.RegisterConsumer(queueName, consumerTag, consumer)
}

func (a *StorageBrokerAdapter) UnregisterConsumer(consumerTag string) error {
	return a.broker.UnregisterConsumer(consumerTag)
}

func (a *StorageBrokerAdapter) AcknowledgeMessage(consumerTag string, deliveryTag uint64, multiple bool) error {
	return a.broker.AcknowledgeMessage(consumerTag, deliveryTag, multiple)
}

func (a *StorageBrokerAdapter) RejectMessage(consumerTag string, deliveryTag uint64, requeue bool) error {
	return a.broker.RejectMessage(consumerTag, deliveryTag, requeue)
}

func (a *StorageBrokerAdapter) NacknowledgeMessage(consumerTag string, deliveryTag uint64, multiple, requeue bool) error {
	return a.broker.NacknowledgeMessage(consumerTag, deliveryTag, multiple, requeue)
}

func (a *StorageBrokerAdapter) GetQueues() map[string]*protocol.Queue {
	return a.broker.GetQueues()
}

func (a *StorageBrokerAdapter) GetExchanges() map[string]*protocol.Exchange {
	return a.broker.GetExchanges()
}

func (a *StorageBrokerAdapter) GetConsumers() map[string]*protocol.Consumer {
	return a.broker.GetConsumers()
}
