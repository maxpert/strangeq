package server

import (
	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

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

func (a *StorageBrokerAdapter) DeleteQueue(name string, ifUnused, ifEmpty bool) (int, error) {
	return a.broker.DeleteQueue(name, ifUnused, ifEmpty)
}

func (a *StorageBrokerAdapter) BindQueue(queueName, exchangeName, routingKey string, arguments map[string]interface{}) error {
	return a.broker.BindQueue(queueName, exchangeName, routingKey, arguments)
}

func (a *StorageBrokerAdapter) UnbindQueue(queueName, exchangeName, routingKey string) error {
	return a.broker.UnbindQueue(queueName, exchangeName, routingKey)
}

func (a *StorageBrokerAdapter) BindExchange(destination, source, routingKey string, arguments map[string]interface{}) error {
	return a.broker.BindExchange(destination, source, routingKey, arguments)
}

func (a *StorageBrokerAdapter) UnbindExchange(destination, source, routingKey string) error {
	return a.broker.UnbindExchange(destination, source, routingKey)
}

func (a *StorageBrokerAdapter) PublishMessage(exchangeName, routingKey string, message *protocol.Message) error {
	return a.broker.PublishMessage(exchangeName, routingKey, message)
}

// PublishMessageAsyncConfirm routes a durable/publisher-confirm publish through
// the broker's async-completion path (deferred visibility + WAL group-commit
// fsync completion). See UnifiedBroker.PublishMessageAsyncConfirm.
func (a *StorageBrokerAdapter) PublishMessageAsyncConfirm(exchangeName, routingKey string, message *protocol.Message, onDurable func(error)) (bool, protocol.DepthGate, error) {
	return a.broker.PublishMessageAsyncConfirm(exchangeName, routingKey, message, onDurable)
}

// PublishMessageTx routes a transactional publish through the atomic storage
// staging view (SQ-8), returning deferred visibility actions to run post-commit.
func (a *StorageBrokerAdapter) PublishMessageTx(txnStore interfaces.Storage, exchangeName, routingKey string, message *protocol.Message) ([]func(), error) {
	return a.broker.PublishMessageTx(txnStore, exchangeName, routingKey, message)
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

func (a *StorageBrokerAdapter) RequeueAllForConsumer(consumerTag string) error {
	return a.broker.RequeueAllForConsumer(consumerTag)
}

func (a *StorageBrokerAdapter) GetConsumerForDelivery(deliveryTag uint64) (string, bool) {
	return a.broker.GetConsumerForDelivery(deliveryTag)
}

func (a *StorageBrokerAdapter) GetMessageForGet(queueName string, noAck bool) (*protocol.Message, uint64, uint32, error) {
	return a.broker.GetMessageForGet(queueName, noAck)
}

func (a *StorageBrokerAdapter) PurgeQueue(name string) (int, error) {
	return a.broker.PurgeQueue(name)
}

func (a *StorageBrokerAdapter) AcknowledgeGetDelivery(deliveryTag uint64) error {
	return a.broker.AcknowledgeGetDelivery(deliveryTag)
}

func (a *StorageBrokerAdapter) RejectGetDelivery(deliveryTag uint64, requeue bool) error {
	return a.broker.RejectGetDelivery(deliveryTag, requeue)
}

func (a *StorageBrokerAdapter) NackGetDelivery(deliveryTag uint64, requeue bool) error {
	return a.broker.NackGetDelivery(deliveryTag, requeue)
}

func (a *StorageBrokerAdapter) RequeueAllGetDeliveries() {
	a.broker.RequeueAllGetDeliveries()
}

func (a *StorageBrokerAdapter) AdvanceDeliveryTag(tag uint64) {
	a.broker.AdvanceDeliveryTag(tag)
}

func (a *StorageBrokerAdapter) RecoverQueue(queueName string, minTag, maxTag, count uint64) {
	a.broker.RecoverQueue(queueName, minTag, maxTag, count)
}

func (a *StorageBrokerAdapter) RebuildDeliveryIndex(deliveryTag uint64, consumerTag string) {
	a.broker.RebuildDeliveryIndex(deliveryTag, consumerTag)
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

func (a *StorageBrokerAdapter) UpdateConsumerPrefetch(consumerTag string, prefetchCount uint16) {
	a.broker.UpdateConsumerPrefetch(consumerTag, prefetchCount)
}
