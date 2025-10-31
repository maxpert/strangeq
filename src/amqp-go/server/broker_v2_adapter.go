package server

import (
	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/protocol"
)

// BrokerV2Adapter wraps BrokerV2 to implement UnifiedBroker
type BrokerV2Adapter struct {
	broker *broker.BrokerV2
}

// NewBrokerV2Adapter creates an adapter for BrokerV2
func NewBrokerV2Adapter(b *broker.BrokerV2) UnifiedBroker {
	return &BrokerV2Adapter{broker: b}
}

func (a *BrokerV2Adapter) DeclareExchange(name, exchangeType string, durable, autoDelete, internal bool, arguments map[string]interface{}) error {
	return a.broker.DeclareExchange(name, exchangeType, durable, autoDelete, internal, arguments)
}

func (a *BrokerV2Adapter) DeleteExchange(name string, ifUnused bool) error {
	return a.broker.DeleteExchange(name, ifUnused)
}

func (a *BrokerV2Adapter) DeclareQueue(name string, durable, autoDelete, exclusive bool, arguments map[string]interface{}) (*protocol.Queue, error) {
	return a.broker.DeclareQueue(name, durable, autoDelete, exclusive, arguments)
}

func (a *BrokerV2Adapter) DeleteQueue(name string, ifUnused, ifEmpty bool) error {
	return a.broker.DeleteQueue(name, ifUnused, ifEmpty)
}

func (a *BrokerV2Adapter) BindQueue(queueName, exchangeName, routingKey string, arguments map[string]interface{}) error {
	return a.broker.BindQueue(queueName, exchangeName, routingKey, arguments)
}

func (a *BrokerV2Adapter) UnbindQueue(queueName, exchangeName, routingKey string) error {
	return a.broker.UnbindQueue(queueName, exchangeName, routingKey)
}

func (a *BrokerV2Adapter) PublishMessage(exchangeName, routingKey string, message *protocol.Message) error {
	return a.broker.PublishMessage(exchangeName, routingKey, message)
}

func (a *BrokerV2Adapter) RegisterConsumer(queueName, consumerTag string, consumer *protocol.Consumer) error {
	return a.broker.RegisterConsumer(queueName, consumerTag, consumer)
}

func (a *BrokerV2Adapter) UnregisterConsumer(consumerTag string) error {
	return a.broker.UnregisterConsumer(consumerTag)
}

func (a *BrokerV2Adapter) AcknowledgeMessage(consumerTag string, deliveryTag uint64, multiple bool) error {
	return a.broker.AcknowledgeMessage(consumerTag, deliveryTag, multiple)
}

func (a *BrokerV2Adapter) RejectMessage(consumerTag string, deliveryTag uint64, requeue bool) error {
	return a.broker.RejectMessage(consumerTag, deliveryTag, requeue)
}

func (a *BrokerV2Adapter) NacknowledgeMessage(consumerTag string, deliveryTag uint64, multiple, requeue bool) error {
	return a.broker.NacknowledgeMessage(consumerTag, deliveryTag, multiple, requeue)
}

// GetQueues is used for metrics/stats
func (a *BrokerV2Adapter) GetQueues() map[string]*protocol.Queue {
	queues := make(map[string]*protocol.Queue)
	a.broker.Queues.Range(func(key, value interface{}) bool {
		qp := value.(*broker.QueueProcess)
		queues[key.(string)] = qp.Queue
		return true
	})
	return queues
}

// GetExchanges is used for metrics/stats
func (a *BrokerV2Adapter) GetExchanges() map[string]*protocol.Exchange {
	exchanges := make(map[string]*protocol.Exchange)
	a.broker.Exchanges.Range(func(key, value interface{}) bool {
		ex := value.(*protocol.Exchange)
		exchanges[key.(string)] = ex
		return true
	})
	return exchanges
}

// GetConsumers is used for metrics/stats
func (a *BrokerV2Adapter) GetConsumers() map[string]*protocol.Consumer {
	consumers := make(map[string]*protocol.Consumer)
	a.broker.Consumers.Range(func(key, value interface{}) bool {
		consumer := value.(*protocol.Consumer)
		consumers[key.(string)] = consumer
		return true
	})
	return consumers
}
