package transaction

import (
	"fmt"

	"github.com/maxpert/amqp-go/protocol"
)

// UnifiedBrokerExecutorInterface defines interface for unified brokers that can act as transaction executors
type UnifiedBrokerExecutorInterface interface {
	PublishMessage(exchangeName, routingKey string, message *protocol.Message) error
	AcknowledgeMessage(consumerTag string, deliveryTag uint64, multiple bool) error
	RejectMessage(consumerTag string, deliveryTag uint64, requeue bool) error
	NacknowledgeMessage(consumerTag string, deliveryTag uint64, multiple, requeue bool) error
	AcknowledgeGetDelivery(deliveryTag uint64) error
	RejectGetDelivery(deliveryTag uint64, requeue bool) error
	NackGetDelivery(deliveryTag uint64, requeue bool) error
}

// UnifiedBrokerExecutor adapts any broker with the required interface to work as a transaction executor
type UnifiedBrokerExecutor struct {
	broker UnifiedBrokerExecutorInterface
}

// NewUnifiedBrokerExecutor creates a new unified broker executor adapter
func NewUnifiedBrokerExecutor(broker UnifiedBrokerExecutorInterface) *UnifiedBrokerExecutor {
	return &UnifiedBrokerExecutor{
		broker: broker,
	}
}

// ExecutePublish executes a message publish operation
func (ube *UnifiedBrokerExecutor) ExecutePublish(exchange, routingKey string, message *protocol.Message) error {
	if ube.broker == nil {
		return fmt.Errorf("no unified broker available for executing publish")
	}

	return ube.broker.PublishMessage(exchange, routingKey, message)
}

// ExecuteAck executes a message acknowledgment operation
func (ube *UnifiedBrokerExecutor) ExecuteAck(consumerTag string, deliveryTag uint64, multiple bool) error {
	if ube.broker == nil {
		return fmt.Errorf("no unified broker available for executing ack")
	}

	if consumerTag == "" {
		return ube.broker.AcknowledgeGetDelivery(deliveryTag)
	}
	return ube.broker.AcknowledgeMessage(consumerTag, deliveryTag, multiple)
}

// ExecuteNack executes a negative acknowledgment operation
func (ube *UnifiedBrokerExecutor) ExecuteNack(consumerTag string, deliveryTag uint64, multiple, requeue bool) error {
	if ube.broker == nil {
		return fmt.Errorf("no unified broker available for executing nack")
	}

	if consumerTag == "" {
		return ube.broker.NackGetDelivery(deliveryTag, requeue)
	}
	return ube.broker.NacknowledgeMessage(consumerTag, deliveryTag, multiple, requeue)
}

// ExecuteReject executes a message rejection operation
func (ube *UnifiedBrokerExecutor) ExecuteReject(consumerTag string, deliveryTag uint64, requeue bool) error {
	if ube.broker == nil {
		return fmt.Errorf("no unified broker available for executing reject")
	}

	if consumerTag == "" {
		return ube.broker.RejectGetDelivery(deliveryTag, requeue)
	}
	return ube.broker.RejectMessage(consumerTag, deliveryTag, requeue)
}
