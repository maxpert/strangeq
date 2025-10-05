package transaction

import (
	"fmt"
	
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

// BrokerExecutor adapts a broker to work as a transaction executor
type BrokerExecutor struct {
	broker interfaces.Broker
}

// NewBrokerExecutor creates a new broker executor adapter
func NewBrokerExecutor(broker interfaces.Broker) *BrokerExecutor {
	return &BrokerExecutor{
		broker: broker,
	}
}

// ExecutePublish executes a message publish operation
func (be *BrokerExecutor) ExecutePublish(exchange, routingKey string, message *protocol.Message) error {
	if be.broker == nil {
		return fmt.Errorf("no broker available for executing publish")
	}
	
	return be.broker.PublishMessage(exchange, routingKey, message)
}

// ExecuteAck executes a message acknowledgment operation
func (be *BrokerExecutor) ExecuteAck(queueName string, deliveryTag uint64, multiple bool) error {
	if be.broker == nil {
		return fmt.Errorf("no broker available for executing ack")
	}
	
	// Use the interfaces.Broker method signature
	// Convert deliveryTag to string as expected by the interface
	messageID := fmt.Sprintf("%d", deliveryTag)
	return be.broker.AckMessage(queueName, messageID)
}

// ExecuteNack executes a negative acknowledgment operation
func (be *BrokerExecutor) ExecuteNack(queueName string, deliveryTag uint64, multiple, requeue bool) error {
	if be.broker == nil {
		return fmt.Errorf("no broker available for executing nack")
	}
	
	// Use the interfaces.Broker method signature
	messageID := fmt.Sprintf("%d", deliveryTag)
	return be.broker.NackMessage(queueName, messageID, requeue)
}

// ExecuteReject executes a message rejection operation
func (be *BrokerExecutor) ExecuteReject(queueName string, deliveryTag uint64, requeue bool) error {
	if be.broker == nil {
		return fmt.Errorf("no broker available for executing reject")
	}
	
	// For the original broker interface, reject is equivalent to nack
	messageID := fmt.Sprintf("%d", deliveryTag)
	return be.broker.NackMessage(queueName, messageID, requeue)
}

// UnifiedBrokerExecutorInterface defines interface for unified brokers that can act as transaction executors
type UnifiedBrokerExecutorInterface interface {
	PublishMessage(exchangeName, routingKey string, message *protocol.Message) error
	AcknowledgeMessage(consumerTag string, deliveryTag uint64, multiple bool) error
	RejectMessage(consumerTag string, deliveryTag uint64, requeue bool) error
	NacknowledgeMessage(consumerTag string, deliveryTag uint64, multiple, requeue bool) error
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
func (ube *UnifiedBrokerExecutor) ExecuteAck(queueName string, deliveryTag uint64, multiple bool) error {
	if ube.broker == nil {
		return fmt.Errorf("no unified broker available for executing ack")
	}
	
	// For ack operations, we need to find the consumer by delivery tag
	return ube.broker.AcknowledgeMessage("", deliveryTag, multiple)
}

// ExecuteNack executes a negative acknowledgment operation  
func (ube *UnifiedBrokerExecutor) ExecuteNack(queueName string, deliveryTag uint64, multiple, requeue bool) error {
	if ube.broker == nil {
		return fmt.Errorf("no unified broker available for executing nack")
	}
	
	return ube.broker.NacknowledgeMessage("", deliveryTag, multiple, requeue)
}

// ExecuteReject executes a message rejection operation
func (ube *UnifiedBrokerExecutor) ExecuteReject(queueName string, deliveryTag uint64, requeue bool) error {
	if ube.broker == nil {
		return fmt.Errorf("no unified broker available for executing reject")
	}
	
	return ube.broker.RejectMessage("", deliveryTag, requeue)
}