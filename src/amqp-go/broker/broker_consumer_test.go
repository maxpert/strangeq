package broker

import (
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
)

func TestRegisterConsumer(t *testing.T) {
	broker := NewBroker()

	// Declare a queue
	_, err := broker.DeclareQueue("test-queue", false, false, false, nil)
	assert.NoError(t, err)

	// Create a consumer
	consumer := &protocol.Consumer{
		Tag:      "consumer-1",
		Queue:    "test-queue",
		NoAck:    false,
		Messages: make(chan *protocol.Delivery, 10),
		Cancel:   make(chan struct{}),
	}

	// Register the consumer
	err = broker.RegisterConsumer("test-queue", "consumer-1", consumer)
	assert.NoError(t, err)

	// Verify consumer is registered
	broker.Mutex.RLock()
	_, exists := broker.Consumers["consumer-1"]
	assert.True(t, exists)
	broker.Mutex.RUnlock()

	// Test registering consumer for non-existent queue
	err = broker.RegisterConsumer("non-existent", "consumer-2", consumer)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "queue does not exist")
}

func TestUnregisterConsumer(t *testing.T) {
	broker := NewBroker()

	// Setup
	_, err := broker.DeclareQueue("test-queue", false, false, false, nil)
	assert.NoError(t, err)

	consumer := &protocol.Consumer{
		Tag:      "consumer-1",
		Queue:    "test-queue",
		NoAck:    false,
		Messages: make(chan *protocol.Delivery, 10),
		Cancel:   make(chan struct{}),
	}

	err = broker.RegisterConsumer("test-queue", "consumer-1", consumer)
	assert.NoError(t, err)

	// Unregister the consumer
	err = broker.UnregisterConsumer("consumer-1")
	assert.NoError(t, err)

	// Verify consumer is removed
	broker.Mutex.RLock()
	_, exists := broker.Consumers["consumer-1"]
	assert.False(t, exists)
	broker.Mutex.RUnlock()

	// Test unregistering non-existent consumer
	err = broker.UnregisterConsumer("non-existent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "consumer not found")
}

func TestConsumerReceivesMessages(t *testing.T) {
	broker := NewBroker()

	// Setup exchange, queue, and binding
	err := broker.DeclareExchange("test-exchange", "direct", false, false, false, nil)
	assert.NoError(t, err)

	_, err = broker.DeclareQueue("test-queue", false, false, false, nil)
	assert.NoError(t, err)

	err = broker.BindQueue("test-queue", "test-exchange", "test-key", nil)
	assert.NoError(t, err)

	// Create a proper channel setup
	conn := &protocol.Connection{
		Channels: make(map[uint16]*protocol.Channel),
	}
	channel := &protocol.Channel{
		ID:         1,
		Connection: conn,
		Consumers:  make(map[string]*protocol.Consumer),
	}

	// Create a consumer
	consumer := &protocol.Consumer{
		Tag:      "consumer-1",
		Queue:    "test-queue",
		NoAck:    false,
		Channel:  channel,
		Messages: make(chan *protocol.Delivery, 10),
		Cancel:   make(chan struct{}),
	}

	// Register consumer
	err = broker.RegisterConsumer("test-queue", "consumer-1", consumer)
	assert.NoError(t, err)

	// Publish a message
	message := &protocol.Message{
		Body:       []byte("test message"),
		Exchange:   "test-exchange",
		RoutingKey: "test-key",
	}

	err = broker.PublishMessage("test-exchange", "test-key", message)
	assert.NoError(t, err)

	// Consumer should receive the message
	select {
	case delivery := <-consumer.Messages:
		assert.NotNil(t, delivery)
		assert.NotNil(t, delivery.Message)
		assert.Equal(t, []byte("test message"), delivery.Message.Body)
		assert.Equal(t, "test-key", delivery.RoutingKey)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Consumer did not receive message")
	}
}

func TestAcknowledgeMessage(t *testing.T) {
	broker := NewBroker()

	// Setup - need a channel to track unacked messages
	conn := &protocol.Connection{
		Channels: make(map[uint16]*protocol.Channel),
	}
	channel := &protocol.Channel{
		ID:         1,
		Connection: conn,
		Consumers:  make(map[string]*protocol.Consumer),
	}

	_, err := broker.DeclareQueue("test-queue", false, false, false, nil)
	assert.NoError(t, err)

	consumer := &protocol.Consumer{
		Tag:            "consumer-1",
		Queue:          "test-queue",
		NoAck:          false,
		Messages:       make(chan *protocol.Delivery, 10),
		Cancel:         make(chan struct{}),
		Channel:        channel,
		CurrentUnacked: 1, // Simulate one unacked message
	}

	err = broker.RegisterConsumer("test-queue", "consumer-1", consumer)
	assert.NoError(t, err)

	// Acknowledge the message
	err = broker.AcknowledgeMessage("consumer-1", 1, false)
	assert.NoError(t, err)

	// Verify unacked count is decremented
	assert.Equal(t, uint64(0), consumer.CurrentUnacked)

	// Test acknowledging from non-existent consumer
	err = broker.AcknowledgeMessage("non-existent", 1, false)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "consumer not found")
}

func TestRejectMessage(t *testing.T) {
	broker := NewBroker()

	// Setup
	conn := &protocol.Connection{
		Channels: make(map[uint16]*protocol.Channel),
	}
	channel := &protocol.Channel{
		ID:         1,
		Connection: conn,
		Consumers:  make(map[string]*protocol.Consumer),
	}

	_, err := broker.DeclareQueue("test-queue", false, false, false, nil)
	assert.NoError(t, err)

	consumer := &protocol.Consumer{
		Tag:            "consumer-1",
		Queue:          "test-queue",
		NoAck:          false,
		Messages:       make(chan *protocol.Delivery, 10),
		Cancel:         make(chan struct{}),
		Channel:        channel,
		CurrentUnacked: 1,
	}

	err = broker.RegisterConsumer("test-queue", "consumer-1", consumer)
	assert.NoError(t, err)

	// Reject the message without requeue
	err = broker.RejectMessage("consumer-1", 1, false)
	assert.NoError(t, err)

	// Verify unacked count is decremented
	assert.Equal(t, uint64(0), consumer.CurrentUnacked)

	// Test rejecting from non-existent consumer
	err = broker.RejectMessage("non-existent", 1, false)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "consumer not found")
}

func TestNacknowledgeMessage(t *testing.T) {
	broker := NewBroker()

	// Setup
	conn := &protocol.Connection{
		Channels: make(map[uint16]*protocol.Channel),
	}
	channel := &protocol.Channel{
		ID:         1,
		Connection: conn,
		Consumers:  make(map[string]*protocol.Consumer),
	}

	_, err := broker.DeclareQueue("test-queue", false, false, false, nil)
	assert.NoError(t, err)

	consumer := &protocol.Consumer{
		Tag:            "consumer-1",
		Queue:          "test-queue",
		NoAck:          false,
		Messages:       make(chan *protocol.Delivery, 10),
		Cancel:         make(chan struct{}),
		Channel:        channel,
		CurrentUnacked: 3, // Simulate multiple unacked messages
	}

	err = broker.RegisterConsumer("test-queue", "consumer-1", consumer)
	assert.NoError(t, err)

	// Nack message with multiple=true (should reset unacked count to 0)
	err = broker.NacknowledgeMessage("consumer-1", 2, true, false)
	assert.NoError(t, err)

	// Verify unacked count is reset to 0
	assert.Equal(t, uint64(0), consumer.CurrentUnacked)

	// Test nacking from non-existent consumer
	err = broker.NacknowledgeMessage("non-existent", 1, false, false)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "consumer not found")
}

func TestHeadersExchangeRouting(t *testing.T) {
	broker := NewBroker()

	// Setup exchange and queue
	err := broker.DeclareExchange("test-exchange", "headers", false, false, false, nil)
	assert.NoError(t, err)

	_, err = broker.DeclareQueue("test-queue", false, false, false, nil)
	assert.NoError(t, err)

	// Bind queue with headers match (current implementation doesn't use x-match)
	bindArgs := map[string]interface{}{
		"format": "pdf",
		"type":   "report",
	}
	err = broker.BindQueue("test-queue", "test-exchange", "", bindArgs)
	assert.NoError(t, err)

	// Create a message with matching headers (must include x-match and bind args)
	message := &protocol.Message{
		Body:     []byte("test message"),
		Exchange: "test-exchange",
		Headers: map[string]interface{}{
			"format": "pdf",
			"type":   "report",
		},
	}

	// Publish message
	err = broker.PublishMessage("test-exchange", "", message)
	assert.NoError(t, err)

	// Check that the message was routed to the queue
	queue, exists := broker.Queues["test-queue"]
	assert.True(t, exists)
	// All queues now use index+cache storage
	assert.Equal(t, 1, queue.MessageCount())
}

func TestAutoGeneratedQueueName(t *testing.T) {
	broker := NewBroker()

	// Declare a queue with empty name (should auto-generate)
	queue1, err := broker.DeclareQueue("", false, false, false, nil)
	assert.NoError(t, err)
	assert.NotEmpty(t, queue1.Name)
	// Generated queue names start with "queue_"
	assert.Contains(t, queue1.Name, "queue_")

	// Generate another and ensure it's different
	queue2, err := broker.DeclareQueue("", false, false, false, nil)
	assert.NoError(t, err)
	assert.NotEqual(t, queue1.Name, queue2.Name)
}

func TestDeleteQueueWithMessages(t *testing.T) {
	broker := NewBroker()

	// Declare queue and add a message
	_, err := broker.DeclareQueue("test-queue", false, false, false, nil)
	assert.NoError(t, err)

	message := &protocol.Message{
		DeliveryTag: 1,
		Body:        []byte("test message"),
		Exchange:    "",
		RoutingKey:  "test-queue",
	}

	// Publish message through proper API (goes to index+cache now)
	err = broker.PublishMessage("", "test-queue", message)
	assert.NoError(t, err)

	// Try to delete with ifEmpty=true (should fail)
	err = broker.DeleteQueue("test-queue", false, true)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "queue not empty")

	// Delete without ifEmpty constraint (should succeed)
	err = broker.DeleteQueue("test-queue", false, false)
	assert.NoError(t, err)

	// Verify queue is deleted
	broker.Mutex.RLock()
	_, exists := broker.Queues["test-queue"]
	assert.False(t, exists)
	broker.Mutex.RUnlock()
}
