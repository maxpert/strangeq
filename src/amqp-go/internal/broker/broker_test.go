package broker

import (
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
)

func TestExchangeOperations(t *testing.T) {
	broker := NewBroker()

	// Test exchange declaration
	err := broker.DeclareExchange("test-exchange", "direct", false, false, false, nil)
	assert.NoError(t, err)

	// Test duplicate exchange declaration (should succeed)
	err = broker.DeclareExchange("test-exchange", "direct", false, false, false, nil)
	assert.NoError(t, err)

	// Test exchange deletion
	err = broker.DeleteExchange("test-exchange", false)
	assert.NoError(t, err)

	// Test deleting non-existent exchange
	err = broker.DeleteExchange("non-existent", false)
	assert.Error(t, err)
}

func TestQueueOperations(t *testing.T) {
	broker := NewBroker()

	// Test queue declaration
	queue, err := broker.DeclareQueue("test-queue", false, false, false, nil)
	assert.NoError(t, err)
	assert.Equal(t, "test-queue", queue.Name)

	// Test duplicate queue declaration (should succeed)
	queue, err = broker.DeclareQueue("test-queue", false, false, false, nil)
	assert.NoError(t, err)
	assert.Equal(t, "test-queue", queue.Name)

	// Test queue deletion
	err = broker.DeleteQueue("test-queue", false, true) // ifEmpty=true
	assert.NoError(t, err)

	// Test deleting non-existent queue
	err = broker.DeleteQueue("non-existent", false, false)
	assert.Error(t, err)
}

func TestBindingOperations(t *testing.T) {
	broker := NewBroker()

	// Setup exchange and queue
	err := broker.DeclareExchange("test-exchange", "direct", false, false, false, nil)
	assert.NoError(t, err)

	_, err = broker.DeclareQueue("test-queue", false, false, false, nil)
	assert.NoError(t, err)

	// Test binding
	err = broker.BindQueue("test-queue", "test-exchange", "test-key", nil)
	assert.NoError(t, err)

	// Test duplicate binding (should succeed)
	err = broker.BindQueue("test-queue", "test-exchange", "test-key", nil)
	assert.NoError(t, err)

	// Test unbinding
	err = broker.UnbindQueue("test-queue", "test-exchange", "test-key")
	assert.NoError(t, err)

	// Test unbinding non-existent binding
	err = broker.UnbindQueue("test-queue", "test-exchange", "test-key")
	assert.Error(t, err)

	// Test unbinding from non-existent exchange
	err = broker.UnbindQueue("test-queue", "non-existent", "test-key")
	assert.Error(t, err)
}

func TestDirectExchangeRouting(t *testing.T) {
	broker := NewBroker()

	// Setup exchange and queue
	err := broker.DeclareExchange("test-exchange", "direct", false, false, false, nil)
	assert.NoError(t, err)

	_, err = broker.DeclareQueue("test-queue", false, false, false, nil)
	assert.NoError(t, err)

	// Bind queue to exchange with routing key
	err = broker.BindQueue("test-queue", "test-exchange", "test-key", nil)
	assert.NoError(t, err)

	// Create a test message
	message := &protocol.Message{
		Body:       []byte("test message"),
		Exchange:   "test-exchange",
		RoutingKey: "test-key",
	}

	// Publish message
	err = broker.PublishMessage("test-exchange", "test-key", message)
	assert.NoError(t, err)

	// Check that the message was routed to the queue
	queue, exists := broker.Queues["test-queue"]
	assert.True(t, exists)
	assert.Equal(t, 1, len(queue.Messages))
	assert.Equal(t, "test-key", queue.Messages[0].RoutingKey)
}

func TestFanoutExchangeRouting(t *testing.T) {
	broker := NewBroker()

	// Setup exchange and queues
	err := broker.DeclareExchange("test-exchange", "fanout", false, false, false, nil)
	assert.NoError(t, err)

	_, err = broker.DeclareQueue("test-queue-1", false, false, false, nil)
	assert.NoError(t, err)

	_, err = broker.DeclareQueue("test-queue-2", false, false, false, nil)
	assert.NoError(t, err)

	// Bind both queues to exchange (routing key is ignored for fanout)
	err = broker.BindQueue("test-queue-1", "test-exchange", "", nil)
	assert.NoError(t, err)

	err = broker.BindQueue("test-queue-2", "test-exchange", "", nil)
	assert.NoError(t, err)

	// Create a test message
	message := &protocol.Message{
		Body:       []byte("test message"),
		Exchange:   "test-exchange",
		RoutingKey: "any-key",
	}

	// Publish message
	err = broker.PublishMessage("test-exchange", "any-key", message)
	assert.NoError(t, err)

	// Check that the message was routed to both queues
	queue1, exists := broker.Queues["test-queue-1"]
	assert.True(t, exists)
	assert.Equal(t, 1, len(queue1.Messages))

	queue2, exists := broker.Queues["test-queue-2"]
	assert.True(t, exists)
	assert.Equal(t, 1, len(queue2.Messages))
}

func TestTopicExchangeRouting(t *testing.T) {
	broker := NewBroker()

	// Setup exchange and queues
	err := broker.DeclareExchange("test-exchange", "topic", false, false, false, nil)
	assert.NoError(t, err)

	_, err = broker.DeclareQueue("test-queue-1", false, false, false, nil)
	assert.NoError(t, err)

	// Bind queue to exchange with wildcard pattern
	err = broker.BindQueue("test-queue-1", "test-exchange", "stock.#", nil)
	assert.NoError(t, err)

	// Create a test message that should match the pattern
	message := &protocol.Message{
		Body:       []byte("test message"),
		Exchange:   "test-exchange",
		RoutingKey: "stock.usd.nyse",
	}

	// Publish message
	err = broker.PublishMessage("test-exchange", "stock.usd.nyse", message)
	assert.NoError(t, err)

	// Check that the message was routed to the queue
	queue, exists := broker.Queues["test-queue-1"]
	assert.True(t, exists)
	assert.Equal(t, 1, len(queue.Messages))
	assert.Equal(t, "stock.usd.nyse", queue.Messages[0].RoutingKey)
}

func TestTopicExchangeNoMatch(t *testing.T) {
	broker := NewBroker()

	// Setup exchange and queue
	err := broker.DeclareExchange("test-exchange", "topic", false, false, false, nil)
	assert.NoError(t, err)

	_, err = broker.DeclareQueue("test-queue-1", false, false, false, nil)
	assert.NoError(t, err)

	// Bind queue to exchange with specific pattern
	err = broker.BindQueue("test-queue-1", "test-exchange", "stock.*", nil)
	assert.NoError(t, err)

	// Create a test message that should NOT match the pattern
	message := &protocol.Message{
		Body:       []byte("test message"),
		Exchange:   "test-exchange",
		RoutingKey: "news.usd.nyse",
	}

	// Publish message
	err = broker.PublishMessage("test-exchange", "news.usd.nyse", message)
	assert.NoError(t, err)

	// Check that the message was NOT routed to the queue
	queue, exists := broker.Queues["test-queue-1"]
	assert.True(t, exists)
	assert.Equal(t, 0, len(queue.Messages))
}