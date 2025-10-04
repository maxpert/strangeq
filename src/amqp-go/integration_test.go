package main

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
)

// TestConnectionAndChannel tests basic connection and channel functionality
func TestConnectionAndChannel(t *testing.T) {
	// Start a server in a goroutine
	server := NewServer(":5680") // Use a different port to avoid conflicts
	go func() {
		_ = server.Start()
	}()

	// Give the server a moment to start
	time.Sleep(200 * time.Millisecond)

	// Connect using the standard AMQP client
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5680//") // Note: vhost is specified as "/" after the double slash
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	channel, err := conn.Channel()
	assert.NoError(t, err, "Channel creation should succeed")
	if err != nil {
		t.Fatalf("Failed to open channel: %v", err)
	}
	defer channel.Close()

	// Test that we can use the channel (this tests basic functionality)
	err = channel.Qos(1, 0, true) // Prefetch settings
	assert.NoError(t, err, "Qos setting should succeed")
}

// TestExchangeOperations tests exchange operations using the standard AMQP client
func TestExchangeOperations(t *testing.T) {
	// Start a server in a goroutine
	server := NewServer(":5681") // Use a different port
	go func() {
		_ = server.Start()
	}()

	// Give the server a moment to start
	time.Sleep(200 * time.Millisecond)

	// Connect using the standard AMQP client
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5681//")
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		t.Fatalf("Failed to open channel: %v", err)
	}
	defer channel.Close()

	// Test exchange declare
	err = channel.ExchangeDeclare(
		"test-exchange", // name
		"direct",        // type
		false,           // durable
		true,            // auto-deleted (to clean up)
		false,           // internal
		false,           // no-wait
		nil,             // arguments
	)
	assert.NoError(t, err, "Exchange declare should succeed")

	// Test exchange delete
	err = channel.ExchangeDelete("test-exchange", false, false)
	assert.NoError(t, err, "Exchange delete should succeed")
}

// TestQueueOperations tests queue operations using the standard AMQP client
func TestQueueOperations(t *testing.T) {
	// Start a server in a goroutine
	server := NewServer(":5682") // Use a different port
	go func() {
		_ = server.Start()
	}()

	// Give the server a moment to start
	time.Sleep(200 * time.Millisecond)

	// Connect using the standard AMQP client
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5682//")
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		t.Fatalf("Failed to open channel: %v", err)
	}
	defer channel.Close()

	// Test queue declare
	queue, err := channel.QueueDeclare(
		"test-queue", // name
		false,        // durable
		true,         // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	assert.NoError(t, err, "Queue declare should succeed")
	assert.Equal(t, "test-queue", queue.Name)

	// Test queue delete
	deletedCount, err := channel.QueueDelete("test-queue", false, false, false)
	assert.NoError(t, err, "Queue delete should succeed")
	assert.GreaterOrEqual(t, deletedCount, 0, "Should return number of deleted messages")
}

// TestQueueBinding tests queue binding operations
func TestQueueBinding(t *testing.T) {
	// Start a server in a goroutine
	server := NewServer(":5683") // Use a different port
	go func() {
		_ = server.Start()
	}()

	// Give the server a moment to start
	time.Sleep(200 * time.Millisecond)

	// Connect using the standard AMQP client
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5683//")
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		t.Fatalf("Failed to open channel: %v", err)
	}
	defer channel.Close()

	// Declare a direct exchange
	err = channel.ExchangeDeclare(
		"test-bind-exchange", // name
		"direct",             // type
		false,                // durable
		true,                 // auto-deleted
		false,                // internal
		false,                // no-wait
		nil,                  // arguments
	)
	assert.NoError(t, err)

	// Declare a queue
	queue, err := channel.QueueDeclare(
		"test-bind-queue", // name
		false,             // durable
		true,              // delete when unused
		false,             // exclusive
		false,             // no-wait
		nil,               // arguments
	)
	assert.NoError(t, err)

	// Bind the queue to the exchange
	err = channel.QueueBind(
		queue.Name,           // queue
		"test.routing.key",   // key
		"test-bind-exchange", // exchange
		false,                // no-wait
		nil,                  // arguments
	)
	assert.NoError(t, err)

	// Unbind the queue from the exchange
	err = channel.QueueUnbind(
		queue.Name,           // queue
		"test.routing.key",   // key
		"test-bind-exchange", // exchange
		nil,                  // arguments
	)
	assert.NoError(t, err)

	// Clean up by deleting the exchange and queue
	err = channel.ExchangeDelete("test-bind-exchange", false, false)
	assert.NoError(t, err)

	_, err = channel.QueueDelete("test-bind-queue", false, false, false)
	assert.NoError(t, err)
}
