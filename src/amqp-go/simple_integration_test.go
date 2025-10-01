package main

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSimpleQueueOperations tests basic queue operations without complex routing
func TestSimpleQueueOperations(t *testing.T) {
	// Start a server in a goroutine
	server := NewServer(":5673")
	go func() {
		_ = server.Start()
	}()
	
	// Give the server a moment to start
	time.Sleep(200 * time.Millisecond)
	
	// Connect using the standard AMQP client
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5673//")
	require.NoError(t, err, "Should connect to server")
	defer conn.Close()
	
	channel, err := conn.Channel()
	require.NoError(t, err, "Should open channel")
	defer channel.Close()
	
	// Declare a queue
	queue, err := channel.QueueDeclare(
		"test-simple-queue", // name
		false,               // durable
		true,                // delete when unused
		false,               // exclusive
		false,               // no-wait
		nil,                 // arguments
	)
	require.NoError(t, err, "Should declare queue")
	assert.Equal(t, "test-simple-queue", queue.Name)
	
	// Publish a message directly to the queue (empty exchange)
	err = channel.Publish(
		"",                 // exchange (empty = default direct exchange)
		queue.Name,         // routing key (queue name for default exchange)
		false,              // mandatory
		false,              // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte("Hello, Simple Queue!"),
		},
	)
	assert.NoError(t, err, "Should publish message")
	
	// Consume messages from the queue
	msgs, err := channel.Consume(
		queue.Name, // queue
		"",         // consumer
		true,       // auto-ack (acknowledge automatically)
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	require.NoError(t, err, "Should start consuming")
	
	// Try to receive the message with a timeout
	select {
	case msg := <-msgs:
		assert.Equal(t, "Hello, Simple Queue!", string(msg.Body))
	case <-time.After(1 * time.Second):
		t.Fatal("Did not receive message within timeout")
	}
}