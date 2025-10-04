package main

import (
	"testing"
	"time"

	"github.com/maxpert/amqp-go/server"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSimpleQueueOperations tests basic queue operations with different payload sizes and content types
func TestSimpleQueueOperations(t *testing.T) {
	// Start a server in a goroutine
	srv := server.NewServer(":5673")
	go func() {
		_ = srv.Start()
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

	// Test cases with different payload sizes and content types
	testCases := []struct {
		name        string
		contentType string
		body        string
	}{
		{"Small text message", "text/plain", "Hello!"},
		{"Medium text message", "text/plain", "Hello, Simple Queue!"},
		{"Large text message", "text/plain", "This is a longer message with more content to test larger payloads and ensure the protocol handles different message sizes correctly."},
		{"JSON message", "application/json", `{"message": "test", "number": 42}`},
		{"Empty message", "text/plain", ""},
		{"Special characters", "text/plain", "Hello 🌍 with émojis and spécial châractérs!"},
		{"Binary-like content", "application/octet-stream", "\x00\x01\x02\x03\xFF\xFE\xFD"},
	}

	// Consume messages from the queue
	msgs, err := channel.Consume(
		queue.Name, // queue
		"",         // consumer
		true,       // auto-ack (automatic acknowledgment)
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	require.NoError(t, err, "Should start consuming")

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Testing: %s - ContentType: %s, BodyLength: %d", tc.name, tc.contentType, len(tc.body))
			
			// Publish the test message
			err = channel.Publish(
				"",         // exchange (empty = default direct exchange)
				queue.Name, // routing key (queue name for default exchange)
				false,      // mandatory
				false,      // immediate
				amqp.Publishing{
					ContentType: tc.contentType,
					Body:        []byte(tc.body),
				},
			)
			assert.NoError(t, err, "Should publish message")

			// Try to receive the message with a timeout
			select {
			case msg := <-msgs:
				t.Logf("Received - Body: '%s' (%d bytes), ContentType: '%s'", 
					string(msg.Body), len(msg.Body), msg.ContentType)
				
				// Verify the message content
				assert.Equal(t, tc.body, string(msg.Body), "Body should match expected content")
				assert.Equal(t, tc.contentType, msg.ContentType, "ContentType should match expected type")
				
				// Log additional message properties for debugging
				t.Logf("Additional properties - DeliveryTag: %d, Redelivered: %v, Exchange: '%s', RoutingKey: '%s'",
					msg.DeliveryTag, msg.Redelivered, msg.Exchange, msg.RoutingKey)
					
			case <-time.After(2 * time.Second):
				t.Fatalf("Did not receive message within timeout for test case: %s", tc.name)
			}
		})
	}
}
