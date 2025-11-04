package main

import (
	"testing"
	"time"

	"github.com/maxpert/amqp-go/server"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBasicPublishConsume tests basic message publishing and consumption
func TestBasicPublishConsume(t *testing.T) {
	// Start a server in a goroutine
	srv := server.NewServer(":5690") // Use a different port to avoid conflicts
	go func() {
		_ = srv.Start()
	}()

	// Give the server a moment to start
	time.Sleep(200 * time.Millisecond)

	// Connect using the standard AMQP client
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5690//")
	require.NoError(t, err, "Should connect to server")
	defer conn.Close()

	channel, err := conn.Channel()
	require.NoError(t, err, "Should open channel")
	defer channel.Close()

	// Declare a queue
	queue, err := channel.QueueDeclare(
		"test-publish-consume-queue", // name
		false,                        // durable
		true,                         // delete when unused
		false,                        // exclusive
		false,                        // no-wait
		nil,                          // arguments
	)
	require.NoError(t, err, "Should declare queue")

	// Consume the message FIRST to ensure we're ready to receive
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

	// Give consumer time to register
	time.Sleep(100 * time.Millisecond)

	// Publish a message
	err = channel.Publish(
		"",         // exchange
		queue.Name, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte("Hello, World!"),
		},
	)
	assert.NoError(t, err, "Should publish message")

	// Try to receive the message
	select {
	case msg := <-msgs:
		assert.Equal(t, "Hello, World!", string(msg.Body))
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Did not receive message within timeout")
	}
}

// TestManualAcknowledgment tests manual message acknowledgment
func TestManualAcknowledgment(t *testing.T) {
	// Start a server in a goroutine
	srv := server.NewServer(":5691") // Use a different port to avoid conflicts
	go func() {
		_ = srv.Start()
	}()

	// Give the server a moment to start
	time.Sleep(200 * time.Millisecond)

	// Connect using the standard AMQP client
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5691//")
	require.NoError(t, err, "Should connect to server")
	defer conn.Close()

	channel, err := conn.Channel()
	require.NoError(t, err, "Should open channel")
	defer channel.Close()

	// Declare a queue
	queue, err := channel.QueueDeclare(
		"test-manual-ack-queue", // name
		false,                   // durable
		true,                    // delete when unused
		false,                   // exclusive
		false,                   // no-wait
		nil,                     // arguments
	)
	require.NoError(t, err, "Should declare queue")

	// Consume the message with manual acknowledgment FIRST
	msgs, err := channel.Consume(
		queue.Name, // queue
		"",         // consumer
		false,      // auto-ack (manual acknowledgment)
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	require.NoError(t, err, "Should start consuming")

	// Give consumer time to register
	time.Sleep(100 * time.Millisecond)

	// Publish a message
	err = channel.Publish(
		"",         // exchange
		queue.Name, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte("Test message for manual ack"),
		},
	)
	assert.NoError(t, err, "Should publish message")

	// Try to receive the message
	select {
	case msg := <-msgs:
		assert.Equal(t, "Test message for manual ack", string(msg.Body))

		// Manually acknowledge the message
		err = msg.Ack(false)
		assert.NoError(t, err, "Should acknowledge message")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Did not receive message within timeout")
	}
}

// TestMessageRejection tests message rejection with requeuing
func TestMessageRejection(t *testing.T) {
	// Start a server in a goroutine
	srv := server.NewServer(":5692") // Use a different port to avoid conflicts
	go func() {
		_ = srv.Start()
	}()

	// Give the server a moment to start
	time.Sleep(200 * time.Millisecond)

	// Connect using the standard AMQP client
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5692//")
	require.NoError(t, err, "Should connect to server")
	defer conn.Close()

	channel, err := conn.Channel()
	require.NoError(t, err, "Should open channel")
	defer channel.Close()

	// Declare a queue
	queue, err := channel.QueueDeclare(
		"test-reject-queue", // name
		false,               // durable
		true,                // delete when unused
		false,               // exclusive
		false,               // no-wait
		nil,                 // arguments
	)
	require.NoError(t, err, "Should declare queue")

	// Consume the message with manual acknowledgment FIRST
	msgs, err := channel.Consume(
		queue.Name, // queue
		"",         // consumer
		false,      // auto-ack (manual acknowledgment)
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	require.NoError(t, err, "Should start consuming")

	// Give consumer time to register
	time.Sleep(100 * time.Millisecond)

	// Publish a message
	err = channel.Publish(
		"",         // exchange
		queue.Name, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte("Test message for rejection"),
		},
	)
	assert.NoError(t, err, "Should publish message")

	// Try to receive the message
	select {
	case msg := <-msgs:
		assert.Equal(t, "Test message for rejection", string(msg.Body))

		// Reject the message with requeuing
		err = msg.Reject(true) // true = requeue
		assert.NoError(t, err, "Should reject message with requeuing")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Did not receive message within timeout")
	}
}
// TestMultipleConsumers tests multiple consumers on the same queue
func TestMultipleConsumers(t *testing.T) {
	// Start a server in a goroutine
	srv := server.NewServer(":5694") // Use a different port to avoid conflicts
	go func() {
		_ = srv.Start()
	}()

	// Give the server a moment to start
	time.Sleep(200 * time.Millisecond)

	// Connect using the standard AMQP client
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5694//")
	require.NoError(t, err, "Should connect to server")
	defer conn.Close()

	// Open two channels
	channel1, err := conn.Channel()
	require.NoError(t, err, "Should open first channel")
	defer channel1.Close()

	channel2, err := conn.Channel()
	require.NoError(t, err, "Should open second channel")
	defer channel2.Close()

	// Declare a queue
	queue, err := channel1.QueueDeclare(
		"test-multi-consumer-queue", // name
		false,                       // durable
		true,                        // delete when unused
		false,                       // exclusive
		false,                       // no-wait
		nil,                         // arguments
	)
	require.NoError(t, err, "Should declare queue")

	// Start two consumers FIRST
	msgs1, err := channel1.Consume(
		queue.Name,   // queue
		"consumer-1", // consumer
		false,        // auto-ack (manual acknowledgment)
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	require.NoError(t, err, "Should start first consumer")

	msgs2, err := channel2.Consume(
		queue.Name,   // queue
		"consumer-2", // consumer
		false,        // auto-ack (manual acknowledgment)
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	require.NoError(t, err, "Should start second consumer")

	// Give consumers time to register
	time.Sleep(100 * time.Millisecond)

	// Publish multiple messages
	messages := []string{"Message A", "Message B", "Message C", "Message D"}
	for _, msg := range messages {
		err = channel1.Publish(
			"",         // exchange
			queue.Name, // routing key
			false,      // mandatory
			false,      // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(msg),
			},
		)
		assert.NoError(t, err, "Should publish message")
	}

	// Collect messages from both consumers
	receivedMessages := make(map[string]bool)

	// Receive messages with timeout
	timeout := time.After(3 * time.Second)
	done := make(chan bool)

	go func() {
		for {
			select {
			case msg1 := <-msgs1:
				receivedMessages[string(msg1.Body)] = true
				msg1.Ack(false)
			case msg2 := <-msgs2:
				receivedMessages[string(msg2.Body)] = true
				msg2.Ack(false)
			case <-timeout:
				done <- true
				return
			}
		}
	}()

	<-done

	// Verify we received all messages
	expectedMessages := map[string]bool{
		"Message A": true,
		"Message B": true,
		"Message C": true,
		"Message D": true,
	}

	assert.Equal(t, expectedMessages, receivedMessages, "Should receive all messages distributed among consumers")
}

// TestNegativeAcknowledgment tests message negative acknowledgment (nack)
func TestNegativeAcknowledgment(t *testing.T) {
	// Start a server in a goroutine
	srv := server.NewServer(":5695") // Use a different port to avoid conflicts
	go func() {
		_ = srv.Start()
	}()

	// Give the server a moment to start
	time.Sleep(200 * time.Millisecond)

	// Connect using the standard AMQP client
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5695//")
	require.NoError(t, err, "Should connect to server")
	defer conn.Close()

	channel, err := conn.Channel()
	require.NoError(t, err, "Should open channel")
	defer channel.Close()

	// Declare a queue
	queue, err := channel.QueueDeclare(
		"test-nack-queue", // name
		false,             // durable
		true,              // delete when unused
		false,             // exclusive
		false,             // no-wait
		nil,               // arguments
	)
	require.NoError(t, err, "Should declare queue")

	// Consume the message with manual acknowledgment FIRST
	msgs, err := channel.Consume(
		queue.Name, // queue
		"",         // consumer
		false,      // auto-ack (manual acknowledgment)
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	require.NoError(t, err, "Should start consuming")

	// Give consumer time to register
	time.Sleep(100 * time.Millisecond)

	// Publish a message
	err = channel.Publish(
		"",         // exchange
		queue.Name, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte("Test message for nack"),
		},
	)
	assert.NoError(t, err, "Should publish message")

	// Try to receive the message
	select {
	case msg := <-msgs:
		assert.Equal(t, "Test message for nack", string(msg.Body))

		// Negative acknowledge the message with requeuing
		err = msg.Nack(false, true) // multiple=false, requeue=true
		assert.NoError(t, err, "Should negative acknowledge message with requeuing")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Did not receive message within timeout")
	}
}

// TestNegativeAcknowledgmentMultiple tests message negative acknowledgment with multiple flag
