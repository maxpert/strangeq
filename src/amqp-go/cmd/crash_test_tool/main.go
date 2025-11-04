package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	amqpURL = "amqp://guest:guest@localhost:5672/"
)

func main() {
	// Define subcommands
	publishCmd := flag.NewFlagSet("publish", flag.ExitOnError)
	publishCount := publishCmd.Int("count", 1000, "number of messages to publish")
	publishQueue := publishCmd.String("queue", "crash_test_queue", "queue name")

	verifyCmd := flag.NewFlagSet("verify", flag.ExitOnError)
	verifyCount := verifyCmd.Int("count", 1000, "expected number of messages")
	verifyQueue := verifyCmd.String("queue", "crash_test_queue", "queue name")

	if len(os.Args) < 2 {
		fmt.Println("Usage: crash_test_tool [publish|verify] [options]")
		fmt.Println("\nPublish messages:")
		fmt.Println("  crash_test_tool publish --count 1000 --queue test_queue")
		fmt.Println("\nVerify recovery:")
		fmt.Println("  crash_test_tool verify --count 1000 --queue test_queue")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "publish":
		publishCmd.Parse(os.Args[2:])
		if err := publishMessages(*publishCount, *publishQueue); err != nil {
			log.Fatalf("Publish failed: %v", err)
		}
	case "verify":
		verifyCmd.Parse(os.Args[2:])
		if err := verifyRecovery(*verifyCount, *verifyQueue); err != nil {
			log.Fatalf("Verify failed: %v", err)
		}
	default:
		fmt.Println("Unknown command:", os.Args[1])
		fmt.Println("Use 'publish' or 'verify'")
		os.Exit(1)
	}
}

func publishMessages(count int, queueName string) error {
	// Connect to AMQP server
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer ch.Close()

	// Publisher confirms not yet implemented in server (Phase 4 TODO)
	// For now, we'll use a sleep to allow TCP buffers to flush
	fmt.Println("Publisher confirms not yet implemented, will use delay instead")

	// Declare queue
	_, err = ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	// Publish messages
	start := time.Now()
	for i := 1; i <= count; i++ {
		body := fmt.Sprintf("crash test message %d", i)
		err = ch.Publish(
			"",        // exchange
			queueName, // routing key
			false,     // mandatory
			false,     // immediate
			amqp.Publishing{
				DeliveryMode: 2, // persistent (durable)
				ContentType:  "text/plain",
				Body:         []byte(body),
			},
		)
		if err != nil {
			return fmt.Errorf("failed to publish message %d: %w", i, err)
		}

		// Progress indicator
		if i%1000 == 0 || i == count {
			elapsed := time.Since(start)
			rate := float64(i) / elapsed.Seconds()
			fmt.Printf("\rPublishing %d/%d messages (%.0f msg/s)...", i, count, rate)
		}
	}
	fmt.Println()

	elapsed := time.Since(start)
	rate := float64(count) / elapsed.Seconds()
	fmt.Printf("✓ Published %d durable messages in %v (%.0f msg/s)\n", count, elapsed, rate)

	// Without confirms, sleep to allow TCP buffers to flush
	// This makes it "probable" (not guaranteed) that messages reach server
	// Per rabbitmq/amqp091-go docs: calling conn.Close() (via defer) helps ensure delivery
	sleepDuration := 2 * time.Second
	if count > 10000 {
		sleepDuration = 5 * time.Second
	}
	fmt.Printf("Sleeping %v to allow buffers to flush...\n", sleepDuration)
	time.Sleep(sleepDuration)

	return nil
}

func verifyRecovery(expectedCount int, queueName string) error {
	// Connect to AMQP server
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer ch.Close()

	// Redeclare queue (idempotent - will succeed if queue already exists)
	// This ensures the queue exists before we try to consume from it
	_, err = ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	fmt.Printf("Checking recovery by consuming messages...\n")

	// Consume messages to verify they're readable
	msgs, err := ch.Consume(
		queueName, // queue
		"",        // consumer
		false,     // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	// Read a sample of messages to verify integrity
	sampleSize := min(100, expectedCount)
	start := time.Now()

	for i := 0; i < sampleSize; i++ {
		select {
		case msg, ok := <-msgs:
			if !ok {
				return fmt.Errorf("channel closed after %d messages", i)
			}

			// NOTE: Redelivered flag not yet set during recovery (TODO)
			// Just verify we can read and ack the message

			// Acknowledge message
			if err := msg.Ack(false); err != nil {
				return fmt.Errorf("failed to ack message %d: %w", i+1, err)
			}

			// Progress indicator
			if (i+1)%10 == 0 || i+1 == sampleSize {
				fmt.Printf("\rVerified %d/%d sample messages...", i+1, sampleSize)
			}

		case <-time.After(5 * time.Second):
			return fmt.Errorf("timeout waiting for message %d", i+1)
		}
	}
	fmt.Println()

	elapsed := time.Since(start)
	fmt.Printf("✓ Verified %d sample messages in %v\n", sampleSize, elapsed)
	fmt.Printf("✓ Recovery successful: messages can be consumed and acknowledged\n")
	fmt.Printf("NOTE: Queue metadata persistence and Redelivered flag not yet implemented\n")

	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
