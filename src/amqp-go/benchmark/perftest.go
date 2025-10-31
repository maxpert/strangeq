package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Metrics tracks benchmark statistics
type Metrics struct {
	published  atomic.Int64
	consumed   atomic.Int64
	confirmed  atomic.Int64
	returned   atomic.Int64
	latencies  []time.Duration
	latencyMu  sync.Mutex
	startTime  time.Time
}

func (m *Metrics) recordLatency(d time.Duration) {
	m.latencyMu.Lock()
	m.latencies = append(m.latencies, d)
	m.latencyMu.Unlock()
}

func main() {
	var (
		uri         = flag.String("uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
		producers   = flag.Int("producers", 1, "Number of producers")
		consumers   = flag.Int("consumers", 1, "Number of consumers")
		duration    = flag.Duration("duration", 30*time.Second, "Test duration")
		rate        = flag.Int("rate", 0, "Publishing rate limit (msg/s, 0 = unlimited)")
		size        = flag.Int("size", 1024, "Message size in bytes")
		persistent  = flag.Bool("persistent", false, "Use persistent messages")
		queueName   = flag.String("queue", "perftest", "Queue name")
		autoAck     = flag.Bool("auto-ack", false, "Auto-acknowledge messages")
		prefetch    = flag.Int("prefetch", 1, "Consumer prefetch count")
	)
	flag.Parse()

	metrics := &Metrics{
		startTime: time.Now(),
		latencies: make([]time.Duration, 0, 100000),
	}

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	// Handle interrupts
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	var wg sync.WaitGroup

	// Start producers
	for i := 0; i < *producers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runProducer(ctx, *uri, *queueName, *size, *persistent, *rate / *producers, metrics)
		}(i)
	}

	// Start consumers
	for i := 0; i < *consumers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runConsumer(ctx, *uri, *queueName, *autoAck, *prefetch, metrics)
		}(i)
	}

	// Print stats every second
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		lastPub := int64(0)
		lastCon := int64(0)
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
				pub := metrics.published.Load()
				con := metrics.consumed.Load()
				fmt.Printf("published: %d msg/s, consumed: %d msg/s, confirmed: %d\n",
					pub-lastPub, con-lastCon, metrics.confirmed.Load())
				lastPub = pub
				lastCon = con
			}
		}
	}()

	wg.Wait()
	ticker.Stop()

	printResults(metrics)
}

func runProducer(ctx context.Context, uri, queueName string, size int, persistent bool, rateLimit int, metrics *Metrics) {
	conn, err := amqp.Dial(uri)
	if err != nil {
		log.Printf("Producer failed to connect: %v", err)
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Printf("Producer failed to open channel: %v", err)
		return
	}
	defer ch.Close()

	// Declare queue
	_, err = ch.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		log.Printf("Producer failed to declare queue: %v", err)
		return
	}

	// Enable publisher confirms
	if err := ch.Confirm(false); err != nil {
		log.Printf("Producer failed to enable confirms: %v", err)
		return
	}

	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 100))
	go func() {
		for range confirms {
			metrics.confirmed.Add(1)
		}
	}()

	body := make([]byte, size)
	for i := range body {
		body[i] = byte(i % 256)
	}

	deliveryMode := amqp.Transient
	if persistent {
		deliveryMode = amqp.Persistent
	}

	var rateLimiter <-chan time.Time
	if rateLimit > 0 {
		rateLimiter = time.Tick(time.Second / time.Duration(rateLimit))
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			if rateLimiter != nil {
				<-rateLimiter
			}

			startTime := time.Now()
			err := ch.PublishWithContext(
				ctx,
				"",        // exchange
				queueName, // routing key
				false,     // mandatory
				false,     // immediate
				amqp.Publishing{
					DeliveryMode: deliveryMode,
					ContentType:  "application/octet-stream",
					Body:         body,
					Timestamp:    startTime,
				},
			)

			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("Publish error: %v", err)
				time.Sleep(100 * time.Millisecond)
				continue
			}

			metrics.published.Add(1)
		}
	}
}

func runConsumer(ctx context.Context, uri, queueName string, autoAck bool, prefetch int, metrics *Metrics) {
	conn, err := amqp.Dial(uri)
	if err != nil {
		log.Printf("Consumer failed to connect: %v", err)
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Printf("Consumer failed to open channel: %v", err)
		return
	}
	defer ch.Close()

	// Declare queue
	_, err = ch.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		log.Printf("Consumer failed to declare queue: %v", err)
		return
	}

	// Set prefetch
	if err := ch.Qos(prefetch, 0, false); err != nil {
		log.Printf("Consumer failed to set QoS: %v", err)
		return
	}

	msgs, err := ch.Consume(
		queueName,
		"",      // consumer tag
		autoAck, // auto-ack
		false,   // exclusive
		false,   // no-local
		false,   // no-wait
		nil,     // args
	)
	if err != nil {
		log.Printf("Consumer failed to consume: %v", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-msgs:
			if !ok {
				return
			}

			// Calculate latency
			latency := time.Since(msg.Timestamp)
			metrics.recordLatency(latency)
			metrics.consumed.Add(1)

			if !autoAck {
				if err := msg.Ack(false); err != nil {
					log.Printf("Ack error: %v", err)
				}
			}
		}
	}
}

func printResults(m *Metrics) {
	elapsed := time.Since(m.startTime)
	pub := m.published.Load()
	con := m.consumed.Load()
	conf := m.confirmed.Load()

	fmt.Println("\n=== Performance Test Results ===")
	fmt.Printf("Duration: %.2fs\n", elapsed.Seconds())
	fmt.Printf("\nThroughput:\n")
	fmt.Printf("  Published: %d messages (%.0f msg/s)\n", pub, float64(pub)/elapsed.Seconds())
	fmt.Printf("  Consumed:  %d messages (%.0f msg/s)\n", con, float64(con)/elapsed.Seconds())
	fmt.Printf("  Confirmed: %d messages\n", conf)

	if len(m.latencies) > 0 {
		printLatencyStats(m.latencies)
	}
}

func printLatencyStats(latencies []time.Duration) {
	// Sort latencies
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)

	// Simple bubble sort (good enough for benchmarks)
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			if sorted[i] > sorted[j] {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}

	fmt.Printf("\nConsumer Latency:\n")
	fmt.Printf("  min:    %v\n", sorted[0])
	fmt.Printf("  median: %v\n", sorted[len(sorted)/2])
	fmt.Printf("  75th:   %v\n", sorted[len(sorted)*75/100])
	fmt.Printf("  95th:   %v\n", sorted[len(sorted)*95/100])
	fmt.Printf("  99th:   %v\n", sorted[len(sorted)*99/100])
	fmt.Printf("  max:    %v\n", sorted[len(sorted)-1])
}
