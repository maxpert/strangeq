package main

import (
	"context"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

// BenchmarkSQ5_ConfirmedPublish measures pipelined publisher-confirm
// throughput — the path SQ-5 batches. The benchmark publishes b.N messages
// without waiting per message while a drainer collects every confirmation; the
// timer runs until ALL b.N publishes are confirmed, so the metric is confirmed
// msgs/s.
//
// Messages are transient deliberately: with the synchronous durability barrier
// a persistent publish blocks the connection's frame processor for a full WAL
// group-commit (~5ms batch timeout + fsync), so per-connection durable confirm
// throughput is fsync-bound (~100-400/s) and the ack-write cost SQ-5 removes
// is invisible. The transient path exercises the identical confirm bookkeeping
// and flush machinery at rates where it matters. An auto-ack consumer drains
// the queue so the benchmark never parks in queue-full backpressure.
//
//	go test . -run='^$' -bench=BenchmarkSQ5 -benchtime=30000x -count=3
func BenchmarkSQ5_ConfirmedPublish(b *testing.B) {
	ch, qname, teardown := versusSetup(b)
	defer teardown()

	// Drain the queue so sustained publishing never hits the queue's
	// high-water-mark backpressure park.
	deliveries, err := ch.Consume(qname, "", true, false, false, false, nil)
	if err != nil {
		b.Fatalf("consume failed: %v", err)
	}
	go func() {
		for range deliveries {
		}
	}()

	if err := ch.Confirm(false); err != nil {
		b.Fatalf("confirm.select failed: %v", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 8192))
	allConfirmed := make(chan int, 1)
	go func() {
		acked := 0
		for i := 0; i < b.N; i++ {
			c, ok := <-confirms
			if !ok {
				break
			}
			if c.Ack {
				acked++
			}
		}
		allConfirmed <- acked
	}()

	body := make([]byte, 512)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ch.PublishWithContext(ctx, "", qname, false, false, amqp.Publishing{
			DeliveryMode: amqp.Transient,
			ContentType:  "application/octet-stream",
			Body:         body,
		}); err != nil {
			b.Fatalf("publish %d failed: %v", i, err)
		}
	}
	acked := <-allConfirmed
	b.StopTimer()

	if acked != b.N {
		b.Fatalf("confirmed %d of %d publishes", acked, b.N)
	}
}
