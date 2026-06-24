package main

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/server"
)

// ============================================================================
// Phase 16 End-to-End Benchmarks
//
// These benchmarks start a real AMQP server and use the amqp091-go client
// to measure actual publish/consume throughput. They can be run before/after
// changes to measure real-world impact.
//
// Run: go test . -run='^$' -bench="BenchmarkE2E" -benchmem -benchtime=3s -count=3
// ============================================================================

var benchPortCounter atomic.Int64

// startBenchServer starts a server on a unique port for benchmarking.
func startBenchServer(b *testing.B) (string, func()) {
	b.Helper()
	port := 16000 + int(benchPortCounter.Add(1))
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	cfg := config.DefaultConfig()
	cfg.Network.Address = addr
	cfg.Storage.Path = b.TempDir()

	srv, err := server.NewServerBuilder().WithConfig(cfg).Build()
	if err != nil {
		b.Fatalf("Build failed: %v", err)
	}

	go func() {
		if err := srv.Start(); err != nil {
			b.Logf("Server stopped: %v", err)
		}
	}()

	// Poll until listener is ready
	for i := 0; i < 50; i++ {
		if srv.IsListening() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !srv.IsListening() {
		b.Fatal("server listener not ready")
	}

	uri := fmt.Sprintf("amqp://guest:guest@%s/", addr)
	cleanup := func() {
		srv.Stop()
	}
	return uri, cleanup
}

// BenchmarkE2E_PublishTransient measures end-to-end publish throughput
// for transient messages (no WAL fsync, tests ring buffer + broker + frame path).
func BenchmarkE2E_PublishTransient(b *testing.B) {
	uri, cleanup := startBenchServer(b)
	defer cleanup()

	conn, err := amqp.Dial(uri)
	if err != nil {
		b.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		b.Fatalf("Channel failed: %v", err)
	}
	defer ch.Close()

	_, err = ch.QueueDeclare("bench_transient", false, false, false, false, nil)
	if err != nil {
		b.Fatalf("QueueDeclare failed: %v", err)
	}

	body := make([]byte, 1024) // 1KB message

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := ch.Publish("", "bench_transient", false, false, amqp.Publishing{
			Body:        body,
			ContentType: "application/octet-stream",
		})
		if err != nil {
			b.Fatalf("Publish failed at %d: %v", i, err)
		}
	}
}

// BenchmarkE2E_PublishConsumeRoundtrip measures end-to-end publish + consume
// roundtrip throughput for transient messages.
func BenchmarkE2E_PublishConsumeRoundtrip(b *testing.B) {
	uri, cleanup := startBenchServer(b)
	defer cleanup()

	conn, err := amqp.Dial(uri)
	if err != nil {
		b.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		b.Fatalf("Channel failed: %v", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare("bench_roundtrip", false, false, false, false, nil)
	if err != nil {
		b.Fatalf("QueueDeclare failed: %v", err)
	}

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		b.Fatalf("Consume failed: %v", err)
	}

	body := make([]byte, 1024)

	// Start consumer goroutine
	done := make(chan struct{})
	go func() {
		for range msgs {
			// Drain messages
		}
		close(done)
	}()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := ch.Publish("", q.Name, false, false, amqp.Publishing{
			Body: body,
		})
		if err != nil {
			b.Fatalf("Publish failed at %d: %v", i, err)
		}
	}

	b.StopTimer()
	// Give consumer time to drain remaining messages
	time.Sleep(500 * time.Millisecond)
	conn.Close() // triggers done channel
	<-done
}

// BenchmarkE2E_MultiQueuePublish measures publish throughput across multiple
// queues — tests the lock contention optimization (sync.Map vs RWMutex).
func BenchmarkE2E_MultiQueuePublish(b *testing.B) {
	uri, cleanup := startBenchServer(b)
	defer cleanup()

	conn, err := amqp.Dial(uri)
	if err != nil {
		b.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		b.Fatalf("Channel failed: %v", err)
	}
	defer ch.Close()

	// Declare 10 queues
	queues := make([]string, 10)
	for i := 0; i < 10; i++ {
		queues[i] = fmt.Sprintf("bench_multi_%d", i)
		_, err = ch.QueueDeclare(queues[i], false, false, false, false, nil)
		if err != nil {
			b.Fatalf("QueueDeclare %d failed: %v", i, err)
		}
	}

	body := make([]byte, 1024)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		queueName := queues[i%10]
		err := ch.Publish("", queueName, false, false, amqp.Publishing{
			Body: body,
		})
		if err != nil {
			b.Fatalf("Publish failed at %d: %v", i, err)
		}
	}
}

// BenchmarkE2E_MultiConnectionPublish measures publish throughput with
// multiple concurrent connections — tests connection-level parallelism.
func BenchmarkE2E_MultiConnectionPublish(b *testing.B) {
	uri, cleanup := startBenchServer(b)
	defer cleanup()

	// Set up 5 connections, each with their own channel and queue
	type connState struct {
		conn *amqp.Connection
		ch   *amqp.Channel
		q    amqp.Queue
	}

	states := make([]*connState, 5)
	for i := 0; i < 5; i++ {
		conn, err := amqp.Dial(uri)
		if err != nil {
			b.Fatalf("Dial %d failed: %v", i, err)
		}
		ch, err := conn.Channel()
		if err != nil {
			b.Fatalf("Channel %d failed: %v", i, err)
		}
		qName := fmt.Sprintf("bench_mc_%d", i)
		q, err := ch.QueueDeclare(qName, false, false, false, false, nil)
		if err != nil {
			b.Fatalf("QueueDeclare %d failed: %v", i, err)
		}
		states[i] = &connState{conn: conn, ch: ch, q: q}
	}

	defer func() {
		for _, s := range states {
			s.ch.Close()
			s.conn.Close()
		}
	}()

	body := make([]byte, 1024)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s := states[i%5]
			err := s.ch.Publish("", s.q.Name, false, false, amqp.Publishing{
				Body: body,
			})
			if err != nil {
				b.Fatalf("Publish failed: %v", err)
			}
			i++
		}
	})
}

// BenchmarkE2E_LargeMessage measures throughput with large messages (64KB)
// — tests body pre-allocation optimization and frame fragmentation.
func BenchmarkE2E_LargeMessage(b *testing.B) {
	uri, cleanup := startBenchServer(b)
	defer cleanup()

	conn, err := amqp.Dial(uri)
	if err != nil {
		b.Fatalf("Dial failed: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		b.Fatalf("Channel failed: %v", err)
	}
	defer ch.Close()

	_, err = ch.QueueDeclare("bench_large", false, false, false, false, nil)
	if err != nil {
		b.Fatalf("QueueDeclare failed: %v", err)
	}

	body := make([]byte, 64*1024) // 64KB message

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := ch.Publish("", "bench_large", false, false, amqp.Publishing{
			Body: body,
		})
		if err != nil {
			b.Fatalf("Publish failed at %d: %v", i, err)
		}
	}
}
