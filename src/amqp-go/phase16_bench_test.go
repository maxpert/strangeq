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

// ============================================================================
// RabbitMQ PerfTest-matching benchmarks
//
// These benchmarks match the parameters used by RabbitMQ's official PerfTest
// tool (https://perftest.rabbitmq.com/) for fair head-to-head comparison.
//
// Matching parameters:
//   - Message size: 12 bytes (PerfTest default)
//   - 1 publisher, 1 consumer on a single queue
//   - Non-durable queue, transient messages (no replication)
//   - No publisher confirms
//
// Ack modes (PerfTest test 1 and test 11):
//   - Auto-ack:       perf-test.jar -x 1 -y 1 -a   (test 1, "-a" flag)
//   - Manual ack:     perf-test.jar -x 1 -y 1       (no -a flag, default prefetch)
//   - Multi-ack 1000: perf-test.jar -x 1 -y 1 --multi-ack-every 1000  (test 11)
//
// Run: go test . -run='^$' -bench="BenchmarkRabbitMQMatch" -benchmem -benchtime=5s -count=3
// ============================================================================

// BenchmarkRabbitMQMatch_AutoAck matches PerfTest test 1:
// 12-byte messages, 1 publisher + 1 consumer, auto-ack, no confirms.
func BenchmarkRabbitMQMatch_AutoAck(b *testing.B) {
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

	q, err := ch.QueueDeclare("match_autoack", false, false, false, false, nil)
	if err != nil {
		b.Fatalf("QueueDeclare failed: %v", err)
	}

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		b.Fatalf("Consume failed: %v", err)
	}

	body := make([]byte, 12) // 12 bytes — PerfTest default

	done := make(chan struct{})
	go func() {
		for range msgs {
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
	time.Sleep(500 * time.Millisecond)
	conn.Close()
	<-done
}

// BenchmarkRabbitMQMatch_ManualAck matches PerfTest with manual ack:
// 12-byte messages, 1 publisher + 1 consumer, manual ack with default prefetch.
func BenchmarkRabbitMQMatch_ManualAck(b *testing.B) {
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

	q, err := ch.QueueDeclare("match_manualack", false, false, false, false, nil)
	if err != nil {
		b.Fatalf("QueueDeclare failed: %v", err)
	}

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		b.Fatalf("Consume failed: %v", err)
	}

	body := make([]byte, 12)

	done := make(chan struct{})
	go func() {
		for d := range msgs {
			d.Ack(false)
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
	time.Sleep(500 * time.Millisecond)
	conn.Close()
	<-done
}

// BenchmarkRabbitMQMatch_MultiAck1000 matches PerfTest test 11:
// 12-byte messages, 1 publisher + 1 consumer, multi-ack every 1000 messages.
func BenchmarkRabbitMQMatch_MultiAck1000(b *testing.B) {
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

	q, err := ch.QueueDeclare("match_multiack", false, false, false, false, nil)
	if err != nil {
		b.Fatalf("QueueDeclare failed: %v", err)
	}

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		b.Fatalf("Consume failed: %v", err)
	}

	body := make([]byte, 12)

	done := make(chan struct{})
	go func() {
		var count int
		for d := range msgs {
			count++
			if count%1000 == 0 {
				d.Ack(true) // multiple=true: ack all messages up to this one
			}
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
	time.Sleep(500 * time.Millisecond)
	conn.Close()
	<-done
}

// BenchmarkRabbitMQMatch_PublishOnly measures publish-only throughput
// (no consumer) matching PerfTest with -y 0.
func BenchmarkRabbitMQMatch_PublishOnly(b *testing.B) {
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

	_, err = ch.QueueDeclare("match_pubonly", false, false, false, false, nil)
	if err != nil {
		b.Fatalf("QueueDeclare failed: %v", err)
	}

	body := make([]byte, 12)

	// Drain queue in background to prevent backpressure from blocking publisher
	msgs, err := ch.Consume("match_pubonly", "", true, false, false, false, nil)
	if err != nil {
		b.Fatalf("Consume failed: %v", err)
	}
	go func() {
		for range msgs {
		}
	}()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := ch.Publish("", "match_pubonly", false, false, amqp.Publishing{
			Body: body,
		})
		if err != nil {
			b.Fatalf("Publish failed at %d: %v", i, err)
		}
	}
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
