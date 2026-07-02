package main

import (
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/server"
)

// ============================================================================
// Head-to-head benchmarks: StrangeQ vs RabbitMQ
//
// Same Go client (amqp091-go), same message size (12 bytes), same ack modes.
// The only variable is the broker.
//
// To benchmark RabbitMQ:
//   AMQP_TARGET=rabbitmq go test . -run='^$' -bench="BenchmarkVersus" -benchmem -benchtime=50000x -count=3
//
// To benchmark StrangeQ (default, no AMQP_TARGET env var):
//   go test . -run='^$' -bench="BenchmarkVersus" -benchmem -benchtime=50000x -count=3
//
// RabbitMQ must be running on localhost:5672 (e.g. docker run -d -p 5672:5672 rabbitmq:4.3-management).
// ============================================================================

const versusMsgSize = 12 // bytes — matches RabbitMQ PerfTest default

func versusURI(b *testing.B) (string, func()) {
	b.Helper()
	if target := os.Getenv("AMQP_TARGET"); target == "rabbitmq" {
		return "amqp://guest:guest@localhost:5672/", func() {}
	}

	// StrangeQ: start embedded server
	port := 17000 + int(benchPortCounter.Add(1))
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
	for i := 0; i < 50; i++ {
		if srv.IsListening() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !srv.IsListening() {
		b.Fatal("server listener not ready")
	}
	return fmt.Sprintf("amqp://guest:guest@%s/", addr), func() { srv.Stop() }
}

var versusQueueCounter atomic.Int64

// versusSetup creates a durable queue (RabbitMQ 4.3+ requires durable for
// non-exclusive queues) with a unique name per run, and returns the channel
// and queue name. Messages remain transient (no DeliveryMode=2) so the
// per-message hot path is in-memory on both brokers.
func versusSetup(b *testing.B) (*amqp.Channel, string, func()) {
	b.Helper()
	uri, cleanup := versusURI(b)

	conn, err := amqp.Dial(uri)
	if err != nil {
		cleanup()
		b.Fatalf("Dial failed: %v", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		cleanup()
		b.Fatalf("Channel failed: %v", err)
	}

	qname := fmt.Sprintf("versus_%s_%d", b.Name(), versusQueueCounter.Add(1))
	_, err = ch.QueueDeclare(qname, true, false, false, false, nil) // durable=true
	if err != nil {
		ch.Close()
		conn.Close()
		cleanup()
		b.Fatalf("QueueDeclare failed: %v", err)
	}

	teardown := func() {
		ch.Close()
		conn.Close()
		cleanup()
	}
	return ch, qname, teardown
}

// BenchmarkVersus_AutoAck: 12B, 1 pub + 1 consumer, auto-ack
func BenchmarkVersus_AutoAck(b *testing.B) {
	ch, qname, teardown := versusSetup(b)
	defer teardown()

	msgs, err := ch.Consume(qname, "", true, false, false, false, nil)
	if err != nil {
		b.Fatalf("Consume failed: %v", err)
	}

	body := make([]byte, versusMsgSize)
	done := make(chan struct{})
	go func() {
		for range msgs {
		}
		close(done)
	}()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := ch.Publish("", qname, false, false, amqp.Publishing{Body: body}); err != nil {
			b.Fatalf("Publish failed at %d: %v", i, err)
		}
	}

	b.StopTimer()
	time.Sleep(500 * time.Millisecond)
	ch.Close()
	<-done
}

// BenchmarkVersus_ManualAck: 12B, 1 pub + 1 consumer, manual ack per message
func BenchmarkVersus_ManualAck(b *testing.B) {
	ch, qname, teardown := versusSetup(b)
	defer teardown()

	msgs, err := ch.Consume(qname, "", false, false, false, false, nil)
	if err != nil {
		b.Fatalf("Consume failed: %v", err)
	}

	body := make([]byte, versusMsgSize)
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
		if err := ch.Publish("", qname, false, false, amqp.Publishing{Body: body}); err != nil {
			b.Fatalf("Publish failed at %d: %v", i, err)
		}
	}

	b.StopTimer()
	time.Sleep(500 * time.Millisecond)
	ch.Close()
	<-done
}

// BenchmarkVersus_MultiAck1000: 12B, 1 pub + 1 consumer, multi-ack every 1000
func BenchmarkVersus_MultiAck1000(b *testing.B) {
	ch, qname, teardown := versusSetup(b)
	defer teardown()

	msgs, err := ch.Consume(qname, "", false, false, false, false, nil)
	if err != nil {
		b.Fatalf("Consume failed: %v", err)
	}

	body := make([]byte, versusMsgSize)
	done := make(chan struct{})
	go func() {
		var count int
		for d := range msgs {
			count++
			if count%1000 == 0 {
				d.Ack(true)
			}
		}
		close(done)
	}()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := ch.Publish("", qname, false, false, amqp.Publishing{Body: body}); err != nil {
			b.Fatalf("Publish failed at %d: %v", i, err)
		}
	}

	b.StopTimer()
	time.Sleep(500 * time.Millisecond)
	ch.Close()
	<-done
}
