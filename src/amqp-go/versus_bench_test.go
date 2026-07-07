package main

import (
	"fmt"
	"os"
	"strconv"
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
	// Silence the embedded server's logger. go test folds a test binary's
	// stdout and stderr into a single stream, so any Info/Error log write
	// (e.g. the unconditional "Error reading frame from connection" on
	// srv.Stop()'s abrupt close) lands mid-line with the -bench result line
	// go test is concurrently assembling, corrupting the exact ns/op fields
	// benchstat parses. See createZapLogger's "silent" level.
	cfg.Server.LogLevel = "silent"
	// Optional override for tuning the prefetch-0 gate cap (see
	// EngineConfig.UnlimitedPrefetchCap), e.g. PREFETCH0_CAP=1024 to compare cap
	// values head-to-head. Must stay > the MultiAck1000 ack cadence (1000).
	if v := os.Getenv("PREFETCH0_CAP"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.Engine.UnlimitedPrefetchCap = n
		}
	}

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
	var received atomic.Int64
	done := make(chan struct{})
	go func() {
		for range msgs {
			received.Add(1)
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
	waitForConsumed(b, &received, int64(b.N))
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
	var received atomic.Int64
	done := make(chan struct{})
	go func() {
		for d := range msgs {
			received.Add(1)
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
	waitForConsumed(b, &received, int64(b.N))
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
	// received is asserted after the run: a benchmark that only times the
	// publish loop can "pass" while consumption silently wedges (this exact
	// benchmark once wedged at ~2000 received because the server dropped the
	// multiple flag from basic.ack, and nothing noticed).
	var received atomic.Int64
	done := make(chan struct{})
	go func() {
		var count int
		for d := range msgs {
			count++
			received.Store(int64(count))
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
	waitForConsumed(b, &received, int64(b.N))
	ch.Close()
	<-done
}

// waitForConsumed fails the benchmark if the consumer does not receive every
// published message within a generous deadline. This turns silent consumption
// wedges (deadlocks, credit leaks) into hard benchmark failures instead of
// cosmetic publish-throughput passes.
func waitForConsumed(b *testing.B, received *atomic.Int64, want int64) {
	b.Helper()
	deadline := time.Now().Add(30 * time.Second)
	last, lastProgress := int64(-1), time.Now()
	for {
		r := received.Load()
		if r >= want {
			return
		}
		if r != last {
			last, lastProgress = r, time.Now()
		} else if time.Since(lastProgress) > 10*time.Second {
			b.Fatalf("consumption stalled: received %d of %d (no progress for 10s)", r, want)
		}
		if time.Now().After(deadline) {
			b.Fatalf("consumption incomplete: received %d of %d after 30s", r, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
