package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/server"
)

// TestWritev_MixedSizes_RaceNoCorruption (W5) stresses the zero-copy vectored
// delivery path's buffer-lifetime invariant (BL): many concurrent publishers
// send a mix of 30 B / 1 KiB / 64 KiB persistent messages to a durable queue
// while several manual-ack consumers drain and ack them. Large bodies (>=32
// KiB) are delivered via net.Buffers whose body iovec aliases the stored
// Message.Body; acking a delivery deletes the ring slot (a pointer swap) which
// races the in-flight write. Each body is self-describing (a seed in the first
// 8 bytes deterministically generates the rest), so a torn, aliased, or
// cross-message body is caught as a content mismatch — and -race catches any
// concurrent mutation of a body backing array that a writev is still reading.
func TestWritev_MixedSizes_RaceNoCorruption(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping race stress in -short mode")
	}

	const (
		numPublishers    = 4
		numConsumers     = 3
		msgsPerPublisher = 120
		total            = numPublishers * msgsPerPublisher
	)
	sizes := []int{30, 1024, 64 * 1024}

	uri := writevRaceServer(t)

	queue := "writev-race-q"
	// Declare the durable queue once up front.
	setupConn, err := amqp.Dial(uri)
	if err != nil {
		t.Fatalf("dial (setup): %v", err)
	}
	setupCh, err := setupConn.Channel()
	if err != nil {
		t.Fatalf("channel (setup): %v", err)
	}
	if _, err := setupCh.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		t.Fatalf("queue declare: %v", err)
	}
	_ = setupCh.Close()
	_ = setupConn.Close()

	var received atomic.Int64
	var corrupt atomic.Int64
	done := make(chan struct{})
	var closeOnce sync.Once

	// Consumers: manual-ack, each on its own connection/channel.
	var consumerWG sync.WaitGroup
	for c := 0; c < numConsumers; c++ {
		conn, err := amqp.Dial(uri)
		if err != nil {
			t.Fatalf("dial (consumer %d): %v", c, err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		ch, err := conn.Channel()
		if err != nil {
			t.Fatalf("channel (consumer %d): %v", c, err)
		}
		if err := ch.Qos(50, 0, false); err != nil {
			t.Fatalf("qos (consumer %d): %v", c, err)
		}
		deliveries, err := ch.Consume(queue, "", false /*autoAck*/, false, false, false, nil)
		if err != nil {
			t.Fatalf("consume (consumer %d): %v", c, err)
		}
		consumerWG.Add(1)
		go func() {
			defer consumerWG.Done()
			for {
				select {
				case <-done:
					return
				case d, ok := <-deliveries:
					if !ok {
						return
					}
					if !verifyPatternBody(d.Body) {
						corrupt.Add(1)
					}
					_ = d.Ack(false)
					if received.Add(1) == int64(total) {
						closeOnce.Do(func() { close(done) })
					}
				}
			}
		}()
	}

	// Publishers: persistent messages, mixed sizes, each on its own channel.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var pubWG sync.WaitGroup
	for p := 0; p < numPublishers; p++ {
		conn, err := amqp.Dial(uri)
		if err != nil {
			t.Fatalf("dial (publisher %d): %v", p, err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		ch, err := conn.Channel()
		if err != nil {
			t.Fatalf("channel (publisher %d): %v", p, err)
		}
		pubWG.Add(1)
		go func(p int, ch *amqp.Channel) {
			defer pubWG.Done()
			for i := 0; i < msgsPerPublisher; i++ {
				size := sizes[(p+i)%len(sizes)]
				seed := uint64(p)<<32 | uint64(i)
				body := patternBody(seed, size)
				if err := ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
					DeliveryMode: amqp.Persistent,
					Body:         body,
				}); err != nil {
					return
				}
			}
		}(p, ch)
	}
	pubWG.Wait()

	// Wait for all messages to be consumed (or time out).
	select {
	case <-done:
	case <-time.After(60 * time.Second):
	}
	closeOnce.Do(func() { close(done) })
	consumerWG.Wait()

	if got := received.Load(); got != int64(total) {
		t.Fatalf("received %d/%d messages", got, total)
	}
	if c := corrupt.Load(); c != 0 {
		t.Fatalf("%d delivered bodies failed the self-describing integrity check", c)
	}
}

// writevRaceServer starts a broker with an isolated temp data dir on an
// OS-assigned free port (":0") and returns its AMQP URI. Using an OS-assigned
// port avoids fixed-port collisions when the whole suite runs concurrently.
func writevRaceServer(t *testing.T) string {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Network.Address = "127.0.0.1:0"
	cfg.Storage.Path = t.TempDir()

	srv, err := server.NewServerBuilder().WithConfig(cfg).Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	go func() { _ = srv.Start() }()
	for i := 0; i < 500; i++ {
		if srv.IsListening() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !srv.IsListening() {
		t.Fatal("server listener not ready")
	}
	t.Cleanup(func() { _ = srv.Stop() })
	return fmt.Sprintf("amqp://guest:guest@%s/", srv.Listener.Addr().String())
}

// patternBody builds an n-byte self-describing body: the first 8 bytes hold a
// seed; the remaining bytes are a deterministic function of that seed and the
// byte position, so the whole body can be validated from the seed alone.
func patternBody(seed uint64, n int) []byte {
	b := make([]byte, n)
	binary.BigEndian.PutUint64(b[:8], seed)
	for i := 8; i < n; i++ {
		b[i] = byte(seed) ^ byte(i*31+7)
	}
	return b
}

// verifyPatternBody re-derives the expected bytes from the embedded seed and
// checks every byte, so any torn/aliased/cross-message body fails.
func verifyPatternBody(b []byte) bool {
	if len(b) < 8 {
		return false
	}
	seed := binary.BigEndian.Uint64(b[:8])
	for i := 8; i < len(b); i++ {
		if b[i] != byte(seed)^byte(i*31+7) {
			return false
		}
	}
	return true
}
