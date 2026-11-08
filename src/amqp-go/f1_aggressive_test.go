package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/server"
)

// ============================================================================
// F1 Aggressive: harder-to-pass variants of the starvation test with
// smaller ring buffers, more messages, multiple producers, and tight prefetch.
// ============================================================================

var f1AggPortCounter atomic.Int64

func f1AggServer(t *testing.T, fsync bool, ringSize int, walChanBuf int) (string, func()) {
	t.Helper()
	port := 19300 + int(f1AggPortCounter.Add(1))
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	cfg := config.DefaultConfig()
	cfg.Network.Address = addr
	cfg.Storage.Path = t.TempDir()
	cfg.Server.LogLevel = "silent"
	if fsync {
		cfg.Storage.Durability = interfaces.DurabilityBarrier
	} else {
		cfg.Storage.Durability = interfaces.DurabilityNone
	}
	if ringSize > 0 {
		cfg.Engine.RingBufferSize = ringSize
	}
	if walChanBuf > 0 {
		cfg.Engine.WALChannelBuffer = walChanBuf
	}

	srv, err := server.NewServerBuilder().WithConfig(cfg).Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	waitForListening(t, srv)
	return fmt.Sprintf("amqp://guest:guest@%s/", addr), func() { testShutdownServer(t, srv) }
}

func f1AggDump(t *testing.T, label string) {
	t.Helper()
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	t.Logf("=== GOROUTINE DUMP (%s) ===\n%s", label, string(buf[:n]))
}

// TestF1_Agg_SmallRing_100K tests with a tiny ring buffer (512 messages, spill
// at 80% = 410) and 100K messages. This forces most messages to spill to WAL,
// making GetMessage fall through to wal.Read (which takes fileMutex).
func TestF1_Agg_SmallRing_100K(t *testing.T) {
	uri, cleanup := f1AggServer(t, false, 512, 1000)
	defer cleanup()

	qName := fmt.Sprintf("f1-agg-smallring-%d", f1AggPortCounter.Load())

	cconn, err := amqp.Dial(uri)
	if err != nil {
		t.Fatalf("consumer dial: %v", err)
	}
	defer cconn.Close()
	cch, err := cconn.Channel()
	if err != nil {
		t.Fatalf("consumer channel: %v", err)
	}
	defer cch.Close()
	if _, err := cch.QueueDeclare(qName, true, false, false, false, nil); err != nil {
		t.Fatalf("queue declare: %v", err)
	}
	msgs, err := cch.Consume(qName, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	pconn, err := amqp.Dial(uri)
	if err != nil {
		t.Fatalf("producer dial: %v", err)
	}
	defer pconn.Close()
	pch, err := pconn.Channel()
	if err != nil {
		t.Fatalf("producer channel: %v", err)
	}
	defer pch.Close()
	if err := pch.Confirm(false); err != nil {
		t.Fatalf("confirm.select: %v", err)
	}
	confirms := pch.NotifyPublish(make(chan amqp.Confirmation, 10100))

	const n = 100000
	body := make([]byte, 1024)
	for i := 0; i < n; i++ {
		if err := pch.PublishWithContext(context.Background(), "", qName, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Body:         body,
		}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	recv := 0
	deadline := time.After(30 * time.Second)
	for recv < 1 {
		select {
		case _, ok := <-msgs:
			if !ok {
				f1AggDump(t, fmt.Sprintf("closed recv=%d", recv))
				t.Fatalf("consumer channel closed after %d messages", recv)
			}
			recv++
		case <-deadline:
			f1AggDump(t, fmt.Sprintf("starved recv=%d", recv))
			t.Fatalf("CONSUMER STARVED: received %d messages within 30s (smallRing, 100K)", recv)
		}
	}
	t.Logf("PASS (smallRing, 100K): consumer received %d messages within 30s", recv)

	go func() {
		for i := 0; i < n; i++ {
			select {
			case <-confirms:
			case <-time.After(30 * time.Second):
				return
			}
		}
	}()
}

// TestF1_Agg_TinyWALChan tests with a tiny WAL channel buffer (10) and 50K
// messages. A tiny WAL channel could cause StoreMessageAsync to block, which
// would block the frame processor, potentially interacting with backpressure.
func TestF1_Agg_TinyWALChan(t *testing.T) {
	uri, cleanup := f1AggServer(t, false, 0, 10)
	defer cleanup()

	qName := fmt.Sprintf("f1-agg-tinywal-%d", f1AggPortCounter.Load())

	cconn, err := amqp.Dial(uri)
	if err != nil {
		t.Fatalf("consumer dial: %v", err)
	}
	defer cconn.Close()
	cch, err := cconn.Channel()
	if err != nil {
		t.Fatalf("consumer channel: %v", err)
	}
	defer cch.Close()
	if _, err := cch.QueueDeclare(qName, true, false, false, false, nil); err != nil {
		t.Fatalf("queue declare: %v", err)
	}
	msgs, err := cch.Consume(qName, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	pconn, err := amqp.Dial(uri)
	if err != nil {
		t.Fatalf("producer dial: %v", err)
	}
	defer pconn.Close()
	pch, err := pconn.Channel()
	if err != nil {
		t.Fatalf("producer channel: %v", err)
	}
	defer pch.Close()
	if err := pch.Confirm(false); err != nil {
		t.Fatalf("confirm.select: %v", err)
	}

	const n = 50000
	body := make([]byte, 1024)
	for i := 0; i < n; i++ {
		if err := pch.PublishWithContext(context.Background(), "", qName, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Body:         body,
		}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	recv := 0
	deadline := time.After(30 * time.Second)
	for recv < 1 {
		select {
		case _, ok := <-msgs:
			if !ok {
				f1AggDump(t, fmt.Sprintf("closed recv=%d", recv))
				t.Fatalf("consumer channel closed after %d messages", recv)
			}
			recv++
		case <-deadline:
			f1AggDump(t, fmt.Sprintf("starved recv=%d", recv))
			t.Fatalf("CONSUMER STARVED: received %d messages within 30s (tinyWALChan, 50K)", recv)
		}
	}
	t.Logf("PASS (tinyWALChan, 50K): consumer received %d messages within 30s", recv)
}

// TestF1_Agg_MultiProducer tests 4 producers blasting 25K messages each at one
// queue with one auto-ack consumer. This stresses the shared queue's frontier
// and depth backpressure under concurrent flood.
func TestF1_Agg_MultiProducer(t *testing.T) {
	uri, cleanup := f1AggServer(t, false, 0, 0)
	defer cleanup()

	qName := fmt.Sprintf("f1-agg-multi-%d", f1AggPortCounter.Load())

	cconn, err := amqp.Dial(uri)
	if err != nil {
		t.Fatalf("consumer dial: %v", err)
	}
	cch, err := cconn.Channel()
	if err != nil {
		t.Fatalf("consumer channel: %v", err)
	}
	// No defer close: under heavy flood the server's UnregisterConsumer
	// teardown can hang (a separate bug). srv.Stop() in cleanup force-closes
	// the socket, which unblocks everything.
	if _, err := cch.QueueDeclare(qName, true, false, false, false, nil); err != nil {
		t.Fatalf("queue declare: %v", err)
	}
	msgs, err := cch.Consume(qName, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	const producers = 4
	const perProducer = 25000
	body := make([]byte, 1024)

	// The producers deliberately outlive the assertion below: this is a
	// liveness test (the consumer must receive a message while 4x25K publishes
	// are in flight), and blocking on 100K publishes would turn it into a
	// throughput test. Because they outlive it they MUST NOT touch t — a t.*
	// call from a goroutine after its test has returned panics the whole test
	// binary and kills every remaining test in the package. So producer errors
	// are collected here and reported by the test itself, in the deferred
	// reporter below, while the test is still live.
	//
	// Errors seen after pubCancel are dropped: cancellation is the test's own
	// teardown signal, and a publish that fails because we are shutting the
	// producer down is not a finding.
	pubCtx, pubCancel := context.WithCancel(context.Background())
	var prodErrMu sync.Mutex
	var prodErrs []string
	prodErrf := func(format string, args ...interface{}) {
		if pubCtx.Err() != nil {
			return
		}
		prodErrMu.Lock()
		prodErrs = append(prodErrs, fmt.Sprintf(format, args...))
		prodErrMu.Unlock()
	}

	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(pid int) {
			defer wg.Done()
			pconn, err := amqp.Dial(uri)
			if err != nil {
				prodErrf("producer %d dial: %v", pid, err)
				return
			}
			defer pconn.Close()
			pch, err := pconn.Channel()
			if err != nil {
				prodErrf("producer %d channel: %v", pid, err)
				return
			}
			defer pch.Close()
			if err := pch.Confirm(false); err != nil {
				prodErrf("producer %d confirm: %v", pid, err)
				return
			}
			for i := 0; i < perProducer; i++ {
				if err := pch.PublishWithContext(pubCtx, "", qName, false, false, amqp.Publishing{
					DeliveryMode: amqp.Persistent,
					Body:         body,
				}); err != nil {
					prodErrf("producer %d publish %d: %v", pid, i, err)
					return
				}
			}
		}(p)
	}

	// Runs before the deferred cleanup (registered earlier, so it runs later):
	// cancel the producers, give them a bounded window to exit, then report
	// whatever they found.
	//
	// What makes this safe is NOT that stragglers are guaranteed to have
	// stopped — prodErrf tests pubCtx and only then takes the lock, so a
	// producer can pass that test, be descheduled, and append after this
	// reporter has already read the slice. It is safe because producers never
	// call t.* on any path, the mutex makes the slice itself race-free, and a
	// late append is simply dropped, which is acceptable precisely because
	// post-cancellation errors are teardown artifacts rather than findings.
	defer func() {
		pubCancel()
		producersDone := make(chan struct{})
		go func() { wg.Wait(); close(producersDone) }()
		select {
		case <-producersDone:
		case <-time.After(10 * time.Second):
			t.Logf("warning: producers did not exit within 10s of cancellation")
		}
		prodErrMu.Lock()
		defer prodErrMu.Unlock()
		for _, e := range prodErrs {
			t.Errorf("%s", e)
		}
	}()

	recv := 0
	deadline := time.After(60 * time.Second)
	for recv < 1 {
		select {
		case _, ok := <-msgs:
			if !ok {
				f1AggDump(t, fmt.Sprintf("closed recv=%d", recv))
				t.Fatalf("consumer channel closed after %d messages", recv)
			}
			recv++
		case <-deadline:
			f1AggDump(t, fmt.Sprintf("starved recv=%d", recv))
			t.Fatalf("CONSUMER STARVED: received %d messages within 60s (multiProducer, 4x25K)", recv)
		}
	}
	t.Logf("PASS (multiProducer, 4x25K): consumer received %d messages within 60s", recv)

	// Producers are cancelled and joined by the deferred reporter above.
	// Close the consumer connection in a background goroutine. Under heavy
	// flood the server's UnregisterConsumer teardown can hang (a known
	// teardown path where the poll loop's stopCh requeue is slow), which is
	// a separate bug from consumer starvation. The server Stop() in cleanup
	// will force-close the socket.
	go func() {
		_ = cch.Close()
		_ = cconn.Close()
	}()
}

// TestF1_Agg_Prefetch1_ManualAck tests with prefetch=1 (tight gate) and
// manual-ack. The consumer can only have 1 unacked message at a time, so the
// poll loop blocks on gate.acquire after each delivery until the ack arrives.
// A fast producer could flood the queue while the consumer is gated.
func TestF1_Agg_Prefetch1_ManualAck(t *testing.T) {
	uri, cleanup := f1AggServer(t, false, 0, 0)
	defer cleanup()

	qName := fmt.Sprintf("f1-agg-pf1-%d", f1AggPortCounter.Load())

	cconn, err := amqp.Dial(uri)
	if err != nil {
		t.Fatalf("consumer dial: %v", err)
	}
	defer cconn.Close()
	cch, err := cconn.Channel()
	if err != nil {
		t.Fatalf("consumer channel: %v", err)
	}
	defer cch.Close()
	if _, err := cch.QueueDeclare(qName, true, false, false, false, nil); err != nil {
		t.Fatalf("queue declare: %v", err)
	}
	if err := cch.Qos(1, 0, false); err != nil {
		t.Fatalf("qos: %v", err)
	}
	msgs, err := cch.Consume(qName, "", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	pconn, err := amqp.Dial(uri)
	if err != nil {
		t.Fatalf("producer dial: %v", err)
	}
	defer pconn.Close()
	pch, err := pconn.Channel()
	if err != nil {
		t.Fatalf("producer channel: %v", err)
	}
	defer pch.Close()
	if err := pch.Confirm(false); err != nil {
		t.Fatalf("confirm.select: %v", err)
	}

	const n = 10000
	body := make([]byte, 1024)
	for i := 0; i < n; i++ {
		if err := pch.PublishWithContext(context.Background(), "", qName, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Body:         body,
		}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	recv := 0
	deadline := time.After(15 * time.Second)
	for recv < 1 {
		select {
		case d, ok := <-msgs:
			if !ok {
				f1AggDump(t, fmt.Sprintf("closed recv=%d", recv))
				t.Fatalf("consumer channel closed after %d messages", recv)
			}
			recv++
			d.Ack(false)
		case <-deadline:
			f1AggDump(t, fmt.Sprintf("starved recv=%d", recv))
			t.Fatalf("CONSUMER STARVED: received %d messages within 15s (prefetch1, manualAck)", recv)
		}
	}
	t.Logf("PASS (prefetch1, manualAck): consumer received %d messages within 15s", recv)
}
