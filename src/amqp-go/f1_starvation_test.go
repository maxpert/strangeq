package main

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/server"
)

// ============================================================================
// F1 Investigation: consumer starvation under fsync:false with a fast
// unpaced durable+confirm producer.
//
// A diagnostic agent reported that on a fsync:false build, 1KB durable+confirm
// 1P/1C had the consumer STARVED (recv=0) under a fast unpaced producer
// (reproduced 2x). fsync-ON was clean. The hypothesis: without fsync pacing the
// producer floods, and a delivery/consumer-registration race or
// ring-fill/backpressure path starves the consumer.
//
// These tests attempt to reproduce that starvation. If they PASS (consumer
// receives at least 1 message), the bug may have been fixed by recent changes.
// If they FAIL, a goroutine dump is captured to diagnose where the consumer is
// parked.
// ============================================================================

var f1PortCounter atomic.Int64

// f1Server builds and starts an embedded server with the given fsync setting.
func f1Server(t *testing.T, fsync bool) (string, func()) {
	t.Helper()
	port := 19100 + int(f1PortCounter.Add(1))
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	cfg := config.DefaultConfig()
	cfg.Network.Address = addr
	cfg.Storage.Path = t.TempDir()
	cfg.Server.LogLevel = "silent"
	// Tri-state fsync: false disables the WAL fdatasync barrier.
	fsyncVal := fsync
	cfg.Storage.Fsync = &fsyncVal

	srv, err := server.NewServerBuilder().WithConfig(cfg).Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	go func() { _ = srv.Start() }()
	for i := 0; i < 200; i++ {
		if srv.IsListening() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !srv.IsListening() {
		t.Fatal("server listener not ready")
	}
	cleanup := func() { _ = srv.Stop() }
	return fmt.Sprintf("amqp://guest:guest@%s/", addr), cleanup
}

// f1GoroutineDump captures all goroutine stacks for starvation diagnosis.
func f1GoroutineDump(t *testing.T, label string) {
	t.Helper()
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	t.Logf("=== GOROUTINE DUMP (%s) ===\n%s", label, string(buf[:n]))
}

// f1StarvationTest is the core scenario: 1P/1C durable+confirm, fast unpaced
// producer, 1KB messages, with the given fsync setting. The consumer is on a
// SEPARATE connection (the normal benchmark setup). It asserts the consumer
// receives at least minRecv messages within the timeout.
func f1StarvationTest(t *testing.T, fsync bool, msgCount, msgSize int, autoAck bool, minRecv int, timeout time.Duration) {
	t.Helper()
	uri, cleanup := f1Server(t, fsync)
	defer cleanup()

	queueName := fmt.Sprintf("f1-starve-%d", f1PortCounter.Load())

	// --- Consumer connection (separate from producer) ---
	cconn, err := amqp.Dial(uri)
	if err != nil {
		t.Fatalf("consumer dial: %v", err)
	}
	// No defer close: under -race or heavy flood the server's UnregisterConsumer
	// teardown can be slow. srv.Stop() in cleanup force-closes the socket.
	cch, err := cconn.Channel()
	if err != nil {
		t.Fatalf("consumer channel: %v", err)
	}

	if _, err := cch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		t.Fatalf("queue declare: %v", err)
	}

	msgs, err := cch.Consume(queueName, "", autoAck, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	// Give the consumer time to register with the broker (consumerPollLoop
	// starts in RegisterConsumer; consumerDeliveryLoop discovers it via
	// ConsumersDirty on its next iteration).
	time.Sleep(200 * time.Millisecond)

	// --- Producer connection (separate, confirm mode) ---
	pconn, err := amqp.Dial(uri)
	if err != nil {
		t.Fatalf("producer dial: %v", err)
	}
	// No defer close: srv.Stop() force-closes all sockets.
	pch, err := pconn.Channel()
	if err != nil {
		t.Fatalf("producer channel: %v", err)
	}

	if err := pch.Confirm(false); err != nil {
		t.Fatalf("confirm.select: %v", err)
	}
	confirms := pch.NotifyPublish(make(chan amqp.Confirmation, msgCount+100))

	body := make([]byte, msgSize)
	for i := range body {
		body[i] = byte(i & 0xff)
	}

	// --- Fast unpaced producer: blast all messages as fast as possible ---
	for i := 0; i < msgCount; i++ {
		if err := pch.PublishWithContext(context.Background(), "", queueName, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Body:         body,
		}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// --- Assert consumer receives at least minRecv messages within timeout ---
	recv := 0
	deadline := time.After(timeout)
	for recv < minRecv {
		select {
		case _, ok := <-msgs:
			if !ok {
				t.Fatalf("consumer channel closed after receiving %d/%d messages", recv, minRecv)
			}
			recv++
		case <-deadline:
			f1GoroutineDump(t, fmt.Sprintf("fsync=%v recv=%d", fsync, recv))
			t.Fatalf("CONSUMER STARVED: received %d messages (want >= %d) within %v (fsync=%v, msgCount=%d, msgSize=%d, autoAck=%v)",
				recv, minRecv, timeout, fsync, msgCount, msgSize, autoAck)
		}
	}

	t.Logf("PASS: consumer received %d messages within %v (fsync=%v)", recv, timeout, fsync)

	// Drain any remaining confirms so the producer connection closes cleanly.
	go func() {
		for i := 0; i < msgCount; i++ {
			select {
			case <-confirms:
			case <-time.After(10 * time.Second):
				return
			}
		}
	}()
}

// TestF1_Starvation_fsyncOff_AutoAck is the primary reproduction: fsync:false,
// 1KB durable+confirm, auto-ack consumer, fast unpaced producer, 10K messages.
// Asserts the consumer receives at least 1 message within 5 seconds.
func TestF1_Starvation_fsyncOff_AutoAck(t *testing.T) {
	f1StarvationTest(t, false, 10000, 1024, true, 1, 5*time.Second)
}

// TestF1_Starvation_fsyncOff_ManualAck is the manual-ack variant: the prefetch
// gate is in play (prefetch-0 → cap 2000), which could interact with the
// delivery pipeline under flood.
func TestF1_Starvation_fsyncOff_ManualAck(t *testing.T) {
	f1StarvationTest(t, false, 10000, 1024, false, 1, 5*time.Second)
}

// TestF1_Starvation_fsyncOn_AutoAck is the control: fsync:true should NOT
// starve (the fsync barrier paces the producer naturally).
func TestF1_Starvation_fsyncOn_AutoAck(t *testing.T) {
	f1StarvationTest(t, true, 1000, 1024, true, 1, 10*time.Second)
}

// TestF1_Starvation_fsyncOff_AutoAck_AllReceived asserts the consumer receives
// ALL published messages (not just 1) within a generous timeout. This catches
// partial starvation or stuck consumers.
func TestF1_Starvation_fsyncOff_AutoAck_AllReceived(t *testing.T) {
	const n = 5000
	uri, cleanup := f1Server(t, false)
	defer cleanup()

	queueName := fmt.Sprintf("f1-all-%d", f1PortCounter.Load())

	cconn, err := amqp.Dial(uri)
	if err != nil {
		t.Fatalf("consumer dial: %v", err)
	}
	cch, err := cconn.Channel()
	if err != nil {
		t.Fatalf("consumer channel: %v", err)
	}
	if _, err := cch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		t.Fatalf("queue declare: %v", err)
	}
	msgs, err := cch.Consume(queueName, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	pconn, err := amqp.Dial(uri)
	if err != nil {
		t.Fatalf("producer dial: %v", err)
	}
	pch, err := pconn.Channel()
	if err != nil {
		t.Fatalf("producer channel: %v", err)
	}
	if err := pch.Confirm(false); err != nil {
		t.Fatalf("confirm.select: %v", err)
	}
	confirms := pch.NotifyPublish(make(chan amqp.Confirmation, n+100))

	body := make([]byte, 1024)
	for i := 0; i < n; i++ {
		if err := pch.PublishWithContext(context.Background(), "", queueName, false, false, amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Body:         body,
		}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	recv := 0
	deadline := time.After(30 * time.Second)
	for recv < n {
		select {
		case _, ok := <-msgs:
			if !ok {
				t.Fatalf("consumer channel closed after %d/%d", recv, n)
			}
			recv++
		case <-deadline:
			f1GoroutineDump(t, fmt.Sprintf("recv=%d/%d", recv, n))
			t.Fatalf("CONSUMER STARVED: received %d/%d messages within 30s", recv, n)
		}
	}
	t.Logf("PASS: consumer received all %d messages within 30s", n)

	// Drain confirms.
	go func() {
		for i := 0; i < n; i++ {
			select {
			case <-confirms:
			case <-time.After(10 * time.Second):
				return
			}
		}
	}()
}

// TestF1_Starvation_fsyncOff_AutoAck_SameConnection tests the same-connection
// P+C scenario: depth backpressure is NOT armed (HasActiveConsumer is true), so
// the reader keeps reading both publishes and acks on one connection.
func TestF1_Starvation_fsyncOff_AutoAck_SameConnection(t *testing.T) {
	uri, cleanup := f1Server(t, false)
	defer cleanup()

	queueName := fmt.Sprintf("f1-same-%d", f1PortCounter.Load())

	conn, err := amqp.Dial(uri)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	if _, err := ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		t.Fatalf("queue declare: %v", err)
	}
	if err := ch.Confirm(false); err != nil {
		t.Fatalf("confirm.select: %v", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 10100))
	msgs, err := ch.Consume(queueName, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	const n = 10000
	body := make([]byte, 1024)
	for i := 0; i < n; i++ {
		if err := ch.PublishWithContext(context.Background(), "", queueName, false, false, amqp.Publishing{
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
		case _, ok := <-msgs:
			if !ok {
				t.Fatalf("consumer channel closed after %d messages", recv)
			}
			recv++
		case <-deadline:
			f1GoroutineDump(t, fmt.Sprintf("same-conn recv=%d", recv))
			t.Fatalf("CONSUMER STARVED (same-conn): received %d messages within 15s", recv)
		}
	}
	t.Logf("PASS (same-conn): consumer received %d messages within 15s", recv)

	go func() {
		for i := 0; i < n; i++ {
			select {
			case <-confirms:
			case <-time.After(10 * time.Second):
				return
			}
		}
	}()
}
