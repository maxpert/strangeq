package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/server"
)

// ============================================================================
// SQ-5 integration: batched publisher confirms observed through a real
// amqp091-go client over TCP.
//
// The server now coalesces confirms into basic.ack multiple=true batches; the
// client library expands a multiple ack into per-tag confirmations, so the
// client-visible contract stays: every published tag is confirmed exactly
// once, in tag order. These tests pin that contract plus the SQ-5 edges:
// mandatory-unroutable messages (basic.return before the covering ack), prompt
// confirmation of a lone publish (no batch/timer stall), and channel close
// with confirms in flight.
// ============================================================================

var sq5PortCounter atomic.Int64

func sq5Server(t *testing.T) string {
	t.Helper()
	port := 18700 + int(sq5PortCounter.Add(1))
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	cfg := config.DefaultConfig()
	cfg.Network.Address = addr
	cfg.Storage.Path = t.TempDir()

	srv, err := server.NewServerBuilder().WithConfig(cfg).Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	go func() { _ = srv.Start() }()
	for i := 0; i < 100; i++ {
		if srv.IsListening() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !srv.IsListening() {
		t.Fatal("server listener not ready")
	}
	t.Cleanup(func() { _ = srv.Stop() })
	return fmt.Sprintf("amqp://guest:guest@%s/", addr)
}

func sq5ConfirmChannel(t *testing.T, uri, queue string) (*amqp.Connection, *amqp.Channel, chan amqp.Confirmation) {
	t.Helper()
	conn, err := amqp.Dial(uri)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	if queue != "" {
		if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
			t.Fatalf("queue declare: %v", err)
		}
	}
	if err := ch.Confirm(false); err != nil {
		t.Fatalf("confirm.select: %v", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 4096))
	return conn, ch, confirms
}

func sq5PublishPersistent(t *testing.T, ch *amqp.Channel, key string, mandatory bool, body string) {
	t.Helper()
	err := ch.PublishWithContext(context.Background(), "", key, mandatory, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Body:         []byte(body),
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
}

// collectConfirms drains exactly n confirmations, failing on timeout.
func collectConfirms(t *testing.T, confirms <-chan amqp.Confirmation, n int, timeout time.Duration) []amqp.Confirmation {
	t.Helper()
	got := make([]amqp.Confirmation, 0, n)
	deadline := time.After(timeout)
	for len(got) < n {
		select {
		case c, ok := <-confirms:
			if !ok {
				t.Fatalf("confirm channel closed after %d/%d confirmations", len(got), n)
			}
			got = append(got, c)
		case <-deadline:
			t.Fatalf("timed out waiting for confirmations: got %d/%d", len(got), n)
		}
	}
	return got
}

// TestSQ5_AllConfirmsExactlyOnceInOrder pipelines N durable publishes and
// asserts every tag 1..N is positively confirmed exactly once, in order.
func TestSQ5_AllConfirmsExactlyOnceInOrder(t *testing.T) {
	uri := sq5Server(t)
	_, ch, confirms := sq5ConfirmChannel(t, uri, "sq5-order-q")

	const n = 500
	for i := 1; i <= n; i++ {
		sq5PublishPersistent(t, ch, "sq5-order-q", false, fmt.Sprintf("msg-%d", i))
	}

	got := collectConfirms(t, confirms, n, 30*time.Second)
	for i, c := range got {
		want := uint64(i + 1)
		if c.DeliveryTag != want {
			t.Fatalf("confirmation %d: got tag %d, want %d (out of order or duplicated)", i, c.DeliveryTag, want)
		}
		if !c.Ack {
			t.Fatalf("tag %d was nacked", c.DeliveryTag)
		}
	}

	// No extra confirmations may trail in.
	select {
	case c := <-confirms:
		t.Fatalf("unexpected extra confirmation: %+v", c)
	case <-time.After(300 * time.Millisecond):
	}
}

// TestSQ5_MandatoryUnroutableMixedWithRoutable interleaves routable publishes
// with mandatory-unroutable ones. Every publish (both kinds) must be confirmed
// exactly once in tag order, and every unroutable one must produce a
// basic.return.
func TestSQ5_MandatoryUnroutableMixedWithRoutable(t *testing.T) {
	uri := sq5Server(t)
	_, ch, confirms := sq5ConfirmChannel(t, uri, "sq5-mixed-q")
	returns := ch.NotifyReturn(make(chan amqp.Return, 256))

	const n = 100 // every 3rd publish is mandatory-unroutable
	unroutable := 0
	for i := 1; i <= n; i++ {
		if i%3 == 0 {
			sq5PublishPersistent(t, ch, "sq5-no-such-queue", true, fmt.Sprintf("lost-%d", i))
			unroutable++
		} else {
			sq5PublishPersistent(t, ch, "sq5-mixed-q", false, fmt.Sprintf("msg-%d", i))
		}
	}

	got := collectConfirms(t, confirms, n, 30*time.Second)
	for i, c := range got {
		want := uint64(i + 1)
		if c.DeliveryTag != want {
			t.Fatalf("confirmation %d: got tag %d, want %d", i, c.DeliveryTag, want)
		}
		if !c.Ack {
			t.Fatalf("tag %d was nacked", c.DeliveryTag)
		}
	}

	// Every mandatory-unroutable publish must have produced a basic.return.
	gotReturns := 0
	deadline := time.After(10 * time.Second)
	for gotReturns < unroutable {
		select {
		case _, ok := <-returns:
			if !ok {
				t.Fatalf("return channel closed after %d/%d returns", gotReturns, unroutable)
			}
			gotReturns++
		case <-deadline:
			t.Fatalf("timed out waiting for basic.return: got %d/%d", gotReturns, unroutable)
		}
	}
}

// TestSQ5_LonePublishConfirmedPromptly proves a single confirmed publish with
// no follow-up traffic is confirmed without waiting on any batch size or long
// timer — a publisher doing publish-then-Wait must not stall.
func TestSQ5_LonePublishConfirmedPromptly(t *testing.T) {
	uri := sq5Server(t)
	_, ch, confirms := sq5ConfirmChannel(t, uri, "sq5-lone-q")

	start := time.Now()
	sq5PublishPersistent(t, ch, "sq5-lone-q", false, "lone")

	select {
	case c := <-confirms:
		elapsed := time.Since(start)
		if c.DeliveryTag != 1 || !c.Ack {
			t.Fatalf("unexpected confirmation: %+v", c)
		}
		t.Logf("lone publish confirmed in %v", elapsed)
		if elapsed > 500*time.Millisecond {
			t.Fatalf("lone confirm took %v; want prompt flush (no batch/timer stall)", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("lone publish never confirmed")
	}
}

// TestSQ5_ChannelCloseWithConfirmsInFlight publishes a burst and immediately
// closes the channel. The close must not panic, hang, or corrupt the
// connection; a fresh channel on the same connection must still work in
// confirm mode with its own tag sequence.
func TestSQ5_ChannelCloseWithConfirmsInFlight(t *testing.T) {
	uri := sq5Server(t)
	conn, ch, _ := sq5ConfirmChannel(t, uri, "sq5-close-q")

	for i := 1; i <= 50; i++ {
		sq5PublishPersistent(t, ch, "sq5-close-q", false, fmt.Sprintf("msg-%d", i))
	}
	if err := ch.Close(); err != nil {
		t.Fatalf("channel close with confirms in flight: %v", err)
	}

	// Connection must remain healthy: open a new confirm channel and use it.
	ch2, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel after close: %v", err)
	}
	if err := ch2.Confirm(false); err != nil {
		t.Fatalf("confirm.select on fresh channel: %v", err)
	}
	confirms2 := ch2.NotifyPublish(make(chan amqp.Confirmation, 16))
	if err := ch2.PublishWithContext(context.Background(), "", "sq5-close-q", false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Body:         []byte("after-close"),
	}); err != nil {
		t.Fatalf("publish on fresh channel: %v", err)
	}
	select {
	case c := <-confirms2:
		if c.DeliveryTag != 1 || !c.Ack {
			t.Fatalf("fresh channel confirmation: %+v (tag sequence must restart per channel)", c)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("publish on fresh channel never confirmed")
	}
}

// TestSQ5_ConfirmSelectMidStream turns confirm mode on only after some
// unconfirmed publishes; the confirm tag sequence must start at 1 from the
// first post-confirm.select publish and stay uncorrupted.
func TestSQ5_ConfirmSelectMidStream(t *testing.T) {
	uri := sq5Server(t)
	conn, err := amqp.Dial(uri)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	if _, err := ch.QueueDeclare("sq5-mid-q", true, false, false, false, nil); err != nil {
		t.Fatalf("queue declare: %v", err)
	}

	// Publishes before confirm.select are not part of the confirm tag space.
	for i := 0; i < 3; i++ {
		sq5PublishPersistent(t, ch, "sq5-mid-q", false, "pre-confirm")
	}

	if err := ch.Confirm(false); err != nil {
		t.Fatalf("confirm.select: %v", err)
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 16))

	const n = 4
	for i := 1; i <= n; i++ {
		sq5PublishPersistent(t, ch, "sq5-mid-q", false, fmt.Sprintf("post-%d", i))
	}
	got := collectConfirms(t, confirms, n, 10*time.Second)
	for i, c := range got {
		if c.DeliveryTag != uint64(i+1) || !c.Ack {
			t.Fatalf("confirmation %d: %+v, want tag %d acked", i, c, i+1)
		}
	}
}
