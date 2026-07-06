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
// SQ-18 verification: delivery-tag keyspace / per-channel sequence across queues.
//
// Scenario under test: ONE connection, ONE channel, TWO queues, TWO manual-ack
// consumers, interleaved publish, interleaved settlement.
//
// Findings this suite pins down:
//
//   - Single acks were ALWAYS exact: the wire delivery tag historically WAS the
//     broker's globally-unique msgID, so queue A's messages and queue B's
//     messages never shared a delivery-tag ledger slot. The suspected literal
//     "per-queue ring sequence collision" does NOT occur (TestSQ18_Interleaved
//     SingleAckExactSettle passes both before and after the fix).
//
//   - The real, client-visible defects were: (a) delivery tags on a channel
//     spanning 2+ queues were the interleaved GLOBAL msgID space, not a
//     per-channel 1-based sequence as AMQP 0.9.1 requires (TestSQ18_PerChannel
//     DeliveryTagSequence); and (b) a cumulative ack (multiple=true) settled
//     only the owning consumer's queue, silently leaving another queue's
//     earlier deliveries unacked -> redelivered as duplicates (TestSQ18_
//     CumulativeMultiAckAcrossQueues). Both share one root cause — no
//     per-channel wire-tag remapping — and both fail before the fix.
//
// NOTE on observability: the amqp091 client demultiplexes a channel's frames
// into one Go channel per consumer, so a test cannot observe the raw single
// TCP wire order across consumers. The per-channel-sequence test instead pins
// the tag VALUES (a 1-based dense sequence), which is observable and which the
// pre-fix global-msgID scheme violates once the global counter is advanced.
// ============================================================================

var sq18PortCounter atomic.Int64

// ackDrain is the settle time allowed for the asynchronous ack processor
// (conn.AckQueue / ackProcessor) to drain a just-sent ack BEFORE the client
// closes the channel. Without it, channel teardown (UnregisterConsumer ->
// requeue of still-unacked deliveries) races the pending ack. This isolates the
// SQ-18 settlement behaviour under test from that unrelated close-ordering race.
const ackDrain = 750 * time.Millisecond

func sq18Server(t *testing.T) (string, func()) {
	t.Helper()
	port := 18500 + int(sq18PortCounter.Add(1))
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
	return fmt.Sprintf("amqp://guest:guest@%s/", addr), func() { _ = srv.Stop() }
}

// sq18Collect pulls exactly want deliveries out of the two consumer channels.
func sq18Collect(t *testing.T, chA, chB <-chan amqp.Delivery, want int, timeout time.Duration) []amqp.Delivery {
	t.Helper()
	got := make([]amqp.Delivery, 0, want)
	deadline := time.After(timeout)
	for len(got) < want {
		select {
		case d, ok := <-chA:
			if !ok {
				t.Fatalf("consumer A channel closed after %d/%d", len(got), want)
			}
			got = append(got, d)
		case d, ok := <-chB:
			if !ok {
				t.Fatalf("consumer B channel closed after %d/%d", len(got), want)
			}
			got = append(got, d)
		case <-deadline:
			t.Fatalf("timed out collecting deliveries: got %d/%d", len(got), want)
		}
	}
	return got
}

func parseIdx(body string) int {
	for i := len(body) - 1; i >= 0; i-- {
		if body[i] == '-' {
			var idx int
			fmt.Sscanf(body[i+1:], "%d", &idx)
			return idx
		}
	}
	return -1
}

// sq18WarmGlobalTag advances the broker's global msgID counter by fully
// publishing+consuming (auto-ack) n messages on a throwaway queue. This
// deliberately decouples the broker's global msgID space from the tag values a
// fresh channel will observe, so a per-channel 1-based sequence is
// distinguishable from raw global msgIDs.
func sq18WarmGlobalTag(t *testing.T, uri string, n int) {
	t.Helper()
	conn, err := amqp.Dial(uri)
	if err != nil {
		t.Fatalf("warmup dial: %v", err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("warmup channel: %v", err)
	}
	q := "sq18-warmup"
	if _, err := ch.QueueDeclare(q, false, false, false, false, nil); err != nil {
		t.Fatalf("warmup declare: %v", err)
	}
	for i := 0; i < n; i++ {
		if err := ch.Publish("", q, false, false, amqp.Publishing{Body: []byte("w")}); err != nil {
			t.Fatalf("warmup publish: %v", err)
		}
	}
	deliv, err := ch.Consume(q, "warm", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("warmup consume: %v", err)
	}
	deadline := time.After(10 * time.Second)
	for got := 0; got < n; {
		select {
		case <-deliv:
			got++
		case <-deadline:
			t.Fatalf("warmup drained only %d/%d", got, n)
		}
	}
}

// TestSQ18_PerChannelDeliveryTagSequence asserts that the delivery tags handed
// to a single channel consuming from two queues form a per-channel 1-based
// sequence (values {1..N}) rather than the interleaved global msgID space.
//
// WHY THIS FAILS PRE-FIX: before the fix the wire delivery tag was the broker's
// global msgID. After warming the global counter to `warm`, a pre-fix channel's
// tags would be {warm+1 .. warm+N}, so min != 1 and max != N. The fix mints a
// per-channel monotonic sequence, restoring {1..N}.
func TestSQ18_PerChannelDeliveryTagSequence(t *testing.T) {
	uri, stop := sq18Server(t)
	defer stop()

	const warm = 500
	sq18WarmGlobalTag(t, uri, warm)

	conn, err := amqp.Dial(uri)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	if err := ch.Qos(10000, 0, false); err != nil {
		t.Fatalf("qos: %v", err)
	}

	qA := "sq18-seq-A"
	qB := "sq18-seq-B"
	for _, q := range []string{qA, qB} {
		if _, err := ch.QueueDeclare(q, false, false, false, false, nil); err != nil {
			t.Fatalf("declare %s: %v", q, err)
		}
	}
	const perQueue = 50
	for i := 0; i < perQueue; i++ {
		for _, q := range []string{qA, qB} {
			body := fmt.Sprintf("%s-%d", q, i)
			if err := ch.Publish("", q, false, false, amqp.Publishing{Body: []byte(body)}); err != nil {
				t.Fatalf("publish: %v", err)
			}
		}
	}

	consA, err := ch.Consume(qA, "cA", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume A: %v", err)
	}
	consB, err := ch.Consume(qB, "cB", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume B: %v", err)
	}

	const total = perQueue * 2
	got := sq18Collect(t, consA, consB, total, 10*time.Second)

	seen := map[uint64]int{}
	var maxTag uint64
	for _, d := range got {
		seen[d.DeliveryTag]++
		if d.DeliveryTag > maxTag {
			maxTag = d.DeliveryTag
		}
		_ = d.Ack(false)
	}

	// Anti-collision: every delivery carries a distinct tag.
	if len(seen) != total {
		t.Errorf("expected %d distinct delivery tags, got %d (duplicate tags => keyspace collision)", total, len(seen))
	}
	// Per-channel 1-based sequence: tags are exactly {1..total}. Pre-fix these
	// would be {warm+1 .. warm+total}.
	for tag := uint64(1); tag <= total; tag++ {
		if seen[tag] == 0 {
			t.Errorf("delivery tag %d missing: channel tags are not the 1-based per-channel sequence {1..%d} (max observed=%d). Pre-fix the tags are the global msgID space starting near %d.",
				tag, total, maxTag, warm+1)
			break
		}
	}
	if maxTag != total {
		t.Errorf("max delivery tag = %d, want %d (a per-channel 1-based sequence). A larger value means raw global msgIDs are leaking onto the wire.", maxTag, total)
	}
}

// TestSQ18_InterleavedSingleAckExactSettle verifies interleaved single acks
// across two queues each settle exactly their own message. This passes both
// before and after the fix, documenting that the literal per-queue collision
// hypothesis does not hold — delivery tags are globally unique, so a single ack
// never settles the wrong message.
func TestSQ18_InterleavedSingleAckExactSettle(t *testing.T) {
	uri, stop := sq18Server(t)
	defer stop()

	qA := "sq18-single-A"
	qB := "sq18-single-B"
	const perQueue = 40

	conn, err := amqp.Dial(uri)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	if err := ch.Qos(10000, 0, false); err != nil {
		t.Fatalf("qos: %v", err)
	}
	for _, q := range []string{qA, qB} {
		if _, err := ch.QueueDeclare(q, false, false, false, false, nil); err != nil {
			t.Fatalf("declare %s: %v", q, err)
		}
	}
	for i := 0; i < perQueue; i++ {
		for _, q := range []string{qA, qB} {
			body := fmt.Sprintf("%s-%d", q, i)
			if err := ch.Publish("", q, false, false, amqp.Publishing{Body: []byte(body)}); err != nil {
				t.Fatalf("publish: %v", err)
			}
		}
	}

	consA, err := ch.Consume(qA, "cA", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume A: %v", err)
	}
	consB, err := ch.Consume(qB, "cB", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume B: %v", err)
	}
	got := sq18Collect(t, consA, consB, perQueue*2, 10*time.Second)

	// Ack only even-index bodies within each queue; leave odd ones un-acked.
	acked := map[string]bool{}
	for _, d := range got {
		body := string(d.Body)
		if parseIdx(body)%2 == 0 {
			if err := d.Ack(false); err != nil {
				t.Fatalf("ack %q: %v", body, err)
			}
			acked[body] = true
		}
	}
	if len(acked) != perQueue { // even indices 0..38 in each of 2 queues
		t.Fatalf("expected to ack %d bodies, acked %d", perQueue, len(acked))
	}

	// Let the async ack processor drain the settled tags before teardown, so the
	// close-time requeue only sees genuinely un-acked deliveries.
	time.Sleep(ackDrain)
	_ = ch.Close()
	_ = conn.Close()
	time.Sleep(300 * time.Millisecond)

	conn2, err := amqp.Dial(uri)
	if err != nil {
		t.Fatalf("dial2: %v", err)
	}
	defer conn2.Close()
	ch2, err := conn2.Channel()
	if err != nil {
		t.Fatalf("channel2: %v", err)
	}
	if err := ch2.Qos(10000, 0, false); err != nil {
		t.Fatalf("qos2: %v", err)
	}
	c2A, err := ch2.Consume(qA, "c2A", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume2 A: %v", err)
	}
	c2B, err := ch2.Consume(qB, "c2B", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume2 B: %v", err)
	}

	seen := map[string]int{}
	deadline := time.After(3 * time.Second)
	expected := perQueue
loop:
	for len(seen) < expected {
		select {
		case d := <-c2A:
			seen[string(d.Body)]++
			_ = d.Ack(false)
		case d := <-c2B:
			seen[string(d.Body)]++
			_ = d.Ack(false)
		case <-deadline:
			break loop
		}
	}

	for body := range acked {
		if seen[body] > 0 {
			t.Errorf("body %q was acked but got redelivered %d time(s): an ack settled the WRONG message", body, seen[body])
		}
	}
	missing, dups := 0, 0
	for i := 0; i < perQueue; i++ {
		if i%2 == 0 {
			continue
		}
		for _, q := range []string{qA, qB} {
			body := fmt.Sprintf("%s-%d", q, i)
			switch seen[body] {
			case 0:
				missing++
				t.Errorf("un-acked body %q was LOST (not redelivered)", body)
			case 1:
			default:
				dups++
				t.Errorf("un-acked body %q was redelivered %d times (double-settle/duplicate)", body, seen[body])
			}
		}
	}
	if missing == 0 && dups == 0 {
		t.Logf("single-ack interleaved: all %d un-acked bodies redelivered exactly once; no acked body reappeared", expected)
	}
}

// TestSQ18_CumulativeMultiAckAcrossQueues issues ONE cumulative ack
// (multiple=true) on the highest delivery tag the channel has observed and
// asserts every earlier still-unacked delivery on the channel — from BOTH
// queues — is settled, exactly as a RabbitMQ channel behaves.
//
// WHY THIS FAILS PRE-FIX: pre-fix the server routed a cumulative ack to the
// single consumer that owned the tag and enumerated only that consumer's queue
// (GetUnackedTags is per-queue+consumer). The other queue's earlier deliveries
// stayed unacked and were redelivered on reconnect — observed here as the whole
// of queue A (or B) reappearing.
func TestSQ18_CumulativeMultiAckAcrossQueues(t *testing.T) {
	uri, stop := sq18Server(t)
	defer stop()

	qA := "sq18-multi-A"
	qB := "sq18-multi-B"
	const perQueue = 30

	conn, err := amqp.Dial(uri)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	if err := ch.Qos(10000, 0, false); err != nil {
		t.Fatalf("qos: %v", err)
	}
	for _, q := range []string{qA, qB} {
		if _, err := ch.QueueDeclare(q, false, false, false, false, nil); err != nil {
			t.Fatalf("declare %s: %v", q, err)
		}
	}
	for i := 0; i < perQueue; i++ {
		for _, q := range []string{qA, qB} {
			body := fmt.Sprintf("%s-%d", q, i)
			if err := ch.Publish("", q, false, false, amqp.Publishing{Body: []byte(body)}); err != nil {
				t.Fatalf("publish: %v", err)
			}
		}
	}

	consA, err := ch.Consume(qA, "cA", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume A: %v", err)
	}
	consB, err := ch.Consume(qB, "cB", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume B: %v", err)
	}
	got := sq18Collect(t, consA, consB, perQueue*2, 10*time.Second)

	var maxDelivery amqp.Delivery
	var maxTag uint64
	for _, d := range got {
		if d.DeliveryTag >= maxTag {
			maxTag = d.DeliveryTag
			maxDelivery = d
		}
	}

	// Cumulative ack: settle every unacked delivery on the channel up to and
	// including maxTag. On a spec-conformant channel this settles ALL
	// 2*perQueue messages regardless of source queue.
	if err := maxDelivery.Ack(true); err != nil {
		t.Fatalf("cumulative ack: %v", err)
	}

	// Let the async ack processor drain the cumulative ack before teardown, so
	// the close-time requeue only sees genuinely un-acked deliveries (see
	// ackDrain doc). This isolates the settlement fix from the ack/close race.
	time.Sleep(ackDrain)
	_ = ch.Close()
	_ = conn.Close()
	time.Sleep(300 * time.Millisecond)

	conn2, err := amqp.Dial(uri)
	if err != nil {
		t.Fatalf("dial2: %v", err)
	}
	defer conn2.Close()
	ch2, err := conn2.Channel()
	if err != nil {
		t.Fatalf("channel2: %v", err)
	}
	if err := ch2.Qos(10000, 0, false); err != nil {
		t.Fatalf("qos2: %v", err)
	}
	c2A, err := ch2.Consume(qA, "c2A", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume2 A: %v", err)
	}
	c2B, err := ch2.Consume(qB, "c2B", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume2 B: %v", err)
	}

	redelivered := map[string]int{}
	deadline := time.After(2 * time.Second)
	done := false
	for !done {
		select {
		case d := <-c2A:
			redelivered[string(d.Body)]++
			_ = d.Ack(false)
		case d := <-c2B:
			redelivered[string(d.Body)]++
			_ = d.Ack(false)
		case <-deadline:
			done = true
		}
	}

	if len(redelivered) != 0 {
		t.Errorf("cumulative multi-ack on tag %d (body %q, consumer %s) failed to settle %d message(s) that a spec-conformant channel would have settled; they were redelivered: %v",
			maxTag, string(maxDelivery.Body), maxDelivery.ConsumerTag, len(redelivered), redelivered)
	} else {
		t.Logf("cumulative multi-ack on tag %d settled all %d channel deliveries; nothing redelivered", maxTag, perQueue*2)
	}
}
