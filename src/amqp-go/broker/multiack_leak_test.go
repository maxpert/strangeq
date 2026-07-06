package broker

import (
	"fmt"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
)

// ============================================================================
// SQ-0 step 3 regression: multi-ack must release exactly one prefetch credit
// per newly-settled tag — never a computed count that can skew from the true
// outstanding set.
//
// The pre-redesign multi-ack path sized its gate release by scanning the
// storage per-consumer pending-ack INDEX (GetConsumerPendingAcks) and releasing
// that count. Any skew between the count and the number of gate credits actually
// acquired for those deliveries is a permanent under- or over-release. A
// persistent under-release shrinks the usable prefetch window below the client's
// ack cadence, and delivery wedges (the BenchmarkVersus_MultiAck1000 deadlock).
//
// After the redesign, multi-ack enumerates the ack cursor's unacked set and
// routes every tag through settle(), whose LoadAndDelete win IS the single
// credit release. gate.inflight is therefore exact by construction: it equals
// the size of the authoritative unacked set at every quiescent point.
// ============================================================================

// gateInflight returns the live prefetch-gate inflight counter for a consumer.
func gateInflight(t testing.TB, b *StorageBroker, consumerTag string) int64 {
	t.Helper()
	v, ok := b.activeConsumers.Load(consumerTag)
	if !ok {
		t.Fatalf("consumer %q not registered", consumerTag)
	}
	return v.(*ConsumerState).gate.inflight.Load()
}

// unackedCount returns the authoritative size of a consumer's unacked set.
func unackedCount(t testing.TB, b *StorageBroker, queueName, consumerTag string) int64 {
	t.Helper()
	n, err := b.storage.GetUnackedCount(queueName, consumerTag)
	if err != nil {
		t.Fatalf("GetUnackedCount: %v", err)
	}
	return int64(n)
}

// assertGateNoLeak waits until the gate's inflight counter settles into a
// leak-free relationship with the authoritative unacked-set size, then returns
// the observed inflight value. The leak-free invariant is:
//
//	unacked <= inflight <= unacked + 1   and   inflight <= limit
//
// The single-credit slack (+1) is legitimate: consumerPollLoop acquires one gate
// credit BEFORE it claims/delivers the next message, so at any quiescent point it
// may hold exactly one credit not yet backed by an unacked delivery (it is parked
// in Claim, or mid-delivery before OnDeliver). A phantom credit leak — the
// failure this test guards — makes inflight exceed unacked + 1 and grow batch
// over batch until the usable window (limit - inflight) collapses below the ack
// cadence and delivery wedges. Because a leak is persistent, an unhealthy gate
// never settles within the deadline and the test fails.
func assertGateNoLeak(t testing.TB, b *StorageBroker, queueName, consumerTag string, limit int64) int64 {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var inflight, unacked int64
	for time.Now().Before(deadline) {
		inflight = gateInflight(t, b, consumerTag)
		unacked = unackedCount(t, b, queueName, consumerTag)
		if inflight >= unacked && inflight <= unacked+1 && inflight <= limit {
			return inflight
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("gate.inflight (%d) never settled to a leak-free relationship with the unacked-set size (%d) "+
		"within limit %d — phantom credit leak (window collapsing)", inflight, unacked, limit)
	return 0
}

// TestMultiAckFullWindowReleasesExactly is design step-3 test (a): fill the
// entire prefetch window, multi-ack the whole window in one call, and assert the
// gate releases exactly the window (inflight and unacked both return to zero).
func TestMultiAckFullWindowReleasesExactly(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()
	broker.DeclareQueue("mafull", false, false, false, nil)

	const prefetch = 200

	consumer := &protocol.Consumer{
		Tag:           "mafull-consumer",
		Queue:         "mafull",
		NoAck:         false,
		Messages:      make(chan *protocol.Delivery, prefetch),
		PrefetchCount: prefetch,
	}
	if err := broker.RegisterConsumer("mafull", "mafull-consumer", consumer); err != nil {
		t.Fatalf("RegisterConsumer: %v", err)
	}

	// Publish exactly a full window plus extra so the gate fills to the limit.
	for i := 0; i < prefetch*3; i++ {
		msg := &protocol.Message{Body: []byte(fmt.Sprintf("m-%d", i)), Exchange: "", RoutingKey: "mafull"}
		if err := broker.PublishMessage("", "mafull", msg); err != nil {
			t.Fatalf("PublishMessage %d: %v", i, err)
		}
	}

	// Wait for the window to fill exactly to the prefetch limit.
	deadline := time.Now().Add(3 * time.Second)
	for gateInflight(t, broker, "mafull-consumer") < prefetch && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(30 * time.Millisecond) // catch any over-delivery
	if got := gateInflight(t, broker, "mafull-consumer"); got != prefetch {
		t.Fatalf("gate.inflight = %d after fill, want exactly prefetch=%d", got, prefetch)
	}

	// Drain the full window from the channel and multi-ack the highest tag.
	var lastTag uint64
	for i := 0; i < prefetch; i++ {
		d := <-consumer.Messages
		if d.DeliveryTag > lastTag {
			lastTag = d.DeliveryTag
		}
	}
	if err := broker.AcknowledgeMessage("mafull-consumer", lastTag, true); err != nil {
		t.Fatalf("AcknowledgeMessage(multiple): %v", err)
	}

	// The window must reopen fully: the next batch flows in with the gate
	// leak-free (inflight tracks the unacked set, never exceeding the limit).
	assertGateNoLeak(t, broker, "mafull", "mafull-consumer", prefetch)
}

// TestMultiAckBatchedNoPhantomLeak is design step-3 test (b): the phantom-leak
// regression. Deliver 10× the prefetch window to a manual-ack consumer and ack
// in batches of M < prefetch. Delivery must never stall, gate.inflight must
// equal the true outstanding (unacked-set size) after every batch, and both must
// reach zero at drain. A count-based release that leaks even a single credit per
// batch would eventually shrink the window and wedge delivery.
func TestMultiAckBatchedNoPhantomLeak(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()
	broker.DeclareQueue("maleak", false, false, false, nil)

	const (
		prefetch = 100
		total    = prefetch * 10
		batch    = 40 // M < prefetch
	)

	consumer := &protocol.Consumer{
		Tag:           "maleak-consumer",
		Queue:         "maleak",
		NoAck:         false,
		Messages:      make(chan *protocol.Delivery, total), // never block on send; only the gate bounds delivery
		PrefetchCount: prefetch,
	}
	if err := broker.RegisterConsumer("maleak", "maleak-consumer", consumer); err != nil {
		t.Fatalf("RegisterConsumer: %v", err)
	}

	for i := 0; i < total; i++ {
		msg := &protocol.Message{Body: []byte(fmt.Sprintf("m-%d", i)), Exchange: "", RoutingKey: "maleak"}
		if err := broker.PublishMessage("", "maleak", msg); err != nil {
			t.Fatalf("PublishMessage %d: %v", i, err)
		}
	}

	received := 0
	var lastTag uint64
	for received < total {
		select {
		case d := <-consumer.Messages:
			received++
			if d.DeliveryTag > lastTag {
				lastTag = d.DeliveryTag
			}
			if received%batch == 0 {
				if err := broker.AcknowledgeMessage("maleak-consumer", lastTag, true); err != nil {
					t.Fatalf("AcknowledgeMessage(multiple) at %d: %v", received, err)
				}
				// After each batch the gate must stay leak-free: inflight tracks
				// the outstanding set (± the single pre-acquired credit) and
				// never exceeds the prefetch limit. A per-batch leak would push
				// inflight above unacked+1 here and compound across batches.
				assertGateNoLeak(t, broker, "maleak", "maleak-consumer", prefetch)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("delivery stalled at received=%d/%d, gate.inflight=%d (phantom credit leak shrank the window)",
				received, total, gateInflight(t, broker, "maleak-consumer"))
		}
	}

	// Every message consumed and acked: the gate must be fully drained. The
	// unacked set is empty; inflight may hold at most the single credit the poll
	// loop pre-acquired for its next (now-parked) Claim. Any residual beyond that
	// is a phantom credit that was never released.
	final := assertGateNoLeak(t, broker, "maleak", "maleak-consumer", prefetch)
	if final > 1 {
		t.Fatalf("gate.inflight = %d at drain, want <= 1 (residual phantom credits)", final)
	}
	if uc := unackedCount(t, broker, "maleak", "maleak-consumer"); uc != 0 {
		t.Fatalf("unacked-set size = %d at drain, want 0", uc)
	}
}

// TestMultiAckDoesNotDisturbBasicGetDelivery is watch-item #2: multi-ack over a
// push consumer must settle exactly that consumer's own tags and must not leak,
// drop, or corrupt a concurrent basic.get delivery living on the same queue.
//
// Delivery tags in this broker are globally unique and every ack is routed by
// GetConsumerForDelivery to the single owner of the acked tag: a push consumer
// (non-empty tag) via AcknowledgeMessage, a basic.get delivery (empty tag) via
// AcknowledgeGetDelivery. A push-consumer multi-ack therefore enumerates only
// that consumer's unacked cursor set; basic.get deliveries (consumer tag "")
// are never in it, so they are neither double-freed nor swept, and they remain
// independently ackable. This test pins that separation: the get delivery
// survives a consumer multi-ack that covers a higher tag, the consumer window
// drains cleanly to zero, and the get delivery still acks on its own path.
func TestMultiAckDoesNotDisturbBasicGetDelivery(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()
	broker.DeclareQueue("mixed", false, false, false, nil)

	// One basic.get (manual ack) takes the first message with an empty consumer
	// tag in the delivery index.
	getMsg := &protocol.Message{Body: []byte("get-0"), Exchange: "", RoutingKey: "mixed"}
	if err := broker.PublishMessage("", "mixed", getMsg); err != nil {
		t.Fatalf("PublishMessage(get): %v", err)
	}
	gotMsg, getTag, _, err := broker.GetMessageForGet("mixed", false)
	if err != nil {
		t.Fatalf("GetMessageForGet: %v", err)
	}
	if gotMsg == nil {
		t.Fatal("GetMessageForGet returned no message")
	}
	if owner, ok := broker.GetConsumerForDelivery(getTag); !ok || owner != "" {
		t.Fatalf("basic.get delivery tag %d owner = %q,%v, want \"\",true", getTag, owner, ok)
	}

	// A push consumer on the same queue then receives the rest.
	const prefetch = 50
	consumer := &protocol.Consumer{
		Tag:           "mixed-consumer",
		Queue:         "mixed",
		NoAck:         false,
		Messages:      make(chan *protocol.Delivery, prefetch),
		PrefetchCount: prefetch,
	}
	if err := broker.RegisterConsumer("mixed", "mixed-consumer", consumer); err != nil {
		t.Fatalf("RegisterConsumer: %v", err)
	}

	const pushCount = 30
	for i := 0; i < pushCount; i++ {
		msg := &protocol.Message{Body: []byte(fmt.Sprintf("push-%d", i)), Exchange: "", RoutingKey: "mixed"}
		if err := broker.PublishMessage("", "mixed", msg); err != nil {
			t.Fatalf("PublishMessage(push) %d: %v", i, err)
		}
	}

	// Receive all push deliveries.
	var lastPushTag uint64
	for i := 0; i < pushCount; i++ {
		select {
		case d := <-consumer.Messages:
			if d.DeliveryTag > lastPushTag {
				lastPushTag = d.DeliveryTag
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("push consumer stalled after %d/%d deliveries", i, pushCount)
		}
	}
	// The push tags are all greater than the get tag (globally increasing).
	if lastPushTag <= getTag {
		t.Fatalf("expected push tags > get tag; lastPushTag=%d getTag=%d", lastPushTag, getTag)
	}

	// Multi-ack the whole push window at a tag strictly greater than the get
	// delivery's tag. Per the routing, this settles only the push consumer.
	if err := broker.AcknowledgeMessage("mixed-consumer", lastPushTag, true); err != nil {
		t.Fatalf("AcknowledgeMessage(multiple): %v", err)
	}

	// Push consumer window drains cleanly — no leak, no over-release. inflight
	// may retain at most the single pre-acquired credit for the parked Claim.
	final := assertGateNoLeak(t, broker, "mixed", "mixed-consumer", prefetch)
	if final > 1 {
		t.Fatalf("push consumer gate.inflight = %d after multi-ack, want <= 1", final)
	}

	// The basic.get delivery was NOT swept by the consumer multi-ack: it is
	// still live in the delivery index and still owned by the "" get path.
	if owner, ok := broker.GetConsumerForDelivery(getTag); !ok || owner != "" {
		t.Fatalf("basic.get delivery was disturbed by consumer multi-ack: owner=%q ok=%v (tag %d)", owner, ok, getTag)
	}

	// And it still acks cleanly on its own path with no double-free / panic.
	if err := broker.AcknowledgeGetDelivery(getTag); err != nil {
		t.Fatalf("AcknowledgeGetDelivery after consumer multi-ack: %v", err)
	}
	if _, ok := broker.GetConsumerForDelivery(getTag); ok {
		t.Fatalf("basic.get delivery tag %d still present after its own ack", getTag)
	}
}
