package broker

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
)

// ============================================================================
// SQ-0 regression: prefetch gate must be bypassed for no-ack consumers
//
// Per AMQP 0.9.1, prefetch (QoS) limits are IGNORED when a consumer sets
// no-ack. Previously the broker created a fixed gate limit of 2000 for a
// consumer with PrefetchCount == 0 and only ever returned gate credit on an
// explicit ack/nack/reject. A no-ack consumer never acks, so after exactly
// 2000 deliveries the gate exhausted, the poll loop blocked forever, queue
// depth climbed to the high-water mark, and publishers deadlocked behind
// backpressure. These tests lock in the fix.
// ============================================================================

// TestNoAckConsumerBypassesPrefetchGate publishes far more than the old 2000
// gate limit to a no-ack consumer with PrefetchCount == 0 and asserts every
// message is delivered. Against the pre-fix broker this stalls at ~2000 and
// the test times out. After the fix all messages flow, no pending acks are
// stored, and queue depth returns to zero.
func TestNoAckConsumerBypassesPrefetchGate(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()
	broker.DeclareQueue("noack", false, false, false, nil)

	const total = 5000

	consumer := &protocol.Consumer{
		Tag:           "noack-consumer",
		Queue:         "noack",
		NoAck:         true,
		Messages:      make(chan *protocol.Delivery, 256),
		PrefetchCount: 0, // unlimited per spec when no-ack is set
	}
	if err := broker.RegisterConsumer("noack", "noack-consumer", consumer); err != nil {
		t.Fatalf("RegisterConsumer: %v", err)
	}

	// Drain deliveries in the background. A no-ack consumer never acks.
	var received int64
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-consumer.Messages:
				if atomic.AddInt64(&received, 1) == total {
					close(done)
					return
				}
			case <-time.After(15 * time.Second):
				return
			}
		}
	}()

	for i := 0; i < total; i++ {
		msg := &protocol.Message{Body: []byte(fmt.Sprintf("m-%d", i)), Exchange: "", RoutingKey: "noack"}
		if err := broker.PublishMessage("", "noack", msg); err != nil {
			t.Fatalf("PublishMessage %d: %v", i, err)
		}
	}

	select {
	case <-done:
		// all messages received
	case <-time.After(15 * time.Second):
		t.Fatalf("no-ack consumer stalled: received %d/%d (prefetch gate not bypassed)",
			atomic.LoadInt64(&received), total)
	}

	if got := atomic.LoadInt64(&received); got != total {
		t.Fatalf("received %d, want %d", got, total)
	}

	// No-ack deliveries are implicit acks: no pending acks should linger.
	pending, err := broker.storage.GetConsumerPendingAcks("noack-consumer")
	if err != nil {
		t.Fatalf("GetConsumerPendingAcks: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected 0 pending acks for no-ack consumer, got %d", len(pending))
	}

	// Queue depth must drain back to zero once every message is delivered.
	qs := broker.getOrCreateQueueState("noack")
	deadline := time.Now().Add(2 * time.Second)
	for qs.Depth() != 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if d := qs.Depth(); d != 0 {
		t.Fatalf("expected queue depth 0 after no-ack drain, got %d", d)
	}
}

// TestManualAckConsumerStillGated verifies the fix does not weaken manual-ack
// semantics: the prefetch gate must still cap outstanding unacked deliveries,
// and pending acks must exist until the client acks. After acking, further
// messages flow.
func TestManualAckConsumerStillGated(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()
	broker.DeclareQueue("manual", false, false, false, nil)

	const prefetch = 10
	const total = 50

	consumer := &protocol.Consumer{
		Tag:           "manual-consumer",
		Queue:         "manual",
		NoAck:         false,
		Messages:      make(chan *protocol.Delivery, total),
		PrefetchCount: prefetch,
	}
	if err := broker.RegisterConsumer("manual", "manual-consumer", consumer); err != nil {
		t.Fatalf("RegisterConsumer: %v", err)
	}

	for i := 0; i < total; i++ {
		msg := &protocol.Message{Body: []byte(fmt.Sprintf("m-%d", i)), Exchange: "", RoutingKey: "manual"}
		if err := broker.PublishMessage("", "manual", msg); err != nil {
			t.Fatalf("PublishMessage %d: %v", i, err)
		}
	}

	// Wait for the gate to fill: exactly `prefetch` messages should buffer in
	// the channel (delivered but not acked). Give the poll loop time, then
	// assert it does not exceed the prefetch limit.
	deadline := time.Now().Add(3 * time.Second)
	for len(consumer.Messages) < prefetch && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	// Small settle window to catch any over-delivery beyond the gate.
	time.Sleep(50 * time.Millisecond)
	if got := len(consumer.Messages); got != prefetch {
		t.Fatalf("manual-ack consumer delivered %d, want exactly prefetch=%d", got, prefetch)
	}

	pending, err := broker.storage.GetConsumerPendingAcks("manual-consumer")
	if err != nil {
		t.Fatalf("GetConsumerPendingAcks: %v", err)
	}
	if len(pending) != prefetch {
		t.Fatalf("expected %d pending acks, got %d", prefetch, len(pending))
	}

	// Ack all buffered deliveries; the gate should reopen and the rest flow.
	var lastTag uint64
	for i := 0; i < prefetch; i++ {
		d := <-consumer.Messages
		if d.DeliveryTag > lastTag {
			lastTag = d.DeliveryTag
		}
	}
	if err := broker.AcknowledgeMessage("manual-consumer", lastTag, true); err != nil {
		t.Fatalf("AcknowledgeMessage: %v", err)
	}

	// After acking, more messages must be delivered.
	deadline = time.Now().Add(3 * time.Second)
	for len(consumer.Messages) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(consumer.Messages) == 0 {
		t.Fatalf("no messages delivered after acking; gate did not reopen")
	}
}
