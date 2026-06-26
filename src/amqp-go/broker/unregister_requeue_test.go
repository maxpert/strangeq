package broker

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
)

// ============================================================================
// UnregisterConsumer in-flight message requeue tests
//
// When a consumer is cancelled, all messages that were delivered to it but
// not yet ACKed must be requeued so other consumers can redeliver them.
// This covers two categories of in-flight messages:
//
//   1. Messages buffered in consumer.Messages (delivered by the poll loop
//      but not yet pulled by the server's forwarder goroutine).
//   2. Messages that already left consumer.Messages (pulled by the
//      forwarder, sitting in fanIn, or sent to the client) but were never
//      ACKed — tracked in QueueState.inflightOwners.
//
// Without the fix, both categories were permanently orphaned: deliveryIndex
// entries, inflight counter, and pendingAck records all leaked.
// ============================================================================

// waitForChannelDepth polls len(ch) until it reaches n or times out.
// Unlike receiving from the channel, this leaves messages buffered.
func waitForChannelDepth(t *testing.T, ch <-chan *protocol.Delivery, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if len(ch) >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for channel depth %d, got %d", n, len(ch))
		}
		time.Sleep(time.Millisecond)
	}
}

// receiveN receives n deliveries from ch or fails after timeout.
func receiveN(t *testing.T, ch <-chan *protocol.Delivery, n int, timeout time.Duration) []*protocol.Delivery {
	t.Helper()
	var result []*protocol.Delivery
	deadline := time.Now().Add(timeout)
	for len(result) < n {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("timed out receiving deliveries: got %d/%d", len(result), n)
		}
		select {
		case d := <-ch:
			result = append(result, d)
		case <-time.After(remaining):
			t.Fatalf("timed out receiving deliveries: got %d/%d", len(result), n)
		}
	}
	return result
}

// TestUnregisterRequeuesBufferedMessages verifies that messages sitting in
// consumer.Messages (not yet pulled by a forwarder) are requeued and
// redelivered to another consumer after UnregisterConsumer.
func TestUnregisterRequeuesBufferedMessages(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()
	broker.DeclareQueue("rq", false, false, false, nil)

	// Consumer A: large buffer + prefetch so all messages land in the
	// Messages channel. No forwarder exists at the broker level, so
	// messages stay buffered until we explicitly drain them.
	consumerA := &protocol.Consumer{
		Tag:           "consumer-a",
		Queue:         "rq",
		Messages:      make(chan *protocol.Delivery, 100),
		PrefetchCount: 100,
	}
	if err := broker.RegisterConsumer("rq", "consumer-a", consumerA); err != nil {
		t.Fatalf("RegisterConsumer A: %v", err)
	}

	// Publish 5 messages
	for i := 0; i < 5; i++ {
		msg := &protocol.Message{Body: []byte(fmt.Sprintf("msg-%d", i)), Exchange: "", RoutingKey: "rq"}
		if err := broker.PublishMessage("", "rq", msg); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	// Wait for all 5 to be buffered in Messages (WITHOUT receiving)
	waitForChannelDepth(t, consumerA.Messages, 5, 5*time.Second)

	// Snapshot the delivery tags by peeking (receive + put back is not
	// possible on a channel, so we receive and track them, then rely on
	// the drain to requeue them).
	originalTags := make(map[uint64]bool)
	for i := 0; i < 5; i++ {
		d := <-consumerA.Messages
		originalTags[d.DeliveryTag] = true
	}
	// Put them back so the drain in UnregisterConsumer finds them
	for tag := range originalTags {
		// We can't put the original Delivery back since we didn't keep
		// references, but the drain only needs DeliveryTag. Create a
		// minimal delivery for each tag.
		consumerA.Messages <- &protocol.Delivery{DeliveryTag: tag}
	}

	// Register consumer B BEFORE cancelling A
	consumerB := &protocol.Consumer{
		Tag:           "consumer-b",
		Queue:         "rq",
		Messages:      make(chan *protocol.Delivery, 100),
		PrefetchCount: 100,
	}
	if err := broker.RegisterConsumer("rq", "consumer-b", consumerB); err != nil {
		t.Fatalf("RegisterConsumer B: %v", err)
	}

	// Cancel consumer A — should drain Messages and requeue all 5
	if err := broker.UnregisterConsumer("consumer-a"); err != nil {
		t.Fatalf("UnregisterConsumer A: %v", err)
	}

	// Consumer B should receive all 5 requeued messages
	requeued := receiveN(t, consumerB.Messages, 5, 5*time.Second)

	for _, d := range requeued {
		if !originalTags[d.DeliveryTag] {
			t.Errorf("redelivered tag %d was not in original deliveries", d.DeliveryTag)
		}
	}

	broker.UnregisterConsumer("consumer-b")
}

// TestUnregisterRequeuesInflightOwners verifies that messages tracked in
// QueueState.inflightOwners but NOT in consumer.Messages (simulating the
// server's forwarder goroutine having already pulled them) are requeued
// after UnregisterConsumer.
func TestUnregisterRequeuesInflightOwners(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()
	broker.DeclareQueue("rq2", false, false, false, nil)

	consumerA := &protocol.Consumer{
		Tag:           "consumer-a",
		Queue:         "rq2",
		Messages:      make(chan *protocol.Delivery, 100),
		PrefetchCount: 100,
	}
	if err := broker.RegisterConsumer("rq2", "consumer-a", consumerA); err != nil {
		t.Fatalf("RegisterConsumer A: %v", err)
	}

	// Publish 5 messages
	for i := 0; i < 5; i++ {
		msg := &protocol.Message{Body: []byte(fmt.Sprintf("msg-%d", i)), Exchange: "", RoutingKey: "rq2"}
		if err := broker.PublishMessage("", "rq2", msg); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	// Receive all 5 from the channel — this simulates the server's
	// forwarder goroutine pulling them. The messages are now in
	// inflightOwners but NOT in consumer.Messages.
	deliveries := receiveN(t, consumerA.Messages, 5, 5*time.Second)
	originalTags := make(map[uint64]bool)
	for _, d := range deliveries {
		originalTags[d.DeliveryTag] = true
	}

	qs := broker.getOrCreateQueueState("rq2")
	if qs.InflightCount() != 5 {
		t.Fatalf("inflight before unregister = %d, want 5", qs.InflightCount())
	}

	// Register consumer B
	consumerB := &protocol.Consumer{
		Tag:           "consumer-b",
		Queue:         "rq2",
		Messages:      make(chan *protocol.Delivery, 100),
		PrefetchCount: 100,
	}
	if err := broker.RegisterConsumer("rq2", "consumer-b", consumerB); err != nil {
		t.Fatalf("RegisterConsumer B: %v", err)
	}

	// Cancel consumer A — Messages is empty, so the drain finds nothing.
	// The inflightOwners scan must catch all 5.
	if err := broker.UnregisterConsumer("consumer-a"); err != nil {
		t.Fatalf("UnregisterConsumer A: %v", err)
	}

	// Consumer B should receive all 5 via the requeue ring
	requeued := receiveN(t, consumerB.Messages, 5, 5*time.Second)

	for _, d := range requeued {
		if !originalTags[d.DeliveryTag] {
			t.Errorf("redelivered tag %d not in original set", d.DeliveryTag)
		}
	}
	if len(requeued) != 5 {
		t.Errorf("expected 5 redelivered, got %d", len(requeued))
	}

	broker.UnregisterConsumer("consumer-b")
}

// TestUnregisterInflightCounterConsistency verifies that the inflight counter
// on QueueState is consistent after UnregisterConsumer — no double decrement,
// no orphaned count.
func TestUnregisterInflightCounterConsistency(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()
	broker.DeclareQueue("rq3", false, false, false, nil)

	consumerA := &protocol.Consumer{
		Tag:           "consumer-a",
		Queue:         "rq3",
		Messages:      make(chan *protocol.Delivery, 50),
		PrefetchCount: 50,
	}
	if err := broker.RegisterConsumer("rq3", "consumer-a", consumerA); err != nil {
		t.Fatalf("RegisterConsumer A: %v", err)
	}

	for i := 0; i < 10; i++ {
		msg := &protocol.Message{Body: []byte("x"), Exchange: "", RoutingKey: "rq3"}
		if err := broker.PublishMessage("", "rq3", msg); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}
	// Receive all 10 (simulating forwarder) so they're in inflightOwners only
	receiveN(t, consumerA.Messages, 10, 5*time.Second)

	qs := broker.getOrCreateQueueState("rq3")
	beforeInflight := qs.InflightCount()
	if beforeInflight != 10 {
		t.Fatalf("inflight before unregister = %d, want 10", beforeInflight)
	}

	// Cancel consumer A — no other consumer, so requeued messages stay
	// in the requeue ring. inflight should be 0 (Requeue decrements it).
	if err := broker.UnregisterConsumer("consumer-a"); err != nil {
		t.Fatalf("UnregisterConsumer: %v", err)
	}

	afterInflight := qs.InflightCount()
	if afterInflight != 0 {
		t.Errorf("inflight after unregister = %d, want 0 (all requeued, none claimed)", afterInflight)
	}

	// Requeue depth should be 10
	if rd := qs.RequeueDepth(); rd != 10 {
		t.Errorf("requeue depth = %d, want 10", rd)
	}
}

// TestUnregisterNoDoubleRequeue verifies that messages are not requeued
// twice — once by deliverMessage's stopCh path and again by the drain/scan.
// The deliveryIndex.LoadAndDelete guard and the fact that deliverMessage's
// stopCh path deletes from inflightOwners prevent this.
func TestUnregisterNoDoubleRequeue(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()
	broker.DeclareQueue("rq4", false, false, false, nil)

	// Consumer with buffer=1, prefetch=1: the poll loop delivers one
	// message at a time, maximizing the chance that deliverMessage's
	// select races with stopCh.
	consumerA := &protocol.Consumer{
		Tag:           "consumer-a",
		Queue:         "rq4",
		Messages:      make(chan *protocol.Delivery, 1),
		PrefetchCount: 1,
	}
	if err := broker.RegisterConsumer("rq4", "consumer-a", consumerA); err != nil {
		t.Fatalf("RegisterConsumer A: %v", err)
	}

	for i := 0; i < 3; i++ {
		msg := &protocol.Message{Body: []byte("x"), Exchange: "", RoutingKey: "rq4"}
		if err := broker.PublishMessage("", "rq4", msg); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}

	// Wait for at least 1 delivery, then cancel — creating a race
	// between deliverMessage's select and stopCh.
	waitForChannelDepth(t, consumerA.Messages, 1, 5*time.Second)

	// Register consumer B to catch requeued messages
	consumerB := &protocol.Consumer{
		Tag:           "consumer-b",
		Queue:         "rq4",
		Messages:      make(chan *protocol.Delivery, 10),
		PrefetchCount: 10,
	}
	if err := broker.RegisterConsumer("rq4", "consumer-b", consumerB); err != nil {
		t.Fatalf("RegisterConsumer B: %v", err)
	}

	if err := broker.UnregisterConsumer("consumer-a"); err != nil {
		t.Fatalf("UnregisterConsumer: %v", err)
	}

	// Collect all redelivered messages
	requeued := receiveN(t, consumerB.Messages, 3, 5*time.Second)

	// Verify no duplicate delivery tags (double-requeue would cause
	// the same tag to appear twice)
	seen := make(map[uint64]bool)
	for _, d := range requeued {
		if seen[d.DeliveryTag] {
			t.Errorf("tag %d redelivered twice (double requeue)", d.DeliveryTag)
		}
		seen[d.DeliveryTag] = true
	}

	broker.UnregisterConsumer("consumer-b")
}

// TestUnregisterPollLoopExits verifies that the done channel is closed after
// UnregisterConsumer, proving the poll loop has fully exited.
func TestUnregisterPollLoopExits(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()
	broker.DeclareQueue("rq5", false, false, false, nil)

	consumer := &protocol.Consumer{
		Tag:           "consumer-x",
		Queue:         "rq5",
		Messages:      make(chan *protocol.Delivery, 10),
		PrefetchCount: 10,
	}
	if err := broker.RegisterConsumer("rq5", "consumer-x", consumer); err != nil {
		t.Fatalf("RegisterConsumer: %v", err)
	}

	val, ok := broker.activeConsumers.Load("consumer-x")
	if !ok {
		t.Fatal("consumer not found")
	}
	state := val.(*ConsumerState)

	// done should be open (not closed) while the poll loop is running
	select {
	case <-state.done:
		t.Fatal("done channel closed before unregister")
	default:
	}

	if err := broker.UnregisterConsumer("consumer-x"); err != nil {
		t.Fatalf("UnregisterConsumer: %v", err)
	}

	// done should be closed after unregister
	select {
	case <-state.done:
	default:
		t.Error("done channel not closed after unregister (poll loop still running)")
	}
}

// TestUnregisterConcurrentAckNoDoubleDecrement verifies that a concurrent ACK
// racing with UnregisterConsumer's cleanup does not double-decrement the
// inflight counter. The deliveryIndex.LoadAndDelete guard ensures only one
// side processes each tag.
//
// Each of the 5 raced tags must end up in exactly one of two terminal states:
//   - ACKed: not in deliveryIndex (ACK handler won the LoadAndDelete race)
//   - Requeued: in deliveryIndex mapped to consumer-b (cleanup won, the
//     tag was requeued and redelivered to B)
//
// The key invariant: the inflight counter must never go negative, proving
// no tag was processed by both the ACK handler and the cleanup.
func TestUnregisterConcurrentAckNoDoubleDecrement(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()
	broker.DeclareQueue("rq6", false, false, false, nil)

	consumerA := &protocol.Consumer{
		Tag:           "consumer-a",
		Queue:         "rq6",
		Messages:      make(chan *protocol.Delivery, 50),
		PrefetchCount: 50,
	}
	if err := broker.RegisterConsumer("rq6", "consumer-a", consumerA); err != nil {
		t.Fatalf("RegisterConsumer A: %v", err)
	}

	for i := 0; i < 20; i++ {
		msg := &protocol.Message{Body: []byte("x"), Exchange: "", RoutingKey: "rq6"}
		if err := broker.PublishMessage("", "rq6", msg); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}
	deliveries := receiveN(t, consumerA.Messages, 20, 5*time.Second)

	// Register consumer B to catch requeued messages
	consumerB := &protocol.Consumer{
		Tag:           "consumer-b",
		Queue:         "rq6",
		Messages:      make(chan *protocol.Delivery, 50),
		PrefetchCount: 50,
	}
	if err := broker.RegisterConsumer("rq6", "consumer-b", consumerB); err != nil {
		t.Fatalf("RegisterConsumer B: %v", err)
	}

	// Race: ACK some messages while unregistering
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			broker.AcknowledgeMessage("consumer-a", deliveries[i].DeliveryTag, false)
		}
	}()

	if err := broker.UnregisterConsumer("consumer-a"); err != nil {
		t.Fatalf("UnregisterConsumer: %v", err)
	}
	wg.Wait()

	qs := broker.getOrCreateQueueState("rq6")

	// Core invariant: inflight must never go negative (double decrement)
	inflight := qs.InflightCount()
	if inflight < 0 {
		t.Errorf("inflight counter = %d (negative — double decrement detected)", inflight)
	}

	// Each of the 5 raced tags must be either ACKed (not in deliveryIndex)
	// or requeued (mapped to consumer-b, not consumer-a).
	for i := 0; i < 5; i++ {
		tag := deliveries[i].DeliveryTag
		val, exists := broker.deliveryIndex.Load(tag)
		if !exists {
			// ACK won the race — tag was properly acked, no longer in index
			continue
		}
		if val == "consumer-a" {
			t.Errorf("tag %d still mapped to consumer-a (orphaned — neither ACKed nor requeued)", tag)
		}
		// else: mapped to consumer-b (requeued and redelivered) — correct
	}

	broker.UnregisterConsumer("consumer-b")
}

// TestUnregisterMixedDrainAndInflight verifies the combined scenario: some
// messages are in consumer.Messages (buffered) and some are in
// inflightOwners only (pulled by forwarder). UnregisterConsumer must
// requeue both sets without loss or duplication.
func TestUnregisterMixedDrainAndInflight(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()
	broker.DeclareQueue("rq7", false, false, false, nil)

	consumerA := &protocol.Consumer{
		Tag:           "consumer-a",
		Queue:         "rq7",
		Messages:      make(chan *protocol.Delivery, 100),
		PrefetchCount: 100,
	}
	if err := broker.RegisterConsumer("rq7", "consumer-a", consumerA); err != nil {
		t.Fatalf("RegisterConsumer A: %v", err)
	}

	// Publish 10 messages
	for i := 0; i < 10; i++ {
		msg := &protocol.Message{Body: []byte("x"), Exchange: "", RoutingKey: "rq7"}
		if err := broker.PublishMessage("", "rq7", msg); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}
	waitForChannelDepth(t, consumerA.Messages, 10, 5*time.Second)

	// Pull 6 from channel (simulating forwarder), leave 4 buffered
	pulledTags := make(map[uint64]bool)
	for i := 0; i < 6; i++ {
		d := <-consumerA.Messages
		pulledTags[d.DeliveryTag] = true
	}
	if len(consumerA.Messages) != 4 {
		t.Fatalf("expected 4 buffered, got %d", len(consumerA.Messages))
	}

	// Register consumer B
	consumerB := &protocol.Consumer{
		Tag:           "consumer-b",
		Queue:         "rq7",
		Messages:      make(chan *protocol.Delivery, 100),
		PrefetchCount: 100,
	}
	if err := broker.RegisterConsumer("rq7", "consumer-b", consumerB); err != nil {
		t.Fatalf("RegisterConsumer B: %v", err)
	}

	// Cancel consumer A
	if err := broker.UnregisterConsumer("consumer-a"); err != nil {
		t.Fatalf("UnregisterConsumer: %v", err)
	}

	// Consumer B should receive all 10 (4 from drain + 6 from inflightOwners scan)
	requeued := receiveN(t, consumerB.Messages, 10, 5*time.Second)

	// Verify no duplicates
	seen := make(map[uint64]bool)
	for _, d := range requeued {
		if seen[d.DeliveryTag] {
			t.Errorf("tag %d redelivered twice", d.DeliveryTag)
		}
		seen[d.DeliveryTag] = true
	}
	if len(seen) != 10 {
		t.Errorf("expected 10 unique tags, got %d", len(seen))
	}

	broker.UnregisterConsumer("consumer-b")
}
