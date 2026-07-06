package broker

import (
	"fmt"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
)

// TestCancelInflightScanExcludesBasicGet is the Step-5 cancel-with-inflight scan
// test. After deleting QueueState.inflightOwners, UnregisterConsumer finds a
// cancelled consumer's still-in-flight deliveries by scanning the deliveryIndex
// ledger filtered by consumer tag. This pins the §5.1 correctness claim that the
// filter is exact: a concurrent basic.get delivery (which lives in the same
// deliveryIndex under the empty consumer tag "") is NOT swept by the scan, while
// every one of the push consumer's own in-flight tags IS requeued for
// redelivery to another consumer.
func TestCancelInflightScanExcludesBasicGet(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()
	broker.DeclareQueue("cscan", false, false, false, nil)

	// First message is taken by a manual-ack basic.get -> deliveryIndex[tag]="".
	if err := broker.PublishMessage("", "cscan", &protocol.Message{Body: []byte("get"), RoutingKey: "cscan"}); err != nil {
		t.Fatalf("Publish(get): %v", err)
	}
	_, getTag, _, err := broker.GetMessageForGet("cscan", false)
	if err != nil {
		t.Fatalf("GetMessageForGet: %v", err)
	}
	if owner, ok := broker.GetConsumerForDelivery(getTag); !ok || owner != "" {
		t.Fatalf("basic.get owner = %q,%v want \"\",true", owner, ok)
	}

	// A push consumer takes the next batch.
	consumerA := &protocol.Consumer{
		Tag:           "cscan-a",
		Queue:         "cscan",
		Messages:      make(chan *protocol.Delivery, 100),
		PrefetchCount: 100,
	}
	if err := broker.RegisterConsumer("cscan", "cscan-a", consumerA); err != nil {
		t.Fatalf("RegisterConsumer A: %v", err)
	}

	const pushCount = 8
	for i := 0; i < pushCount; i++ {
		msg := &protocol.Message{Body: []byte(fmt.Sprintf("push-%d", i)), RoutingKey: "cscan"}
		if err := broker.PublishMessage("", "cscan", msg); err != nil {
			t.Fatalf("Publish(push) %d: %v", i, err)
		}
	}

	// Pull all push deliveries OUT of the channel so they are in-flight
	// (recorded only in deliveryIndex, not buffered in consumer.Messages and not
	// acked) — this is exactly the forwarder-already-pulled case the scan exists
	// to recover.
	deliveries := receiveN(t, consumerA.Messages, pushCount, 5*time.Second)
	pushTags := make(map[uint64]bool, pushCount)
	for _, d := range deliveries {
		pushTags[d.DeliveryTag] = true
	}

	// A second consumer will receive the requeued push messages.
	consumerB := &protocol.Consumer{
		Tag:           "cscan-b",
		Queue:         "cscan",
		Messages:      make(chan *protocol.Delivery, 100),
		PrefetchCount: 100,
	}
	if err := broker.RegisterConsumer("cscan", "cscan-b", consumerB); err != nil {
		t.Fatalf("RegisterConsumer B: %v", err)
	}

	// Cancel A: the deliveryIndex scan must requeue exactly A's in-flight tags.
	if err := broker.UnregisterConsumer("cscan-a"); err != nil {
		t.Fatalf("UnregisterConsumer A: %v", err)
	}

	requeued := receiveN(t, consumerB.Messages, pushCount, 5*time.Second)
	for _, d := range requeued {
		if !pushTags[d.DeliveryTag] {
			t.Errorf("consumer B got tag %d that was not one of A's in-flight push tags", d.DeliveryTag)
		}
	}

	// The basic.get delivery must be completely untouched by A's cancel scan:
	// still owned by "" and still independently ackable.
	if owner, ok := broker.GetConsumerForDelivery(getTag); !ok || owner != "" {
		t.Fatalf("basic.get delivery disturbed by cancel scan: owner=%q ok=%v (tag %d)", owner, ok, getTag)
	}
	if err := broker.AcknowledgeGetDelivery(getTag); err != nil {
		t.Fatalf("AcknowledgeGetDelivery after cancel: %v", err)
	}

	broker.UnregisterConsumer("cscan-b")
}
