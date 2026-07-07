package broker

import (
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// publishToSource publishes a message into the source queue via the default
// exchange (routing key == queue name) so it lands in storage and the dispatch
// cursor, ready to be delivered/get.
func publishToSource(t *testing.T, b *StorageBroker, queue string, body string) {
	t.Helper()
	require.NoError(t, b.PublishMessage("", queue, &protocol.Message{
		Exchange: "", RoutingKey: queue, Body: []byte(body),
	}))
}

// countInQueue drains a queue via basic.get (no-ack) and returns how many
// messages it held.
func countInQueue(t *testing.T, b *StorageBroker, queue string) int {
	t.Helper()
	n := 0
	for {
		if drainOne(t, b, queue) == nil {
			return n
		}
		n++
	}
}

// TestRejectGetDelivery_DeadLettersOnRequeueFalse verifies a basic.get reject
// with requeue=false dead-letters the message to the configured DLX and removes
// it from the source.
func TestRejectGetDelivery_DeadLettersOnRequeueFalse(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	setupDLX(t, b, dlxOpts{
		exchange: "dlx", exchangeKind: "direct", targets: []string{"dlq"}, bindKey: "src",
		sourceArgs: map[string]interface{}{"x-dead-letter-exchange": "dlx"},
	})
	publishToSource(t, b, "src", "hello")

	msg, tag, _, err := b.GetMessageForGet("src", false)
	require.NoError(t, err)
	require.NotNil(t, msg)

	require.NoError(t, b.RejectGetDelivery(tag, false))

	got := drainOne(t, b, "dlq")
	require.NotNil(t, got, "rejected (requeue=false) basic.get message must be dead-lettered")
	entries := xDeathEntries(t, got.Headers)
	assertEntry(t, entries[0], "src", DeadLetterRejected, 1)

	// Source no longer holds it.
	srcMsg, _ := b.storage.GetMessage("src", tag)
	require.Nil(t, srcMsg, "source message must be deleted after dead-lettering")
}

// TestRejectGetDelivery_RequeueTrueDoesNotDeadLetter verifies requeue=true never
// dead-letters (the message stays available on the source).
func TestRejectGetDelivery_RequeueTrueDoesNotDeadLetter(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	setupDLX(t, b, dlxOpts{
		exchange: "dlx", exchangeKind: "direct", targets: []string{"dlq"}, bindKey: "src",
		sourceArgs: map[string]interface{}{"x-dead-letter-exchange": "dlx"},
	})
	publishToSource(t, b, "src", "hello")

	_, tag, _, err := b.GetMessageForGet("src", false)
	require.NoError(t, err)

	require.NoError(t, b.RejectGetDelivery(tag, true))

	require.Nil(t, drainOne(t, b, "dlq"), "requeue=true must NOT dead-letter")
	// Message is back on the source, redelivered.
	require.Equal(t, 1, countInQueue(t, b, "src"), "requeued message must remain available on the source")
}

// TestRejectGetDelivery_NoDLXPolicyJustDiscards verifies that without a DLX
// policy a requeue=false reject simply discards (no panic, nothing republished).
func TestRejectGetDelivery_NoDLXPolicyJustDiscards(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	_, err := b.DeclareQueue("plain", false, false, false, nil)
	require.NoError(t, err)
	publishToSource(t, b, "plain", "hello")

	_, tag, _, err := b.GetMessageForGet("plain", false)
	require.NoError(t, err)
	require.NoError(t, b.RejectGetDelivery(tag, false))

	srcMsg, _ := b.storage.GetMessage("plain", tag)
	require.Nil(t, srcMsg, "message must be discarded")
}

// registerManualConsumer registers a manual-ack consumer with a buffered
// delivery channel large enough to hold `buf` deliveries.
func registerManualConsumer(t *testing.T, b *StorageBroker, queue, tag string, prefetch uint16, buf int) *protocol.Consumer {
	t.Helper()
	c := &protocol.Consumer{
		Tag:           tag,
		Queue:         queue,
		NoAck:         false,
		Messages:      make(chan *protocol.Delivery, buf),
		PrefetchCount: prefetch,
	}
	require.NoError(t, b.RegisterConsumer(queue, tag, c))
	return c
}

// recvDeliveries blocks until it has received exactly n deliveries or times out.
func recvDeliveries(t *testing.T, c *protocol.Consumer, n int) []*protocol.Delivery {
	t.Helper()
	out := make([]*protocol.Delivery, 0, n)
	deadline := time.After(5 * time.Second)
	for len(out) < n {
		select {
		case d := <-c.Messages:
			out = append(out, d)
		case <-deadline:
			t.Fatalf("received %d/%d deliveries before timeout", len(out), n)
		}
	}
	return out
}

// TestRejectMessage_ConsumerDeadLettersOnRequeueFalse verifies the consumer
// basic.reject path (requeue=false) dead-letters via the DLX seam.
func TestRejectMessage_ConsumerDeadLettersOnRequeueFalse(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	setupDLX(t, b, dlxOpts{
		exchange: "dlx", exchangeKind: "direct", targets: []string{"dlq"}, bindKey: "src",
		sourceArgs: map[string]interface{}{"x-dead-letter-exchange": "dlx"},
	})

	c := registerManualConsumer(t, b, "src", "c1", 10, 10)
	publishToSource(t, b, "src", "hello")
	d := recvDeliveries(t, c, 1)[0]

	require.NoError(t, b.RejectMessage("c1", d.DeliveryTag, false))

	got := drainOne(t, b, "dlq")
	require.NotNil(t, got, "consumer reject (requeue=false) must dead-letter")
	assertEntry(t, xDeathEntries(t, got.Headers)[0], "src", DeadLetterRejected, 1)
}

// TestNackSingle_DeadLettersOnRequeueFalse verifies basic.nack (multiple=false,
// requeue=false) dead-letters.
func TestNackSingle_DeadLettersOnRequeueFalse(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	setupDLX(t, b, dlxOpts{
		exchange: "dlx", exchangeKind: "direct", targets: []string{"dlq"}, bindKey: "src",
		sourceArgs: map[string]interface{}{"x-dead-letter-exchange": "dlx"},
	})

	c := registerManualConsumer(t, b, "src", "c1", 10, 10)
	publishToSource(t, b, "src", "hello")
	d := recvDeliveries(t, c, 1)[0]

	require.NoError(t, b.NacknowledgeMessage("c1", d.DeliveryTag, false, false))
	require.NotNil(t, drainOne(t, b, "dlq"), "nack (multiple=false, requeue=false) must dead-letter")
}

// TestNackMultiple_DeadLettersAll verifies a multi-nack (requeue=false) dead-
// letters EVERY covered message (the batched cold path), preserving x-death on
// each.
func TestNackMultiple_DeadLettersAll(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	setupDLX(t, b, dlxOpts{
		exchange: "dlx", exchangeKind: "direct", targets: []string{"dlq"}, bindKey: "src",
		sourceArgs: map[string]interface{}{"x-dead-letter-exchange": "dlx"},
	})

	const n = 25
	c := registerManualConsumer(t, b, "src", "c1", uint16(n), n)
	for i := 0; i < n; i++ {
		publishToSource(t, b, "src", "m")
	}
	ds := recvDeliveries(t, c, n)

	// Highest delivery tag received; multi-nack covers everything <= it.
	var maxTag uint64
	for _, d := range ds {
		if d.DeliveryTag > maxTag {
			maxTag = d.DeliveryTag
		}
	}
	require.NoError(t, b.NacknowledgeMessage("c1", maxTag, true, false))

	require.Equal(t, n, countInQueue(t, b, "dlq"), "every multi-nacked message must be dead-lettered")
}

// TestNackMultiple_RequeueTrueNoDeadLetter verifies multi-nack requeue=true never
// dead-letters (messages are requeued for redelivery).
func TestNackMultiple_RequeueTrueNoDeadLetter(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	setupDLX(t, b, dlxOpts{
		exchange: "dlx", exchangeKind: "direct", targets: []string{"dlq"}, bindKey: "src",
		sourceArgs: map[string]interface{}{"x-dead-letter-exchange": "dlx"},
	})

	const n = 5
	c := registerManualConsumer(t, b, "src", "c1", uint16(n), n)
	for i := 0; i < n; i++ {
		publishToSource(t, b, "src", "m")
	}
	ds := recvDeliveries(t, c, n)
	var maxTag uint64
	for _, d := range ds {
		if d.DeliveryTag > maxTag {
			maxTag = d.DeliveryTag
		}
	}
	require.NoError(t, b.NacknowledgeMessage("c1", maxTag, true, true))
	require.Nil(t, drainOne(t, b, "dlq"), "requeue=true multi-nack must NOT dead-letter")
}
