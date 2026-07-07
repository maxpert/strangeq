//go:build conformance

package main

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ----------------------------------------------------------------------------
// SQ-9 W7-lock divergences: ttl=0 with a ready consumer, and malformed /
// non-numeric per-message expiration.
// ----------------------------------------------------------------------------

// ttl=0 with a READY consumer.
// RabbitMQ: delivers a ttl=0 message iff a consumer is ready at publish time.
// StrangeQ: has no publish-time direct-handoff, so a ttl=0 message is stamped
// deadline==enqueue and dropped at the first head-check — never delivered.
// Documented divergence (w4-report ttl=0 decision).
func TestConformance_TTLZero_ReadyConsumer(t *testing.T) {
	b := newConfBroker(t)
	defer b.cleanup()
	_, ch := b.dial(t)

	q := uniqueName("ttl0")
	declareDurable(t, ch, q, nil)

	// Register the consumer FIRST so a ready consumer exists at publish time.
	deliveries, err := ch.Consume(q, uniqueName("ctag"), false, false, false, false, nil)
	require.NoError(t, err)
	time.Sleep(200 * time.Millisecond) // let the consumer register server-side

	require.NoError(t, ch.PublishWithContext(t.Context(), "", q, false, false,
		amqp.Publishing{Body: []byte("zero"), Expiration: "0"}))

	wantDelivered := expectByTarget(t, "ttl0-ready-consumer-delivered",
		true /*rabbit: delivers to the ready consumer*/, false /*strangeq: drops*/)

	select {
	case d, ok := <-deliveries:
		require.True(t, ok)
		_ = d.Ack(false)
		lockLog(t, "ttl0 delivered body", string(d.Body))
		assert.True(t, wantDelivered, "target=%s: ttl=0 delivered to a ready consumer", confTarget())
	case <-time.After(2 * time.Second):
		assert.False(t, wantDelivered, "target=%s: ttl=0 NOT delivered (dropped)", confTarget())
	}
}

// Malformed / non-numeric per-message expiration.
// StrangeQ: treats a malformed Expiration as NO per-message TTL (delivered
// normally; queue TTL still applies). Lock what RabbitMQ does (it may reject
// the publish with a channel exception, or ignore the field).
func TestConformance_MalformedExpiration(t *testing.T) {
	b := newConfBroker(t)
	defer b.cleanup()
	_, ch := b.dial(t)

	// Use a fresh channel: a malformed expiration may close the channel on RabbitMQ.
	q := uniqueName("malexp")
	declareDurable(t, ch, q, nil)

	pubErr := ch.PublishWithContext(t.Context(), "", q, false, false,
		amqp.Publishing{Body: []byte("bad-exp"), Expiration: "not-a-number"})

	// PublishWithContext returns synchronously; a channel exception surfaces on
	// the next channel op or via NotifyClose. Probe the channel liveness.
	time.Sleep(300 * time.Millisecond)
	_, liveErr := ch.QueueDeclarePassive(q, true, false, false, false, nil)

	channelSurvived := pubErr == nil && liveErr == nil
	lockLog(t, "malformed-expiration channel-survived", channelSurvived)
	if liveErr != nil {
		lockLog(t, "malformed-expiration error", liveErr.Error())
	}

	wantSurvived := expectByTarget(t, "malformed-expiration-channel-survives",
		targetIsRabbit() && channelSurvived, // rabbit: captured live (see report)
		true,                                // strangeq: treats as no-TTL, channel survives
	)
	if !targetIsRabbit() {
		assert.True(t, wantSurvived, "StrangeQ must treat a malformed expiration as no-TTL and keep the channel usable")
		// Message should be deliverable (no TTL applied).
		msg, ok, getErr := ch.Get(q, true)
		require.NoError(t, getErr)
		assert.True(t, ok, "StrangeQ must deliver a message with a malformed (ignored) expiration")
		if ok {
			assert.Equal(t, []byte("bad-exp"), msg.Body)
		}
	}
}
