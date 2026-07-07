//go:build conformance

package main

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// collectConfirm waits for the publisher-confirm for a specific delivery tag.
func collectConfirm(t *testing.T, confirms <-chan amqp.Confirmation, tag uint64, timeout time.Duration) amqp.Confirmation {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case c, ok := <-confirms:
			require.True(t, ok, "confirm channel closed before tag %d", tag)
			if c.DeliveryTag == tag {
				return c
			}
		case <-deadline:
			t.Fatalf("no confirm for delivery tag %d within %s", tag, timeout)
		}
	}
}

// ----------------------------------------------------------------------------
// Case 4 — x-overflow reject-publish
// ----------------------------------------------------------------------------

func TestConformance_OverflowRejectPublish(t *testing.T) {
	b := newConfBroker(t)
	defer b.cleanup()

	t.Run("ConfirmChannel_RefusedGetsNack_NoAckCoversTag", func(t *testing.T) {
		_, ch := b.dial(t)
		require.NoError(t, ch.Confirm(false))
		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 8))

		q := uniqueName("rejpub")
		declareDurable(t, ch, q, amqp.Table{"x-max-length": int32(1), "x-overflow": "reject-publish"})

		// tag 1 — accepted → ack
		require.NoError(t, ch.PublishWithContext(t.Context(), "", q, false, false, amqp.Publishing{Body: []byte("a")}))
		c1 := collectConfirm(t, confirms, 1, 5*time.Second)
		assert.True(t, c1.Ack, "first publish (queue below limit) must be ack'd")

		// tag 2 — queue is full → refused → nack, and NO ack may cover tag 2.
		require.NoError(t, ch.PublishWithContext(t.Context(), "", q, false, false, amqp.Publishing{Body: []byte("b")}))
		c2 := collectConfirm(t, confirms, 2, 5*time.Second)
		assert.False(t, c2.Ack, "refused publish must be NACK'd (SettleConfirmNack interlock: no ack covers tag 2)")

		assert.Equal(t, 1, readyCount(t, ch, q), "refused message must not be enqueued")
	})

	t.Run("PlainChannel_SilentlyDropped", func(t *testing.T) {
		_, ch := b.dial(t)
		q := uniqueName("rejpubplain")
		declareDurable(t, ch, q, amqp.Table{"x-max-length": int32(1), "x-overflow": "reject-publish"})

		require.NoError(t, ch.PublishWithContext(t.Context(), "", q, false, false, amqp.Publishing{Body: []byte("a")}))
		// This publish is refused; on a plain (no-confirm, no-mandatory) channel
		// the client is told nothing and the message is silently dropped.
		require.NoError(t, ch.PublishWithContext(t.Context(), "", q, false, false, amqp.Publishing{Body: []byte("b")}))
		time.Sleep(300 * time.Millisecond)
		assert.Equal(t, 1, readyCount(t, ch, q), "refused publish on a plain channel is silently dropped")
	})

	// D1 — mandatory + reject-publish (non-confirm).
	// RabbitMQ: mandatory covers ONLY unroutable messages; a routable-but-full
	// message is NOT returned (silently dropped). StrangeQ EXTENSION: returns it
	// via basic.return with reply-code 312/NO_ROUTE. Documented divergence.
	t.Run("D1_MandatoryRejectPublish_ReturnBehaviour", func(t *testing.T) {
		_, ch := b.dial(t)
		returns := ch.NotifyReturn(make(chan amqp.Return, 4))
		q := uniqueName("rejpubmand")
		declareDurable(t, ch, q, amqp.Table{"x-max-length": int32(1), "x-overflow": "reject-publish"})

		require.NoError(t, ch.PublishWithContext(t.Context(), "", q, true /*mandatory*/, false, amqp.Publishing{Body: []byte("a")}))
		require.NoError(t, ch.PublishWithContext(t.Context(), "", q, true /*mandatory*/, false, amqp.Publishing{Body: []byte("b")}))

		expectReturn := expectByTarget(t, "mandatory+reject-publish-return", false /*rabbit: no return*/, true /*strangeq: returns*/)
		select {
		case r := <-returns:
			lockLog(t, "mandatory reject-publish basic.return", map[string]any{"reply-code": r.ReplyCode, "reply-text": r.ReplyText})
			assert.True(t, expectReturn, "target=%s: received a basic.return for a routable-but-full mandatory publish", confTarget())
			if !targetIsRabbit() {
				// StrangeQ reuses 312/NO_ROUTE for this extension path.
				assert.Equal(t, uint16(312), r.ReplyCode, "StrangeQ reject-publish+mandatory return reuses 312/NO_ROUTE")
			}
		case <-time.After(2 * time.Second):
			assert.False(t, expectReturn, "target=%s: no basic.return arrived", confTarget())
		}
	})

	// D4 — fan-out where ANY reject-publish target is full.
	// StrangeQ nacks the WHOLE publish (safest bounded signal) even though other
	// targets accepted it. RESOLVED in W7: real RabbitMQ 4.3.2 does the SAME —
	// the confirm is NACK'd (Ack=false) when a reject-publish target overflows,
	// even though the copy still lands in the unbounded target (open depth == 2 on
	// both brokers). So this is parity, not a divergence.
	t.Run("D4_FanoutAnyTargetFull", func(t *testing.T) {
		_, ch := b.dial(t)
		require.NoError(t, ch.Confirm(false))
		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 8))

		fx := uniqueName("fx")
		declareFanout(t, ch, fx)
		full := uniqueName("full") // reject-publish, capacity 1
		open := uniqueName("open") // unbounded
		declareDurable(t, ch, full, amqp.Table{"x-max-length": int32(1), "x-overflow": "reject-publish"})
		declareDurable(t, ch, open, nil)
		require.NoError(t, ch.QueueBind(full, "", fx, false, nil))
		require.NoError(t, ch.QueueBind(open, "", fx, false, nil))

		// Fill the bounded target to its limit first (tag 1).
		require.NoError(t, ch.PublishWithContext(t.Context(), fx, "", false, false, amqp.Publishing{Body: []byte("fill")}))
		_ = collectConfirm(t, confirms, 1, 5*time.Second)

		// tag 2: fans out to both; `full` is at capacity, `open` can accept.
		require.NoError(t, ch.PublishWithContext(t.Context(), fx, "", false, false, amqp.Publishing{Body: []byte("split")}))
		c2 := collectConfirm(t, confirms, 2, 5*time.Second)
		lockLog(t, "fanout-any-full confirm.Ack", c2.Ack)

		// Whatever the confirm verdict, lock the observable side effect: how many
		// copies landed in the unbounded target.
		time.Sleep(300 * time.Millisecond)
		openDepth := readyCount(t, ch, open)
		lockLog(t, "fanout-any-full open-target depth", openDepth)

		// PARITY (both nack): RabbitMQ 4.3.2 and StrangeQ both NACK the publish
		// when any reject-publish fan-out target is full.
		wantAck := expectByTarget(t, "fanout-any-full-confirm-ack", false /*rabbit: nacks*/, false /*strangeq: nacks*/)
		assert.Equal(t, wantAck, c2.Ack, "target=%s fanout-any-full confirm verdict", confTarget())
		assert.Equal(t, 2, openDepth, "the unbounded fan-out target still receives both copies on both brokers")
	})
}
