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
// Case 6 — cycle behavior (frozen seam #2: reason-conditional cycle filter).
//   - reject A↔B loop must NOT drop (cycles forever).
//   - expired/maxlen cycle drops on queue-name match.
//   - fanout DLX with mixed cyclic/non-cyclic targets: delivers to the
//     non-cyclic target, drops only the cyclic one.
// ----------------------------------------------------------------------------

func TestConformance_RejectCycleDoesNotDrop(t *testing.T) {
	b := newConfBroker(t)
	defer b.cleanup()
	_, ch := b.dial(t)

	ax := uniqueName("ax")
	bx := uniqueName("bx")
	require.NoError(t, ch.ExchangeDeclare(ax, "direct", true, false, false, false, nil))
	require.NoError(t, ch.ExchangeDeclare(bx, "direct", true, false, false, false, nil))
	t.Cleanup(func() { _ = ch.ExchangeDelete(ax, false, false); _ = ch.ExchangeDelete(bx, false, false) })

	aQ := uniqueName("A")
	bQ := uniqueName("B")
	declareDurable(t, ch, aQ, amqp.Table{"x-dead-letter-exchange": bx, "x-dead-letter-routing-key": "toB"})
	declareDurable(t, ch, bQ, amqp.Table{"x-dead-letter-exchange": ax, "x-dead-letter-routing-key": "toA"})
	require.NoError(t, ch.QueueBind(aQ, "toA", ax, false, nil))
	require.NoError(t, ch.QueueBind(bQ, "toB", bx, false, nil))

	require.NoError(t, ch.PublishWithContext(t.Context(), ax, "toA", false, false, amqp.Publishing{Body: []byte("loop")}))

	// A → reject → B
	da := consumeOne(t, ch, aQ, 8*time.Second)
	require.NoError(t, da.Reject(false))
	// B → reject → A  (a rejected cycle must NOT be dropped)
	db := consumeOne(t, ch, bQ, 8*time.Second)
	require.NoError(t, db.Reject(false))
	// message must reappear in A — proves reject cycles are never dropped.
	da2 := consumeOne(t, ch, aQ, 8*time.Second)
	require.NoError(t, da2.Ack(false))
	assert.Equal(t, []byte("loop"), da2.Body)
	deaths := xDeathArray(t, da2.Headers)
	lockLog(t, "reject-cycle x-death after 2 hops", deaths)
	assert.GreaterOrEqual(t, len(deaths), 2, "both A and B rejected deaths must be recorded (no cycle drop)")
}

func TestConformance_ExpiredCycleDropsOnQueueNameMatch(t *testing.T) {
	b := newConfBroker(t)
	defer b.cleanup()
	_, ch := b.dial(t)

	// A dead-letters (on TTL expiry) via a fanout bound back to A itself → cycle.
	selfx := uniqueName("selfx")
	declareFanout(t, ch, selfx)
	aQ := uniqueName("Aexp")
	declareDurable(t, ch, aQ, amqp.Table{
		"x-message-ttl":          int32(300),
		"x-dead-letter-exchange": selfx,
	})
	require.NoError(t, ch.QueueBind(aQ, "", selfx, false, nil))

	require.NoError(t, ch.PublishWithContext(t.Context(), "", aQ, false, false, amqp.Publishing{Body: []byte("selfloop")}))

	// After expiry, the dead-letter would re-enter A, which is already in the
	// x-death cycle set → dropped. Queue must drain to empty and STAY empty.
	require.Eventually(t, func() bool { return queueDepth(t, ch, aQ) == 0 }, 8*time.Second, 100*time.Millisecond,
		"expired self-cycle message must be dropped, leaving A empty")
	time.Sleep(1500 * time.Millisecond) // if it re-looped it would oscillate; confirm it stays gone
	assert.Equal(t, 0, queueDepth(t, ch, aQ), "expired self-cycle must not re-loop")
	lockLog(t, "expired-self-cycle final depth", 0)
}

func TestConformance_FanoutMixedCycle(t *testing.T) {
	b := newConfBroker(t)
	defer b.cleanup()
	_, ch := b.dial(t)

	// A expires into a fanout bound to A itself (cyclic) AND C (non-cyclic).
	// The C copy must be delivered; the A copy must be dropped.
	mx := uniqueName("mx")
	declareFanout(t, ch, mx)
	aQ := uniqueName("Amix")
	cQ := uniqueName("Cmix")
	declareDurable(t, ch, aQ, amqp.Table{"x-message-ttl": int32(300), "x-dead-letter-exchange": mx})
	declareDurable(t, ch, cQ, nil)
	require.NoError(t, ch.QueueBind(aQ, "", mx, false, nil))
	require.NoError(t, ch.QueueBind(cQ, "", mx, false, nil))

	require.NoError(t, ch.PublishWithContext(t.Context(), "", aQ, false, false, amqp.Publishing{Body: []byte("mixed")}))

	// C (non-cyclic) receives the dead-letter.
	d := consumeOne(t, ch, cQ, 8*time.Second)
	require.NoError(t, d.Ack(false))
	assert.Equal(t, []byte("mixed"), d.Body)
	deaths := xDeathArray(t, d.Headers)
	reason, _ := assertXDeathEntryTypes(t, deaths[0], false)
	assert.Equal(t, reasonExpired, reason)

	// A (cyclic) must NOT retain a re-looped copy.
	require.Eventually(t, func() bool { return queueDepth(t, ch, aQ) == 0 }, 8*time.Second, 100*time.Millisecond,
		"cyclic fanout target A must be dropped")
	lockLog(t, "fanout-mixed A final depth", 0)
}
