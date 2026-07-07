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
// x-death parsing helpers. amqp091-go decodes the header as []interface{} of
// amqp.Table; count is int64, time is time.Time, routing-keys is []interface{}
// of string — asserting those Go types asserts the exact wire tags.
// ----------------------------------------------------------------------------

func xDeathArray(t *testing.T, h amqp.Table) []amqp.Table {
	t.Helper()
	raw, ok := h["x-death"]
	require.True(t, ok, "x-death header must be present; headers=%#v", h)
	arr, ok := raw.([]interface{})
	require.Truef(t, ok, "x-death must decode as a field array ('A'), got %T", raw)
	out := make([]amqp.Table, 0, len(arr))
	for i, e := range arr {
		tbl, ok := e.(amqp.Table)
		require.Truef(t, ok, "x-death[%d] must decode as a field table ('F'), got %T", i, e)
		out = append(out, tbl)
	}
	return out
}

// assertXDeathEntryTypes asserts the field TYPES of one x-death entry match the
// RabbitMQ wire contract; expectOrigExp is whether original-expiration must be
// present. Returns (reason, queue).
func assertXDeathEntryTypes(t *testing.T, e amqp.Table, expectOrigExp bool) (string, string) {
	t.Helper()

	cnt, ok := e["count"]
	require.True(t, ok, "x-death entry must carry count")
	_, isInt64 := cnt.(int64)
	assert.Truef(t, isInt64, "x-death.count must be int64 ('l'), got %T (%v)", cnt, cnt)

	reason, ok := e["reason"].(string)
	require.Truef(t, ok, "x-death.reason must be a string, got %T", e["reason"])

	queue, ok := e["queue"].(string)
	require.Truef(t, ok, "x-death.queue must be a string, got %T", e["queue"])

	_, ok = e["time"].(time.Time)
	assert.Truef(t, ok, "x-death.time must decode as time.Time ('T' timestamp), got %T", e["time"])

	_, ok = e["exchange"].(string)
	assert.Truef(t, ok, "x-death.exchange must be a string, got %T", e["exchange"])

	rks, ok := e["routing-keys"].([]interface{})
	assert.Truef(t, ok, "x-death.routing-keys must decode as a field array, got %T", e["routing-keys"])
	for i, rk := range rks {
		_, ok := rk.(string)
		assert.Truef(t, ok, "x-death.routing-keys[%d] must be a string, got %T", i, rk)
	}

	if expectOrigExp {
		_, ok := e["original-expiration"].(string)
		assert.Truef(t, ok, "x-death.original-expiration must be a string, got %T", e["original-expiration"])
	} else {
		_, present := e["original-expiration"]
		assert.Falsef(t, present, "x-death.original-expiration must be ABSENT here (reason=%s)", reason)
	}
	return reason, queue
}

// assertLastDeath checks x-last-death-* headers target-awarely.
// RabbitMQ 4.x emits x-last-death-{reason,queue,exchange}; StrangeQ does NOT
// (frozen seam #4 deferred it). Documented divergence F2.
func assertLastDeath(t *testing.T, h amqp.Table, wantReason, wantQueue string) {
	t.Helper()
	wantPresent := expectByTarget(t, "x-last-death-present", true /*rabbit*/, false /*strangeq*/)
	_, present := h["x-last-death-reason"]
	assert.Equalf(t, wantPresent, present, "target=%s: x-last-death-reason presence", confTarget())
	if wantPresent && present {
		assert.Equal(t, wantReason, h["x-last-death-reason"])
		assert.Equal(t, wantQueue, h["x-last-death-queue"])
	}
}

// deadLetterWiring declares a durable DLX (direct) + DLQ bound on dlrk, and a
// durable source queue dead-lettering to it. Everything scheduled for deletion.
func deadLetterWiring(t *testing.T, ch *amqp.Channel, srcArgs amqp.Table) (src, dlx, dlq, dlrk string) {
	t.Helper()
	dlx = uniqueName("dlx")
	dlq = uniqueName("dlq")
	dlrk = "dead"
	require.NoError(t, ch.ExchangeDeclare(dlx, "direct", true, false, false, false, nil))
	t.Cleanup(func() { _ = ch.ExchangeDelete(dlx, false, false) })
	declareDurable(t, ch, dlq, nil)
	require.NoError(t, ch.QueueBind(dlq, dlrk, dlx, false, nil))

	src = uniqueName("src")
	full := amqp.Table{"x-dead-letter-exchange": dlx, "x-dead-letter-routing-key": dlrk}
	for k, v := range srcArgs {
		full[k] = v
	}
	declareDurable(t, ch, src, full)
	return src, dlx, dlq, dlrk
}

// ----------------------------------------------------------------------------
// Case 1 — TTL expiry → DLX (per-message + queue x-message-ttl; effective =
// min(msg, queue); x-death reason=expired; original-expiration set for a
// per-message TTL, stripped from the dead-lettered message).
// ----------------------------------------------------------------------------

func TestConformance_TTLExpiryToDLX(t *testing.T) {
	t.Run("QueueTTL_ExpiresToDLX", func(t *testing.T) {
		b := newConfBroker(t)
		defer b.cleanup()
		_, ch := b.dial(t)

		src, _, dlq, dlrk := deadLetterWiring(t, ch, amqp.Table{"x-message-ttl": int32(300)})
		require.NoError(t, ch.PublishWithContext(t.Context(), "", src, false, false,
			amqp.Publishing{Body: []byte("qttl")}))

		d := consumeOne(t, ch, dlq, 8*time.Second)
		require.NoError(t, d.Ack(false))
		assert.Equal(t, dlrk, d.RoutingKey, "delivered routing key must be rewritten to DLRK")

		deaths := xDeathArray(t, d.Headers)
		require.GreaterOrEqual(t, len(deaths), 1)
		// Queue-TTL-only expiry (no per-message Expiration): no original-expiration
		// on EITHER broker (confirmed against RabbitMQ 4.3.2).
		reason, queue := assertXDeathEntryTypes(t, deaths[0], false)
		assert.Equal(t, reasonExpired, reason)
		assert.Equal(t, src, queue)
		assertLastDeath(t, d.Headers, reasonExpired, src)
		lockLog(t, "queue-ttl x-death[0]", deaths[0])
	})

	t.Run("EffectiveTTL_IsMinOfMsgAndQueue", func(t *testing.T) {
		b := newConfBroker(t)
		defer b.cleanup()
		_, ch := b.dial(t)

		// queue TTL 10s, per-message 300ms → effective 300ms → expires fast.
		src, _, dlq, _ := deadLetterWiring(t, ch, amqp.Table{"x-message-ttl": int32(10000)})
		require.NoError(t, ch.PublishWithContext(t.Context(), "", src, false, false,
			amqp.Publishing{Body: []byte("mttl"), Expiration: "300"}))

		start := time.Now()
		d := consumeOne(t, ch, dlq, 8*time.Second)
		require.NoError(t, d.Ack(false))
		assert.Lessf(t, time.Since(start), 6*time.Second,
			"effective TTL must be min(300ms,10s); message should dead-letter quickly")

		deaths := xDeathArray(t, d.Headers)
		// expired WITH a per-message expiration: original-expiration recorded on both.
		reason, _ := assertXDeathEntryTypes(t, deaths[0], true)
		assert.Equal(t, reasonExpired, reason)
		assert.Equal(t, "300", deaths[0]["original-expiration"], "original-expiration must equal the per-message Expiration")
		assert.Empty(t, d.Expiration, "expired dead-letter must have Expiration stripped")
	})
}

// ----------------------------------------------------------------------------
// Case 2 — reject/nack requeue=false → DLX with reason=rejected.
//
// DIVERGENCE F1 (StrangeQ is WRONG on the wire): RabbitMQ 4.3.2 STRIPS the
// message Expiration on dead-letter for ANY reason and records the original in
// x-death.original-expiration. StrangeQ preserves Expiration on rejected/maxlen
// and omits original-expiration (frozen seam #3). Both branches asserted here.
// ----------------------------------------------------------------------------

func TestConformance_RejectRequeueFalseToDLX(t *testing.T) {
	b := newConfBroker(t)
	defer b.cleanup()
	_, ch := b.dial(t)

	src, _, dlq, dlrk := deadLetterWiring(t, ch, nil)

	require.NoError(t, ch.PublishWithContext(t.Context(), "", src, false, false,
		amqp.Publishing{Body: []byte("reject-me"), Expiration: "600000"}))

	del := consumeOne(t, ch, src, 8*time.Second)
	require.NoError(t, del.Reject(false))

	d := consumeOne(t, ch, dlq, 8*time.Second)
	require.NoError(t, d.Ack(false))
	assert.Equal(t, dlrk, d.RoutingKey)

	wantOrigExp := expectByTarget(t, "reject-original-expiration-present", true /*rabbit records it*/, false /*strangeq omits*/)
	deaths := xDeathArray(t, d.Headers)
	reason, queue := assertXDeathEntryTypes(t, deaths[0], wantOrigExp)
	assert.Equal(t, reasonRejected, reason)
	assert.Equal(t, src, queue)
	if wantOrigExp {
		assert.Equal(t, "600000", deaths[0]["original-expiration"])
	}

	wantExpiration := expectByTarget(t, "reject-delivered-expiration", "" /*rabbit strips*/, "600000" /*strangeq preserves*/)
	assert.Equal(t, wantExpiration, d.Expiration, "target=%s: dead-lettered message Expiration", confTarget())
	assertLastDeath(t, d.Headers, reasonRejected, src)
	lockLog(t, "rejected x-death[0]", deaths[0])
}

// ----------------------------------------------------------------------------
// Case 3 — maxlen drop-head: oldest evicted into DLX with reason=maxlen.
// ----------------------------------------------------------------------------

func TestConformance_MaxLenDropHeadToDLX(t *testing.T) {
	t.Run("DropHeadEvictsOldestToDLX", func(t *testing.T) {
		b := newConfBroker(t)
		defer b.cleanup()
		_, ch := b.dial(t)

		src, _, dlq, _ := deadLetterWiring(t, ch, amqp.Table{
			"x-max-length": int32(3),
			"x-overflow":   "drop-head",
		})
		for i := 1; i <= 4; i++ {
			require.NoError(t, ch.PublishWithContext(t.Context(), "", src, false, false,
				amqp.Publishing{Body: []byte{byte('0' + i)}}))
		}
		time.Sleep(400 * time.Millisecond) // settle the eviction

		// The oldest message ("1") is dead-lettered with reason=maxlen.
		d := consumeOne(t, ch, dlq, 8*time.Second)
		require.NoError(t, d.Ack(false))
		assert.Equal(t, []byte("1"), d.Body, "oldest message (drop-head) must be the one dead-lettered")
		deaths := xDeathArray(t, d.Headers)
		reason, queue := assertXDeathEntryTypes(t, deaths[0], false) // no expiration → no original-expiration
		assert.Equal(t, reasonMaxLen, reason)
		assert.Equal(t, src, queue)
		assertLastDeath(t, d.Headers, reasonMaxLen, src)
		lockLog(t, "maxlen x-death[0]", deaths[0])

		// The survivors are exactly {2,3,4} on both brokers.
		assert.Equal(t, [][]byte{[]byte("2"), []byte("3"), []byte("4")}, drainBodies(t, ch, src),
			"source must retain the newest max-length messages")
	})

	// Single body larger than x-max-length-bytes into an EMPTY queue.
	// DIVERGENCE F4: RabbitMQ drop-head DROPS it (queue ends empty); StrangeQ
	// ACCEPTS it (at-or-over: the first message is always kept). Documented.
	t.Run("SingleOversizeBody_DropHead", func(t *testing.T) {
		b := newConfBroker(t)
		defer b.cleanup()
		_, ch := b.dial(t)

		q := uniqueName("bytelimit")
		declareDurable(t, ch, q, amqp.Table{"x-max-length-bytes": int32(100), "x-overflow": "drop-head"})
		require.NoError(t, ch.PublishWithContext(t.Context(), "", q, false, false, amqp.Publishing{Body: make([]byte, 250)}))
		time.Sleep(300 * time.Millisecond)

		wantCount := expectByTarget(t, "oversize-single-body-kept", 0 /*rabbit drops*/, 1 /*strangeq keeps*/)
		got := readyCount(t, ch, q)
		lockLog(t, "oversize single-body ready count", got)
		assert.Equal(t, wantCount, got, "target=%s: single oversize body under max-length-bytes drop-head", confTarget())
	})
}

// ----------------------------------------------------------------------------
// SQ-10 W7-lock divergence — DLX-target-full behavior (CONFIRMED-REAL, W7 finding).
//
// When a dead-lettered message is routed into a DLX target queue that is already
// at its own x-max-length, the two brokers differ:
//   - RabbitMQ applies the TARGET queue's own x-overflow. With the default
//     drop-head, the target evicts its oldest and ACCEPTS the dead-letter, so it
//     stays at its cap (depth 1) holding the NEWEST body.
//   - StrangeQ does NOT enforce the target's x-max-length on the dead-letter
//     republish path: republishToTargets gates only on the ring backpressure
//     high-water mark (AtHighWaterMark), not on policy.MaxLength / x-overflow
//     (which are enforced only on the normal PublishMessage path). So the target
//     OVERSHOOTS its cap — it keeps BOTH messages (depth 2). No message is lost,
//     but x-max-length is silently exceeded on dead-letter, unlike RabbitMQ.
// This is a real gap in the DLX/max-length interaction (SQ-10/SQ-11 territory),
// not a cheap W7 fix — locked here and flagged in w7-report as a finding.
// ----------------------------------------------------------------------------

func TestConformance_DeadLetterIntoFullTarget(t *testing.T) {
	b := newConfBroker(t)
	defer b.cleanup()
	_, ch := b.dial(t)

	// DLX target with its own drop-head cap of 1.
	dlx := uniqueName("dlx")
	dlq := uniqueName("dlq")
	dlrk := "dead"
	require.NoError(t, ch.ExchangeDeclare(dlx, "direct", true, false, false, false, nil))
	t.Cleanup(func() { _ = ch.ExchangeDelete(dlx, false, false) })
	declareDurable(t, ch, dlq, amqp.Table{"x-max-length": int32(1), "x-overflow": "drop-head"})
	require.NoError(t, ch.QueueBind(dlq, dlrk, dlx, false, nil))

	src := uniqueName("src")
	declareDurable(t, ch, src, amqp.Table{"x-dead-letter-exchange": dlx, "x-dead-letter-routing-key": dlrk})

	// Dead-letter "old" into the (empty) target — accepted, target now at cap.
	require.NoError(t, ch.PublishWithContext(t.Context(), "", src, false, false, amqp.Publishing{Body: []byte("old")}))
	require.NoError(t, consumeOne(t, ch, src, 8*time.Second).Reject(false))
	require.Eventually(t, func() bool { return queueDepth(t, ch, dlq) == 1 }, 8*time.Second, 100*time.Millisecond,
		"first dead-letter must land in the DLX target")

	// Dead-letter "new" into the now-full target.
	require.NoError(t, ch.PublishWithContext(t.Context(), "", src, false, false, amqp.Publishing{Body: []byte("new")}))
	require.NoError(t, consumeOne(t, ch, src, 8*time.Second).Reject(false))
	time.Sleep(500 * time.Millisecond) // settle the overflow decision

	got := drainBodies(t, ch, dlq)
	lockLog(t, "dead-letter-into-full-target surviving bodies", got)
	// RabbitMQ drop-head evicts old, keeps newest (respects the cap); StrangeQ
	// overshoots the cap and retains BOTH (no target-overflow on the DLX path).
	wantBodies := expectByTarget(t, "dead-letter-into-full-target",
		[][]byte{[]byte("new")},                // rabbit: target drop-head → newest only
		[][]byte{[]byte("old"), []byte("new")}, // strangeq: overshoots cap → both retained
	)
	assert.Equal(t, wantBodies, got, "target=%s: dead-letter into a full DLX target", confTarget())
}

// ----------------------------------------------------------------------------
// Case 5 — x-death structure: field types, aggregation by (queue,reason),
// most-recent-first ordering, x-first-death-* set once, x-last-death-*
// (RabbitMQ 4.x emits it; StrangeQ does not — divergence F2).
//
// Drives a reject A↔B cycle (reject cycles never drop, frozen seam #2) for 4
// hops: deaths A,B,A,B. That aggregates (A,rejected)=2 and (B,rejected)=2 and
// leaves first-death=A, last/most-recent-death=B (first != last).
// ----------------------------------------------------------------------------

func TestConformance_XDeathStructure(t *testing.T) {
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

	require.NoError(t, ch.PublishWithContext(t.Context(), ax, "toA", false, false, amqp.Publishing{Body: []byte("hop")}))

	// 4 reject hops: A→B→A→B→A. Deaths recorded: A,B,A,B. End in A.
	da1 := consumeOne(t, ch, aQ, 8*time.Second)
	require.NoError(t, da1.Reject(false)) // death A (#1, first)
	db1 := consumeOne(t, ch, bQ, 8*time.Second)
	require.NoError(t, db1.Reject(false)) // death B (#2)
	da2 := consumeOne(t, ch, aQ, 8*time.Second)
	require.NoError(t, da2.Reject(false)) // death A (#3, aggregates)
	db2 := consumeOne(t, ch, bQ, 8*time.Second)
	require.NoError(t, db2.Reject(false)) // death B (#4, aggregates; most-recent)

	d := consumeOne(t, ch, aQ, 8*time.Second)
	require.NoError(t, d.Ack(false))
	deaths := xDeathArray(t, d.Headers)
	lockLog(t, "aggregated x-death", deaths)
	require.Len(t, deaths, 2, "aggregation by (queue,reason) must collapse 4 hops into 2 entries")

	// Most-recent-first ordering: head is the last death (queue B).
	rHead, qHead := assertXDeathEntryTypes(t, deaths[0], false)
	assert.Equal(t, reasonRejected, rHead)
	assert.Equal(t, bQ, qHead, "x-death[0] must be the most-recent death (queue B)")

	// Aggregation: (A,rejected)=2 and (B,rejected)=2.
	counts := map[string]int64{}
	for _, e := range deaths {
		q, _ := e["queue"].(string)
		c, ok := e["count"].(int64)
		require.Truef(t, ok, "count must be int64, got %T", e["count"])
		counts[q] = c
	}
	assert.Equal(t, int64(2), counts[aQ], "(A,rejected) count aggregated to 2")
	assert.Equal(t, int64(2), counts[bQ], "(B,rejected) count aggregated to 2")

	// x-first-death-* set ONCE to the ORIGINAL death (queue A, via ax exchange).
	assert.Equal(t, reasonRejected, d.Headers["x-first-death-reason"])
	assert.Equal(t, aQ, d.Headers["x-first-death-queue"], "x-first-death-queue must be the ORIGINAL queue A")
	assert.Equal(t, ax, d.Headers["x-first-death-exchange"], "x-first-death-exchange must be the ORIGINAL exchange ax")

	// x-last-death-* (RabbitMQ 4.x): most-recent death (queue B). StrangeQ omits.
	assertLastDeath(t, d.Headers, reasonRejected, bQ)
}

// ----------------------------------------------------------------------------
// Case 8 — durable x-death round-trip: a persistent dead-lettered message
// retains x-death across a broker restart (embedded StrangeQ reopens the SAME
// storage path; RabbitMQ's durable queue survives in the live broker).
// ----------------------------------------------------------------------------

func TestConformance_DurableXDeathRoundTrip(t *testing.T) {
	b := newConfBroker(t)
	defer b.cleanup()

	conn, ch := b.dial(t)

	src, _, dlq, _ := deadLetterWiring(t, ch, nil)
	require.NoError(t, ch.PublishWithContext(t.Context(), "", src, false, false,
		amqp.Publishing{Body: []byte("durable"), DeliveryMode: amqp.Persistent, Expiration: "600000"}))
	del := consumeOne(t, ch, src, 8*time.Second)
	require.NoError(t, del.Reject(false)) // → dlq, persistent

	// Let the persistent dead-letter flush to storage before the restart.
	time.Sleep(1500 * time.Millisecond)
	_ = conn.Close()

	b.restart()

	_, ch2 := b.dial(t)
	t.Cleanup(func() { _, _ = ch2.QueueDelete(dlq, false, false, false) })

	d := consumeOne(t, ch2, dlq, 10*time.Second)
	require.NoError(t, d.Ack(false))
	assert.Equal(t, []byte("durable"), d.Body)
	deaths := xDeathArray(t, d.Headers)
	require.GreaterOrEqual(t, len(deaths), 1, "x-death must survive the restart")

	wantOrigExp := expectByTarget(t, "reject-original-expiration-present", true, false)
	reason, queue := assertXDeathEntryTypes(t, deaths[0], wantOrigExp)
	assert.Equal(t, reasonRejected, reason)
	assert.Equal(t, src, queue)

	wantExpiration := expectByTarget(t, "reject-delivered-expiration", "", "600000")
	assert.Equal(t, wantExpiration, d.Expiration, "target=%s: reject Expiration handling must survive the restart", confTarget())
	lockLog(t, "durable-roundtrip x-death[0]", deaths[0])
}
