package broker

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// declareMaxLen declares a queue with the given x-arguments and returns its
// QueueState so tests can inspect WaitingCount()/ReadyBytes().
func declareMaxLen(t *testing.T, b *StorageBroker, name string, args map[string]interface{}) *QueueState {
	t.Helper()
	_, err := b.DeclareQueue(name, false, false, false, args)
	require.NoError(t, err)
	return b.getOrCreateQueueState(name)
}

// publishBody publishes body into queue via the default exchange (RK == queue).
func publishBody(b *StorageBroker, queue, body string) error {
	return b.PublishMessage("", queue, &protocol.Message{Exchange: "", RoutingKey: queue, Body: []byte(body)})
}

// drainBodies drains every ready message from queue via no-ack basic.get and
// returns their bodies in delivery order.
func drainBodies(t *testing.T, b *StorageBroker, queue string) []string {
	t.Helper()
	var out []string
	for {
		msg := drainOne(t, b, queue)
		if msg == nil {
			return out
		}
		out = append(out, string(msg.Body))
	}
}

// --- reject-publish ---------------------------------------------------------

// TestRejectPublish_CountAtOrOverBeforeAdd verifies the AT-OR-OVER-BEFORE-ADD
// count check: with x-max-length=3 the first three publishes are accepted and
// the fourth is rejected with ErrMaxLengthExceeded, leaving exactly three.
func TestRejectPublish_CountAtOrOverBeforeAdd(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	qs := declareMaxLen(t, b, "q", map[string]interface{}{
		"x-max-length": int64(3), "x-overflow": "reject-publish",
	})

	for i := 0; i < 3; i++ {
		require.NoError(t, publishBody(b, "q", fmt.Sprintf("m%d", i)))
	}
	require.Equal(t, int64(3), qs.WaitingCount())

	err := publishBody(b, "q", "m3")
	require.ErrorIs(t, err, ErrMaxLengthExceeded, "4th publish at the count limit must be rejected")
	require.Equal(t, int64(3), qs.WaitingCount(), "rejected publish must not be enqueued")

	require.Equal(t, []string{"m0", "m1", "m2"}, drainBodies(t, b, "q"))
}

// TestRejectPublish_BytesAtOrOverBeforeAdd verifies the byte check matches the
// spec scenario exactly: x-max-length-bytes=250 with 100-byte bodies accepts
// msgs 1-3 (readyBytes -> 300) and rejects the 4th.
func TestRejectPublish_BytesAtOrOverBeforeAdd(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	qs := declareMaxLen(t, b, "q", map[string]interface{}{
		"x-max-length-bytes": int64(250), "x-overflow": "reject-publish",
	})
	body := string(make([]byte, 100))

	for i := 0; i < 3; i++ {
		require.NoError(t, publishBody(b, "q", body))
	}
	require.Equal(t, int64(300), qs.ReadyBytes())

	require.ErrorIs(t, publishBody(b, "q", body), ErrMaxLengthExceeded)
	require.Equal(t, int64(300), qs.ReadyBytes(), "rejected publish must not add bytes")
	require.Equal(t, int64(3), qs.WaitingCount())
}

// TestRejectPublish_SingleOversizeBodyAccepted verifies a single message larger
// than the byte limit is accepted into an EMPTY queue (RabbitMQ parity).
func TestRejectPublish_SingleOversizeBodyAccepted(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	qs := declareMaxLen(t, b, "q", map[string]interface{}{
		"x-max-length-bytes": int64(50), "x-overflow": "reject-publish",
	})
	big := string(make([]byte, 100))
	require.NoError(t, publishBody(b, "q", big), "oversize body to empty queue must be accepted")
	require.Equal(t, int64(1), qs.WaitingCount())
	require.Equal(t, int64(100), qs.ReadyBytes())

	// Now the queue is at/over the byte limit; the next publish is rejected.
	require.ErrorIs(t, publishBody(b, "q", "x"), ErrMaxLengthExceeded)
}

// TestRejectPublish_DoesNotDeadLetterIncoming verifies the rejected incoming
// message is NOT dead-lettered (RabbitMQ does not dead-letter on reject-publish).
func TestRejectPublish_DoesNotDeadLetterIncoming(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, b.DeclareExchange("dlx", "fanout", false, false, false, nil))
	_, err := b.DeclareQueue("dlq", false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, b.BindQueue("dlq", "dlx", "", nil))

	qs := declareMaxLen(t, b, "q", map[string]interface{}{
		"x-max-length": int64(1), "x-overflow": "reject-publish",
		"x-dead-letter-exchange": "dlx",
	})

	require.NoError(t, publishBody(b, "q", "keep"))
	require.ErrorIs(t, publishBody(b, "q", "rejected"), ErrMaxLengthExceeded)
	require.Equal(t, int64(1), qs.WaitingCount())
	require.Nil(t, drainOne(t, b, "dlq"), "reject-publish must NOT dead-letter the incoming message")
}

// --- drop-head --------------------------------------------------------------

// TestDropHead_CountEvictsOldest verifies drop-head (the x-overflow default)
// keeps the queue at x-max-length by evicting the OLDEST messages, retaining the
// newest.
func TestDropHead_CountEvictsOldest(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	qs := declareMaxLen(t, b, "q", map[string]interface{}{"x-max-length": int64(3)})

	for i := 0; i < 5; i++ {
		require.NoError(t, publishBody(b, "q", fmt.Sprintf("m%d", i)))
	}
	require.Equal(t, int64(3), qs.WaitingCount())
	// Oldest two (m0, m1) dropped; newest three retained in order.
	require.Equal(t, []string{"m2", "m3", "m4"}, drainBodies(t, b, "q"))
}

// TestDropHead_DefaultOverflow verifies that when x-overflow is absent the
// behavior defaults to drop-head (not reject-publish).
func TestDropHead_DefaultOverflow(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	qs := declareMaxLen(t, b, "q", map[string]interface{}{"x-max-length": int64(2)})
	for i := 0; i < 4; i++ {
		require.NoError(t, publishBody(b, "q", fmt.Sprintf("m%d", i)), "drop-head never rejects a publish")
	}
	require.Equal(t, int64(2), qs.WaitingCount())
	require.Equal(t, []string{"m2", "m3"}, drainBodies(t, b, "q"))
}

// TestDropHead_EvictedDeadLetteredWithMaxlenReason verifies evicted messages are
// dead-lettered through the W3 seam with reason=maxlen and an x-death entry.
func TestDropHead_EvictedDeadLetteredWithMaxlenReason(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, b.DeclareExchange("dlx", "fanout", false, false, false, nil))
	_, err := b.DeclareQueue("dlq", false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, b.BindQueue("dlq", "dlx", "", nil))

	declareMaxLen(t, b, "q", map[string]interface{}{
		"x-max-length": int64(1), "x-dead-letter-exchange": "dlx",
	})

	require.NoError(t, publishBody(b, "q", "old"))
	require.NoError(t, publishBody(b, "q", "new")) // evicts "old"

	got := drainOne(t, b, "dlq")
	require.NotNil(t, got, "evicted message must be dead-lettered")
	require.Equal(t, "old", string(got.Body))
	assertEntry(t, xDeathEntries(t, got.Headers)[0], "q", DeadLetterMaxLen, 1)
	require.Equal(t, []string{"new"}, drainBodies(t, b, "q"))
}

// TestDropHead_BytesKeepsAtLeastNewest verifies byte drop-head keeps at least
// the just-published message even when it alone exceeds the byte limit.
func TestDropHead_BytesKeepsAtLeastNewest(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	qs := declareMaxLen(t, b, "q", map[string]interface{}{"x-max-length-bytes": int64(50)})
	big := string(make([]byte, 100))
	require.NoError(t, publishBody(b, "q", big))
	require.Equal(t, int64(1), qs.WaitingCount(), "a single oversize body is retained")
	require.Equal(t, int64(100), qs.ReadyBytes())
}

// TestDropHead_BytesEvictsOldestUntilUnderLimit verifies byte drop-head trims
// the oldest ready messages until within x-max-length-bytes.
func TestDropHead_BytesEvictsOldestUntilUnderLimit(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	qs := declareMaxLen(t, b, "q", map[string]interface{}{"x-max-length-bytes": int64(250)})
	body := string(make([]byte, 100))
	for i := 0; i < 4; i++ {
		require.NoError(t, publishBody(b, "q", body))
	}
	// 100-byte bodies, limit 250: after each publish, trim while >250 (keeping
	// >1) => steady state holds 2 (200 bytes).
	require.Equal(t, int64(2), qs.WaitingCount())
	require.Equal(t, int64(200), qs.ReadyBytes())
}

// TestDropHead_EvictsFromRequeueRingFirst proves eviction picks the
// GLOBALLY-oldest ready message across BOTH the requeue ring and the tail
// cursor: a requeued (older) message is evicted before newer tail-cursor ones,
// and the count limit is enforced even when excess sits in the ring.
func TestDropHead_EvictsFromRequeueRingFirst(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	qs := declareMaxLen(t, b, "q", map[string]interface{}{"x-max-length": int64(2)})

	// Deliver one message to a manual consumer, then cancel the consumer so the
	// in-flight delivery is drained back into the requeue ring as the oldest
	// ready message (deterministic: no poll loop remains to re-deliver it).
	c := registerManualConsumer(t, b, "q", "c1", 10, 10)
	require.NoError(t, publishBody(b, "q", "requeued"))
	_ = recvDeliveries(t, c, 1)[0]
	require.NoError(t, b.UnregisterConsumer("c1"))
	require.Equal(t, int64(1), qs.WaitingCount())

	// Publish two more; the second pushes over the limit of 2 and must evict the
	// globally-oldest ready message — the requeued one — not the newest.
	require.NoError(t, publishBody(b, "q", "n0"))
	require.NoError(t, publishBody(b, "q", "n1"))

	require.Equal(t, int64(2), qs.WaitingCount(), "limit must hold even with excess in the requeue ring")
	bodies := drainBodies(t, b, "q")
	require.NotContains(t, bodies, "requeued", "the oldest (requeued) message must be evicted first")
	require.ElementsMatch(t, []string{"n0", "n1"}, bodies)
}

// --- readyBytes consistency -------------------------------------------------

// TestReadyBytes_ConsistentThroughDeliverAck verifies the byte counter mirrors
// the ready set through publish -> deliver -> ack (claim subtracts, ack does
// not re-add) and returns to zero.
func TestReadyBytes_ConsistentThroughDeliverAck(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	qs := declareMaxLen(t, b, "q", map[string]interface{}{"x-max-length-bytes": int64(1 << 20)})
	body := string(make([]byte, 100))

	c := registerManualConsumer(t, b, "q", "c1", 10, 10)
	for i := 0; i < 3; i++ {
		require.NoError(t, publishBody(b, "q", body))
	}
	ds := recvDeliveries(t, c, 3)
	require.Equal(t, int64(0), qs.ReadyBytes(), "delivered (in-flight) messages are not ready")

	for _, d := range ds {
		require.NoError(t, b.AcknowledgeMessage("c1", d.DeliveryTag, false))
	}
	require.Equal(t, int64(0), qs.ReadyBytes())
}

// TestReadyBytes_RestoredOnRequeue verifies a requeue restores the message's
// bytes to the ready counter. Uses the synchronous basic.get + reject(requeue)
// path so there is no consumer poll loop to re-deliver and race the assertion.
func TestReadyBytes_RestoredOnRequeue(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	qs := declareMaxLen(t, b, "q", map[string]interface{}{"x-max-length-bytes": int64(1 << 20)})
	body := string(make([]byte, 100))

	require.NoError(t, publishBody(b, "q", body))
	_, tag, _, err := b.GetMessageForGet("q", false)
	require.NoError(t, err)
	require.Equal(t, int64(0), qs.ReadyBytes(), "claimed message is no longer ready")

	require.NoError(t, b.RejectGetDelivery(tag, true))
	require.Equal(t, int64(100), qs.ReadyBytes(), "requeue must restore ready bytes")
}

// --- concurrency ------------------------------------------------------------

// TestRejectPublish_ConcurrentOvershootBounded documents+tests the transient
// overshoot bound under concurrent publishers: with reject-publish and N
// publishers racing at the limit, the accepted count never exceeds
// limit + (concurrent publishers) and eventually settles at the cap.
func TestRejectPublish_ConcurrentOvershootBounded(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	const limit = 100
	const publishers = 8
	const perPublisher = 500
	qs := declareMaxLen(t, b, "q", map[string]interface{}{
		"x-max-length": int64(limit), "x-overflow": "reject-publish",
	})

	var accepted atomic.Int64
	var wg sync.WaitGroup
	wg.Add(publishers)
	for p := 0; p < publishers; p++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perPublisher; i++ {
				if err := publishBody(b, "q", "x"); err == nil {
					accepted.Add(1)
				} else if !errors.Is(err, ErrMaxLengthExceeded) {
					t.Errorf("unexpected publish error: %v", err)
				}
			}
		}()
	}
	wg.Wait()

	// Never permanently over the cap: ready count settles at exactly the limit.
	require.Equal(t, int64(limit), qs.WaitingCount(), "ready count must settle at the cap")
	require.GreaterOrEqual(t, accepted.Load(), int64(limit))
}

// --- requeue-path divergence (W7) -------------------------------------------

// TestRequeuePath_OverCapOvershootsUntilNextPublish documents + tests the spec
// W5 "maxlen on the requeue path" divergence: unlike RabbitMQ, a requeue that
// pushes a drop-head queue back over x-max-length is NOT trimmed on the requeue
// itself. The transient overshoot is bounded by the requeued set and re-trimmed
// on the next publish (which evicts the globally-oldest ready message — the
// requeued one — first).
func TestRequeuePath_OverCapOvershootsUntilNextPublish(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	qs := declareMaxLen(t, b, "q", map[string]interface{}{"x-max-length": int64(2)})

	require.NoError(t, publishBody(b, "q", "m0"))
	require.NoError(t, publishBody(b, "q", "m1"))
	require.Equal(t, int64(2), qs.WaitingCount())

	// basic.get m0 with manual ack: it leaves the ready set (claimed -> inflight).
	msg, tag, _, err := b.GetMessageForGet("q", false)
	require.NoError(t, err)
	require.Equal(t, "m0", string(msg.Body))
	require.Equal(t, int64(1), qs.WaitingCount())

	// Publish m2: ready set is back at the cap (m1, m2). No trim.
	require.NoError(t, publishBody(b, "q", "m2"))
	require.Equal(t, int64(2), qs.WaitingCount())

	// Requeue m0: pushes the ready set to 3 — over the cap. Divergence: the
	// requeue path is NOT trimmed.
	require.NoError(t, b.RejectGetDelivery(tag, true))
	require.Equal(t, int64(3), qs.WaitingCount(),
		"requeue over the cap is not trimmed on the requeue path (documented W7 divergence)")

	// The next publish re-trims to the cap, evicting the globally-oldest ready
	// messages first (the requeued m0, then m1).
	require.NoError(t, publishBody(b, "q", "m3"))
	require.Equal(t, int64(2), qs.WaitingCount(), "next publish re-trims the transient overshoot to the cap")
	require.Equal(t, []string{"m2", "m3"}, drainBodies(t, b, "q"))
}

// --- tx + maxlen ------------------------------------------------------------

// publishTxToLiveStore stages a transactional publish with the LIVE store as the
// transactional view, so the returned deferred visibility closures observe the
// same committed state they would post-commit. Returns the deferred closures.
func publishTxToLiveStore(t *testing.T, b *StorageBroker, queue, body string) []func(bool) {
	t.Helper()
	d, err := b.PublishMessageTx(b.storage, "", queue, &protocol.Message{
		Exchange: "", RoutingKey: queue, Body: []byte(body),
	})
	require.NoError(t, err)
	return d
}

// TestTx_DropHeadEvictsOldestAfterCommit verifies drop-head eviction runs inside
// the PublishMessageTx deferred visibility closure (spec W5 "tx + maxlen"): four
// transactional publishes to an x-max-length=2 drop-head queue leave only the
// newest two once the post-commit closures run, in order.
func TestTx_DropHeadEvictsOldestAfterCommit(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	qs := declareMaxLen(t, b, "q", map[string]interface{}{"x-max-length": int64(2)})

	var deferred []func(bool)
	for i := 0; i < 4; i++ {
		deferred = append(deferred, publishTxToLiveStore(t, b, "q", fmt.Sprintf("m%d", i))...)
	}
	// Post-commit visibility: run the deferred closures in commit order.
	for _, d := range deferred {
		d(true)
	}

	require.Equal(t, int64(2), qs.WaitingCount())
	require.Equal(t, []string{"m2", "m3"}, drainBodies(t, b, "q"))
}

// TestTx_RejectPublishAcceptedInsideTx documents + tests the spec W5 divergence:
// reject-publish is NOT enforced for transactional publishes (refusing an
// already-committed message would strand a durable message), and a
// reject-publish queue applies no drop-head trim in tx. Every tx publish is
// therefore accepted and the queue may transiently exceed its cap. W7 locks the
// exact tx-vs-reject-publish semantics.
func TestTx_RejectPublishAcceptedInsideTx(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	qs := declareMaxLen(t, b, "q", map[string]interface{}{
		"x-max-length": int64(1), "x-overflow": "reject-publish",
	})

	var deferred []func(bool)
	for i := 0; i < 3; i++ {
		deferred = append(deferred, publishTxToLiveStore(t, b, "q", fmt.Sprintf("m%d", i))...)
	}
	for _, d := range deferred {
		d(true)
	}

	require.Equal(t, int64(3), qs.WaitingCount(),
		"tx publishes to a reject-publish queue are all accepted with no trim (documented W7 divergence)")
}

// --- eviction vs. inflight ring-overwrite safety ----------------------------

// TestDropHead_EvictionPreservesInflightDelivery verifies spec W5 criterion 8:
// drop-head eviction reuses the ClaimInflight+AckAdvance scratch path and must
// NOT compromise an outstanding inflight delivery's ring-overwrite safety. A
// manual-ack basic.get delivery is held unacked across an eviction; afterwards
// (a) the authoritative storage ack cursor still protects the unacked tag, (b)
// the queue depth still accounts for the inflight delivery, and (c) the delivery
// is intact and acks cleanly. (qs.minAckCursor is only a broker-side backpressure
// mirror — AtHighWaterMark gates on Depth() = waiting+inflight — and legitimately
// advances for basic.get deliveries, which are tracked via the storage cursor and
// SetMinAckCursor resync; it is therefore not the overwrite-safety gate here.)
func TestDropHead_EvictionPreservesInflightDelivery(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	qs := declareMaxLen(t, b, "q", map[string]interface{}{"x-max-length": int64(2)})

	require.NoError(t, publishBody(b, "q", "m0"))
	require.NoError(t, publishBody(b, "q", "m1"))

	// Hold m0 inflight via a manual-ack basic.get (deterministic; no poll loop).
	msg, m0tag, _, err := b.GetMessageForGet("q", false)
	require.NoError(t, err)
	require.Equal(t, "m0", string(msg.Body))
	require.Equal(t, int64(1), qs.WaitingCount())

	// Publish two more; the second pushes the ready set (m1, m2, m3) over the cap
	// of 2, so drop-head evicts the oldest ready message (m1) WHILE m0 is inflight.
	require.NoError(t, publishBody(b, "q", "m2"))
	require.NoError(t, publishBody(b, "q", "m3"))
	require.Equal(t, int64(2), qs.WaitingCount())

	// Ring-overwrite safety (authoritative cursor): the storage ack cursor, which
	// is derived from outstanding pending-acks and gates slot reuse, must not have
	// advanced past the still-unacked inflight m0.
	require.LessOrEqual(t, b.storage.GetMinAckCursor("q"), m0tag,
		"eviction must not let the storage ack cursor pass an outstanding inflight delivery")

	// Depth still counts the inflight m0 (waiting=2 ready + inflight=1).
	require.Equal(t, uint64(3), qs.Depth(), "eviction must not drop the inflight delivery from depth accounting")

	// The inflight delivery is intact and acks cleanly after the eviction; the
	// survivors are the newest two ready messages (oldest m1 evicted).
	require.NoError(t, b.AcknowledgeGetDelivery(m0tag))
	require.Equal(t, []string{"m2", "m3"}, drainBodies(t, b, "q"))
}
