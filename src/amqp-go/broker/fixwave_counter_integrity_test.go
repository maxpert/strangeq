package broker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// Wave 2 consolidated fix wave — counter-integrity at ready-set removal
// transitions. The unifying invariant these tests pin: every path that removes a
// message from the READY set (no-ack basic.get, TTL head-check on delivery/get,
// TTL reaper, drop-head eviction) must decrement `waiting` AND (when the queue
// enforces x-max-length-bytes) `readyBytes` EXACTLY ONCE per message, gated on
// winning the single ring delete so it reconciles with the SQ-9 reaper.

// --- C1: no-ack basic.get must settle the message -------------------------

// TestNoAckGet_RemovesMessageAndDecrementsCounters proves the C1 fix: a no-ack
// basic.get is an implicit ack, so it must remove the message from storage and
// decrement both the ready count (`waiting`) and `readyBytes`. Before the fix the
// settle block was gated `if !noAck` with no else, so a no-ack get popped the
// cursor but left the message in storage (leak) and both counters inflated
// (drift).
func TestNoAckGet_RemovesMessageAndDecrementsCounters(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	// x-max-length-bytes (large, no eviction) so readyBytes is tracked.
	qs := declareMaxLen(t, b, "q", map[string]interface{}{"x-max-length-bytes": int64(1 << 20)})
	body := string(make([]byte, 100))
	require.NoError(t, publishBody(b, "q", body))
	require.Equal(t, int64(1), qs.WaitingCount())
	require.Equal(t, int64(100), qs.ReadyBytes())

	// No-ack basic.get: implicit ack — the message must be settled here.
	msg, _, _, err := b.GetMessageForGet("q", true)
	require.NoError(t, err)
	require.NotNil(t, msg)
	require.Equal(t, body, string(msg.Body))

	require.Equal(t, int64(0), qs.WaitingCount(), "no-ack get must decrement the ready count")
	require.Equal(t, int64(0), qs.ReadyBytes(), "no-ack get must decrement readyBytes")
	require.Equal(t, uint32(0), b.GetQueueReadyCount("q"), "ready count must not drift")

	cnt, err := b.storage.GetQueueMessageCount("q")
	require.NoError(t, err)
	require.Equal(t, 0, cnt, "no-ack get must remove the message from storage (no leak)")
}

// TestNoAckGet_DurableAcknowledgesMessage proves the durability half of C1: a
// durable message consumed by a no-ack basic.get must be durably ACKNOWLEDGED so
// crash recovery does NOT replay (redeliver) it. Recovery replays the storage
// "recoverable" set (server.RecoveryManager reads GetRecoverableMessages); a
// no-ack basic.get is an implicit ack, so once it settles the message must drop
// out of that set. Before the fix the no-ack get never deleted or acknowledged
// the message, so it stayed recoverable forever — a durable message redelivered
// after restart (silent duplicate). The ack is registered asynchronously (WAL
// ack channel), so the assertion polls.
func TestNoAckGet_DurableAcknowledgesMessage(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	_, err := b.DeclareQueue("dq", true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, b.PublishMessage("", "dq", &protocol.Message{
		Exchange: "", RoutingKey: "dq", Body: []byte("consume-once"), DeliveryMode: 2,
	}))

	// The durable message is recoverable (would be replayed on restart) before it
	// is consumed.
	rec, err := b.storage.GetRecoverableMessages()
	require.NoError(t, err)
	require.Len(t, rec["dq"], 1, "durable message is recoverable before consumption")

	msg, _, _, err := b.GetMessageForGet("dq", true)
	require.NoError(t, err)
	require.NotNil(t, msg)
	require.Equal(t, []byte("consume-once"), msg.Body)

	// After the no-ack consume the message must no longer be in the recoverable
	// set — recovery will not replay it, so it is not redelivered after a restart.
	require.Eventually(t, func() bool {
		rec, err := b.storage.GetRecoverableMessages()
		return err == nil && len(rec["dq"]) == 0
	}, 2*time.Second, 5*time.Millisecond,
		"a no-ack-consumed durable message must be acknowledged so recovery does not redeliver it")
}

// --- I1: readyBytes must be decremented on every TTL-expiry removal --------

// TestReadyBytes_ReaperTTLExpiry_NoDrift proves the I1 fix at the reaper site
// (reapTTLSweep): a consumer-less queue with BOTH x-max-length-bytes AND
// x-message-ttl must return readyBytes to zero once the reaper expires its
// messages — otherwise the byte counter drifts up permanently and a
// reject-publish queue spuriously rejects a later LIVE publish.
func TestReadyBytes_ReaperTTLExpiry_NoDrift(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	qs := declareMaxLen(t, b, "q", map[string]interface{}{
		"x-max-length-bytes": int64(300),
		"x-message-ttl":      40, // short: the consumer-less reaper expires these
		"x-overflow":         "reject-publish",
	})
	body := string(make([]byte, 100))
	for i := 0; i < 3; i++ {
		require.NoError(t, publishBody(b, "q", body))
	}
	require.Equal(t, int64(300), qs.ReadyBytes())
	require.Equal(t, int64(3), qs.WaitingCount())

	// The reaper expires all three; both counters must return to zero.
	require.Eventually(t, func() bool {
		return qs.WaitingCount() == 0 && qs.ReadyBytes() == 0
	}, 2*time.Second, 5*time.Millisecond,
		"reaper TTL expiry must decrement readyBytes (no byte drift)")

	// With no byte drift the queue is empty, so a fresh live publish is accepted
	// — not spuriously rejected by a stale at-limit readyBytes.
	require.NoError(t, publishBody(b, "q", body),
		"a live publish after expiry must not be spuriously reject-published")
}

// TestReadyBytes_GetHeadCheckExpiry_NoDrift proves the I1 fix at the basic.get
// head-check site (dropExpiredOnGet). Uses a per-message expiration and NO
// queue-level x-message-ttl, so no reaper is started and the basic.get head-check
// is the SOLE expiry path — deterministic, no reaper race. All three messages
// are dropped inside a single GetMessageForGet cursor walk; readyBytes must
// return to zero.
func TestReadyBytes_GetHeadCheckExpiry_NoDrift(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	clk := &ttlTestClock{}
	clk.set(1_000_000)
	b.SetTTLClock(clk.now)

	// No x-message-ttl -> no reaper; per-message Expiration drives expiry.
	qs := declareMaxLen(t, b, "q", map[string]interface{}{
		"x-max-length-bytes": int64(300),
		"x-overflow":         "reject-publish",
	})
	body := string(make([]byte, 100))
	for i := 0; i < 3; i++ {
		require.NoError(t, b.PublishMessage("", "q", &protocol.Message{
			Exchange: "", RoutingKey: "q", Body: []byte(body), Expiration: "40",
		}))
	}
	require.Equal(t, int64(300), qs.ReadyBytes())

	// Advance past the per-message deadline, then a single basic.get walks the
	// cursor dropping all expired messages via the head-check.
	clk.set(1_000_000 + 100)
	msg, _, _, err := b.GetMessageForGet("q", true)
	require.NoError(t, err)
	require.Nil(t, msg, "all messages expired: basic.get returns nothing")

	require.Equal(t, int64(0), qs.WaitingCount(), "expired-on-get must decrement the ready count")
	require.Equal(t, int64(0), qs.ReadyBytes(), "expired-on-get must decrement readyBytes (no byte drift)")
	require.NoError(t, publishBody(b, "q", body),
		"a live publish after expiry must not be spuriously reject-published")
}

// TestReadyBytes_DeliveryHeadCheckExpiry_NoDrift proves the I1 fix at the
// consumer delivery-time head-check site (dropExpiredOnDelivery). Per-message
// expiration + NO x-message-ttl means no reaper; the consumer's delivery-time
// head-check is the sole expiry path. The expired message is never delivered and
// readyBytes must return to zero.
func TestReadyBytes_DeliveryHeadCheckExpiry_NoDrift(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	clk := &ttlTestClock{}
	clk.set(1_000_000)
	b.SetTTLClock(clk.now)

	qs := declareMaxLen(t, b, "q", map[string]interface{}{
		"x-max-length-bytes": int64(1 << 20),
	})
	body := string(make([]byte, 100))
	require.NoError(t, b.PublishMessage("", "q", &protocol.Message{
		Exchange: "", RoutingKey: "q", Body: []byte(body), Expiration: "40",
	}))
	require.Equal(t, int64(100), qs.ReadyBytes())

	// Expire the message, THEN register a consumer whose delivery-time head-check
	// drops it instead of delivering.
	clk.set(1_000_000 + 100)
	c := registerManualConsumer(t, b, "q", "c1", 10, 4)

	require.Eventually(t, func() bool {
		return qs.WaitingCount() == 0 && qs.ReadyBytes() == 0
	}, 2*time.Second, 5*time.Millisecond,
		"delivery-time expiry must decrement readyBytes (no byte drift)")

	select {
	case d := <-c.Messages:
		t.Fatalf("expired message must not be delivered, got %q", d.Message.Body)
	case <-time.After(50 * time.Millisecond):
	}
}

// --- I2: drop-head eviction vs reaper must not double-decrement ------------

// TestDropHeadEvict_ReaperRace_NoDoubleDecrement is the I2 -race repro. On a
// consumer-less queue with BOTH x-message-ttl and x-max-length (drop-head), the
// SQ-9 reaper and a publisher's dropHeadTrim can select the SAME oldest expired
// tag. Before the fix, dropHeadEvictOne decremented depth UNCONDITIONALLY
// (ClaimInflight + unconditional DeleteMessage) while the reaper decremented only
// on winning deleteIfPresent — so both could decrement `waiting` for one message,
// driving it negative (then clamped). The fix gates dropHeadEvictOne's depth
// accounting on winning deleteIfPresent too, so `waiting` is decremented exactly
// once per message and can never be driven negative.
//
// x-max-length=1 keeps `waiting` pinned at 0/1, so a double decrement reliably
// crosses zero into the negative; a tight sampler records the minimum observed.
// The fix guarantees it is never negative. Extra reaper sweepers (mirroring
// TestReaperClaimLinearization) maximize reaper-vs-eviction collisions. Run under
// -race for the lock-free ring/cursor coordination.
func TestDropHeadEvict_ReaperRace_NoDoubleDecrement(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	qs := declareMaxLen(t, b, "q", map[string]interface{}{
		"x-max-length":  int64(1), // cap 1: waiting stays at 0/1, so a double dec goes negative
		"x-message-ttl": 1,        // ~immediate expiry: the reaper is always busy
	})

	const publishers = 8
	const perPublisher = 4000

	var minWaiting atomic.Int64
	minWaiting.Store(1 << 30)
	stop := make(chan struct{})

	var bg sync.WaitGroup

	// Extra reaper sweepers (on top of the auto-started reaper) to maximize
	// reaper-vs-drop-head-eviction collisions over the same oldest tags.
	for i := 0; i < 3; i++ {
		bg.Add(1)
		go func() {
			defer bg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					b.reapSweep("q", qs)
				}
			}
		}()
	}

	// Samplers: record the smallest raw `waiting` seen. The fix keeps it >= 0.
	for i := 0; i < 2; i++ {
		bg.Add(1)
		go func() {
			defer bg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				w := qs.WaitingCount()
				for {
					cur := minWaiting.Load()
					if w >= cur || minWaiting.CompareAndSwap(cur, w) {
						break
					}
				}
			}
		}()
	}

	var wg sync.WaitGroup
	wg.Add(publishers)
	for p := 0; p < publishers; p++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perPublisher; i++ {
				_ = publishBody(b, "q", "x")
			}
		}()
	}
	wg.Wait()
	close(stop)
	bg.Wait()

	require.GreaterOrEqual(t, minWaiting.Load(), int64(0),
		"waiting must never be driven negative (no double decrement between reaper and drop-head eviction)")

	// Once everything drains, `waiting` must settle at exactly the true ready
	// count in storage (no permanent under-count from a double decrement).
	require.Eventually(t, func() bool {
		cnt, err := b.storage.GetQueueMessageCount("q")
		return err == nil && int64(cnt) == qs.WaitingCount() && qs.InflightCount() == 0
	}, 3*time.Second, 5*time.Millisecond,
		"waiting must reconcile exactly with storage (no counter drift)")
	require.GreaterOrEqual(t, qs.WaitingCount(), int64(0))
}
