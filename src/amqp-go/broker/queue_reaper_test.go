package broker

import (
	"sync"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// publishTracked publishes a message to a queue via the default exchange and
// returns the assigned delivery tag (PublishMessage stamps it onto the message).
func publishTracked(t *testing.T, b *StorageBroker, queue, body, expiration string) uint64 {
	t.Helper()
	m := &protocol.Message{RoutingKey: queue, Body: []byte(body), Expiration: expiration}
	require.NoError(t, b.PublishMessage("", queue, m))
	return m.DeliveryTag
}

// TestReaper_MidQueueExpiry_ConsumerLess exercises the "internally better than
// RabbitMQ" mid-queue expiry: on a consumer-less x-message-ttl queue the reaper
// expires a short-TTL message that sits BEHIND a longer-lived head message —
// something RabbitMQ (head-only expiry without a consumer) cannot do. The head
// and tail (long TTL) survive; only the middle (short per-message TTL) is reaped.
func TestReaper_MidQueueExpiry_ConsumerLess(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	declareTTLQueue(t, b, "q", map[string]interface{}{"x-message-ttl": 60000})

	headTag := publishTracked(t, b, "q", "head", "60000") // effective 60s
	midTag := publishTracked(t, b, "q", "mid", "40")      // effective 40ms (min)
	tailTag := publishTracked(t, b, "q", "tail", "60000") // effective 60s

	qs := b.getOrCreateQueueState("q")
	require.Equal(t, uint64(3), qs.Depth())

	// The consumer-less reaper expires only the middle message.
	require.Eventually(t, func() bool {
		_, err := b.storage.GetMessage("q", midTag)
		return err != nil
	}, 2*time.Second, 5*time.Millisecond, "reaper must expire the mid-queue short-TTL message")

	// The longer-lived head and tail are untouched, and depth dropped by exactly
	// one (the reaped middle).
	headMsg, err := b.storage.GetMessage("q", headTag)
	require.NoError(t, err)
	require.Equal(t, []byte("head"), headMsg.Body)
	tailMsg, err := b.storage.GetMessage("q", tailTag)
	require.NoError(t, err)
	require.Equal(t, []byte("tail"), tailMsg.Body)
	require.Equal(t, uint64(2), qs.Depth(), "only the mid-queue message may be reaped")
}

// TestReaper_DeadLettersExpired_ConsumerLess verifies the consumer-less reaper
// dead-letters (reason=expired) an expired message through the W3 seam and
// removes it from the source, decrementing depth exactly once. A single reaper
// goroutine owns the sweep, so the dead-letter is exactly-once here.
func TestReaper_DeadLettersExpired_ConsumerLess(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	setupDLX(t, b, dlxOpts{
		exchange: "dlx", exchangeKind: "direct", targets: []string{"dlq"}, bindKey: "src",
		sourceArgs: map[string]interface{}{"x-dead-letter-exchange": "dlx", "x-message-ttl": 40},
	})

	publishToSource(t, b, "src", "reap-me")

	dlq := b.getOrCreateQueueState("dlq")
	require.Eventually(t, func() bool { return dlq.Depth() == 1 }, 2*time.Second, 5*time.Millisecond,
		"reaper must dead-letter the expired message to the DLX target")

	src := b.getOrCreateQueueState("src")
	require.Eventually(t, func() bool { return src.Depth() == 0 }, time.Second, 5*time.Millisecond,
		"source depth must drop to zero after the reaper removes the expired message")

	got := drainOne(t, b, "dlq")
	require.NotNil(t, got)
	require.Equal(t, []byte("reap-me"), got.Body)
	assertEntry(t, xDeathEntries(t, got.Headers)[0], "src", DeadLetterExpired, 1)
	require.Equal(t, "", got.Expiration, "expired-death must strip Expiration")
}

// TestXExpires_AutoDeletesIdleQueue verifies the full x-expires wiring end to
// end: a queue declared with x-expires and left idle (no consumers, no
// basic.get, no redeclare) is auto-deleted by its reaper after the idle window.
func TestXExpires_AutoDeletesIdleQueue(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	_, err := b.DeclareQueue("idle", false, false, false, map[string]interface{}{"x-expires": 40})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, err := b.storage.GetQueue("idle")
		return err != nil
	}, 2*time.Second, 5*time.Millisecond, "an idle x-expires queue must be auto-deleted")
}

// TestXExpires_ActivityResetsIdleClock verifies the three "queue use" signals —
// basic.get, consumer register, and (re)declare — each reset the x-expires idle
// clock (MarkActivity), so an active queue is never auto-deleted. It asserts the
// reset directly against a manual clock to stay deterministic (independent of
// reaper timing).
func TestXExpires_ActivityResetsIdleClock(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	clk := &ttlTestClock{}
	clk.set(10_000)
	b.SetTTLClock(clk.now)

	qs := declareTTLQueue(t, b, "act", map[string]interface{}{"x-expires": 100000})
	require.Equal(t, int64(10_000), qs.LastActivityMilli(), "declare seeds the idle clock")

	// basic.get resets the clock.
	clk.set(20_000)
	_, _, _, err := b.GetMessageForGet("act", true)
	require.NoError(t, err)
	require.Equal(t, int64(20_000), qs.LastActivityMilli(), "basic.get must reset the idle clock")

	// Consumer register resets the clock.
	clk.set(30_000)
	registerManualConsumer(t, b, "act", "c1", 1, 1)
	require.Equal(t, int64(30_000), qs.LastActivityMilli(), "consumer register must reset the idle clock")

	// Redeclare resets the clock.
	clk.set(40_000)
	_, err = b.DeclareQueue("act", false, false, false, map[string]interface{}{"x-expires": 100000})
	require.NoError(t, err)
	require.Equal(t, int64(40_000), qs.LastActivityMilli(), "redeclare must reset the idle clock")
}

// TestReaperClaimLinearization_NoDoubleDecrement is the correctness heart of
// SQ-9: it proves that for undelivered (waiting) messages, at most one of
// {reaper drop, basic.get head-check} decrements queue depth per tag. It floods
// a consumer-less TTL queue with instantly-expired messages, then hammers the
// reaper sweep AND the basic.get head-check concurrently over the same tags. The
// ring Delete-returns-bool single-winner (coordinated with the tail cursor) must
// leave depth at exactly zero — never negative (double decrement) and never
// stuck above zero (a lost tag). Run under -race, it also exercises the lock-free
// tail/ring coordination for data races.
func TestReaperClaimLinearization_NoDoubleDecrement(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	clk := &ttlTestClock{}
	clk.set(5_000_000)
	b.SetTTLClock(clk.now)

	qs := declareTTLQueue(t, b, "q", map[string]interface{}{"x-message-ttl": 10})

	const N = 400
	for i := 0; i < N; i++ {
		require.NoError(t, b.PublishMessage("", "q", &protocol.Message{RoutingKey: "q", Body: []byte("m")}))
	}
	require.Equal(t, uint64(N), qs.Depth())

	// Every message is now past its 10ms deadline.
	clk.set(5_000_000 + 11)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Reaper sweepers (in addition to the auto-started reaper goroutine).
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
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
	// basic.get head-check drainers (no registered consumer, so the reaper stays
	// active and both paths race the same tags).
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_, _, _, _ = b.GetMessageForGet("q", true)
				}
			}
		}()
	}

	require.Eventually(t, func() bool { return qs.Depth() == 0 }, 5*time.Second, 2*time.Millisecond,
		"every expired tag must be removed exactly once")
	close(stop)
	wg.Wait()

	require.Equal(t, uint64(0), qs.Depth(), "final depth must be exactly zero")
	require.GreaterOrEqual(t, qs.WaitingCount(), int64(0), "waiting must never go negative (no double decrement)")
	cnt, err := b.storage.GetQueueMessageCount("q")
	require.NoError(t, err)
	require.Equal(t, 0, cnt, "storage must hold no leftover messages")
}

// TestPublishMessageTx_StampsDurableAnchorAtCommit pins SQ-9's transactional
// rule: the TTL anchor is stamped inside PublishMessageTx — which the tx manager
// runs at COMMIT time (it buffers publish operations and executes them within
// ExecuteAtomic) — and BEFORE the durable stage, so the anchor both counts from
// commit AND is part of the durable write (it survives restart; see
// TestRecovery_DurableTxTTL_ExpiredWhileDown). The message object is created "at
// client-publish time" with no anchor; only the commit-time PublishMessageTx run
// stamps it, with the commit-instant clock.
func TestPublishMessageTx_StampsDurableAnchorAtCommit(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	clk := &ttlTestClock{}
	clk.set(3_000_000) // "client publish" instant: message buffered, NOT stamped
	b.SetTTLClock(clk.now)

	declareTTLQueue(t, b, "q", map[string]interface{}{"x-message-ttl": 10000})

	msg := &protocol.Message{RoutingKey: "q", Body: []byte("tx")}

	// Time passes while the transaction is open; the manager runs PublishMessageTx
	// only at commit.
	const commitTime = int64(3_005_000)
	clk.set(commitTime)

	deferred, err := b.PublishMessageTx(b.storage, "", "q", msg)
	require.NoError(t, err)
	require.Len(t, deferred, 1)

	// Stamped at the commit instant (PublishMessageTx run), not the earlier
	// buffered-publish instant.
	require.Equal(t, commitTime, msg.EnqueueUnixMilli, "tx TTL anchor must be stamped at commit")

	for _, fn := range deferred {
		fn(true)
	}

	// And it is part of the durable store write (stamped BEFORE StoreMessage).
	stored, err := b.storage.GetMessage("q", msg.DeliveryTag)
	require.NoError(t, err)
	require.Equal(t, commitTime, stored.EnqueueUnixMilli,
		"the persisted copy must carry the commit-time anchor")
}
