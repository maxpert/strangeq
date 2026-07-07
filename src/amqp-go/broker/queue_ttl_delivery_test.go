package broker

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// ttlTestClock is a manually-advanced Unix-milli clock for deterministic SQ-9
// TTL tests: it lets a test stamp a message at one instant and evaluate its
// deadline at a later instant with NO real sleeps and NO wall-clock flakiness.
type ttlTestClock struct{ ms atomic.Int64 }

func (c *ttlTestClock) now() int64      { return c.ms.Load() }
func (c *ttlTestClock) set(ms int64)    { c.ms.Store(ms) }
func (c *ttlTestClock) advance(d int64) { c.ms.Add(d) }

// declareTTLQueue declares queue `name` with the given x-arguments and returns
// its runtime QueueState so tests can assert Depth() directly.
func declareTTLQueue(t *testing.T, b *StorageBroker, name string, args map[string]interface{}) *QueueState {
	t.Helper()
	_, err := b.DeclareQueue(name, false, false, false, args)
	require.NoError(t, err)
	return b.getOrCreateQueueState(name)
}

// TestDeliverMessage_ExpiredHeadCheck_DeadLetters pins the consumer-path
// delivery-time head-check: a message whose effective TTL elapsed before a
// consumer could claim it is dead-lettered (reason=expired) through the W3 seam
// and NEVER delivered, and the source queue depth drops to zero exactly once.
func TestDeliverMessage_ExpiredHeadCheck_DeadLetters(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	clk := &ttlTestClock{}
	clk.set(1_000_000)
	b.SetTTLClock(clk.now)

	setupDLX(t, b, dlxOpts{
		exchange: "dlx", exchangeKind: "direct", targets: []string{"dlq"}, bindKey: "src",
		sourceArgs: map[string]interface{}{"x-dead-letter-exchange": "dlx", "x-message-ttl": 5000},
	})

	// Publish stamps EnqueueUnixMilli = 1_000_000; deadline = 1_005_000.
	publishToSource(t, b, "src", "hello")

	// Advance past the deadline BEFORE a consumer can claim, so the poll loop's
	// head-check sees an expired message.
	clk.set(1_005_001)

	c := registerManualConsumer(t, b, "src", "c1", 10, 10)

	// The expired message must NOT be delivered.
	select {
	case d := <-c.Messages:
		t.Fatalf("expired message was delivered instead of dead-lettered: tag=%d", d.DeliveryTag)
	case <-time.After(300 * time.Millisecond):
	}

	// It must land on the DLX target with an expired x-death entry.
	dlq := b.getOrCreateQueueState("dlq")
	require.Eventually(t, func() bool { return dlq.Depth() == 1 }, 2*time.Second, 5*time.Millisecond,
		"expired message must be dead-lettered to the DLX target")

	got := drainOne(t, b, "dlq")
	require.NotNil(t, got)
	require.Equal(t, []byte("hello"), got.Body)
	assertEntry(t, xDeathEntries(t, got.Headers)[0], "src", DeadLetterExpired, 1)
	require.Equal(t, "", got.Expiration, "expired-death must strip Expiration")

	// Source depth returns to zero (decremented exactly once by the head-check).
	src := b.getOrCreateQueueState("src")
	require.Eventually(t, func() bool { return src.Depth() == 0 }, time.Second, 5*time.Millisecond,
		"source depth must return to zero after the expiry drop")
}

// TestGetMessageForGet_ExpiredHeadCheck_DeadLetters pins the basic.get sibling:
// a basic.get over an expired message dead-letters it (reason=expired) and
// returns "queue empty" rather than handing the expired message to the getter.
// Unlike the consumer path, GetMessageForGet runs the drop synchronously in the
// caller goroutine, so no async wait is needed for the DLX assertion.
func TestGetMessageForGet_ExpiredHeadCheck_DeadLetters(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	clk := &ttlTestClock{}
	clk.set(2_000_000)
	b.SetTTLClock(clk.now)

	setupDLX(t, b, dlxOpts{
		exchange: "dlx", exchangeKind: "direct", targets: []string{"dlq"}, bindKey: "src",
		sourceArgs: map[string]interface{}{"x-dead-letter-exchange": "dlx", "x-message-ttl": 5000},
	})

	publishToSource(t, b, "src", "payload")
	clk.set(2_005_001)

	// basic.get must skip the expired message and report empty.
	msg := drainOne(t, b, "src")
	require.Nil(t, msg, "basic.get must not return an expired message")

	// Dead-lettered synchronously.
	got := drainOne(t, b, "dlq")
	require.NotNil(t, got, "basic.get over an expired message must dead-letter it")
	require.Equal(t, []byte("payload"), got.Body)
	assertEntry(t, xDeathEntries(t, got.Headers)[0], "src", DeadLetterExpired, 1)

	src := b.getOrCreateQueueState("src")
	require.Equal(t, uint64(0), src.Depth(), "source depth must be zero after the expiry drop")
}

// TestTTL_NoDeadLetterExchange_Drops verifies an expired message on a queue with
// x-message-ttl but NO dead-letter exchange is simply dropped (not delivered,
// not republished anywhere), and depth returns to zero.
func TestTTL_NoDeadLetterExchange_Drops(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	clk := &ttlTestClock{}
	clk.set(500_000)
	b.SetTTLClock(clk.now)

	qs := declareTTLQueue(t, b, "q", map[string]interface{}{"x-message-ttl": 1000})
	require.NoError(t, b.PublishMessage("", "q", &protocol.Message{RoutingKey: "q", Body: []byte("x")}))
	clk.set(501_001)

	require.Nil(t, drainOne(t, b, "q"), "expired message must be dropped")
	require.Equal(t, uint64(0), qs.Depth(), "depth must be zero after drop")
}

// TestTTL_NotExpired_Delivered guards against over-eager expiry: a message still
// within its TTL is delivered normally by basic.get.
func TestTTL_NotExpired_Delivered(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	clk := &ttlTestClock{}
	clk.set(700_000)
	b.SetTTLClock(clk.now)

	declareTTLQueue(t, b, "q", map[string]interface{}{"x-message-ttl": 60000})
	require.NoError(t, b.PublishMessage("", "q", &protocol.Message{RoutingKey: "q", Body: []byte("live")}))

	// Only 1s later — well within the 60s TTL.
	clk.advance(1000)
	msg := drainOne(t, b, "q")
	require.NotNil(t, msg, "message within its TTL must be delivered")
	require.Equal(t, []byte("live"), msg.Body)
}

// TestTTL_PerMessageExpiration_IsMinWithQueue pins RabbitMQ's effective-TTL rule
// end to end: a per-message Expiration shorter than the queue x-message-ttl wins,
// so the message expires at the per-message deadline.
func TestTTL_PerMessageExpiration_IsMinWithQueue(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	clk := &ttlTestClock{}
	clk.set(900_000)
	b.SetTTLClock(clk.now)

	declareTTLQueue(t, b, "q", map[string]interface{}{"x-message-ttl": 60000})
	require.NoError(t, b.PublishMessage("", "q", &protocol.Message{
		RoutingKey: "q", Body: []byte("short"), Expiration: "1000",
	}))

	// Past the per-message 1s deadline but far short of the queue's 60s TTL.
	clk.advance(1001)
	require.Nil(t, drainOne(t, b, "q"), "per-message expiration (min) must fire before the queue TTL")
	require.Equal(t, uint64(0), b.getOrCreateQueueState("q").Depth())
}

// TestTTL_ZeroTTL_DroppedNotDelivered documents and pins the ttl=0 divergence
// from RabbitMQ. RabbitMQ delivers a ttl=0 message IFF a consumer is ready at
// publish (direct handoff); it otherwise drops it. StrangeQ has no publish-time
// direct handoff — a ttl=0 message is stamped with deadline==enqueue and is
// therefore expired at the very first delivery/get head-check, so even a parked-
// ready consumer never receives it. This test locks that drop-not-deliver
// behavior (flagged for W7 to confirm against real RabbitMQ).
func TestTTL_ZeroTTL_DroppedNotDelivered(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	clk := &ttlTestClock{}
	clk.set(1_234_000)
	b.SetTTLClock(clk.now) // clock does NOT advance: deadline==enqueue==now.

	qs := declareTTLQueue(t, b, "z", nil)

	// A ready consumer is parked before the publish (the RabbitMQ direct-handoff
	// scenario).
	c := registerManualConsumer(t, b, "z", "c1", 10, 10)

	require.NoError(t, b.PublishMessage("", "z", &protocol.Message{
		RoutingKey: "z", Body: []byte("zero"), Expiration: "0",
	}))

	select {
	case d := <-c.Messages:
		t.Fatalf("ttl=0 message was delivered to a ready consumer (StrangeQ divergence expects a drop): tag=%d", d.DeliveryTag)
	case <-time.After(300 * time.Millisecond):
	}
	require.Eventually(t, func() bool { return qs.Depth() == 0 }, time.Second, 5*time.Millisecond,
		"ttl=0 message must be dropped, not delivered")
}

// TestTTL_MalformedExpiration_NotStampedAndDelivered verifies a message with a
// malformed/non-numeric per-message expiration on a queue with NO queue TTL is
// treated as having no TTL: it is NOT stamped with an anchor at publish (so the
// delivery head-check never even recomputes a deadline for it) and it is
// delivered normally regardless of how much wall-clock time passes. Pins the
// W7 "malformed expiration = no-TTL" divergence at the stamp boundary.
func TestTTL_MalformedExpiration_NotStampedAndDelivered(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	clk := &ttlTestClock{}
	clk.set(800_000)
	b.SetTTLClock(clk.now)

	declareTTLQueue(t, b, "q", nil) // no queue-level TTL
	m := &protocol.Message{RoutingKey: "q", Body: []byte("keep"), Expiration: "not-a-number"}
	require.NoError(t, b.PublishMessage("", "q", m))

	// The anchor must be unset (malformed expiration is not stamped).
	stored, err := b.storage.GetMessage("q", m.DeliveryTag)
	require.NoError(t, err)
	require.Equal(t, int64(0), stored.EnqueueUnixMilli, "malformed expiration must not stamp an anchor")

	// Even far in the "future", the message is delivered — it never expires.
	clk.advance(1 << 40)
	got := drainOne(t, b, "q")
	require.NotNil(t, got, "a malformed-expiration message must be delivered, never expired")
	require.Equal(t, []byte("keep"), got.Body)
}

// BenchmarkDeliver_NoTTL_ZeroCost proves the SQ-9 delivery-time TTL check is
// genuinely zero-cost on the no-TTL hot path: it installs a TTL clock that
// counts every read and drives the full deliverMessage path with messages that
// carry NO per-message expiration on a queue with NO TTL policy. Because the
// anchor (EnqueueUnixMilli) is never stamped, the EnqueueUnixMilli==0 gate
// short-circuits before any wall-clock read — the benchmark FAILS if the path
// performs even one TTL-clock read.
func BenchmarkDeliver_NoTTL_ZeroCost(b *testing.B) {
	broker, cleanup := createTestBroker(b)
	defer cleanup()

	var clockReads atomic.Int64
	broker.SetTTLClock(func() int64 {
		clockReads.Add(1)
		return time.Now().UnixMilli()
	})

	if _, err := broker.DeclareQueue("z", false, false, false, nil); err != nil {
		b.Fatal(err)
	}
	c := &protocol.Consumer{
		Tag: "c", Queue: "z", NoAck: true,
		Messages: make(chan *protocol.Delivery, 1),
	}
	if err := broker.RegisterConsumer("z", "c", c); err != nil {
		b.Fatal(err)
	}

	body := make([]byte, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := broker.PublishMessage("", "z", &protocol.Message{Body: body, RoutingKey: "z"}); err != nil {
			b.Fatal(err)
		}
		<-c.Messages
	}
	b.StopTimer()

	if n := clockReads.Load(); n != 0 {
		b.Fatalf("no-TTL publish+deliver path performed %d TTL-clock reads; want 0", n)
	}
}
