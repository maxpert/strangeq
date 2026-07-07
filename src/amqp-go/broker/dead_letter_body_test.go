package broker

import (
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// dlxTopology declares a dead-letter exchange, one or more target queues bound
// to it, and a source queue whose policy points at that DLX. It returns the
// resolved source policy so tests can call deadLetter directly.
type dlxOpts struct {
	exchange     string   // DLX name
	exchangeKind string   // "direct" or "fanout"
	targets      []string // target queue names bound to the DLX
	bindKey      string   // routing key used to bind direct targets
	sourceArgs   map[string]interface{}
}

func setupDLX(t *testing.T, b *StorageBroker, o dlxOpts) *QueuePolicy {
	t.Helper()
	require.NoError(t, b.DeclareExchange(o.exchange, o.exchangeKind, false, false, false, nil))
	for _, q := range o.targets {
		_, err := b.DeclareQueue(q, false, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, b.BindQueue(q, o.exchange, o.bindKey, nil))
	}
	_, err := b.DeclareQueue("src", false, false, false, o.sourceArgs)
	require.NoError(t, err)
	p := b.GetQueuePolicy("src")
	require.NotNil(t, p, "source queue must resolve a policy")
	return p
}

// drainOne pops a single message from a queue via basic.get (no-ack) and
// returns it, or nil when the queue is empty.
func drainOne(t *testing.T, b *StorageBroker, queue string) *protocol.Message {
	t.Helper()
	msg, _, _, err := b.GetMessageForGet(queue, true)
	require.NoError(t, err)
	return msg
}

// TestDeadLetter_RepublishesToConfiguredDLX verifies the happy path: a message
// dead-lettered from src lands in the queue bound to the configured DLX, with a
// correct x-death entry and x-first-death-* headers.
func TestDeadLetter_RepublishesToConfiguredDLX(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	p := setupDLX(t, b, dlxOpts{
		exchange: "dlx", exchangeKind: "direct", targets: []string{"dlq"}, bindKey: "orders.rk",
		sourceArgs: map[string]interface{}{"x-dead-letter-exchange": "dlx"},
	})

	msg := &protocol.Message{Exchange: "orders-ex", RoutingKey: "orders.rk", Body: []byte("payload")}
	require.NoError(t, b.deadLetter(p, "src", msg, DeadLetterRejected))

	got := drainOne(t, b, "dlq")
	require.NotNil(t, got, "message must be republished to the DLX target")
	require.Equal(t, []byte("payload"), got.Body)
	require.Equal(t, uint64(0), msg.DeliveryTag, "live message's DeliveryTag must be untouched")

	entries := xDeathEntries(t, got.Headers)
	require.Len(t, entries, 1)
	assertEntry(t, entries[0], "src", DeadLetterRejected, 1)
	require.Equal(t, "orders-ex", entries[0]["exchange"])
	rks, ok := entries[0]["routing-keys"].([]interface{})
	require.True(t, ok)
	require.Equal(t, []interface{}{"orders.rk"}, rks)
	require.Equal(t, "rejected", got.Headers["x-first-death-reason"])
	require.Equal(t, "src", got.Headers["x-first-death-queue"])
	require.Equal(t, "orders-ex", got.Headers["x-first-death-exchange"])
}

// TestDeadLetter_DoesNotMutateLiveMessage verifies the clone is deep: annotating
// x-death on the clone must not add x-death to the caller's live message headers.
func TestDeadLetter_DoesNotMutateLiveMessage(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	p := setupDLX(t, b, dlxOpts{
		exchange: "dlx", exchangeKind: "direct", targets: []string{"dlq"}, bindKey: "rk",
		sourceArgs: map[string]interface{}{"x-dead-letter-exchange": "dlx"},
	})

	live := &protocol.Message{Exchange: "ex", RoutingKey: "rk", Body: []byte("x"),
		Headers: map[string]interface{}{"user": "v"}}
	require.NoError(t, b.deadLetter(p, "src", live, DeadLetterRejected))

	if _, ok := live.Headers["x-death"]; ok {
		t.Error("deadLetter mutated the live message's headers (x-death leaked onto the caller's pointer)")
	}
	require.Equal(t, "v", live.Headers["user"], "live headers must be otherwise intact")
}

// TestDeadLetter_RoutingKeyRewrite verifies x-dead-letter-routing-key overrides
// the routing key used to route into the DLX, while x-death routing-keys still
// records the message's ORIGINAL routing key.
func TestDeadLetter_RoutingKeyRewrite(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	p := setupDLX(t, b, dlxOpts{
		exchange: "dlx", exchangeKind: "direct", targets: []string{"dlq"}, bindKey: "dead-key",
		sourceArgs: map[string]interface{}{
			"x-dead-letter-exchange":    "dlx",
			"x-dead-letter-routing-key": "dead-key",
		},
	})

	msg := &protocol.Message{Exchange: "orders-ex", RoutingKey: "orders.original", Body: []byte("x")}
	require.NoError(t, b.deadLetter(p, "src", msg, DeadLetterExpired))

	got := drainOne(t, b, "dlq")
	require.NotNil(t, got, "message must route via the configured dead-letter-routing-key")

	entries := xDeathEntries(t, got.Headers)
	require.Len(t, entries, 1)
	rks := entries[0]["routing-keys"].([]interface{})
	require.Equal(t, []interface{}{"orders.original"}, rks,
		"x-death routing-keys must record the message's original routing key, not the DLX rewrite")
}

// TestDeadLetter_AbsentDLXDrops verifies a policy referencing a non-existent DLX
// silently drops (returns nil, publishes nothing).
func TestDeadLetter_AbsentDLXDrops(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	_, err := b.DeclareQueue("dlq", false, false, false, nil)
	require.NoError(t, err)
	p := &QueuePolicy{HasDeadLetterExchange: true, DeadLetterExchange: "nonexistent-dlx"}

	msg := &protocol.Message{Exchange: "ex", RoutingKey: "rk", Body: []byte("x")}
	require.NoError(t, b.deadLetter(p, "src", msg, DeadLetterRejected))
	require.Nil(t, drainOne(t, b, "dlq"), "no message should be published when the DLX is absent")
}

// TestDeadLetter_NoRoutedQueuesDrops verifies that a DLX with no bound queue
// matching the routing key drops silently.
func TestDeadLetter_NoRoutedQueuesDrops(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	// DLX exists, target bound on a DIFFERENT key so routing yields zero queues.
	p := setupDLX(t, b, dlxOpts{
		exchange: "dlx", exchangeKind: "direct", targets: []string{"dlq"}, bindKey: "other-key",
		sourceArgs: map[string]interface{}{"x-dead-letter-exchange": "dlx"},
	})

	msg := &protocol.Message{Exchange: "ex", RoutingKey: "unrouted", Body: []byte("x")}
	require.NoError(t, b.deadLetter(p, "src", msg, DeadLetterRejected))
	require.Nil(t, drainOne(t, b, "dlq"), "no message when the DLX routes to zero queues")
}

// TestDeadLetter_NilPolicyIsNoop verifies a nil / DLX-less policy is a silent
// no-op (defensive: the caller guard normally prevents this call).
func TestDeadLetter_NilPolicyIsNoop(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()
	msg := &protocol.Message{Exchange: "ex", RoutingKey: "rk", Body: []byte("x")}
	require.NoError(t, b.deadLetter(nil, "src", msg, DeadLetterRejected))
	require.NoError(t, b.deadLetter(&QueuePolicy{}, "src", msg, DeadLetterRejected))
}

// TestDeadLetter_RejectCycleDoesNotDrop is the load-bearing reject-cycle case:
// when a message is REJECTED and its DLX routes back to a queue already in its
// x-death (here the source queue itself), cycle detection is SKIPPED, so the
// message is republished (it would loop forever in RabbitMQ — no drop).
func TestDeadLetter_RejectCycleDoesNotDrop(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	// DLX routes straight back to the source queue "src".
	p := setupDLX(t, b, dlxOpts{
		exchange: "dlx", exchangeKind: "direct", bindKey: "rk",
		sourceArgs: map[string]interface{}{"x-dead-letter-exchange": "dlx"},
	})
	require.NoError(t, b.BindQueue("src", "dlx", "rk", nil))

	msg := &protocol.Message{Exchange: "ex", RoutingKey: "rk", Body: []byte("x")}
	require.NoError(t, b.deadLetter(p, "src", msg, DeadLetterRejected))

	require.NotNil(t, drainOne(t, b, "src"),
		"reject-cycle must NOT drop: the message is republished even though it targets a queue in its x-death")
}

// TestDeadLetter_ExpiredCycleDropsOnQueueNameMatch verifies the flip side: an
// EXPIRED message whose DLX routes back to a queue already in its x-death is
// dropped (queue-name-only cycle match).
func TestDeadLetter_ExpiredCycleDropsOnQueueNameMatch(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	p := setupDLX(t, b, dlxOpts{
		exchange: "dlx", exchangeKind: "direct", bindKey: "rk",
		sourceArgs: map[string]interface{}{"x-dead-letter-exchange": "dlx"},
	})
	require.NoError(t, b.BindQueue("src", "dlx", "rk", nil))

	msg := &protocol.Message{Exchange: "ex", RoutingKey: "rk", Body: []byte("x")}
	require.NoError(t, b.deadLetter(p, "src", msg, DeadLetterExpired))

	require.Nil(t, drainOne(t, b, "src"),
		"expired-cycle must drop: the target queue name is already in x-death")
}

// TestDeadLetter_FanoutMixedCycle verifies per-target filtering: a fanout DLX
// bound to one cyclic queue (the source) and one non-cyclic queue delivers only
// to the non-cyclic queue for an expired message.
func TestDeadLetter_FanoutMixedCycle(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	// Fanout DLX bound to both "src" (cyclic) and "other" (non-cyclic).
	p := setupDLX(t, b, dlxOpts{
		exchange: "dlx", exchangeKind: "fanout", targets: []string{"other"}, bindKey: "",
		sourceArgs: map[string]interface{}{"x-dead-letter-exchange": "dlx"},
	})
	require.NoError(t, b.BindQueue("src", "dlx", "", nil))

	msg := &protocol.Message{Exchange: "ex", RoutingKey: "rk", Body: []byte("x")}
	require.NoError(t, b.deadLetter(p, "src", msg, DeadLetterExpired))

	require.NotNil(t, drainOne(t, b, "other"), "non-cyclic fanout target must receive the message")
	require.Nil(t, drainOne(t, b, "src"), "cyclic fanout target must be dropped")
}

// TestDeadLetter_ExpiredStripsExpiration verifies the clone's Expiration is
// stripped for reason=expired and recorded as original-expiration in x-death.
func TestDeadLetter_ExpiredStripsExpiration(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	p := setupDLX(t, b, dlxOpts{
		exchange: "dlx", exchangeKind: "direct", targets: []string{"dlq"}, bindKey: "rk",
		sourceArgs: map[string]interface{}{"x-dead-letter-exchange": "dlx"},
	})

	msg := &protocol.Message{Exchange: "ex", RoutingKey: "rk", Body: []byte("x"), Expiration: "60000"}
	require.NoError(t, b.deadLetter(p, "src", msg, DeadLetterExpired))

	got := drainOne(t, b, "dlq")
	require.NotNil(t, got)
	require.Equal(t, "", got.Expiration, "Expiration must be stripped from the expired clone")
	require.Equal(t, "60000", msg.Expiration, "live message's Expiration must be untouched")

	entries := xDeathEntries(t, got.Headers)
	require.Equal(t, "60000", entries[0]["original-expiration"])
}

// TestDeadLetter_PreservesExpirationForRejectedAndMaxlen verifies Expiration is
// NOT stripped (and no original-expiration recorded) for rejected/maxlen.
func TestDeadLetter_PreservesExpirationForRejectedAndMaxlen(t *testing.T) {
	for _, reason := range []DeadLetterReason{DeadLetterRejected, DeadLetterMaxLen} {
		t.Run(string(reason), func(t *testing.T) {
			b, cleanup := createTestBroker(t)
			defer cleanup()

			p := setupDLX(t, b, dlxOpts{
				exchange: "dlx", exchangeKind: "direct", targets: []string{"dlq"}, bindKey: "rk",
				sourceArgs: map[string]interface{}{"x-dead-letter-exchange": "dlx"},
			})

			msg := &protocol.Message{Exchange: "ex", RoutingKey: "rk", Body: []byte("x"), Expiration: "60000"}
			require.NoError(t, b.deadLetter(p, "src", msg, reason))

			got := drainOne(t, b, "dlq")
			require.NotNil(t, got)
			require.Equal(t, "60000", got.Expiration, "Expiration must be preserved for %s", reason)
			entries := xDeathEntries(t, got.Headers)
			if _, ok := entries[0]["original-expiration"]; ok {
				t.Errorf("original-expiration must not be recorded for reason=%s", reason)
			}
		})
	}
}

// TestDeadLetter_AtLeastOnce_StoresBeforeSourceRemoval documents the ordering
// guarantee: deadLetter republishes (StoreMessage) but never removes the source
// message — the caller deletes it AFTER deadLetter returns. So at the instant
// deadLetter returns, the message exists in BOTH the source ring and the DLX
// target (duplicates on crash are acceptable, matching RabbitMQ).
func TestDeadLetter_AtLeastOnce_StoresBeforeSourceRemoval(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	p := setupDLX(t, b, dlxOpts{
		exchange: "dlx", exchangeKind: "direct", targets: []string{"dlq"}, bindKey: "rk",
		sourceArgs: map[string]interface{}{"x-dead-letter-exchange": "dlx"},
	})

	// Put a real message into the source queue's storage so we can observe it
	// is still present after deadLetter (only the caller removes it).
	srcMsg := &protocol.Message{Exchange: "ex", RoutingKey: "rk", Body: []byte("x"), DeliveryTag: 999}
	require.NoError(t, b.storage.StoreMessage("src", srcMsg))

	require.NoError(t, b.deadLetter(p, "src", srcMsg, DeadLetterRejected))

	// Source copy still present (deadLetter must not delete it).
	stillThere, err := b.storage.GetMessage("src", 999)
	require.NoError(t, err)
	require.NotNil(t, stillThere, "deadLetter must not remove the source message; the caller does")
	// DLX target copy present.
	require.NotNil(t, drainOne(t, b, "dlq"), "republished copy must exist before the source is deleted")
}

// TestReject_IntoFullTargetReturnsPromptly proves the non-blocking republish
// through the real reject path: a basic.reject whose DLX target sits at its
// high-water mark (a consumer-less dead-letter queue) must never stall the
// caller. Were the republish to use WaitForCapacity, this reject would block
// until the target drained (never, here) — the 2s deadline would fire.
func TestReject_IntoFullTargetReturnsPromptly(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	setupDLX(t, b, dlxOpts{
		exchange: "dlx", exchangeKind: "direct", targets: []string{"dlq"}, bindKey: "src",
		sourceArgs: map[string]interface{}{"x-dead-letter-exchange": "dlx"},
	})

	// Get a real delivery to reject.
	publishToSource(t, b, "src", "victim")
	_, tag, _, err := b.GetMessageForGet("src", false)
	require.NoError(t, err)

	// Wedge the target at its high-water mark: tiny WM + one ready message.
	dlq := b.getOrCreateQueueState("dlq")
	dlq.SetDepthHighWM(1)
	require.NoError(t, b.storage.StoreMessage("dlq", &protocol.Message{
		Body: []byte("fill"), RoutingKey: "src", DeliveryTag: b.globalDeliveryTag.Add(1),
	}))
	dlq.Publish(dlq.Head()) // Depth()>=WM -> AtHighWaterMark

	done := make(chan struct{})
	go func() {
		_ = b.RejectGetDelivery(tag, false)
		close(done)
	}()
	select {
	case <-done:
		// returned promptly (dropped on full) — good.
	case <-time.After(2 * time.Second):
		t.Fatal("reject blocked on a full DLX target (republish must be non-blocking / drop-on-full)")
	}
}
