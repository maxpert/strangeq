package broker

import (
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// Bug#3 — republishToTargets must honor the TARGET queue's own x-max-length /
// x-overflow on the dead-letter republish path (RabbitMQ applies the target's
// overflow policy to a dead-lettered message, so the target never overshoots its
// configured cap). Previously the seam gated only on the ring backpressure
// high-water mark, so a dead-letter into a full target overshot the cap.

// declareTargetOverflow declares a direct DLX, one target queue bound on
// "dead" carrying its own overflow args, and a source queue dead-lettering to
// that DLX on "dead". Returns the source policy and the target's QueueState.
func declareTargetOverflow(t *testing.T, b *StorageBroker, targetArgs map[string]interface{}) (*QueuePolicy, *QueueState) {
	t.Helper()
	require.NoError(t, b.DeclareExchange("dlx", "direct", false, false, false, nil))
	_, err := b.DeclareQueue("dlq", false, false, false, targetArgs)
	require.NoError(t, err)
	require.NoError(t, b.BindQueue("dlq", "dlx", "dead", nil))

	_, err = b.DeclareQueue("src", false, false, false, map[string]interface{}{
		"x-dead-letter-exchange": "dlx", "x-dead-letter-routing-key": "dead",
	})
	require.NoError(t, err)
	p := b.GetQueuePolicy("src")
	require.NotNil(t, p, "source policy must resolve")
	return p, b.getOrCreateQueueState("dlq")
}

// deadLetterBody dead-letters a fresh message (reason=rejected) from "src" into
// the DLX; reason=rejected skips cycle detection so the target-overflow decision
// is exercised in isolation.
func deadLetterBody(t *testing.T, b *StorageBroker, p *QueuePolicy, body string) {
	t.Helper()
	require.NoError(t, b.deadLetter(p, "src",
		&protocol.Message{RoutingKey: "dead", Body: []byte(body)}, DeadLetterRejected))
}

// TestDeadLetterIntoFullTarget_DropHeadStaysAtCap verifies a drop-head target
// stays AT its x-max-length on the DLX republish path: dead-lettering a second
// message into a full (cap-1) target evicts the target's oldest and keeps the
// newest — depth 1, not the previous overshoot to depth 2.
func TestDeadLetterIntoFullTarget_DropHeadStaysAtCap(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	p, dlq := declareTargetOverflow(t, b, map[string]interface{}{
		"x-max-length": int64(1), "x-overflow": "drop-head",
	})

	// First dead-letter into the empty target: accepted, target now AT its cap.
	deadLetterBody(t, b, p, "old")
	require.Equal(t, int64(1), dlq.WaitingCount())

	// Second dead-letter into the now-full target: drop-head keeps the target at
	// its cap by evicting the oldest, so only the newest survives.
	deadLetterBody(t, b, p, "new")
	require.Equal(t, int64(1), dlq.WaitingCount(),
		"drop-head target must stay AT its x-max-length on the DLX republish path")
	require.Equal(t, []string{"new"}, drainBodies(t, b, "dlq"),
		"drop-head evicts the oldest dead-letter, keeping the newest")
}

// TestDeadLetterIntoFullTarget_DropHeadBytesStaysAtCap verifies the byte-limit
// variant: a drop-head target with x-max-length-bytes stays within its byte cap
// on the DLX republish path (keeping at least the newest, RabbitMQ parity).
func TestDeadLetterIntoFullTarget_DropHeadBytesStaysAtCap(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	// 100-byte bodies, byte cap 150 → holds exactly one at steady state.
	p, dlq := declareTargetOverflow(t, b, map[string]interface{}{
		"x-max-length-bytes": int64(150), "x-overflow": "drop-head",
	})
	body := string(make([]byte, 100))

	deadLetterBody(t, b, p, body)
	require.Equal(t, int64(1), dlq.WaitingCount())
	require.Equal(t, int64(100), dlq.ReadyBytes())

	deadLetterBody(t, b, p, body)
	require.Equal(t, int64(1), dlq.WaitingCount(),
		"byte-limited drop-head target must trim on the DLX republish path")
	require.Equal(t, int64(100), dlq.ReadyBytes(), "ready-bytes counter stays within the cap")
}

// TestDeadLetterIntoFullTarget_RejectPublishDropsIncoming verifies a
// reject-publish target refuses the incoming dead-letter when it is AT-OR-OVER
// its limit: the target keeps what it holds (the dead-letter is dropped, since a
// system dead-letter cannot be nacked to a publisher), and the drop is observable
// via the shared metrics hook.
func TestDeadLetterIntoFullTarget_RejectPublishDropsIncoming(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	coll := &dropCountingCollector{}
	b.SetMetricsCollector(coll)

	p, dlq := declareTargetOverflow(t, b, map[string]interface{}{
		"x-max-length": int64(1), "x-overflow": "reject-publish",
	})

	// First dead-letter: accepted, target now AT its cap.
	deadLetterBody(t, b, p, "keep")
	require.Equal(t, int64(1), dlq.WaitingCount())
	require.Equal(t, int64(0), coll.dropped.Load())

	// Second dead-letter into the AT-cap reject-publish target: dropped. The
	// target still holds only the first message.
	deadLetterBody(t, b, p, "dropped")
	require.Equal(t, int64(1), dlq.WaitingCount(),
		"reject-publish target must refuse the dead-letter when at its limit")
	require.Equal(t, int64(1), coll.dropped.Load(),
		"the overflow drop must be recorded via the metrics hook")
	require.Equal(t, []string{"keep"}, drainBodies(t, b, "dlq"),
		"reject-publish keeps what it holds and drops the incoming dead-letter")
}

// TestDeadLetterIntoFullTarget_DropHeadChainsToTargetDLX proves the recursive
// dead-letter path is deadlock-free: when the drop-head target ITSELF has a DLX,
// evicting its head dead-letters (reason=maxlen) through the same seam. No lock
// is held across the recursive republish, and cycle detection admits the second-
// level target (a different queue name), so the evicted message lands there.
func TestDeadLetterIntoFullTarget_DropHeadChainsToTargetDLX(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	// Second-level DLX + its target queue.
	require.NoError(t, b.DeclareExchange("dlx2", "fanout", false, false, false, nil))
	_, err := b.DeclareQueue("dlq2", false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, b.BindQueue("dlq2", "dlx2", "", nil))

	// First-level DLX + a drop-head cap-1 target that itself dead-letters to dlx2.
	require.NoError(t, b.DeclareExchange("dlx", "direct", false, false, false, nil))
	_, err = b.DeclareQueue("dlq", false, false, false, map[string]interface{}{
		"x-max-length": int64(1), "x-overflow": "drop-head",
		"x-dead-letter-exchange": "dlx2",
	})
	require.NoError(t, err)
	require.NoError(t, b.BindQueue("dlq", "dlx", "dead", nil))

	_, err = b.DeclareQueue("src", false, false, false, map[string]interface{}{
		"x-dead-letter-exchange": "dlx", "x-dead-letter-routing-key": "dead",
	})
	require.NoError(t, err)
	p := b.GetQueuePolicy("src")
	require.NotNil(t, p)
	dlq := b.getOrCreateQueueState("dlq")

	deadLetterBody(t, b, p, "old")
	deadLetterBody(t, b, p, "new")

	// The drop-head target stays at its cap holding the newest.
	require.Equal(t, int64(1), dlq.WaitingCount())
	require.Equal(t, []string{"new"}, drainBodies(t, b, "dlq"))

	// The evicted "old" chained through the target's own DLX (reason=maxlen).
	chained := drainOne(t, b, "dlq2")
	require.NotNil(t, chained, "the evicted head must chain to the target's own DLX")
	require.Equal(t, []byte("old"), chained.Body)
	entries := xDeathEntries(t, chained.Headers)
	assertEntry(t, entries[0], "dlq", DeadLetterMaxLen, 1)
}
