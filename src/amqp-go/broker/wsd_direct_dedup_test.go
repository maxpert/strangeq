package broker

import (
	"fmt"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// TestDirectPath_Dedup_ManyBindings (T14.1): with >8 same-key bindings on a
// direct exchange (the pooled-map path), routing dedups to distinct queues —
// and a fanout exchange with the same bindings routes to all of them.
// Pre-fix mechanism: slices.Contains per binding is O(n²) — correct, but
// quadratic; the fix must keep correctness while going linear.
func TestDirectPath_Dedup_ManyBindings(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	const n = 1000
	require.NoError(t, b.DeclareExchange("dx", "direct", false, false, false, nil))
	require.NoError(t, b.DeclareExchange("fx", "fanout", false, false, false, nil))
	for i := 0; i < n; i++ {
		q := fmt.Sprintf("dq-%d", i)
		mustDeclareQueue(t, b, q)
		require.NoError(t, b.BindQueue(q, "dx", "k", nil))
		require.NoError(t, b.BindQueue(q, "fx", "", nil))
	}
	// A duplicate binding row for one queue (same exchange+key) must dedup.
	require.NoError(t, b.BindQueue("dq-0", "dx", "k", nil))

	dx, err := b.storage.GetExchange("dx")
	require.NoError(t, err)
	msg := &protocol.Message{Exchange: "dx", RoutingKey: "k", Body: []byte("x")}
	targets, err := b.findTargetQueues(dx, "k", msg)
	require.NoError(t, err)
	require.Len(t, targets, n, "direct: exactly %d DISTINCT queues (deduped)", n)
	seen := make(map[string]bool, n)
	for _, q := range targets {
		require.False(t, seen[q], "duplicate target %q", q)
		seen[q] = true
	}

	fx, err := b.storage.GetExchange("fx")
	require.NoError(t, err)
	targets, err = b.findTargetQueues(fx, "", msg)
	require.NoError(t, err)
	require.Len(t, targets, n, "fanout: all %d queues", n)
}
