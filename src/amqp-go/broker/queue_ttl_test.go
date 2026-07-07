package broker

import (
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// TestEffectiveTTLMillis_MinOfPerMessageAndQueue pins the RabbitMQ rule that the
// effective TTL is the minimum of the per-message expiration and the queue's
// x-message-ttl, and that either alone applies.
func TestEffectiveTTLMillis_MinOfPerMessageAndQueue(t *testing.T) {
	queueTTL := &QueuePolicy{HasMessageTTL: true, MessageTTL: 30 * time.Second}

	cases := []struct {
		name       string
		expiration string
		policy     *QueuePolicy
		wantTTL    int64
		wantOK     bool
	}{
		{"neither", "", nil, 0, false},
		{"queue-only", "", queueTTL, 30000, true},
		{"per-message-only", "5000", nil, 5000, true},
		{"per-message-smaller", "5000", queueTTL, 5000, true},
		{"queue-smaller", "60000", queueTTL, 30000, true},
		{"equal", "30000", queueTTL, 30000, true},
		{"ttl-zero-per-message", "0", nil, 0, true},
		{"malformed-per-message-no-queue", "soon", nil, 0, false},
		{"malformed-per-message-with-queue", "soon", queueTTL, 30000, true},
		{"negative-per-message-ignored", "-1", queueTTL, 30000, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg := &protocol.Message{Expiration: tc.expiration}
			gotTTL, gotOK := effectiveTTLMillis(msg, tc.policy)
			require.Equal(t, tc.wantOK, gotOK)
			if tc.wantOK {
				require.Equal(t, tc.wantTTL, gotTTL)
			}
		})
	}
}

// TestMessageDeadlineMillis_RequiresAnchor verifies that an unstamped message
// (EnqueueUnixMilli==0) has no deadline even when a TTL policy is present — the
// zero-cost gate — and that a stamped one yields enqueue+ttl.
func TestMessageDeadlineMillis_RequiresAnchor(t *testing.T) {
	p := &QueuePolicy{HasMessageTTL: true, MessageTTL: 10 * time.Second}

	unstamped := &protocol.Message{Expiration: "5000"}
	_, ok := messageDeadlineMillis(unstamped, p)
	require.False(t, ok, "no anchor ⟹ no deadline (zero-cost gate)")

	stamped := &protocol.Message{Expiration: "5000", EnqueueUnixMilli: 1_000_000}
	dl, ok := messageDeadlineMillis(stamped, p)
	require.True(t, ok)
	require.Equal(t, int64(1_005_000), dl, "min(5000, 10000) added to anchor")
}

// TestMessageExpired_Boundaries pins the expiry comparison, including the ttl=0
// immediate-expiry edge and the not-yet-expired case.
func TestMessageExpired_Boundaries(t *testing.T) {
	p := &QueuePolicy{HasMessageTTL: true, MessageTTL: 100 * time.Millisecond}

	// No anchor: never expired regardless of clock.
	require.False(t, messageExpired(&protocol.Message{}, p, 1<<62))

	m := &protocol.Message{EnqueueUnixMilli: 1000}
	require.False(t, messageExpired(m, p, 1099), "before deadline (1100)")
	require.True(t, messageExpired(m, p, 1100), "at deadline is expired")
	require.True(t, messageExpired(m, p, 1101), "after deadline")

	// ttl=0 (queue): deadline==enqueue, expired at the first equal-or-later tick.
	zero := &protocol.Message{EnqueueUnixMilli: 1000}
	pz := &QueuePolicy{HasMessageTTL: true, MessageTTL: 0}
	require.True(t, messageExpired(zero, pz, 1000), "ttl=0 is expired at enqueue instant")
}
