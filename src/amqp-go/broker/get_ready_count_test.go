package broker

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Bug#3 FIX B — basic.get-ok message-count must report the READY-remaining count
// (excluding delivered-but-unacked messages), symmetric with the queue.declare-ok
// ready-count fix and matching RabbitMQ's "messages remaining" semantics. The
// storage ring count (ready+unacked) over-reports when unacked deliveries are
// outstanding.

// TestGetOk_RemainingIsReadyRemaining establishes the baseline ready-remaining
// semantics: three successive manual-ack basic.get calls on a 3-message queue
// report 2, 1, 0 (each get removes one message from the ready set). Manual-ack
// gets are used so `waiting` tracks the ready set precisely.
func TestGetOk_RemainingIsReadyRemaining(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	_, err := b.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		publishToSource(t, b, "q", "m")
	}

	for want := uint32(2); ; want-- {
		msg, _, remaining, err := b.GetMessageForGet("q", false)
		require.NoError(t, err)
		require.NotNil(t, msg)
		require.Equal(t, want, remaining, "get-ok message-count must be the ready-remaining count")
		if want == 0 {
			break
		}
	}
}

// TestGetOk_RemainingExcludesUnacked verifies the fix: an outstanding unacked
// basic.get delivery must NOT inflate a later get's message-count. The storage
// ring still holds the unacked message, so the old ring-count path over-reported.
func TestGetOk_RemainingExcludesUnacked(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	_, err := b.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		publishToSource(t, b, "q", "m")
	}

	// First get with manual ack: m0 leaves the ready set (claimed -> inflight) but
	// stays in the storage ring as an outstanding pending-ack.
	_, tag0, remaining0, err := b.GetMessageForGet("q", false)
	require.NoError(t, err)
	require.Equal(t, uint32(2), remaining0, "first get: 2 ready remain")

	// Second get: two messages are still in the storage ring (m0 unacked + m2
	// ready) plus the one being returned (m1). The ready-remaining count is 1
	// (only m2), NOT 2 (which the ready+unacked ring count would report).
	_, _, remaining1, err := b.GetMessageForGet("q", false)
	require.NoError(t, err)
	require.Equal(t, uint32(1), remaining1,
		"get-ok message-count must exclude the delivered-but-unacked message")

	// Acking the first delivery does not change the remaining ready message.
	require.NoError(t, b.AcknowledgeGetDelivery(tag0))
	_, _, remaining2, err := b.GetMessageForGet("q", false)
	require.NoError(t, err)
	require.Equal(t, uint32(0), remaining2, "third get: the last ready message, none remaining")
}
