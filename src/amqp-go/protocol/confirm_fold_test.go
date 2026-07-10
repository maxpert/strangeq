package protocol

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Iteration 2: the per-channel contiguous confirm fold. AdvanceConfirmDurable
// acks cumulatively (multiple=true), so an async barrier that completes tags
// out of order must NEVER feed it a value that crosses a not-yet-durable tag.
// These unit tests pin the fold primitive that guarantees that (the single most
// subtle piece of the async-confirm design).

// TestConfirmFold_ContiguousUnderReorder: out-of-order durable folds must leave
// the contiguous watermark parked until the gap fills, then jump to the top.
func TestConfirmFold_ContiguousUnderReorder(t *testing.T) {
	ch := NewChannel(1, nil)

	// Tags 2 and 3 complete before 1: the watermark must NOT advance (acking 3
	// would cumulatively confirm the not-yet-durable tag 1).
	assert.Equal(t, uint64(0), ch.MarkConfirmTagDurable(2), "gap at 1: watermark must stay 0")
	assert.Equal(t, uint64(0), ch.MarkConfirmTagDurable(3), "gap at 1: watermark must stay 0")
	assert.Equal(t, uint64(0), ch.ConfirmContig())

	// Tag 1 completes: the watermark folds forward across 1,2,3 in one step.
	assert.Equal(t, uint64(3), ch.MarkConfirmTagDurable(1), "filling the gap folds 1,2,3")
	assert.Equal(t, uint64(3), ch.ConfirmContig())

	// Feeding the fold result to the CAS-max watermark yields a single multi-ack.
	ch.AdvanceConfirmDurable(ch.MarkConfirmTagDurable(1)) // idempotent duplicate
	var ackTag uint64
	var ackMultiple bool
	require.NoError(t, ch.FlushConfirms(func(tag uint64, multiple bool) error {
		ackTag, ackMultiple = tag, multiple
		return nil
	}))
	assert.Equal(t, uint64(3), ackTag)
	assert.True(t, ackMultiple)
}

// TestConfirmFold_NackAdvancesPastHole: a nacked tag must not be ackable, but
// the contiguous frontier must still advance past it (via ResolveConfirmContigNack)
// so later durable tags are not stranded, and the pre-nack ack must never cover
// the nacked tag.
func TestConfirmFold_NackAdvancesPastHole(t *testing.T) {
	ch := NewChannel(1, nil)

	// Tag 1 durable, tag 3 durable-but-buffered (gap at 2). Tag 2 will be nacked.
	assert.Equal(t, uint64(1), ch.MarkConfirmTagDurable(1))
	ch.AdvanceConfirmDurable(1)
	assert.Equal(t, uint64(1), ch.MarkConfirmTagDurable(3), "3 buffered behind the gap at 2")

	// Ack up to the durable prefix (1); the nack at 2 must be settleable now.
	require.NoError(t, ch.FlushConfirms(func(uint64, bool) error { return nil }))

	// Settle the nack at 2: emit ack(<2) + nack(2), advancing acked/durable to 2.
	var nacked uint64
	require.NoError(t, ch.SettleConfirmNack(2,
		func(uint64, bool) error { return nil },
		func(tag uint64) error { nacked = tag; return nil },
	))
	assert.Equal(t, uint64(2), nacked)

	// Fold the nack for contiguity: the frontier advances past 2 and pulls in the
	// buffered durable tag 3.
	assert.Equal(t, uint64(3), ch.ResolveConfirmContigNack(2), "frontier folds past the nacked hole and pulls 3")
	ch.AdvanceConfirmDurable(ch.ConfirmContig())

	// A subsequent flush acks tag 3 only — never re-confirming the nacked tag 2.
	var ackTag uint64
	var ackMultiple bool
	require.NoError(t, ch.FlushConfirms(func(tag uint64, multiple bool) error {
		ackTag, ackMultiple = tag, multiple
		return nil
	}))
	assert.Equal(t, uint64(3), ackTag, "only tag 3 acked after the nack")
	assert.False(t, ackMultiple, "ack must not be cumulative over the nacked tag 2")
}

// TestConfirmFold_ConcurrentDurableFolds: concurrent out-of-order folds of a
// contiguous tag set must resolve to exactly the full set with no lost or
// double-counted tag (the fold is the only shared confirm state driven by both
// the WAL batch writer and the frame processor).
func TestConfirmFold_ConcurrentDurableFolds(t *testing.T) {
	ch := NewChannel(1, nil)
	const n = 500

	var wg sync.WaitGroup
	for i := uint64(1); i <= n; i++ {
		wg.Add(1)
		go func(tag uint64) {
			defer wg.Done()
			ch.AdvanceConfirmDurable(ch.MarkConfirmTagDurable(tag))
		}(i)
	}
	wg.Wait()

	// Every tag folded exactly once, in some order: the frontier must reach n.
	assert.Equal(t, uint64(n), ch.ConfirmContig(), "all tags must fold into a contiguous watermark")
	var ackTag uint64
	require.NoError(t, ch.FlushConfirms(func(tag uint64, multiple bool) error {
		ackTag = tag
		return nil
	}))
	assert.Equal(t, uint64(n), ackTag)
}
