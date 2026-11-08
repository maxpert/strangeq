package main

import (
	"testing"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeQueueListerCounts adds the QueueMessageCounts seam (spec 06 §3.5) to the
// base fake lister.
type fakeQueueListerCounts struct {
	fakeQueueLister
	counts map[string]broker.QueueMessageCounts
}

func (f fakeQueueListerCounts) QueueMessageCounts(name string) broker.QueueMessageCounts {
	return f.counts[name]
}

// TestQueueStatsProvider_UnackedWired (spec 06 §5 test #18): a queue with a
// known QueueMessageCounts is published with its unacked depth; an unknown
// queue emits none.
func TestQueueStatsProvider_UnackedWired(t *testing.T) {
	base := fakeQueueLister{
		queues: map[string]*protocol.Queue{
			"q": {Name: "q"},
			"u": {Name: "u"},
		},
	}
	lister := fakeQueueListerCounts{
		fakeQueueLister: base,
		counts: map[string]broker.QueueMessageCounts{
			"q": {Ready: 5, Unacked: 2, Total: 7, Known: true},
			// "u" is absent -> Known=false.
		},
	}
	stats := queueStatsFrom(lister)
	require.Len(t, stats, 2)
	byName := map[string]struct {
		unacked      int
		unackedKnown bool
	}{}
	for _, s := range stats {
		byName[s.Name] = struct {
			unacked      int
			unackedKnown bool
		}{s.Unacked, s.UnackedKnown}
	}
	assert.Equal(t, 2, byName["q"].unacked)
	assert.True(t, byName["q"].unackedKnown)
	assert.False(t, byName["u"].unackedKnown, "an unknown queue must not fabricate an unacked depth")
}
