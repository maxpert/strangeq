package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func newSegmentAckTestRecord(inv *segmentInventory, segmentID uint64, position int64, queueName string, tag uint64, bytes int64) *segmentRecord {
	r := &segmentRecord{
		pos:         recordPos{segmentID: segmentID, position: position},
		queueName:   queueName,
		deliveryTag: tag,
		bytes:       bytes,
		bodyRef:     -1,
		committed:   true,
	}
	inv.addRecord(r)
	return r
}

func newSegmentAckTestManager(records ...*segmentRecord) (*WALManager, *QueueWAL) {
	qw := &QueueWAL{
		ackChan:      make(chan *ackRequest, 8),
		stopChan:     make(chan struct{}),
		offsetIndex:  make(map[recordKey]*segmentRecord),
		recordCopies: make(map[recordKey][]*segmentRecord),
	}
	for _, r := range records {
		qw.indexRecordLocked(r)
	}
	return &WALManager{sharedWAL: qw}, qw
}

func TestSegmentAckIdempotent(t *testing.T) {
	inv := newSegmentInventory(0)
	r := newSegmentAckTestRecord(inv, 1, 10, "orders", 7, 24)
	inv.addBlock(50, 32)
	r.bodyRef = 50
	inv.blocks[50].refs.Store(1)
	wm, _ := newSegmentAckTestManager(r)

	wm.Acknowledge("orders", 7)
	wm.Acknowledge("orders", 7)

	require.True(t, r.acked.Load())
	require.Zero(t, inv.unacked.Load())
	require.Equal(t, int64(56), inv.deadBytes.Load(), "a duplicate ACK must not charge record or body bytes twice")
	// THE EAGER SETTLING: the body block's LAST reference was settled, so
	// the block left the inventory entirely -- no live record references it
	// any more, and the compaction only reads the block of a record it is
	// about to copy (and such a record holds a reference).
	_, blockPresent := inv.blocks[50]
	require.False(t, blockPresent,
		"a body block whose last reference was settled must leave the inventory")
}

func TestSegmentAckSettlesAllDuplicateCopies(t *testing.T) {
	oldInv := newSegmentInventory(0)
	newInv := newSegmentInventory(0)
	oldCopy := newSegmentAckTestRecord(oldInv, 1, 10, "orders", 7, 24)
	newCopy := newSegmentAckTestRecord(newInv, 2, 20, "orders", 7, 24)
	wm, qw := newSegmentAckTestManager(oldCopy, newCopy)
	require.Same(t, newCopy, qw.offsetIndex[recordKey{queueName: "orders", deliveryTag: 7}])

	wm.Acknowledge("orders", 7)

	require.True(t, oldCopy.acked.Load(), "the older checkpoint duplicate must be settled")
	require.True(t, newCopy.acked.Load(), "the newest copy must be settled")
	require.Zero(t, oldInv.unacked.Load())
	require.Zero(t, newInv.unacked.Load())
	require.Equal(t, int64(24), oldInv.deadBytes.Load())
	require.Equal(t, int64(24), newInv.deadBytes.Load())
}

func TestSegmentAckBeforeIndexCleanupVisible(t *testing.T) {
	inv := newSegmentInventory(0)
	r := newSegmentAckTestRecord(inv, 1, 10, "orders", 7, 24)
	wm, qw := newSegmentAckTestManager(r)

	wm.Acknowledge("orders", 7)

	require.True(t, wm.IsAcknowledged("orders", 7), "the read gate must be visible when Acknowledge returns")
	require.False(t, wm.IsAcknowledged("payments", 7), "an ACK must be scoped to its queue")
	// THE EAGER SETTLING. This assertion used to be the opposite one: the
	// physical read index entry was removed ASYNCHRONOUSLY, by the cleanup
	// goroutine, so the index held a record for every message ever published
	// and grew without bound (~260 MB of live heap after a 30 s F4 window).
	// A settled record now leaves the index SYNCHRONOUSLY, so the index is
	// bounded by the in-flight set, and the read gate is visible when
	// Acknowledge returns.
	_, present := qw.offsetIndex[recordKey{queueName: "orders", deliveryTag: 7}]
	require.False(t, present,
		"an acknowledged record must leave the physical read index when Acknowledge returns")
	require.True(t, r.acked.Load(), "the record must be settled")
}
