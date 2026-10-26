package storage

import (
	"sync/atomic"
	"testing"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// The tag index (review F4).
//
// The ACK path resolves ONE delivery tag against a segment's inventory. The
// inventory used to be walked in full for that, under the segment's read
// lock, so one ACK cost one pass over every record of the segment - ~200k
// records at a 64 MiB segment - and blocked reads and compaction of that
// segment for the duration. PurgeQueue was worse: it collected the band with
// one full pass and then called applyAck per tag, and each applyAck walked
// every segment again, so a purge of k tags over n records cost O(n*k).
//
// These tests pin the cost: the number of inventory entries a lookup walks
// must not grow with the size of the segment, and the semantics must not
// change - every duplicate copy of a tag settles exactly once, and a purge
// returns each tag it won exactly once, sorted.
// ============================================================================

// byTagTestQueueSegments builds a QueueSegments with no background goroutines.
// The tests drive acknowledgeInSegment / PurgeQueue directly, so no ACK
// batching loop is running.
func byTagTestQueueSegments(queueName string) *QueueSegments {
	return &QueueSegments{
		queueName:      queueName,
		sealedSegments: make(map[uint64]*SegmentFile),
		ackBitmap:      roaring64.New(),
		ackChan:        make(chan uint64, 64),
		ackBatchSize:   100,
		stopChan:       make(chan struct{}),
	}
}

// byTagTestSegment builds a segment that holds an inventory and nothing else.
// No file handle is needed: the ACK path only reads the inventory.
func byTagTestSegment(segmentNum uint64, inv *segmentInventory) *SegmentFile {
	return &SegmentFile{
		segmentNum: segmentNum,
		index:      make(map[uint64]int64),
		inv:        inv,
	}
}

// byTagTestInventory builds an inventory holding one committed record per
// delivery tag, each record 16 bytes with no shared body.
func byTagTestInventory(queueName string, segmentID uint64, tags ...uint64) *segmentInventory {
	inv := newSegmentInventoryWithTagIndex(0)
	for i, tag := range tags {
		inv.addRecord(&segmentRecord{
			pos:         recordPos{segmentID: segmentID, position: int64(i) * 16},
			queueName:   queueName,
			deliveryTag: tag,
			bytes:       16,
			bodyRef:     -1,
			committed:   true,
		})
	}
	return inv
}

// byTagTestRecordAt returns the inventory's record for one delivery tag.
func byTagTestRecordAt(t *testing.T, inv *segmentInventory, tag uint64) *segmentRecord {
	t.Helper()
	for _, r := range inv.records {
		if r.deliveryTag == tag {
			return r
		}
	}
	t.Fatalf("no record for delivery tag %d", tag)
	return nil
}

// installByTagScanCounter installs the inventory-lookup observer and returns
// the counter it feeds.
func installByTagScanCounter(t *testing.T) *atomic.Int64 {
	t.Helper()
	var visited atomic.Int64
	restore := SetSegmentInventoryScanObserverForTest(func(n int) {
		visited.Add(int64(n))
	})
	t.Cleanup(restore)
	return &visited
}

// TestSegmentAckResolvesOneTagWithoutWalkingTheSegment is the F4 regression
// test: acknowledging ONE tag over a 100k-record segment must not walk the
// segment.
func TestSegmentAckResolvesOneTagWithoutWalkingTheSegment(t *testing.T) {
	const recordCount = 100000
	const target = uint64(50000)

	queue := "orders"
	qs := byTagTestQueueSegments(queue)
	tags := make([]uint64, recordCount)
	for i := range tags {
		tags[i] = uint64(i + 1)
	}
	inv := byTagTestInventory(queue, 1, tags...)
	qs.currentSegment = byTagTestSegment(1, inv)

	visited := installByTagScanCounter(t)

	require.True(t, qs.acknowledgeInSegment(target), "the ACK must settle the record")
	require.True(t, byTagTestRecordAt(t, inv, target).acked.Load(),
		"the acknowledged record must be settled")
	require.False(t, byTagTestRecordAt(t, inv, target+1).acked.Load(),
		"no other record may be settled")

	// The bound the review states: at most 2*(n+k) inventory entries walked
	// for one ACK over n records.
	require.LessOrEqual(t, visited.Load(), int64(2*(recordCount+1)),
		"one ACK over %d records must not walk the segment", recordCount)
	// The bound that actually catches the regression: the walk is the copies
	// of ONE tag, not the records of the segment.
	require.LessOrEqual(t, visited.Load(), int64(8),
		"resolving one delivery tag must not walk %d records", recordCount)
	t.Logf("one ACK over %d records walked %d inventory entries", recordCount, visited.Load())
}

// TestSegmentAckSettlesEveryDuplicateCopyExactlyOnce pins the semantics the
// index must preserve: a tag with a physical copy in the current segment and
// another in a sealed segment settles BOTH copies, exactly once, and a second
// ACK of the same tag wins nothing.
func TestSegmentAckSettlesEveryDuplicateCopyExactlyOnce(t *testing.T) {
	queue := "payments"
	qs := byTagTestQueueSegments(queue)

	currentInv := byTagTestInventory(queue, 1, 7, 8)
	sealedInv := byTagTestInventory(queue, 2, 7)
	qs.currentSegment = byTagTestSegment(1, currentInv)
	qs.sealedSegments[2] = byTagTestSegment(2, sealedInv)

	visited := installByTagScanCounter(t)

	require.True(t, qs.acknowledgeInSegment(7), "the first ACK must settle a copy")
	require.True(t, byTagTestRecordAt(t, currentInv, 7).acked.Load(),
		"the current segment's copy must be settled")
	require.True(t, byTagTestRecordAt(t, sealedInv, 7).acked.Load(),
		"the sealed segment's copy must be settled")
	require.Equal(t, int64(16), currentInv.deadBytes.Load(),
		"a duplicate copy must charge its bytes exactly once")
	require.Equal(t, int64(16), sealedInv.deadBytes.Load(),
		"a duplicate copy must charge its bytes exactly once")

	require.False(t, qs.acknowledgeInSegment(7), "a second ACK must win nothing")
	require.Equal(t, int64(16), currentInv.deadBytes.Load(),
		"a repeated ACK must not charge the bytes twice")
	require.Equal(t, int64(16), sealedInv.deadBytes.Load(),
		"a repeated ACK must not charge the bytes twice")

	require.LessOrEqual(t, visited.Load(), int64(8),
		"resolving one delivery tag must not walk the segment")
}

// TestSegmentPurgeWalksTheInventoryOnce is the F4 purge regression test: a
// purge of a band of k tags over n records must not cost O(n*k).
func TestSegmentPurgeWalksTheInventoryOnce(t *testing.T) {
	const recordCount = 100000
	const bandLo, bandHi = uint64(10), uint64(14)
	const bandTags = bandHi - bandLo + 1

	queue := "refunds"
	qs := byTagTestQueueSegments(queue)
	tags := make([]uint64, recordCount)
	for i := range tags {
		tags[i] = uint64(i + 1)
	}
	inv := byTagTestInventory(queue, 1, tags...)
	qs.currentSegment = byTagTestSegment(1, inv)

	sm := &SegmentManager{}
	sm.queueSegments.Store(queue, qs)

	visited := installByTagScanCounter(t)

	purged := sm.PurgeQueue(queue, bandLo, bandHi)

	require.Len(t, purged, int(bandTags), "every tag in the band must be purged")
	for i := 1; i < len(purged); i++ {
		require.Less(t, purged[i-1], purged[i], "the purged tags must be sorted")
	}
	for i, tag := range purged {
		require.Equal(t, bandLo+uint64(i), tag, "the purged tags must be the band, in order")
	}

	for tag := bandLo; tag <= bandHi; tag++ {
		require.True(t, byTagTestRecordAt(t, inv, tag).acked.Load(),
			"every copy of a purged tag must be settled")
	}
	require.False(t, byTagTestRecordAt(t, inv, bandHi+1).acked.Load(),
		"a tag outside the band must not be settled")
	require.Equal(t, int64(bandTags*16), inv.deadBytes.Load(),
		"a purged tag must charge its bytes exactly once")
	require.Equal(t, int64((recordCount-bandTags)*16), inv.unacked.Load(),
		"only the purged records may leave the unacked set")

	// The bound the review states: at most 2*(n+k) inventory entries walked
	// for a purge of k tags over n records.
	require.LessOrEqual(t, visited.Load(), int64(2*(recordCount+bandTags)),
		"a purge of %d tags over %d records must not cost O(n*k)", bandTags, recordCount)
	// The bound that actually catches the regression: the band is collected
	// in ONE pass and each tag is then resolved through the index.
	require.LessOrEqual(t, visited.Load(), int64(recordCount+4*bandTags),
		"a purge must not re-walk the inventory per tag")
	t.Logf("a purge of %d tags over %d records walked %d inventory entries",
		bandTags, recordCount, visited.Load())
}

// TestSegmentPurgeReturnsEachTagOnceAcrossDuplicateSegments pins the purge
// contract: a tag with copies in two segments is returned ONCE, and every
// copy is settled.
func TestSegmentPurgeReturnsEachTagOnceAcrossDuplicateSegments(t *testing.T) {
	queue := "invoices"
	qs := byTagTestQueueSegments(queue)

	currentInv := byTagTestInventory(queue, 1, 3, 4, 5)
	sealedInv := byTagTestInventory(queue, 2, 3, 5)
	qs.currentSegment = byTagTestSegment(1, currentInv)
	qs.sealedSegments[2] = byTagTestSegment(2, sealedInv)

	sm := &SegmentManager{}
	sm.queueSegments.Store(queue, qs)

	purged := sm.PurgeQueue(queue, 1, 10)

	require.Equal(t, []uint64{3, 4, 5}, purged,
		"the purged tags must be unique and sorted")

	for _, tag := range []uint64{3, 5} {
		require.True(t, byTagTestRecordAt(t, currentInv, tag).acked.Load())
		require.True(t, byTagTestRecordAt(t, sealedInv, tag).acked.Load())
	}
	require.Equal(t, int64(48), currentInv.deadBytes.Load(),
		"each purged copy must charge its bytes exactly once")
	require.Equal(t, int64(32), sealedInv.deadBytes.Load(),
		"each purged copy must charge its bytes exactly once")
}

// TestSegmentAckFallsBackWhenTheTagIndexDisagrees pins the fallback: the tag
// index is a CACHE over `records`, and `records` is authoritative. A record
// that is in `records` but not in the index (a partially built inventory, or
// one whose index was not maintained) must still be settled - an ACK is never
// dropped.
func TestSegmentAckFallsBackWhenTheTagIndexDisagrees(t *testing.T) {
	queue := "audit"
	qs := byTagTestQueueSegments(queue)

	inv := newSegmentInventoryWithTagIndex(0)
	for i, tag := range []uint64{1, 2, 3} {
		inv.addRecord(&segmentRecord{
			pos:         recordPos{segmentID: 1, position: int64(i) * 16},
			queueName:   queue,
			deliveryTag: tag,
			bytes:       16,
			bodyRef:     -1,
			committed:   true,
		})
	}
	// A record published straight into `records`, bypassing the tag index:
	// the two views now disagree, so the lookup must fall back to the scan.
	ghost := &segmentRecord{
		pos:         recordPos{segmentID: 1, position: 48},
		queueName:   queue,
		deliveryTag: 4,
		bytes:       16,
		bodyRef:     -1,
		committed:   true,
	}
	inv.records[ghost.pos.position] = ghost
	require.Len(t, inv.records, 4)

	qs.currentSegment = byTagTestSegment(1, inv)

	visited := installByTagScanCounter(t)

	require.True(t, qs.acknowledgeInSegment(4),
		"an ACK must never be dropped when the tag index disagrees with records")
	require.True(t, ghost.acked.Load(),
		"the record that is only in `records` must still be settled")
	require.Equal(t, int64(len(inv.records)), visited.Load(),
		"a disagreeing index must fall back to the full scan of records")
}

// TestSegmentPurgeFallsBackWhenTheTagIndexDisagrees pins the same fallback
// for the band collection.
func TestSegmentPurgeFallsBackWhenTheTagIndexDisagrees(t *testing.T) {
	queue := "ledger"
	qs := byTagTestQueueSegments(queue)

	inv := newSegmentInventoryWithTagIndex(0)
	for i, tag := range []uint64{1, 2} {
		inv.addRecord(&segmentRecord{
			pos:         recordPos{segmentID: 1, position: int64(i) * 16},
			queueName:   queue,
			deliveryTag: tag,
			bytes:       16,
			bodyRef:     -1,
			committed:   true,
		})
	}
	ghost := &segmentRecord{
		pos:         recordPos{segmentID: 1, position: 32},
		queueName:   queue,
		deliveryTag: 9,
		bytes:       16,
		bodyRef:     -1,
		committed:   true,
	}
	inv.records[ghost.pos.position] = ghost

	qs.currentSegment = byTagTestSegment(1, inv)

	sm := &SegmentManager{}
	sm.queueSegments.Store(queue, qs)

	purged := sm.PurgeQueue(queue, 1, 10)

	require.Equal(t, []uint64{1, 2, 9}, purged,
		"the band must be collected from `records` when the index disagrees")
	require.True(t, ghost.acked.Load())
}

// TestSegmentCompactionRebuildsTheTagIndex pins that a COMPACTED segment's
// inventory is rebuilt through the same addRecord path, so the tag index
// survives a compaction and an ACK that lands after one still resolves
// through the index instead of walking the rewritten segment.
func TestSegmentCompactionRebuildsTheTagIndex(t *testing.T) {
	queue := "compacted"
	sm, err := NewSegmentManagerWithConfig(t.TempDir(), segTestConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = sm.Close() })

	// segTestConfig's SegmentSize is 512 bytes and these records are far
	// smaller, so all five land in the ACTIVE segment.
	require.NoError(t, writeLegacyBatchForTest(sm, queue, segTestMessages(queue, []uint64{1, 2, 3, 4, 5})))

	qs, err := sm.getOrCreateQueueSegments(queue)
	require.NoError(t, err)
	seg := qs.currentSegment
	require.NotNil(t, seg)
	require.Len(t, seg.inv.records, 5)
	require.True(t, seg.inv.tagIndexTrusted(), "a loaded segment must carry the tag index")

	// Settle one record so the compaction has something to drop.
	require.True(t, qs.acknowledgeInSegment(2))

	visited := installByTagScanCounter(t)

	require.NoError(t, qs.compactSegment(seg))

	require.Len(t, seg.inv.records, 4, "the acknowledged record must not survive the compaction")
	require.True(t, seg.inv.tagIndexTrusted(),
		"the compacted inventory must carry the tag index: it is rebuilt through addRecord")

	// An ACK that lands after the compaction resolves through the index.
	require.True(t, qs.acknowledgeInSegment(3))
	require.True(t, byTagTestRecordAt(t, seg.inv, 3).acked.Load(),
		"the ACK must settle the compacted copy")
	// The compaction resets the legacy counter (it is observability metadata
	// only), so it holds the one copy this ACK settled.
	require.Equal(t, uint64(1), seg.deletedCount.Load(),
		"the compacted segment's legacy counter must count the settled copy")
	require.LessOrEqual(t, visited.Load(), int64(16),
		"an ACK after a compaction must not walk the rewritten segment")
	t.Logf("an ACK after a compaction walked %d inventory entries", visited.Load())
}

// TestSegmentAckWithoutAnInventorySettlesNothing pins the F10 fix: a segment
// with NO inventory settles nothing and advances no counter.
//
// The old code had a compatibility branch for "synthetic legacy fixtures
// without an inventory": it set settled = 1, which advanced seg.deletedCount
// WITHOUT settling anything, so the legacy counter and the inventory's
// deadBytes/unacked disagreed about how much of the segment was dead. The
// branch is gone; a segment without an inventory settles nothing, and the
// counters agree.
func TestSegmentAckWithoutAnInventorySettlesNothing(t *testing.T) {
	queue := "no-inventory"
	qs := byTagTestQueueSegments(queue)

	seg := byTagTestSegment(1, nil)
	// The legacy index the removed branch consulted.
	seg.index[7] = 42
	qs.currentSegment = seg

	require.False(t, qs.acknowledgeInSegment(7),
		"a segment with no inventory must settle nothing")
	require.Zero(t, seg.deletedCount.Load(),
		"a segment with no inventory must not advance the legacy counter")
	require.Zero(t, seg.messageCount.Load())
}
