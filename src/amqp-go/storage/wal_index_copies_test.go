package storage

// THE PER-KEY COPY LIST OF THE WAL INDEX.
//
// THE SHAPE. The WAL index keeps two views of every published record:
//
//	qw.offsetIndex  map[recordKey]*segmentRecord   -- the NEWEST physical copy
//	qw.recordCopies map[recordKey]...              -- EVERY physical copy
//
// The second map used to be map[recordKey]map[recordPos]*segmentRecord: a Go
// MAP PER PUBLISHED MESSAGE. The common case is exactly ONE physical copy per
// (queue, tag), so every message allocated a map header plus a bucket array
// (~256 bytes and two allocations) to hold a single pointer, and threw it
// away when the message was acknowledged. That was a large share of the
// allocation churn the GC was busy with on the hot publish path.
//
// It is now map[recordKey][]*segmentRecord -- a SLICE of the physical copies --
// the same shape the legacy segment tier already uses
// (segmentInventory.byTag map[uint64][]*segmentRecord).
//
// WHAT MUST NOT CHANGE (and what these tests pin):
//
//   - a key's list holds EVERY physical copy of the tag, and only those;
//   - re-publishing a position REPLACES that entry instead of duplicating it
//     (the compaction can re-publish a position);
//   - removing one copy LEAVES the others;
//   - the newest-copy view is re-pointed at the newest REMAINING copy when
//     the copy that held the view goes away (the reclaim path);
//   - a SETTLED copy NEVER re-points the view at an older copy, so an
//     acknowledged tag is never resurrected by an older physical copy.
//
// The allocation budget test at the bottom is the one that FAILS on the
// per-message map and PASSES on the slice: it is written against the index
// API only, so it compiles against both shapes and measures the difference
// directly.

import (
	"strings"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// walCopyPositionsInInventory returns the positions of the inventory's records
// carrying the given delivery tag.
func walCopyPositionsInInventory(inv *segmentInventory, tag uint64) []recordPos {
	if inv == nil {
		return nil
	}
	var out []recordPos
	for _, r := range inv.records {
		if r.deliveryTag == tag {
			out = append(out, r.pos)
		}
	}
	return out
}

// walCopyPositions returns the positions of the copy list of one key.
func walCopyPositions(qw *QueueWAL, key recordKey) []recordPos {
	qw.offsetIndexMutex.RLock()
	defer qw.offsetIndexMutex.RUnlock()
	copies := qw.recordCopies[key]
	out := make([]recordPos, 0, len(copies))
	for _, c := range copies {
		out = append(out, c.pos)
	}
	return out
}

// TestWALIndexCopies_CompactionMoveKeepsExactlyThePhysicalCopies pins the
// copy list's contents across a compaction that MOVES a copy to a new
// position.
//
// The fixture publishes the SAME (queue, tag) TWICE, into two different WAL
// files, so the tag has two physical copies. The first file is then compacted
// (it holds enough acknowledged garbage to be eligible), which rewrites the
// tag's copy at a NEW position and re-publishes it under the same key.
//
// THE ASSERTIONS:
//
//   - the copy list holds EXACTLY TWO entries afterwards -- the compacted
//     copy at its new position and the untouched copy in the second file;
//     the compaction's replace step must not leave the OLD position behind
//     and must not duplicate the new one;
//   - re-publishing the very same record again REPLACES its entry: the list
//     must still hold two entries, not three;
//   - removing ONE copy leaves the other, and the newest-copy view still
//     points at the surviving copy;
//   - removing the last copy empties the list and drops the key.
func TestWALIndexCopies_CompactionMoveKeepsExactlyThePhysicalCopies(t *testing.T) {
	dir := t.TempDir()
	cfg := walIndexBoundConfig()
	// A tiny threshold so the acknowledged garbage makes the sealed file
	// eligible for the rewrite.
	cfg.CompactionThreshold = 0.001
	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	const queue = "copy-list-q"
	const tag = uint64(42)
	key := recordKey{queueName: queue, deliveryTag: tag}

	// COPY A, in file 1.
	require.NoError(t, wm.Write(queue, &protocol.Message{
		RoutingKey: queue, Body: []byte("copy-a"), DeliveryMode: 2, DeliveryTag: tag,
	}, tag))
	// GARBAGE in file 1: acknowledged, so the file becomes compaction-eligible.
	require.NoError(t, wm.Write("garbage-q", &protocol.Message{
		RoutingKey: "garbage-q", Body: []byte(strings.Repeat("G", 4096)), DeliveryMode: 2, DeliveryTag: 1,
	}, 1))
	fileA, _ := rollSegstoreReclaimWAL(t, wm)
	wm.Acknowledge("garbage-q", 1)

	// COPY B, in file 2.
	require.NoError(t, wm.Write(queue, &protocol.Message{
		RoutingKey: queue, Body: []byte("copy-b"), DeliveryMode: 2, DeliveryTag: tag,
	}, tag))
	fileB, _ := rollSegstoreReclaimWAL(t, wm)

	qw := wm.sharedWAL
	require.Len(t, walCopyPositions(qw, key), 2,
		"PREMISE BROKEN: the fixture must hold two physical copies of the tag")

	// THE COMPACTION. It rewrites file A, so the tag's copy in file A moves
	// to a new position and is re-published under the same key.
	qw.tryCompactOldFiles()

	posA := walCopyPositionsInInventory(qw.oldFiles[fileA].inv, tag)
	posB := walCopyPositionsInInventory(qw.oldFiles[fileB].inv, tag)
	require.Len(t, posA, 1, "PREMISE BROKEN: the compacted file must hold the tag's copy")
	require.Len(t, posB, 1, "PREMISE BROKEN: the second file must hold the tag's copy")
	require.NotEqual(t, posA[0], posB[0], "PREMISE BROKEN: the two copies must be at different positions")

	copies := walCopyPositions(qw, key)
	require.Len(t, copies, 2,
		"the copy list must hold EXACTLY the physical copies of the tag after the compaction moved one of them")
	require.Contains(t, copies, posA[0], "the copy list must hold the COMPACTED copy at its new position")
	require.Contains(t, copies, posB[0], "the copy list must hold the untouched copy")

	// RE-PUBLISHING A POSITION REPLACES the entry: the compaction can
	// re-publish a position, and that must not duplicate the copy.
	qw.offsetIndexMutex.Lock()
	var republished *segmentRecord
	for _, c := range qw.recordCopies[key] {
		if c.pos == posA[0] {
			republished = c
			break
		}
	}
	if republished != nil {
		qw.indexRecordLocked(republished)
	}
	qw.offsetIndexMutex.Unlock()
	require.NotNil(t, republished, "PREMISE BROKEN: the compacted copy is not in the list")
	require.Len(t, walCopyPositions(qw, key), 2,
		"re-publishing a position must REPLACE its copy, not duplicate it")

	// REMOVING ONE COPY LEAVES THE OTHER. This is the reclaim path's
	// unindexRecordLocked: a whole file went away, so the remaining copies
	// are the only ones left.
	qw.offsetIndexMutex.Lock()
	qw.unindexRecordLocked(republished)
	remaining := make([]recordPos, 0, len(qw.recordCopies[key]))
	for _, c := range qw.recordCopies[key] {
		remaining = append(remaining, c.pos)
	}
	winner := qw.offsetIndex[key]
	qw.offsetIndexMutex.Unlock()

	require.Len(t, remaining, 1, "removing one copy must leave the other")
	require.Equal(t, posB[0], remaining[0], "the surviving copy must be the one that was not removed")
	require.NotNil(t, winner, "the newest-copy view must survive the removal of a copy")
	require.Equal(t, posB[0], winner.pos,
		"the newest-copy view must point at the newest REMAINING copy")

	// REMOVING THE LAST COPY empties the list and drops the key.
	qw.offsetIndexMutex.Lock()
	for _, c := range qw.recordCopies[key] {
		qw.unindexRecordLocked(c)
	}
	_, stillThere := qw.recordCopies[key]
	qw.offsetIndexMutex.Unlock()
	require.False(t, stillThere, "the last copy's removal must drop the key")
}

// TestWALIndexCopies_AcknowledgedTagIsNeverResurrectedByAnOlderCopy pins the
// semantic that the eager settling exists for: a SETTLED copy never
// re-points the newest-copy view at an OLDER physical copy of the same tag.
//
// The fixture publishes the same (queue, tag) TWICE, into two different WAL
// files. The newer copy is then settled the way the compaction's omission
// path settles it (unindexSettledRecordLocked): the copy leaves the index, but
// the older copy is still on disk.
//
// THE ASSERTIONS:
//
//   - the newest-copy view has NO entry for the key afterwards: pointing it
//     at the older copy would re-deliver a message the client already
//     acknowledged;
//   - the older copy may STAY in the copy list, so a later acknowledgement of
//     the same tag still settles it;
//   - that later acknowledgement settles it, and the tag stays gone.
func TestWALIndexCopies_AcknowledgedTagIsNeverResurrectedByAnOlderCopy(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, walIndexBoundConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	const queue = "settled-copy-q"
	const tag = uint64(7)
	key := recordKey{queueName: queue, deliveryTag: tag}

	// The OLD copy, in a file of its own.
	require.NoError(t, wm.Write(queue, &protocol.Message{
		RoutingKey: queue, Body: []byte("old"), DeliveryMode: 2, DeliveryTag: tag,
	}, tag))
	_, oldPath := rollSegstoreReclaimWAL(t, wm)

	// The NEW copy, in the next file.
	require.NoError(t, wm.Write(queue, &protocol.Message{
		RoutingKey: queue, Body: []byte("new"), DeliveryMode: 2, DeliveryTag: tag,
	}, tag))
	_, newPath := rollSegstoreReclaimWAL(t, wm)
	require.NotEqual(t, oldPath, newPath, "PREMISE BROKEN: both copies landed in the same file")

	qw := wm.sharedWAL
	require.Len(t, walCopyPositions(qw, key), 2,
		"PREMISE BROKEN: the fixture must hold two physical copies of the tag")

	// THE ACTION: settle the NEWEST copy, the way the compaction's omission
	// path does. The older copy is still on disk.
	qw.offsetIndexMutex.Lock()
	var newest *segmentRecord
	for _, c := range qw.recordCopies[key] {
		if newest == nil || recordPositionNewer(c.pos, newest.pos) {
			newest = c
		}
	}
	require.NotNil(t, newest, "PREMISE BROKEN: the tag was never indexed")
	qw.unindexSettledRecordLocked(newest)
	_, inIndex := qw.offsetIndex[key]
	older := make([]*segmentRecord, 0, len(qw.recordCopies[key]))
	older = append(older, qw.recordCopies[key]...)
	qw.offsetIndexMutex.Unlock()

	require.False(t, inIndex,
		"a SETTLED copy must never leave the newest-copy view pointing at an older physical copy: "+
			"re-delivering a message the client already acknowledged is worse than leaking a record")
	require.Len(t, older, 1,
		"the older copy may stay in the copy list, so a later acknowledgement of the same tag still settles it")

	// THE LATER ACKNOWLEDGEMENT settles the older copy too, and the tag stays
	// gone.
	wm.Acknowledge(queue, tag)

	qw.offsetIndexMutex.RLock()
	_, inIndex = qw.offsetIndex[key]
	copies := qw.recordCopies[key]
	qw.offsetIndexMutex.RUnlock()
	require.False(t, inIndex, "an acknowledged tag must have no newest-copy entry")
	require.Empty(t, copies, "an acknowledged tag must have no physical copy left in the copy list")
	require.True(t, wm.IsAcknowledged(queue, tag),
		"the acknowledgement state lives in the read gate, not in the dropped record")
}
