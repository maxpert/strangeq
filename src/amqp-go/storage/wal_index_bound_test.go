package storage

// The WAL index must be BOUNDED BY THE IN-FLIGHT SET, not by the number of
// messages ever published.
//
// THE MEASURED DEFECT THIS FILE EXISTS TO CLOSE. The segstore WAL tier keeps
// two index maps:
//
//	qw.offsetIndex  map[recordKey]*segmentRecord              (the newest copy)
//	qw.recordCopies map[recordKey][]*segmentRecord             (every copy)
//
// A record was removed from them ONLY when its whole WAL FILE was reclaimed,
// and the reclaim sweep runs on the cleanup tick (default 300000 ms = five
// minutes) while a 64 MiB WAL file at F4 rolls about 28 times per SECOND. So
// the maps grew without bound: 861,000 live records (~316 bytes each, ~260 MB)
// after a 30 s window, all of them ALREADY ACKNOWLEDGED. Because Go's GC
// targets twice the live heap, that ~260 MB became ~520 MB of RSS: the new
// build's F4 peak RSS was 2236 MB against the old build's 1029 MB and
// RabbitMQ Classic's 1628 MB.
//
// It was also a throughput defect: the index was written on the hot publish
// path under a lock, so every publish paid an insert into two ever-growing
// maps (plus the rehash and the GC pressure that came with them).
//
// THE FIX. A settled (acknowledged) record leaves the index EAGERLY. What
// stays is the set of records that are NOT yet acknowledged, so the index is
// bounded by the in-flight set instead of by the total ever published.
//
// THE INVARIANT THESE TESTS PIN:
//
//   - BEFORE the acknowledgements, the index holds every published record
//     (otherwise the bound below would be vacuous);
//   - AFTER every record is acknowledged, the index holds O(1) records;
//   - the acknowledgement STATE survives the record's removal: it lives in
//     the queue-scoped roaring64 bitmap, not in the dropped record;
//   - an acknowledged tag is NEVER resurrected by an older physical copy that
//     is still on disk;
//   - the reclaim sweep still unlinks a fully acknowledged file;
//   - the compaction still rewrites a file with exactly the unacknowledged
//     records, each of them readable with its body intact.

import (
	"errors"
	"io/fs"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// walIndexBoundConfig is the config these fixtures use: the background
// maintenance intervals are long enough that NOTHING removes a record from the
// index behind the test's back, so every removal the test observes is the one
// it caused.
func walIndexBoundConfig() WALConfig {
	cfg := DefaultWALConfig()
	cfg.CleanupInterval = time.Hour
	cfg.CheckpointInterval = time.Hour
	return cfg
}

// walIndexCounts reports how many records the WAL index holds: the number of
// published keys in the newest-copy map, and the number of physical copies in
// the complete-copy map.
//
// It reads the REAL maps under the index lock, so the bound it reports is the
// bound the production code actually maintains.
func walIndexCounts(qw *QueueWAL) (indexed int, copies int) {
	qw.offsetIndexMutex.RLock()
	defer qw.offsetIndexMutex.RUnlock()
	indexed = len(qw.offsetIndex)
	for _, set := range qw.recordCopies {
		copies += len(set)
	}
	return indexed, copies
}

// TestWALIndexBound_AcknowledgedRecordsLeaveTheIndex is the headline bound.
//
// 20,000 messages are published through the WAL and every one of them is
// acknowledged. The index must then hold O(1) records -- not 20,000.
//
// The "before" half is what makes the "after" half evidence: the index really
// did hold all 20,000 records while they were unacknowledged, so the bound is
// a property of the acknowledgement path and not of a fixture that never
// indexed anything.
func TestWALIndexBound_AcknowledgedRecordsLeaveTheIndex(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, walIndexBoundConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	const queue = "bound-q"
	const total = 20000

	// Publish through the ASYNC path so the group commit can batch the
	// publishes: 20,000 synchronous fsyncs would test the disk, not the index.
	var durable atomic.Int64
	var writeErr atomic.Value
	for i := uint64(1); i <= total; i++ {
		wm.WriteAsync(queue, &protocol.Message{
			RoutingKey:   queue,
			Body:         []byte("payload"),
			DeliveryMode: 2,
			DeliveryTag:  i,
		}, i, func(err error) {
			if err != nil {
				writeErr.Store(err)
				return
			}
			durable.Add(1)
		})
	}
	require.Eventually(t, func() bool { return durable.Load() == total },
		120*time.Second, 10*time.Millisecond, "every publish must become durable and indexed")
	if v := writeErr.Load(); v != nil {
		t.Fatalf("a publish failed: %v", v)
	}

	qw := wm.sharedWAL
	indexed, copies := walIndexCounts(qw)
	require.Equal(t, total, indexed,
		"PREMISE BROKEN: the index must hold every published record while they are unacknowledged")
	require.Equal(t, total, copies,
		"PREMISE BROKEN: the copy map must hold every published record while they are unacknowledged")

	for i := uint64(1); i <= total; i++ {
		wm.Acknowledge(queue, i)
	}

	indexed, copies = walIndexCounts(qw)
	require.LessOrEqual(t, indexed, 64,
		"the WAL index must be bounded by the IN-FLIGHT set, not by the total ever published: "+
			"%d records are still indexed after all %d messages were acknowledged", indexed, total)
	require.LessOrEqual(t, copies, 64,
		"the WAL copy map must be bounded by the IN-FLIGHT set, not by the total ever published: "+
			"%d physical copies are still indexed after all %d messages were acknowledged", copies, total)
}

// TestWALIndexBound_AckStateLivesInTheBitmap proves the acknowledgement
// STATE does not live in the record the eager removal drops.
//
// The ack state is a queue-scoped roaring64 bitmap (ackBitmaps), which is
// independent of the physical index. So dropping the record must not drop the
// acknowledgement: IsAcknowledged must still answer true, and the record must
// be gone from both index maps.
func TestWALIndexBound_AckStateLivesInTheBitmap(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, walIndexBoundConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	const queue = "ack-state-q"
	const tag = uint64(4242)

	require.NoError(t, wm.Write(queue, &protocol.Message{
		RoutingKey:   queue,
		Body:         []byte("body"),
		DeliveryMode: 2,
		DeliveryTag:  tag,
	}, tag))

	qw := wm.sharedWAL
	key := recordKey{queueName: queue, deliveryTag: tag}
	qw.offsetIndexMutex.RLock()
	_, indexed := qw.offsetIndex[key]
	qw.offsetIndexMutex.RUnlock()
	require.True(t, indexed, "PREMISE BROKEN: the record was never indexed")

	wm.Acknowledge(queue, tag)

	require.True(t, wm.IsAcknowledged(queue, tag),
		"the acknowledgement state lives in the roaring64 bitmap, so dropping the record must not drop it")
	qw.offsetIndexMutex.RLock()
	_, indexed = qw.offsetIndex[key]
	copies := len(qw.recordCopies[key])
	qw.offsetIndexMutex.RUnlock()
	require.False(t, indexed, "an acknowledged record must not stay in the newest-copy index")
	require.Zero(t, copies, "an acknowledged record must not stay in the physical-copy index")
}

// TestWALIndexBound_AcknowledgedTagIsNotResurrectedByAnOlderCopy pins the
// CRITICAL semantic of the eager removal.
//
// The same (queue, tag) is written TWICE, into two different WAL files, so the
// older physical copy is still on disk when the acknowledgement lands. The
// removal of the settled record must NOT re-point the tag at that older copy:
// re-delivering a message the client already acknowledged is worse than
// leaking a record.
//
// So, after the acknowledgement:
//
//   - the tag has NO index entry at all (neither the settled copy nor the
//     older one);
//   - recovery does not serve it;
//   - the storage read path reports it as not found.
func TestWALIndexBound_AcknowledgedTagIsNotResurrectedByAnOlderCopy(t *testing.T) {
	dir := t.TempDir()
	ds, err := NewDisruptorStorageWithDataDir(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	const queue = "dup-q"
	const tag = uint64(7)

	ds.getOrCreateQueueHot(queue)
	wm := ds.wal

	// The OLD copy, in a file of its own.
	require.NoError(t, wm.Write(queue, &protocol.Message{
		RoutingKey:   queue,
		Body:         []byte("OLD"),
		DeliveryMode: 2,
		DeliveryTag:  tag,
	}, tag))
	_, oldPath := rollSegstoreReclaimWAL(t, wm)

	// The NEW copy, in the next file.
	require.NoError(t, wm.Write(queue, &protocol.Message{
		RoutingKey:   queue,
		Body:         []byte("NEW"),
		DeliveryMode: 2,
		DeliveryTag:  tag,
	}, tag))
	_, newPath := rollSegstoreReclaimWAL(t, wm)

	qw := wm.sharedWAL
	key := recordKey{queueName: queue, deliveryTag: tag}
	qw.offsetIndexMutex.RLock()
	winner := qw.offsetIndex[key]
	copies := len(qw.recordCopies[key])
	qw.offsetIndexMutex.RUnlock()
	require.NotNil(t, winner, "PREMISE BROKEN: the tag was never indexed")
	require.Equal(t, 2, copies, "PREMISE BROKEN: the fixture must hold two physical copies of the tag")
	require.NotEqual(t, oldPath, newPath, "PREMISE BROKEN: both copies landed in the same file")

	// THE ACTION.
	wm.Acknowledge(queue, tag)

	qw.offsetIndexMutex.RLock()
	_, indexed := qw.offsetIndex[key]
	qw.offsetIndexMutex.RUnlock()
	require.False(t, indexed,
		"an acknowledged tag must have NO index entry: re-pointing it at the older physical copy "+
			"would re-deliver a message the client already acknowledged")

	// RECOVERY must not serve it.
	recovered, err := wm.RecoverFromWAL()
	require.NoError(t, err)
	for _, rm := range recovered {
		require.NotEqual(t, tag, rm.Offset,
			"an acknowledged tag must not be recovered: the older copy is still on disk, and "+
				"recovery must not treat its presence as evidence that it was never acknowledged")
	}

	// THE STORAGE READ PATH must report it as not found.
	_, err = ds.GetMessage(queue, tag)
	require.ErrorIs(t, err, interfaces.ErrMessageNotFound,
		"an acknowledged tag must not be readable")

	// The bytes are still on disk: the removal is an INDEX removal, not a
	// deletion of durable data.
	for _, p := range []string{oldPath, newPath} {
		_, statErr := os.Stat(p)
		require.NoError(t, statErr, "the durable bytes must not be deleted by an acknowledgement")
	}
}

// TestWALIndexBound_ReclaimUnlinksTheFileAndLeavesTheIndexEmpty proves the
// reclaim path still works after the eager removal.
//
// A fully acknowledged sealed file is unlinked by the reclaim sweep, and the
// index stays empty: with the eager removal the sweep finds nothing left to
// unindex (the inventory holds only unacknowledged records, and a reclaimable
// file has none), and that must remain correct.
func TestWALIndexBound_ReclaimUnlinksTheFileAndLeavesTheIndexEmpty(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, walIndexBoundConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	const queue = "reclaim-q"
	writeSegstoreReclaimRecord(t, wm, queue, 1, "one")
	writeSegstoreReclaimRecord(t, wm, queue, 2, "two")
	_, path := rollSegstoreReclaimWAL(t, wm)

	wm.Acknowledge(queue, 1)
	wm.Acknowledge(queue, 2)

	qw := wm.sharedWAL
	indexed, copies := walIndexCounts(qw)
	require.Zero(t, indexed, "every acknowledged record must have left the index")
	require.Zero(t, copies, "every acknowledged record must have left the copy map")

	qw.tryReclaimOldFiles()

	_, err = os.Stat(path)
	require.True(t, errors.Is(err, fs.ErrNotExist),
		"a fully acknowledged sealed file must still be reclaimed: %v", err)

	indexed, copies = walIndexCounts(qw)
	require.Zero(t, indexed, "the reclaim sweep must leave the index empty")
	require.Zero(t, copies, "the reclaim sweep must leave the copy map empty")
}

// TestWALIndexBound_CompactionKeepsExactlyTheUnacknowledgedRecords proves the
// compaction still produces a CORRECT file after the eager removal.
//
// The compaction enumerates the inventory's records and separates the live
// ones from the acknowledged ones. With the eager removal the acknowledged
// ones are gone from the inventory ALREADY, so the rewrite must carry over
// exactly the unacknowledged records -- and each of them must still be
// readable, with its body intact.
func TestWALIndexBound_CompactionKeepsExactlyTheUnacknowledgedRecords(t *testing.T) {
	dir := t.TempDir()
	cfg := walIndexBoundConfig()
	// A tiny threshold so the acknowledged bytes make the sealed file eligible
	// for the rewrite.
	cfg.CompactionThreshold = 0.001
	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	const queue = "compact-q"
	live := map[uint64]string{
		1: "live-one",
		2: "live-two",
		3: strings.Repeat("L", 1024),
	}
	dead := map[uint64]string{
		1: strings.Repeat("D", 8192),
		2: strings.Repeat("D", 8192),
	}
	for tag, body := range live {
		writeSegstoreReclaimRecord(t, wm, queue, tag, body)
	}
	for tag, body := range dead {
		writeSegstoreReclaimRecord(t, wm, "dead-q", tag, body)
	}
	fileNum, path := rollSegstoreReclaimWAL(t, wm)

	for tag := range dead {
		wm.Acknowledge("dead-q", tag)
	}

	qw := wm.sharedWAL
	// THE EAGER SETTLING IS THE PRECONDITION of the rewrite: the sealed
	// file's inventory holds ONLY the records that are not acknowledged, so
	// the rewrite carries over exactly the live ones and the acknowledged
	// ones are gone from the file rather than being filtered out of it.
	inv := qw.oldFiles[fileNum].inv
	require.NotNil(t, inv)
	require.Len(t, inv.records, len(live),
		"the sealed inventory must hold only the unacknowledged records")

	qw.tryCompactOldFiles()

	// The compacted file holds EXACTLY the unacknowledged records.
	counts, err := WALRecordCountsForTest(dir)
	require.NoError(t, err)
	require.Equal(t, len(live), counts.Messages,
		"the compacted file must hold exactly the unacknowledged records")

	// Each of them is readable, with its body intact.
	for tag, body := range live {
		msg, rerr := wm.Read(queue, tag)
		require.NoError(t, rerr, "live tag %d must survive the compaction", tag)
		require.Equal(t, body, string(msg.Body), "live tag %d must keep its body", tag)
	}

	// The acknowledged ones are gone from the file and from the index.
	for tag := range dead {
		_, rerr := wm.Read("dead-q", tag)
		require.Error(t, rerr, "acknowledged tag %d must not be readable", tag)
	}
	require.NotContains(t, string(compactionReadFile(t, path)), strings.Repeat("D", 64),
		"the acknowledged bodies must be gone from the compacted file")
}
