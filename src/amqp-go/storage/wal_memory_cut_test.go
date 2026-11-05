package storage

// WS-MEMORY-CUT §3.1 / §7: the WAL-tier record bookkeeping must be bounded by
// the IN-FLIGHT SET, not by the WAL FILE.
//
// THE MEASURED DEFECT. The per-file inventory (`inv.records`) is deliberately
// never settled by the ack path: a file's records carry tags of many different
// shards, so two acknowledgements of two tags in the same file would mutate the
// same map under two different shard locks (see wal_index_shards.go). It is
// dropped only when the whole FILE is reclaimed, so a 512 MiB WAL file holds
// every committed record of the file -- ~0.5 M records on F1, ~19.8 MB of map
// plus 61.7 MB of pinned per-batch descriptor arrays -- even though every one
// of those records was already acknowledged.
//
// THE FIX (S1a/S1b). The ack path already holds the settled record; it queues
// it on its own shard. The committer then deletes the queued records from the
// CURRENT file's inventory under fileMutex, taking ONE shard's write lock at a
// time (the lock order the committer already uses), gated on the record being
// acknowledged. The inventory's live count therefore oscillates around the
// unacknowledged set, and the map's bucket arrays stop growing with the file.
//
// WHAT MUST NOT CHANGE (the guards below):
//   - an UNACKNOWLEDGED record is never deleted (T1's second half);
//   - recovery serves exactly the unacknowledged tags (T3);
//   - the reclaim predicate still fires on unacked == 0 (T4);
//   - the lock order fileMutex -> one shard at a time is preserved (T2).

import (
	"errors"
	"io/fs"
	"os"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// memoryCutConfig is the fixture's config: nothing removes a record from the
// index behind the test's back, and no fsync makes 4000 synchronous writes
// slow. The bookkeeping under test is in memory, not on disk.
func memoryCutConfig() WALConfig {
	cfg := walIndexBoundConfig()
	cfg.SyncDisabled = true
	return cfg
}

func memoryCutMessage(queue string, tag uint64) *protocol.Message {
	return &protocol.Message{
		RoutingKey:   queue,
		Body:         []byte("payload"),
		DeliveryMode: 2,
		DeliveryTag:  tag,
	}
}

// TestWALMemoryCut_InventoryBoundedByInFlightSet is T1 (the headline).
//
// 4000 records are written and ALL 4000 tags acknowledged; one more (small)
// batch is then written. The inventory must hold AT MOST that last batch's
// records -- not the ~4000 settled ones the file still holds on disk.
//
// The "before" half is what makes the "after" half evidence: the inventory
// really did hold all 4000 while they were unacknowledged, so the bound is a
// property of the new drain and not of a fixture that never filled the map.
func TestWALMemoryCut_InventoryBoundedByInFlightSet(t *testing.T) {
	wm, err := NewWALManagerWithConfig(t.TempDir(), memoryCutConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	const queue = "memcut-q"
	const total = 4000
	for i := uint64(1); i <= total; i++ {
		require.NoError(t, wm.Write(queue, memoryCutMessage(queue, i), i))
	}

	qw := wm.sharedWAL
	current, _ := qw.inventorySizesForTest()
	require.Equal(t, total, current,
		"PREMISE BROKEN: the inventory must hold every written record while they are unacknowledged")

	for i := uint64(1); i <= total; i++ {
		wm.Acknowledge(queue, i)
	}

	// One more small batch: the committer drains the settled records.
	require.NoError(t, wm.Write(queue, memoryCutMessage(queue, total+1), total+1))

	current, _ = qw.inventorySizesForTest()
	require.LessOrEqual(t, current, 1,
		"the inventory must be bounded by the IN-FLIGHT set, not by the file: %d records are "+
			"still in the active inventory after all %d were acknowledged", current, total)
	require.GreaterOrEqual(t, current, 1,
		"the one unacknowledged record just written must stay in the inventory")
	require.EqualValues(t, total, qw.inventoryDrains.Load(),
		"the drain must have removed exactly the acknowledged records")
}

// TestWALMemoryCut_UnackedRecordIsNeverDeleted pins the acked gate: a record
// that was written and NOT acknowledged must survive the drain even when its
// neighbours were acknowledged.
func TestWALMemoryCut_UnackedRecordIsNeverDeleted(t *testing.T) {
	wm, err := NewWALManagerWithConfig(t.TempDir(), memoryCutConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	const queue = "memcut-unacked-q"
	const total = 200
	for i := uint64(1); i <= total; i++ {
		require.NoError(t, wm.Write(queue, memoryCutMessage(queue, i), i))
	}
	// Acknowledge all but the last one.
	for i := uint64(1); i < total; i++ {
		wm.Acknowledge(queue, i)
	}
	// One more write drives the drain.
	require.NoError(t, wm.Write(queue, memoryCutMessage(queue, total+1), total+1))

	qw := wm.sharedWAL
	current, _ := qw.inventorySizesForTest()
	require.Equal(t, 2, current,
		"only the acknowledged records may leave the inventory; the unacknowledged record and "+
			"the newly written one must both stay")

	// The unacknowledged record is still readable and still in the inventory.
	msg, rerr := wm.Read(queue, total)
	require.NoError(t, rerr, "an unacknowledged record must still be readable")
	require.Equal(t, "payload", string(msg.Body))

	qw.fileMutex.Lock()
	held := false
	for _, r := range qw.currentInv.records {
		if r.queueName == queue && r.deliveryTag == total {
			held = true
			break
		}
	}
	qw.fileMutex.Unlock()
	require.True(t, held, "the unacknowledged record must still be in the active inventory")
}

// TestWALMemoryCut_RecoveryServesExactlyTheUnacknowledgedTags is T3.
//
// The delete touches the INVENTORY only; recovery reads the FILES plus the
// index, so it must serve exactly the unacknowledged tags -- no duplicates, no
// losses. This is green on the pre-cut base too: it is the guard that the new
// inventory mutation did not reach the boot scan.
func TestWALMemoryCut_RecoveryServesExactlyTheUnacknowledgedTags(t *testing.T) {
	wm, err := NewWALManagerWithConfig(t.TempDir(), memoryCutConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	const queue = "memcut-recover-q"
	// Three "batches" of three tags; batch 1 is acknowledged.
	for i := uint64(1); i <= 9; i++ {
		require.NoError(t, wm.Write(queue, memoryCutMessage(queue, i), i))
	}
	for i := uint64(1); i <= 3; i++ {
		wm.Acknowledge(queue, i)
	}
	// A later write drives the drain; recovery must be unaffected by it.
	require.NoError(t, wm.Write(queue, memoryCutMessage(queue, 10), 10))

	recovered, rerr := wm.RecoverFromWAL()
	require.NoError(t, rerr)
	got := map[uint64]bool{}
	for _, rm := range recovered {
		require.False(t, got[rm.Offset], "recovery returned tag %d twice", rm.Offset)
		got[rm.Offset] = true
	}
	want := []uint64{4, 5, 6, 7, 8, 9, 10}
	require.Len(t, got, len(want),
		"recovery must serve exactly the unacknowledged tags; got %v", got)
	for _, tag := range want {
		require.True(t, got[tag], "recovery must serve unacknowledged tag %d", tag)
	}
	for _, tag := range []uint64{1, 2, 3} {
		require.False(t, got[tag], "recovery must NOT serve acknowledged tag %d", tag)
	}
}

// TestWALMemoryCut_ReclaimPredicateStillFires is T4.
//
// A sealed file whose every record is acknowledged must still be unlinked by
// the reclaim sweep, with the new deletes active: the unacked/deadBytes
// accounting is atomic and independent of the map, so unacked == 0 must still
// authorize the reclaim.
func TestWALMemoryCut_ReclaimPredicateStillFires(t *testing.T) {
	wm, err := NewWALManagerWithConfig(t.TempDir(), memoryCutConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	const queue = "memcut-reclaim-q"
	writeSegstoreReclaimRecord(t, wm, queue, 1, "one")
	writeSegstoreReclaimRecord(t, wm, queue, 2, "two")
	fileNum, path := rollSegstoreReclaimWAL(t, wm)

	wm.Acknowledge(queue, 1)
	wm.Acknowledge(queue, 2)

	qw := wm.sharedWAL
	qw.tryReclaimOldFiles()

	_, statErr := os.Stat(path)
	require.True(t, errors.Is(statErr, fs.ErrNotExist),
		"a fully acknowledged sealed file must still be reclaimed: %v", statErr)
	qw.oldFilesMutex.RLock()
	_, registered := qw.oldFiles[fileNum]
	qw.oldFilesMutex.RUnlock()
	require.False(t, registered, "the reclaimed file must leave the old-files registry")
}

// TestWALMemoryCut_DrainDoesNotTouchSealedInventories pins the boundary the
// compaction relies on: a SEALED file's inventory is immutable (the compaction
// snapshots it with no lock), so the drain must delete only from the ACTIVE
// file's inventory even when the settled records belong to a rolled file.
func TestWALMemoryCut_DrainDoesNotTouchSealedInventories(t *testing.T) {
	wm, err := NewWALManagerWithConfig(t.TempDir(), memoryCutConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	const queue = "memcut-sealed-q"
	writeSegstoreReclaimRecord(t, wm, queue, 1, "sealed-one")
	fileNum, _ := rollSegstoreReclaimWAL(t, wm)
	wm.Acknowledge(queue, 1)

	// A write in the NEW file drives the drain; the settled record belongs to
	// the SEALED file and must stay in that file's inventory.
	require.NoError(t, wm.Write(queue, memoryCutMessage(queue, 2), 2))

	qw := wm.sharedWAL
	qw.oldFilesMutex.RLock()
	sealed := qw.oldFiles[fileNum].inv
	qw.oldFilesMutex.RUnlock()
	require.NotNil(t, sealed)
	require.Len(t, sealed.records, 1,
		"a sealed file's inventory must not be mutated by the drain: the compaction snapshots it "+
			"without a lock")
}
