package storage

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// SEGSTORE V1 — WHAT "LOG COMPACTION" MEANS NOW.
//
// THE CHECKPOINT IS GONE. The WAL no longer copies records into the legacy
// segment tier, so nothing moves out of the WAL: a record is written to the
// WAL, served from the WAL, acknowledged in the WAL, and its bytes are
// released either by the RECLAIM SWEEP (a file whose records are all
// acknowledged is unlinked whole) or by the COMPACTION (a sealed file whose
// acknowledged bytes are the majority is rewritten with the records that are
// still live).
//
// THE TESTS IN THIS FILE WERE VACUOUS. They called performCheckpoint() -- a
// NO-OP STUB since checkpoint copying was removed -- and then asserted that
// the number of old WAL files had gone DOWN. The count went down because the
// RECLAIM SWEEP ran on its own ticker, so the assertions passed no matter
// what the code under test did (deleting both performCheckpoint() calls left
// all four tests passing).
//
// WHAT REPLACES THEM: the invariants segstore v1 actually ships.
//
//   - a durable write is in the WAL and is readable from it;
//   - acknowledging a record removes it from the WAL INDEX and stops it being
//     served, while the unacknowledged records stay readable;
//   - a ROLLED WAL file whose records are ALL acknowledged is UNLINKED by
//     qw.tryReclaimOldFiles(), and a rolled file that still holds an
//     unacknowledged record is NOT;
//   - a rolled file that is mostly garbage is COMPACTED by
//     qw.tryCompactOldFiles(), and the records that are still live survive;
//   - retention authorises nothing: age never deletes unacknowledged data.
//
// EVERY MAINTENANCE FUNCTION IS CALLED DIRECTLY, AND THE BACKGROUND CLEANUP
// TICK IS SET TO AN HOUR, so no assertion in this file can be satisfied by a
// ticker that happened to fire. There is no time.Sleep anywhere in this file:
// the durable write path blocks until the fsync, and the maintenance functions
// are invoked explicitly.

// logCompactionEngineConfig returns an engine configuration whose background
// maintenance tick is an hour away.
//
// THE TICK IS AN HOUR BECAUSE EVERY MAINTENANCE FUNCTION IN THIS FILE IS
// CALLED DIRECTLY. A background sweep can then never be what makes an
// assertion pass, and a test can never race one.
func logCompactionEngineConfig() interfaces.EngineConfig {
	return interfaces.EngineConfig{
		WALBatchSize:                100,
		WALBatchTimeoutMS:           5,
		WALFileSize:                 2048, // small: forces WAL file rolls
		WALChannelBuffer:            5000,
		WALCleanupCheckIntervalMS:   3600000, // 1 hour: no background sweep
		CompactionThreshold:         0.5,
		SegmentSize:                 4096,
		CompactionIntervalMS:        3600000,
		SegmentCheckpointIntervalMS: 3600000,
	}
}

// TestLogCompaction_FullLifecycle is the end-to-end lifecycle of the WAL tier
// in segstore v1: durable write → WAL → acknowledge → index and read
// invariants → restart.
//
// WHAT A MUTATION WOULD BREAK:
//
//   - if the eager settling stopped dropping an acknowledged record from the
//     WAL index, the "the acknowledged records are gone from the index"
//     assertions fail;
//   - if the WAL stopped serving the unacknowledged records, the "still
//     readable" assertions fail;
//   - if the durable bytes were lost across a restart, the "survives the
//     restart" assertions fail.
func TestLogCompaction_FullLifecycle(t *testing.T) {
	tmpDir := t.TempDir()
	engineCfg := logCompactionEngineConfig()

	ds, err := NewDisruptorStorageWithEngineConfig(tmpDir, engineCfg)
	require.NoError(t, err)
	qw := ds.wal.sharedWAL

	queues := []string{"orders", "events", "logs"}
	const perQueue = 30
	const acked = 24

	// === 1. EVERY DURABLE WRITE IS IN THE WAL AND IS READABLE FROM IT ===
	for _, queue := range queues {
		ds.getOrCreateQueueHot(queue)
		for i := 1; i <= perQueue; i++ {
			msg := &protocol.Message{
				Exchange:     "amq.direct",
				RoutingKey:   queue,
				Body:         []byte(fmt.Sprintf("message-%s-%d", queue, i)),
				DeliveryMode: 2, // Durable
				DeliveryTag:  uint64(i),
			}
			require.NoError(t, ds.StoreMessage(queue, msg),
				"failed to store message %d for queue %s", i, queue)
		}
	}
	for _, queue := range queues {
		for i := 1; i <= perQueue; i++ {
			msg, err := ds.wal.Read(queue, uint64(i))
			require.NoError(t, err,
				"message %d should be readable from the WAL for queue %s", i, queue)
			require.Contains(t, string(msg.Body), fmt.Sprintf("message-%s-%d", queue, i))
		}
	}

	// === 2. ACKNOWLEDGING A SHARE OF THE MESSAGES ===
	for _, queue := range queues {
		for i := 1; i <= acked; i++ {
			require.NoError(t, ds.DeleteMessage(queue, uint64(i)))
		}
	}

	// THE ACKNOWLEDGED RECORDS ARE GONE FROM THE WAL INDEX AND ARE NOT SERVED.
	for _, queue := range queues {
		for i := 1; i <= acked; i++ {
			key := recordKey{queueName: queue, deliveryTag: uint64(i)}
			qw.offsetIndexMutex.RLock()
			copies := qw.recordCopies[key]
			_, indexed := qw.offsetIndex[key]
			qw.offsetIndexMutex.RUnlock()
			require.Empty(t, copies,
				"an acknowledged record must leave the physical index of queue %s", queue)
			require.False(t, indexed,
				"an acknowledged record must leave the newest-copy index of queue %s", queue)
			// The PER-FILE INVENTORY is the compaction's copy set: a record
			// that is still in it would be copied into the rewritten file.
			require.False(t, walInventoryHolds(qw, queue, uint64(i)),
				"an acknowledged record must leave the file inventory of queue %s", queue)
			_, err := ds.wal.Read(queue, uint64(i))
			require.Error(t, err,
				"an acknowledged record must not be served any more (queue %s, tag %d)", queue, i)
		}
	}

	// THE UNACKNOWLEDGED ONES ARE STILL READABLE FROM THE WAL.
	for _, queue := range queues {
		for i := acked + 1; i <= perQueue; i++ {
			msg, err := ds.wal.Read(queue, uint64(i))
			require.NoError(t, err,
				"an unacknowledged record must still be readable from the WAL (queue %s, tag %d)", queue, i)
			require.Contains(t, string(msg.Body), fmt.Sprintf("message-%s-%d", queue, i))
		}
	}

	// === 3. THE UNACKNOWLEDGED RECORDS SURVIVE A RESTART ===
	//
	// Acknowledgement is PROCESS-VOLATILE: the ack bitmap is not durable, so
	// after a restart a record whose bytes are still on disk is served again
	// even though this incarnation acknowledged it. What the restart proves is
	// that the DURABLE BYTES are still there.
	require.NoError(t, ds.Close())

	ds2, err := NewDisruptorStorageWithEngineConfig(tmpDir, engineCfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds2.Close() })

	for _, queue := range queues {
		for i := acked + 1; i <= perQueue; i++ {
			msg, err := ds2.wal.Read(queue, uint64(i))
			require.NoError(t, err,
				"an unacknowledged record must survive the restart (queue %s, tag %d)", queue, i)
			require.Contains(t, string(msg.Body), fmt.Sprintf("message-%s-%d", queue, i))
		}
	}
}

// walInventoryHolds reports whether ANY WAL file inventory -- the active
// file's or a sealed file's -- still holds a physical copy of one (queue, tag).
//
// The per-file inventory is the COMPACTION's copy set: a record that is still
// in it is a record the compaction would carry over into the rewritten file.
func walInventoryHolds(qw *QueueWAL, queue string, tag uint64) bool {
	qw.offsetIndexMutex.RLock()
	defer qw.offsetIndexMutex.RUnlock()
	invs := make([]*segmentInventory, 0, len(qw.oldFiles)+1)
	qw.oldFilesMutex.RLock()
	for _, info := range qw.oldFiles {
		if info.inv != nil {
			invs = append(invs, info.inv)
		}
	}
	qw.oldFilesMutex.RUnlock()
	if qw.currentInv != nil {
		invs = append(invs, qw.currentInv)
	}
	for _, inv := range invs {
		for _, r := range inv.records {
			if r.queueName == queue && r.deliveryTag == tag {
				return true
			}
		}
	}
	return false
}

// TestLogCompaction_ReclaimUnlinksFullyAckedRolledFile pins the reclaim
// sweep's contract in segstore v1: a ROLLED file whose records are ALL
// acknowledged is UNLINKED, and a rolled file that still holds an
// unacknowledged record is NOT.
//
// The sweep is called DIRECTLY (qw.tryReclaimOldFiles) and the background
// cleanup tick is an hour away, so this assertion is about the sweep and
// nothing else.
//
// WHAT A MUTATION WOULD BREAK:
//
//   - if the sweep stopped unlinking a fully acknowledged rolled file, the
//     file would still be on disk here;
//   - if the sweep unlinked a file that still held an unacknowledged record,
//     the file would be gone here AND the record would be unreadable -- a
//     silent loss of confirmed durable data.
func TestLogCompaction_ReclaimUnlinksFullyAckedRolledFile(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultWALConfig()
	cfg.CleanupInterval = time.Hour
	cfg.CompactionInterval = time.Hour

	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })
	qw := wm.sharedWAL

	write := func(queue string, tags ...uint64) {
		for _, tag := range tags {
			require.NoError(t, wm.Write(queue, &protocol.Message{
				Exchange:     "amq.direct",
				RoutingKey:   queue,
				Body:         []byte(fmt.Sprintf("%s-%d", queue, tag)),
				DeliveryMode: 2,
				DeliveryTag:  tag,
			}, tag))
		}
	}
	roll := func() (uint64, string) {
		qw.fileMutex.Lock()
		defer qw.fileMutex.Unlock()
		num := qw.fileNum.Load()
		path := qw.currentFile.Name()
		require.NoError(t, qw.rollFile())
		return num, path
	}

	// --- A ROLLED FILE WHOSE RECORDS ARE ALL ACKNOWLEDGED IS UNLINKED ---
	write("acked", 1, 2, 3)
	ackedNum, ackedPath := roll()
	for tag := uint64(1); tag <= 3; tag++ {
		wm.Acknowledge("acked", tag)
	}
	qw.tryReclaimOldFiles()

	_, err = os.Stat(ackedPath)
	require.True(t, errors.Is(err, fs.ErrNotExist),
		"a rolled file whose records are ALL acknowledged must be unlinked by the reclaim sweep")
	qw.oldFilesMutex.RLock()
	_, registered := qw.oldFiles[ackedNum]
	qw.oldFilesMutex.RUnlock()
	require.False(t, registered, "an unlinked file must leave the old-files registry")

	// --- A ROLLED FILE THAT STILL HOLDS AN UNACKNOWLEDGED RECORD IS NOT ---
	write("live", 11, 12, 13)
	liveNum, livePath := roll()
	wm.Acknowledge("live", 11)
	wm.Acknowledge("live", 12)
	qw.tryReclaimOldFiles()

	_, err = os.Stat(livePath)
	require.NoError(t, err,
		"a rolled file with an unacknowledged record must never be unlinked")
	qw.oldFilesMutex.RLock()
	_, registered = qw.oldFiles[liveNum]
	qw.oldFilesMutex.RUnlock()
	require.True(t, registered, "a file with an unacknowledged record must stay registered")
	msg, err := wm.Read("live", 13)
	require.NoError(t, err, "the unacknowledged record must still be readable")
	require.Equal(t, "live-13", string(msg.Body))
}

// TestLogCompaction_CompactionKeepsLiveRecords pins the compaction's
// contract in segstore v1: a sealed file whose acknowledged bytes are the
// majority is REWRITTEN with the records that are still live, and the
// acknowledged records are gone from the file and from the index.
//
// The sweep is called DIRECTLY (qw.tryCompactOldFiles) and the background
// cleanup tick is an hour away.
//
// WHAT A MUTATION WOULD BREAK:
//
//   - if the compaction stopped running, the file would keep its garbage and
//     the "the file was rewritten" assertion fails;
//   - if the compaction dropped a LIVE record, the "still readable"
//     assertions fail;
//   - if the compaction kept an ACKNOWLEDGED record, the "gone from the
//     index" assertions fail.
func TestLogCompaction_CompactionKeepsLiveRecords(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultWALConfig()
	cfg.CleanupInterval = time.Hour
	cfg.CompactionInterval = time.Hour

	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })
	qw := wm.sharedWAL

	// Ten records with a body big enough that the acknowledged ones are the
	// majority of the file's durable bytes.
	for tag := uint64(1); tag <= 10; tag++ {
		require.NoError(t, wm.Write("compact", &protocol.Message{
			Exchange:     "amq.direct",
			RoutingKey:   "compact",
			Body:         []byte(fmt.Sprintf("compact-body-%02d-%s", tag, "0123456789")),
			DeliveryMode: 2,
			DeliveryTag:  tag,
		}, tag))
	}
	qw.fileMutex.Lock()
	fileNum := qw.fileNum.Load()
	path := qw.currentFile.Name()
	require.NoError(t, qw.rollFile())
	qw.fileMutex.Unlock()

	// Acknowledge nine of the ten: the dead bytes are now the majority.
	for tag := uint64(1); tag <= 9; tag++ {
		wm.Acknowledge("compact", tag)
	}
	inv := qw.oldFiles[fileNum].inv
	require.Greater(t, float64(inv.deadBytes.Load()), 0.5*float64(inv.durableBytes.Load()),
		"the fixture must put the file strictly above the compaction threshold (dead=%d durable=%d)",
		inv.deadBytes.Load(), inv.durableBytes.Load())

	sizeBefore := compactionFileSize(t, path)
	qw.tryCompactOldFiles()
	sizeAfter := compactionFileSize(t, path)
	require.Less(t, sizeAfter, sizeBefore,
		"a file above the threshold must be rewritten with its live records only")

	// THE LIVE RECORD SURVIVES, WITH ITS BODY INTACT.
	msg, err := wm.Read("compact", 10)
	require.NoError(t, err, "the live record must survive the compaction")
	require.Equal(t, "compact-body-10-0123456789", string(msg.Body))

	// THE ACKNOWLEDGED RECORDS ARE GONE FROM THE INDEX AND FROM THE FILE.
	for tag := uint64(1); tag <= 9; tag++ {
		_, err := wm.Read("compact", tag)
		require.Error(t, err, "an acknowledged record must not be served after the compaction")
	}
	qw.offsetIndexMutex.RLock()
	for tag := uint64(1); tag <= 9; tag++ {
		_, ok := qw.offsetIndex[recordKey{queueName: "compact", deliveryTag: tag}]
		require.False(t, ok, "an acknowledged record must not stay in the index")
	}
	qw.offsetIndexMutex.RUnlock()
}

// TestLogCompaction_RetentionKeepsUnacked proves that expiry neither
// force-checkpoints nor deletes unacknowledged WAL records.
//
// RETENTION AUTHORISES NOTHING. The reclaim sweep deletes a file only when
// every record in it has been acknowledged; age is never a reason.
//
// WHAT A MUTATION WOULD BREAK:
//
//   - if the sweep gained a time-based deletion path, the "the file is still
//     there" assertion fails;
//   - if the sweep deleted a file that still held an unacknowledged record,
//     the file would be gone and the records unreadable.
func TestLogCompaction_RetentionKeepsUnacked(t *testing.T) {
	tmpDir := t.TempDir()

	engineCfg := interfaces.EngineConfig{
		WALBatchSize:      50,
		WALBatchTimeoutMS: 5,
		WALFileSize:       1024, // Small files
		WALChannelBuffer:  5000,
	}
	walCfg := WALConfigFromEngine(engineCfg)
	walCfg.RetentionPeriod = time.Nanosecond // already expired: no waiting
	walCfg.CleanupInterval = time.Hour       // no background sweep
	walCfg.CompactionInterval = time.Hour

	wm, err := NewWALManagerWithConfig(tmpDir, walCfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	sm, err := NewSegmentManager(tmpDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sm.Close() })
	wm.SetSegmentManager(sm)

	// Write messages across 2 queues.
	for i := uint64(1); i <= 30; i++ {
		queue := "queue_a"
		if i > 15 {
			queue = "queue_b"
		}
		msg := &protocol.Message{
			Exchange:     "test",
			RoutingKey:   queue,
			Body:         []byte(fmt.Sprintf("retention-msg-%d", i)),
			DeliveryMode: 2,
			DeliveryTag:  i,
		}
		require.NoError(t, wm.Write(queue, msg, i))
	}

	// Verify old files exist.
	wm.sharedWAL.oldFilesMutex.RLock()
	oldCount := len(wm.sharedWAL.oldFiles)
	wm.sharedWAL.oldFilesMutex.RUnlock()
	require.Greater(t, oldCount, 0)

	// The retention window is already expired, so the sweep's age rule (if it
	// had one) would be live NOW. No waiting: the sweep is called directly.
	wm.sharedWAL.tryReclaimOldFiles()

	wm.sharedWAL.oldFilesMutex.RLock()
	remaining := len(wm.sharedWAL.oldFiles)
	wm.sharedWAL.oldFilesMutex.RUnlock()
	require.Equal(t, oldCount, remaining, "expiry must not delete unacknowledged files")

	// Every unacknowledged record is still readable from the WAL.
	for i := uint64(1); i <= 30; i++ {
		queue := "queue_a"
		if i > 15 {
			queue = "queue_b"
		}
		_, err := wm.Read(queue, i)
		require.NoError(t, err, "record %d must still be readable", i)
		// THE WAL DOES NOT CHECKPOINT INTO THE LEGACY SEGMENT TIER. Nothing
		// copies a WAL record into a segment, so the segment tier holds no
		// copy of it -- and a retention sweep must not "help" by moving one
		// there.
		_, err = sm.Read(queue, i)
		require.Error(t, err, "retention must not checkpoint just to time-delete")
		wm.Acknowledge(queue, i)
	}

	// Now that every record is acknowledged, the whole-file reclaim is
	// permitted.
	wm.sharedWAL.tryReclaimOldFiles()
	wm.sharedWAL.oldFilesMutex.RLock()
	remaining = len(wm.sharedWAL.oldFiles)
	wm.sharedWAL.oldFilesMutex.RUnlock()
	require.Zero(t, remaining, "acknowledgement permits whole-file reclaim")
}

// TestLogCompaction_LargeOffsetLifecycle verifies the whole-file reclaim
// works with uint64 offsets larger than the uint32 maximum, confirming the
// roaring64 fix.
//
// WHAT A MUTATION WOULD BREAK:
//
//   - if the ack bitmap were 32-bit, the acknowledgements below would not
//     take effect and the file would never be reclaimable, so the
//     "the old WAL files are gone" assertion fails;
//   - if the reclaim sweep deleted a file with an unacknowledged record, the
//     "the file is still there" assertion fails.
func TestLogCompaction_LargeOffsetLifecycle(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := DefaultWALConfig()
	cfg.FileSize = 1024 // Small files to force rolls
	cfg.CleanupInterval = time.Hour
	cfg.CompactionInterval = time.Hour

	wm, err := NewWALManagerWithConfig(tmpDir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	sm, err := NewSegmentManager(tmpDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sm.Close() })
	wm.SetSegmentManager(sm)

	baseOffset := uint64(1) << 33 // ~8.5 billion

	// Write messages with large offsets.
	for i := uint64(0); i < 10; i++ {
		msg := &protocol.Message{
			Exchange:     "test",
			RoutingKey:   "large_queue",
			Body:         []byte(fmt.Sprintf("large-offset-%d", i)),
			DeliveryMode: 2,
			DeliveryTag:  baseOffset + i,
		}
		require.NoError(t, wm.Write("large_queue", msg, baseOffset+i))
	}

	// ACK the first five.
	for i := uint64(0); i < 5; i++ {
		wm.Acknowledge("large_queue", baseOffset+i)
	}

	// THE ACKNOWLEDGED ONES ARE GONE FROM THE INDEX AND NOT SERVED; THE REST
	// ARE STILL READABLE.
	qw := wm.sharedWAL
	for i := uint64(0); i < 5; i++ {
		_, err := wm.Read("large_queue", baseOffset+i)
		require.Error(t, err, "an acknowledged large-offset record must not be served")
		qw.offsetIndexMutex.RLock()
		_, ok := qw.offsetIndex[recordKey{queueName: "large_queue", deliveryTag: baseOffset + i}]
		qw.offsetIndexMutex.RUnlock()
		require.False(t, ok, "an acknowledged large-offset record must leave the index")
	}
	for i := uint64(5); i < 10; i++ {
		msg, err := wm.Read("large_queue", baseOffset+i)
		require.NoError(t, err, "an unacknowledged large-offset record must still be readable")
		require.Contains(t, string(msg.Body), fmt.Sprintf("large-offset-%d", i))
	}

	// The WAL does not checkpoint into the legacy segment tier: the segment
	// tier holds no copy of a WAL record.
	for i := uint64(0); i < 10; i++ {
		_, err := sm.Read("large_queue", baseOffset+i)
		require.Error(t, err, "a WAL record must never be copied into the legacy segment tier")
	}

	// ACK the remaining five: now the rolled files are reclaimable.
	for i := uint64(5); i < 10; i++ {
		wm.Acknowledge("large_queue", baseOffset+i)
	}
	wm.sharedWAL.tryReclaimOldFiles()

	wm.sharedWAL.oldFilesMutex.RLock()
	require.Equal(t, 0, len(wm.sharedWAL.oldFiles),
		"all files should be deleted after all large offsets ACKed")
	wm.sharedWAL.oldFilesMutex.RUnlock()
}

// TestLogCompaction_TierFallback walks the three read tiers: the hot cache,
// the WAL, and the legacy segment tier.
//
// SEGSTORE V1 RE-POINT. Tier 3 used to be produced by the CHECKPOINT: the
// record was copied into a segment and the WAL copy was then removed from the
// index, so the read had to fall back to the segment tier. Checkpoint copying
// is gone — nothing moves records out of the WAL — so the segment tier now
// holds only LEGACY records: the ones a broker finds on disk after it upgrades
// from the checkpointing build. The tier-3 copy is therefore written as a
// legacy segment record instead, and the three tiers are still walked in the
// same order against the same delivery tag.
//
// WHAT A MUTATION WOULD BREAK:
//
//   - if the WAL tier stopped answering, the tier-2 assertion fails;
//   - if the legacy segment tier stopped answering, the tier-3 assertion
//     fails;
//   - if the hot cache stopped answering, the tier-1 assertion fails.
func TestLogCompaction_TierFallback(t *testing.T) {
	tmpDir := t.TempDir()

	engineCfg := logCompactionEngineConfig()

	queue := "tier_test"
	msg := &protocol.Message{
		Exchange:     "test",
		RoutingKey:   queue,
		Body:         []byte("tier-fallback-message"),
		DeliveryMode: 2,
		DeliveryTag:  42,
	}

	// The legacy segment copy of the SAME record — the only way the segment
	// tier holds anything now. It is written before the storage opens the
	// directory, exactly as a broker finds it at boot.
	writeLegacyFixture(t, tmpDir, queue, &RecoveryMessage{
		QueueName: queue,
		Offset:    42,
		Message:   msg,
	})

	ds, err := NewDisruptorStorageWithEngineConfig(tmpDir, engineCfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	ds.getOrCreateQueueHot(queue)

	// Write a durable message. The durable write blocks until the fsync, so
	// there is nothing to wait for.
	require.NoError(t, ds.StoreMessage(queue, msg))

	// Tier 1: Read from the hot cache.
	readMsg, err := ds.GetMessage(queue, 42)
	require.NoError(t, err)
	require.Equal(t, []byte("tier-fallback-message"), readMsg.Body)

	// Clear the hot cache to force the WAL fallback.
	ds.hot.Delete(queue, 42)

	// Tier 2: Read from the WAL (hot cache cleared).
	readMsg, err = ds.GetMessage(queue, 42)
	require.NoError(t, err, "should fall back to WAL when not in ring buffer")
	require.Equal(t, []byte("tier-fallback-message"), readMsg.Body)

	// The segment tier learns the queue the way it does at boot: the legacy
	// directory is attributed and opened by the recovery pass.
	_, err = ds.segments.RecoverFromSegments()
	require.NoError(t, err)

	// Remove from the WAL offset index to force the segment fallback.
	ds.wal.sharedWAL.offsetIndexMutex.Lock()
	delete(ds.wal.sharedWAL.offsetIndex, recordKey{queueName: queue, deliveryTag: 42})
	delete(ds.wal.sharedWAL.recordCopies, recordKey{queueName: queue, deliveryTag: 42})
	ds.wal.sharedWAL.offsetIndexMutex.Unlock()

	// Tier 3: Read from the segments (hot cache + WAL cleared).
	readMsg, err = ds.GetMessage(queue, 42)
	require.NoError(t, err, "should fall back to segments when not in ring buffer or WAL")
	require.Equal(t, []byte("tier-fallback-message"), readMsg.Body)
}
