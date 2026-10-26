package storage

import (
	"errors"
	"io/fs"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

func segstoreReclaimWALConfig() WALConfig {
	cfg := DefaultWALConfig()
	cfg.CleanupInterval = time.Hour
	cfg.CheckpointInterval = time.Hour
	return cfg
}

func writeSegstoreReclaimRecord(t *testing.T, wm *WALManager, queue string, tag uint64, body string) {
	t.Helper()
	require.NoError(t, wm.Write(queue, &protocol.Message{
		RoutingKey: queue, Body: []byte(body), DeliveryMode: 2, DeliveryTag: tag,
	}, tag))
}

func rollSegstoreReclaimWAL(t *testing.T, wm *WALManager) (uint64, string) {
	t.Helper()
	qw := wm.sharedWAL
	qw.fileMutex.Lock()
	fileNum := qw.fileNum.Load()
	path := qw.currentFile.Name()
	require.NoError(t, qw.rollFile())
	qw.fileMutex.Unlock()
	return fileNum, path
}

func TestSegstoreReclaimFullyAckedOnly(t *testing.T) {
	dir := t.TempDir()
	cfg := segstoreReclaimWALConfig()
	cfg.RetentionPeriod = time.Nanosecond
	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })
	sm, err := NewSegmentManager(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sm.Close() })
	wm.SetSegmentManager(sm)

	writeSegstoreReclaimRecord(t, wm, "q", 1, "acked")
	writeSegstoreReclaimRecord(t, wm, "q", 2, "still-live")
	fileNum, path := rollSegstoreReclaimWAL(t, wm)
	qw := wm.sharedWAL
	qw.oldFilesMutex.Lock()
	qw.oldFiles[fileNum].createdAt = time.Now().Add(-time.Hour)
	qw.oldFilesMutex.Unlock()

	wm.Acknowledge("q", 1)
	qw.tryReclaimOldFiles()
	_, err = os.Stat(path)
	require.NoError(t, err, "an expired sealed file with an unacked record must never be time-deleted")
	msg, err := wm.Read("q", 2)
	require.NoError(t, err)
	require.Equal(t, []byte("still-live"), msg.Body)

	wm.Acknowledge("q", 2)
	qw.tryReclaimOldFiles()
	_, err = os.Stat(path)
	require.True(t, errors.Is(err, fs.ErrNotExist), "a fully acked sealed file must be reclaimed: %v", err)
	qw.oldFilesMutex.RLock()
	_, registered := qw.oldFiles[fileNum]
	qw.oldFilesMutex.RUnlock()
	require.False(t, registered)
	qw.offsetIndexMutex.RLock()
	_, newest := qw.offsetIndex[recordKey{queueName: "q", deliveryTag: 2}]
	_, copies := qw.recordCopies[recordKey{queueName: "q", deliveryTag: 2}]
	qw.offsetIndexMutex.RUnlock()
	require.False(t, newest, "reclaim must remove the newest-copy index entry")
	require.False(t, copies, "reclaim must remove every physical-copy index entry")
}

func TestSegstoreReclaimSkipsActiveAndInvalid(t *testing.T) {
	t.Run("active", func(t *testing.T) {
		wm, err := NewWALManagerWithConfig(t.TempDir(), segstoreReclaimWALConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = wm.Close() })
		writeSegstoreReclaimRecord(t, wm, "q", 1, "active")
		wm.Acknowledge("q", 1)
		path := wm.sharedWAL.currentFile.Name()

		wm.sharedWAL.tryReclaimOldFiles()
		_, err = os.Stat(path)
		require.NoError(t, err, "the active file is never reclaimable")
		require.Contains(t, wm.sharedWAL.recordCopies, recordKey{queueName: "q", deliveryTag: 1})
	})

	t.Run("invalid inventory", func(t *testing.T) {
		wm, err := NewWALManagerWithConfig(t.TempDir(), segstoreReclaimWALConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = wm.Close() })
		writeSegstoreReclaimRecord(t, wm, "q", 1, "invalid")
		fileNum, path := rollSegstoreReclaimWAL(t, wm)
		wm.Acknowledge("q", 1)
		qw := wm.sharedWAL
		qw.oldFiles[fileNum].inv.valid = false

		qw.tryReclaimOldFiles()
		_, err = os.Stat(path)
		require.NoError(t, err, "an invalid inventory must pin its file")
		require.Contains(t, qw.oldFiles, fileNum)
		require.Contains(t, qw.recordCopies, recordKey{queueName: "q", deliveryTag: 1})
	})
}

func TestSegstoreReclaimUnlinkFailureRetainsReads(t *testing.T) {
	wm, err := NewWALManagerWithConfig(t.TempDir(), segstoreReclaimWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })
	writeSegstoreReclaimRecord(t, wm, "q", 7, "retry-me")
	fileNum, path := rollSegstoreReclaimWAL(t, wm)
	wm.Acknowledge("q", 7)

	restore := SetCheckpointUnlinkForTest(func(candidate string) error {
		if candidate == path {
			return syscall.EACCES
		}
		return os.Remove(candidate)
	})
	wm.sharedWAL.tryReclaimOldFiles()
	restore()

	_, err = os.Stat(path)
	require.NoError(t, err)
	qw := wm.sharedWAL
	require.Contains(t, qw.oldFiles, fileNum)
	key := recordKey{queueName: "q", deliveryTag: 7}
	require.Contains(t, qw.recordCopies, key)
	// Acks are process-volatile. Dropping the read gate models a restart and
	// proves that a failed unlink retained both readable bytes and their index.
	qw.bitmapMutex.Lock()
	qw.ackBitmaps["q"].Remove(7)
	qw.bitmapMutex.Unlock()
	msg, err := wm.Read("q", 7)
	require.NoError(t, err)
	require.Equal(t, []byte("retry-me"), msg.Body)

	wm.Acknowledge("q", 7)
	qw.tryReclaimOldFiles()
	_, err = os.Stat(path)
	require.True(t, errors.Is(err, fs.ErrNotExist))
	require.NotContains(t, qw.recordCopies, key)
}

func TestSegstoreSharedBodyLastAckReclaims(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, coldTailWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })
	queues := []string{"shared-a", "shared-b", "shared-c"}
	body := sharedTestBody(8192)
	require.NoError(t, writeSharedSync(t, wm, body, queues, 11))
	path := firstWALFile(t, dir)

	for i := 0; i < len(queues)-1; i++ {
		wm.Acknowledge(queues[i], uint64(11+i))
	}
	qw := wm.sharedWAL
	qw.tryReclaimOldFiles()
	_, err = os.Stat(path)
	require.NoError(t, err, "a shared body stays pinned until its final reference acks")
	msg, err := wm.Read(queues[2], 13)
	require.NoError(t, err)
	require.Equal(t, body, msg.Body)

	wm.Acknowledge(queues[2], 13)
	qw.tryReclaimOldFiles()
	_, err = os.Stat(path)
	require.True(t, errors.Is(err, fs.ErrNotExist))
	for i, queue := range queues {
		require.NotContains(t, qw.recordCopies, recordKey{queueName: queue, deliveryTag: uint64(11 + i)})
	}
}

func TestSegstorePurgeColdRangeUniqueCount(t *testing.T) {
	ds, err := NewDisruptorStorageWithDataDir(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })
	const queue = "purge-q"
	ds.getOrCreateQueueHot(queue)
	for tag := uint64(1); tag <= 3; tag++ {
		require.True(t, ds.hot.Put(queue, &protocol.Message{DeliveryTag: tag, Body: []byte("hot")}, true))
	}
	// Tag 3 has two physical WAL copies, one sealed and one active. Purge must
	// settle both while returning the logical tag only once.
	writeSegstoreReclaimRecord(t, ds.wal, queue, 2, "wal-2")
	writeSegstoreReclaimRecord(t, ds.wal, queue, 3, "wal-old-3")
	rollSegstoreReclaimWAL(t, ds.wal)
	writeSegstoreReclaimRecord(t, ds.wal, queue, 3, "wal-new-3")
	for tag := uint64(4); tag <= 6; tag++ {
		writeSegstoreReclaimRecord(t, ds.wal, queue, tag, "wal")
	}
	for tag := uint64(3); tag <= 6; tag++ {
		require.NoError(t, writeLegacyForTest(ds.segments, queue, &protocol.Message{DeliveryTag: tag, Body: []byte("legacy")}, tag))
	}

	count, err := ds.PurgeQueue(queue, 1, 5)
	require.NoError(t, err)
	require.Equal(t, 5, count, "hot, WAL, and legacy hits for one tag count once")
	require.Zero(t, ds.hot.Count(queue))
	qw := ds.wal.sharedWAL
	qw.offsetIndexMutex.RLock()
	for tag := uint64(2); tag <= 5; tag++ {
		copies := qw.recordCopies[recordKey{queueName: queue, deliveryTag: tag}]
		require.NotEmpty(t, copies)
		for _, record := range copies {
			require.True(t, record.acked.Load(), "WAL tag %d copy %+v was not purged", tag, record.pos)
		}
	}
	require.False(t, qw.offsetIndex[recordKey{queueName: queue, deliveryTag: 6}].acked.Load())
	qw.offsetIndexMutex.RUnlock()
	value, ok := ds.segments.queueSegments.Load(queue)
	require.True(t, ok)
	qs := value.(*QueueSegments)
	qs.mutex.Lock()
	current := qs.currentSegment
	qs.mutex.Unlock()
	for tag := uint64(3); tag <= 5; tag++ {
		for _, record := range current.inv.records {
			if record.deliveryTag == tag {
				require.True(t, record.acked.Load(), "legacy tag %d was not purged", tag)
			}
		}
	}

	count, err = ds.PurgeQueue(queue, 1, 5)
	require.NoError(t, err)
	require.Zero(t, count, "an already purged range has no newly acked tags")

	// DeleteMessageRange is the same operation without a count return.
	require.True(t, ds.hot.Put(queue, &protocol.Message{DeliveryTag: 7, Body: []byte("hot")}, true))
	writeSegstoreReclaimRecord(t, ds.wal, queue, 7, "wal-7")
	require.NoError(t, ds.DeleteMessageRange(queue, 7, 7))
	_, _, hot := ds.hot.Get(queue, 7)
	require.False(t, hot)
	require.True(t, qw.offsetIndex[recordKey{queueName: queue, deliveryTag: 7}].acked.Load())
}

func TestLegacySegmentCompactionUsesDeadBytes(t *testing.T) {
	newManager := func(t *testing.T) *SegmentManager {
		t.Helper()
		cfg := DefaultSegmentConfig()
		cfg.CompactionInterval = time.Hour
		cfg.CompactionThreshold = 0.5
		sm, err := NewSegmentManagerWithConfig(t.TempDir(), cfg)
		require.NoError(t, err)
		t.Cleanup(func() { _ = sm.Close() })
		return sm
	}
	write := func(t *testing.T, sm *SegmentManager, queue string, tag uint64, size int) {
		t.Helper()
		require.NoError(t, writeLegacyForTest(sm, queue, &protocol.Message{
			DeliveryTag: tag, Body: make([]byte, size),
		}, tag))
	}

	t.Run("large dead record wins over message count", func(t *testing.T) {
		sm := newManager(t)
		write(t, sm, "q", 1, 8192)
		write(t, sm, "q", 2, 1)
		value, _ := sm.queueSegments.Load("q")
		qs := value.(*QueueSegments)
		qs.mutex.Lock()
		seg := qs.currentSegment
		qs.mutex.Unlock()
		qs.mutex.Lock()
		err := qs.rollSegmentLocked()
		qs.mutex.Unlock()
		require.NoError(t, err)
		qs.applyAck(1)
		// Stale count metadata must not participate in selection.
		seg.messageCount.Store(0)
		seg.deletedCount.Store(0)
		before := seg.fileSize.Load()
		require.Greater(t, float64(seg.inv.deadBytes.Load()), 0.5*float64(seg.inv.durableBytes))

		qs.tryCompaction()
		require.Less(t, seg.fileSize.Load(), before)
		_, err = qs.readMessage(2)
		require.NoError(t, err)
		_, err = qs.readMessage(1)
		require.Error(t, err)
	})

	t.Run("many small acks do not select by count", func(t *testing.T) {
		sm := newManager(t)
		write(t, sm, "q", 1, 16384)
		write(t, sm, "q", 2, 1)
		write(t, sm, "q", 3, 1)
		value, _ := sm.queueSegments.Load("q")
		qs := value.(*QueueSegments)
		qs.mutex.Lock()
		seg := qs.currentSegment
		qs.mutex.Unlock()
		qs.mutex.Lock()
		err := qs.rollSegmentLocked()
		qs.mutex.Unlock()
		require.NoError(t, err)
		qs.applyAck(2)
		qs.applyAck(3)
		seg.messageCount.Store(3)
		seg.deletedCount.Store(2)
		before := seg.fileSize.Load()
		require.Less(t, float64(seg.inv.deadBytes.Load()), 0.5*float64(seg.inv.durableBytes))

		qs.tryCompaction()
		require.Equal(t, before, seg.fileSize.Load())
		_, err = qs.readMessage(1)
		require.NoError(t, err)
	})

	t.Run("fully acked sealed file is unlinked", func(t *testing.T) {
		sm := newManager(t)
		write(t, sm, "q", 1, 16)
		write(t, sm, "q", 2, 16)
		value, _ := sm.queueSegments.Load("q")
		qs := value.(*QueueSegments)
		qs.mutex.Lock()
		seg := qs.currentSegment
		qs.mutex.Unlock()
		qs.mutex.Lock()
		err := qs.rollSegmentLocked()
		qs.mutex.Unlock()
		require.NoError(t, err)
		qs.applyAck(1)
		qs.applyAck(2)

		qs.tryCompaction()
		_, err = os.Stat(seg.path)
		require.True(t, errors.Is(err, fs.ErrNotExist), "fully acked legacy file must not be compacted to an empty file: %v", err)
		qs.sealedMutex.RLock()
		_, registered := qs.sealedSegments[seg.segmentNum]
		qs.sealedMutex.RUnlock()
		require.False(t, registered)
	})
}
