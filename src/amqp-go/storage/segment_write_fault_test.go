package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// WS-C C2: a failed segment write/fsync left the partial bytes in the file
// with a stale in-memory fileSize, so a retried CheckpointBatch appended at
// the true EOF while indexing positions from the stale size — and once the WAL
// was unlinked, the segment was the only copy, and recovery quarantined it as
// fatally corrupted.

// segFaultBody keeps segment records big enough that a 100-byte partial write
// never completes one.
const segFaultBody = "segment-write-fault-body-with-plenty-of-padding-padding-padding-padding"

func segFaultMessage(off uint64, body string) *protocol.Message {
	return &protocol.Message{
		Exchange:     "amq.direct",
		RoutingKey:   "q",
		Body:         []byte(body),
		DeliveryMode: 2,
		DeliveryTag:  off,
	}
}

func segFaultRecs(offsets ...uint64) []*RecoveryMessage {
	recs := make([]*RecoveryMessage, 0, len(offsets))
	for _, off := range offsets {
		recs = append(recs, &RecoveryMessage{QueueName: "q", Offset: off, Message: segFaultMessage(off, segFaultBody)})
	}
	return recs
}

// queueSegmentsOf returns the live QueueSegments for a queue (or fails).
func queueSegmentsOf(t *testing.T, sm *SegmentManager, queue string) *QueueSegments {
	t.Helper()
	val, ok := sm.queueSegments.Load(queue)
	require.True(t, ok, "no QueueSegments for %q", queue)
	return val.(*QueueSegments)
}

// TestSegment_PartialCheckpointWrite_RetrySucceeds_ThenCheckpointUnlinksWAL_RestartRecovers
// (TDD test 9 — the main durability test).
func TestSegment_PartialCheckpointWrite_RetrySucceeds_ThenCheckpointUnlinksWAL_RestartRecovers(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, coldTailWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })
	sm, err := NewSegmentManager(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sm.Close() })
	wm.SetSegmentManager(sm)

	const n = 50
	for off := uint64(1); off <= n; off++ {
		require.NoError(t, wm.Write("q", segFaultMessage(off, fmt.Sprintf("body-%d", off)), off))
	}
	sw := wm.sharedWAL
	sw.fileMutex.Lock()
	walPath := sw.currentFile.Name()
	sw.rollFile()
	sw.fileMutex.Unlock()

	// Fault 1: a disk-full short write into the segment (one-shot).
	var fired atomic.Int32
	restore := SetSegmentWriteForTest(func(f *os.File, b []byte) (int, error) {
		if fired.Add(1) == 1 && strings.HasPrefix(f.Name(), dir) {
			nw, werr := f.Write(b[:100])
			if werr != nil {
				return nw, werr
			}
			return nw, syscall.ENOSPC
		}
		return f.Write(b)
	})

	sw.performCheckpoint()
	restore()

	// The WAL file is kept (checkpoint failed) and the damaged region was
	// truncated back: the active segment is empty again.
	_, serr := os.Stat(walPath)
	require.False(t, os.IsNotExist(serr), "the WAL file must be kept while its checkpoint fails")
	sw.oldFilesMutex.RLock()
	_, registered := sw.oldFiles[1]
	sw.oldFilesMutex.RUnlock()
	require.True(t, registered, "the WAL file must stay registered in oldFiles")

	qs := queueSegmentsOf(t, sm, "q")
	qs.mutex.Lock()
	active := qs.currentSegment
	qs.mutex.Unlock()
	st, serr := active.file.Stat()
	require.NoError(t, serr)
	require.Zero(t, st.Size(), "the partial batch's bytes must be truncated out of the active segment")
	require.Zero(t, active.fileSize.Load(), "the in-memory fileSize must match")

	// Fault cleared: the retry succeeds and unlinks the WAL file.
	sw.performCheckpoint()
	_, serr = os.Stat(walPath)
	require.True(t, os.IsNotExist(serr), "after a successful checkpoint the WAL file must be unlinked")

	// Session 2: no quarantine fault, every record exactly once, correct body.
	ds2, derr := NewDisruptorStorageWithEngineConfig(dir, interfaces.EngineConfig{})
	require.NoError(t, derr)
	t.Cleanup(func() { _ = ds2.Close() })
	recovered, derr := ds2.GetRecoverableMessages()
	require.NoError(t, derr, "no fatal segment fault (no interior corruption)")
	msgs := recovered["q"]
	require.Len(t, msgs, n, "each of the %d records exactly once", n)
	for _, m := range msgs {
		require.Equal(t, fmt.Sprintf("body-%d", m.DeliveryTag), string(m.Body), "record %d body", m.DeliveryTag)
	}
}

// TestSegment_CheckpointFsyncError_TruncatesAndKeepsWAL (TDD test 10).
func TestSegment_CheckpointFsyncError_TruncatesAndKeepsWAL(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, coldTailWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })
	sm, err := NewSegmentManager(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sm.Close() })
	wm.SetSegmentManager(sm)

	for off := uint64(1); off <= 3; off++ {
		require.NoError(t, wm.Write("q", segFaultMessage(off, segFaultBody), off))
	}
	sw := wm.sharedWAL
	sw.fileMutex.Lock()
	walPath := sw.currentFile.Name()
	sw.rollFile()
	sw.fileMutex.Unlock()

	var fired atomic.Int32
	restore := SetSegmentFsyncForTest(func(f *os.File) error {
		if fired.Add(1) == 1 && strings.HasPrefix(f.Name(), dir) {
			return syscall.EIO
		}
		return fdatasyncFile(f)
	})

	sw.performCheckpoint()
	restore()

	// WAL kept, segment truncated to empty, nothing indexed.
	_, serr := os.Stat(walPath)
	require.False(t, os.IsNotExist(serr), "the WAL file must be kept when the checkpoint fsync fails")
	qs := queueSegmentsOf(t, sm, "q")
	qs.mutex.Lock()
	active := qs.currentSegment
	qs.mutex.Unlock()
	st, serr := active.file.Stat()
	require.NoError(t, serr)
	require.Zero(t, st.Size())
	_, rerr := sm.Read("q", 1)
	require.Error(t, rerr, "an fsync-failed batch must leave no readable index entries")

	// Retry succeeds, unlinks the WAL, and session 2 recovers each record once.
	sw.performCheckpoint()
	_, serr = os.Stat(walPath)
	require.True(t, os.IsNotExist(serr))

	ds2, derr := NewDisruptorStorageWithEngineConfig(dir, interfaces.EngineConfig{})
	require.NoError(t, derr)
	t.Cleanup(func() { _ = ds2.Close() })
	recovered, derr := ds2.GetRecoverableMessages()
	require.NoError(t, derr)
	require.Len(t, recovered["q"], 3, "each of the 3 records exactly once")
}

// TestSegment_TruncateFails_RetiresSegment_NoQuarantine (TDD test 11).
func TestSegment_TruncateFails_RetiresSegment_NoQuarantine(t *testing.T) {
	dir := t.TempDir()
	sm, err := NewSegmentManager(dir)
	require.NoError(t, err)

	// One successful batch of 1-10.
	require.NoError(t, sm.CheckpointBatch("q", segFaultRecs(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)))
	qs := queueSegmentsOf(t, sm, "q")
	qs.mutex.Lock()
	origSeg := qs.currentSegment
	origNum := origSeg.segmentNum
	qs.mutex.Unlock()

	// Then a failed batch whose truncate ALSO fails: the segment is retired
	// (damagedTail) and a fresh active segment takes over.
	var wFired, tFired atomic.Int32
	wRestore := SetSegmentWriteForTest(func(f *os.File, b []byte) (int, error) {
		if wFired.Add(1) == 1 && strings.HasPrefix(f.Name(), dir) {
			nw, werr := f.Write(b[:100])
			if werr != nil {
				return nw, werr
			}
			return nw, syscall.ENOSPC
		}
		return f.Write(b)
	})
	tRestore := SetSegmentTruncateForTest(func(f *os.File, size int64) error {
		if tFired.Add(1) == 1 && strings.HasPrefix(f.Name(), dir) {
			return syscall.EIO
		}
		return segmentTruncateDefault(f, size)
	})

	err = sm.CheckpointBatch("q", segFaultRecs(11, 12, 13, 14, 15, 16, 17, 18, 19, 20))
	wRestore()
	tRestore()
	require.Error(t, err, "the failed batch must fail the checkpoint (WAL kept)")

	qs.mutex.Lock()
	newSeg := qs.currentSegment
	segPoison := qs.segPoisoned
	qs.mutex.Unlock()
	require.NotSame(t, origSeg, newSeg, "a fresh active segment must take over")
	require.NotEqual(t, origNum, newSeg.segmentNum)
	require.Nil(t, segPoison, "the fresh segment clears the poison")

	qs.sealedMutex.RLock()
	sealed, ok := qs.sealedSegments[origNum]
	qs.sealedMutex.RUnlock()
	require.True(t, ok, "the damaged segment must be retired into sealedSegments")
	require.True(t, sealed.damagedTail, "the retired segment must be marked damagedTail")

	// Retry of 11-20 into the fresh segment succeeds.
	require.NoError(t, sm.CheckpointBatch("q", segFaultRecs(11, 12, 13, 14, 15, 16, 17, 18, 19, 20)))
	for off := uint64(1); off <= 20; off++ {
		_, rerr := sm.Read("q", off)
		require.NoError(t, rerr, "record %d readable after retry", off)
	}

	// Session 2: tail damage is benign; 1-20 each exactly once.
	require.NoError(t, sm.Close())
	sm2, serr := NewSegmentManager(dir)
	require.NoError(t, serr)
	t.Cleanup(func() { _ = sm2.Close() })
	byQueue, serr := sm2.RecoverFromSegments()
	require.NoError(t, serr, "tail damage on a retired segment must not quarantine it")
	seen := map[uint64]bool{}
	for _, rm := range byQueue["q"] {
		require.False(t, seen[rm.Offset], "offset %d recovered twice", rm.Offset)
		seen[rm.Offset] = true
	}
	for off := uint64(1); off <= 20; off++ {
		require.True(t, seen[off], "offset %d must be recovered", off)
	}
	require.Len(t, seen, 20)
}

// TestSegment_SegmentNumbersMonotonicAndExclusive (TDD test 12).
func TestSegment_SegmentNumbersMonotonicAndExclusive(t *testing.T) {
	t.Run("tight rolling loop produces strictly increasing numbers", func(t *testing.T) {
		dir := t.TempDir()
		cfg := DefaultSegmentConfig()
		cfg.SegmentSize = 1 // every batch rolls
		sm, err := NewSegmentManagerWithConfig(dir, cfg)
		require.NoError(t, err)
		t.Cleanup(func() { _ = sm.Close() })

		var nums []uint64
		for i := 0; i < 20; i++ {
			require.NoError(t, sm.CheckpointBatch("q", segFaultRecs(uint64(i+1))))
			qs := queueSegmentsOf(t, sm, "q")
			qs.mutex.Lock()
			num := qs.currentSegment.segmentNum
			qs.mutex.Unlock()
			nums = append(nums, num)
		}
		for i := 1; i < len(nums); i++ {
			require.Greater(t, nums[i], nums[i-1],
				"segment numbers must be strictly increasing (got %v)", nums)
		}
	})

	t.Run("a pre-existing file at the next number is never reopened", func(t *testing.T) {
		dir := t.TempDir()
		sm, err := NewSegmentManager(dir)
		require.NoError(t, err)
		t.Cleanup(func() { _ = sm.Close() })
		require.NoError(t, sm.CheckpointBatch("q", segFaultRecs(1)))
		qs := queueSegmentsOf(t, sm, "q")

		// Simulate a backwards clock step: plant a lastSegmentNum from "the
		// future" (an hour ahead in ns) so the wall clock's now() is below it
		// and the next number comes from the monotone bump; pre-create the
		// file the bump would pick first and prove it is never reopened.
		future := uint64(time.Now().UnixNano()) + uint64(time.Hour)
		qs.mutex.Lock()
		qs.lastSegmentNum = future
		qs.mutex.Unlock()

		collide := filepath.Join(qs.dataDir, fmt.Sprintf("%020d%s", future+1, SegmentFileExtension))
		require.NoError(t, os.WriteFile(collide, []byte("pre-existing junk"), 0644))

		qs.mutex.Lock()
		rerr := qs.rollSegmentLocked()
		qs.mutex.Unlock()
		require.NoError(t, rerr)

		// The pre-existing file was NOT reopened or appended to...
		st, serr := os.Stat(collide)
		require.NoError(t, serr)
		require.EqualValues(t, len("pre-existing junk"), st.Size(),
			"the pre-existing file must not be reopened (O_EXCL) nor appended to")

		// ...and the fresh segment took a bumped number.
		qs.mutex.Lock()
		require.Equal(t, future+2, qs.currentSegment.segmentNum)
		qs.mutex.Unlock()
	})
}

// TestSegment_RollFailureAfterDurableBatch_DoesNotFailCheckpoint (TDD test 13).
func TestSegment_RollFailureAfterDurableBatch_DoesNotFailCheckpoint(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultSegmentConfig()
	cfg.SegmentSize = 1 // the first batch immediately wants a roll
	sm, err := NewSegmentManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sm.Close() })

	// Fail the SECOND segment-file open: the first is the queue's initial
	// segment creation inside getOrCreateQueueSegments, the second is the
	// roll this batch attempts after its durable write.
	var fired atomic.Int32
	restore := SetSegmentOpenFileForTest(func(name string, flag int, perm os.FileMode) (*os.File, error) {
		if fired.Add(1) == 2 && strings.HasPrefix(name, dir) {
			return nil, syscall.ENFILE
		}
		return os.OpenFile(name, flag, perm)
	})
	defer restore()

	// The batch is durable (write+fsync+index) BEFORE the roll is attempted,
	// so a failed roll must not fail the checkpoint — and must not leave the
	// current segment sealed-read-only.
	require.NoError(t, sm.CheckpointBatch("q", segFaultRecs(1)),
		"a roll failure after a durable batch must not fail the checkpoint")

	qs := queueSegmentsOf(t, sm, "q")
	qs.mutex.Lock()
	origSeg := qs.currentSegment
	qs.mutex.Unlock()

	_, rerr := sm.Read("q", 1)
	require.NoError(t, rerr, "the current segment must still serve reads")

	// The next batch rolls (hook one-shot consumed).
	require.NoError(t, sm.CheckpointBatch("q", segFaultRecs(2)))
	qs.mutex.Lock()
	cur := qs.currentSegment
	qs.mutex.Unlock()
	require.NotSame(t, origSeg, cur, "the next batch must roll into a fresh segment (the oversized one stays writable, not sealed-read-only)")
	_, rerr = sm.Read("q", 2)
	require.NoError(t, rerr)
	_, rerr = sm.Read("q", 1)
	require.NoError(t, rerr, "the sealed original segment must still serve reads")
}
