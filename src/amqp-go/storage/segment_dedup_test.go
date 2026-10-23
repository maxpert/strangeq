package storage

import (
	"os"
	"strings"
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// WS-C C8: duplicates ACROSS segment files were emitted twice at recovery.
// They arise legitimately: a multi-queue checkpoint where queue A succeeded
// and queue B failed keeps the WAL, and the retry rewrites A; a crash between
// segment fsync and WAL unlink leaves the segment copy plus a later
// re-checkpoint. Dedup key is (queue, offset) — the same key the WAL-vs-
// segment dedup and the in-file index use — and the NEWEST copy (highest
// segmentNum, active beats sealed) wins.

// dedupRecs builds v-bodied records for the given offsets.
func dedupRecs(body string, offsets ...uint64) []*RecoveryMessage {
	recs := make([]*RecoveryMessage, 0, len(offsets))
	for _, off := range offsets {
		recs = append(recs, &RecoveryMessage{
			QueueName: "q",
			Offset:    off,
			Message: &protocol.Message{
				Exchange: "amq.direct", RoutingKey: "q",
				Body: []byte(body), DeliveryMode: 2, DeliveryTag: off,
			},
		})
	}
	return recs
}

// assertDeduped checks the newest-wins body assignment for offsets 1..10.
func assertDeduped(t *testing.T, msgs []*RecoveryMessage) {
	t.Helper()
	require.Len(t, msgs, 10, "exactly 10 records (1 per offset), regardless of how many segment copies exist")
	for i, rm := range msgs {
		off := uint64(i + 1)
		require.Equal(t, off, rm.Offset, "output must be sorted by offset ascending (deterministic)")
		want := "v2"
		if off == 5 || off == 6 {
			want = "v3"
		}
		require.Equal(t, want, string(rm.Message.Body), "offset %d must carry the newest copy's body", off)
	}
}

// assertDedupedMsgs is assertDeduped for the protocol.Message shape
// GetRecoverableMessages returns (DeliveryTag == offset).
func assertDedupedMsgs(t *testing.T, msgs []*protocol.Message) {
	t.Helper()
	require.Len(t, msgs, 10, "exactly 10 records (1 per offset), regardless of how many segment copies exist")
	for i, m := range msgs {
		off := uint64(i + 1)
		require.Equal(t, off, m.DeliveryTag, "output must be sorted by offset ascending (deterministic)")
		want := "v2"
		if off == 5 || off == 6 {
			want = "v3"
		}
		require.Equal(t, want, string(m.Body), "offset %d must carry the newest copy's body", off)
	}
}

// TestRecovery_CrossSegmentDedup (TDD test 27).
func TestRecovery_CrossSegmentDedup(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultSegmentConfig()
	cfg.SegmentSize = 1 // every batch rolls into a new segment file
	sm, err := NewSegmentManagerWithConfig(dir, cfg)
	require.NoError(t, err)

	all := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	require.NoError(t, sm.CheckpointBatch("q", dedupRecs("v1", all...)))
	require.NoError(t, sm.CheckpointBatch("q", dedupRecs("v2", all...)))
	require.NoError(t, sm.CheckpointBatch("q", dedupRecs("v3", 5, 6)))
	require.NoError(t, sm.Close())

	t.Run("RecoverFromSegments", func(t *testing.T) {
		sm2, serr := NewSegmentManagerWithConfig(dir, cfg)
		require.NoError(t, serr)
		t.Cleanup(func() { _ = sm2.Close() })
		byQueue, serr := sm2.RecoverFromSegments()
		require.NoError(t, serr)
		assertDeduped(t, byQueue["q"])
	})

	t.Run("GetRecoverableMessages", func(t *testing.T) {
		ds2, derr := NewDisruptorStorageWithEngineConfig(dir, interfaces.EngineConfig{})
		require.NoError(t, derr)
		t.Cleanup(func() { _ = ds2.Close() })
		recovered, derr := ds2.GetRecoverableMessages()
		require.NoError(t, derr)
		assertDedupedMsgs(t, recovered["q"])
	})
}

// TestCheckpoint_CrashBetweenSegmentFsyncAndUnlink_RecoversOnce (TDD test 28):
// the unlink hook simulates dying after every CheckpointBatch succeeded but
// before the WAL file was unlinked. The WAL stays; recovery must still see
// each offset exactly once (WAL wins over segments), and a second checkpoint
// pass must ONLY unlink — never rewrite the segment records.
func TestCheckpoint_CrashBetweenSegmentFsyncAndUnlink_RecoversOnce(t *testing.T) {
	t.Run("abandoned after the crash: recovery sees each offset once", func(t *testing.T) {
		dir := t.TempDir()
		wm, err := NewWALManagerWithConfig(dir, coldTailWALConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = wm.Close() }) // abandoned, not closed
		sm, err := NewSegmentManager(dir)
		require.NoError(t, err)
		t.Cleanup(func() { _ = sm.Close() })
		wm.SetSegmentManager(sm)

		for off := uint64(1); off <= 5; off++ {
			require.NoError(t, wm.Write("q", segFaultMessage(off, "crash-once-body"), off))
		}
		sw := wm.sharedWAL
		sw.fileMutex.Lock()
		walPath := sw.currentFile.Name()
		require.NoError(t, sw.rollFile())
		sw.fileMutex.Unlock()

		restore := SetCheckpointUnlinkForTest(func(path string) error {
			if strings.HasPrefix(path, dir) {
				return errSimulatedCrash
			}
			return os.Remove(path)
		})
		sw.performCheckpoint()
		restore()

		// The checkpoint's batches all succeeded; only the unlink "crashed".
		_, serr := os.Stat(walPath)
		require.False(t, os.IsNotExist(serr), "the WAL file must still be on disk (crash before unlink)")
		sw.oldFilesMutex.RLock()
		info := sw.oldFiles[1]
		sw.oldFilesMutex.RUnlock()
		require.NotNil(t, info)
		require.True(t, info.checkpointed, "the file must be marked checkpointed (only the unlink remains)")

		// Session 2: WAL and segments BOTH hold the records; WAL wins, each
		// offset exactly once.
		ds2, derr := NewDisruptorStorageWithEngineConfig(dir, interfaces.EngineConfig{})
		require.NoError(t, derr)
		t.Cleanup(func() { _ = ds2.Close() })
		recovered, derr := ds2.GetRecoverableMessages()
		require.NoError(t, derr)
		seen := map[uint64]bool{}
		for _, m := range recovered["q"] {
			require.False(t, seen[m.DeliveryTag], "offset %d recovered twice", m.DeliveryTag)
			seen[m.DeliveryTag] = true
		}
		for off := uint64(1); off <= 5; off++ {
			require.True(t, seen[off], "offset %d must be recovered", off)
		}
		require.Len(t, seen, 5)
	})

	t.Run("second pass only unlinks; the segment record count does not grow", func(t *testing.T) {
		dir := t.TempDir()
		wm, err := NewWALManagerWithConfig(dir, coldTailWALConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = wm.Close() })
		sm, err := NewSegmentManager(dir)
		require.NoError(t, err)
		t.Cleanup(func() { _ = sm.Close() })
		wm.SetSegmentManager(sm)

		for off := uint64(1); off <= 5; off++ {
			require.NoError(t, wm.Write("q", segFaultMessage(off, "crash-once-body"), off))
		}
		sw := wm.sharedWAL
		sw.fileMutex.Lock()
		walPath := sw.currentFile.Name()
		require.NoError(t, sw.rollFile())
		sw.fileMutex.Unlock()

		restore := SetCheckpointUnlinkForTest(func(path string) error {
			if strings.HasPrefix(path, dir) {
				return errSimulatedCrash
			}
			return os.Remove(path)
		})
		sw.performCheckpoint()
		cardinalityAfterCrash := segmentIndexCardinality(t, sm, "q")

		// Hook restored: the second pass retries ONLY the unlink.
		restore()
		sw.performCheckpoint()

		_, serr := os.Stat(walPath)
		require.True(t, os.IsNotExist(serr), "the second pass must unlink the checkpointed file")
		require.Equal(t, cardinalityAfterCrash, segmentIndexCardinality(t, sm, "q"),
			"the second pass must not re-checkpoint (segment record count unchanged)")

		// And with the WAL gone, the segments alone recover each offset once.
		sm2, cerr := NewSegmentManager(dir)
		require.NoError(t, cerr)
		t.Cleanup(func() { _ = sm2.Close() })
		byQueue, cerr := sm2.RecoverFromSegments()
		require.NoError(t, cerr)
		require.Len(t, byQueue["q"], 5)
	})
}

// segmentIndexCardinality sums the indexed record count across the queue's
// sealed segments and its active segment index.
func segmentIndexCardinality(t *testing.T, sm *SegmentManager, queue string) int {
	t.Helper()
	val, ok := sm.queueSegments.Load(queue)
	require.True(t, ok)
	qs := val.(*QueueSegments)

	total := 0
	qs.sealedMutex.RLock()
	for _, seg := range qs.sealedSegments {
		seg.mutex.RLock()
		total += len(seg.index)
		seg.mutex.RUnlock()
	}
	qs.sealedMutex.RUnlock()

	qs.mutex.Lock()
	cur, curIdx := qs.currentSegment, qs.currentIndex
	qs.mutex.Unlock()
	if cur != nil && curIdx != nil {
		curIdx.mutex.RLock()
		total += len(curIdx.entries)
		curIdx.mutex.RUnlock()
	}
	return total
}
