package storage

import (
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// WS-C review finding 3: writeMessage (the SegmentManager.Write path) had no
// qs.segPoisoned gate, unlike writeMessageBatchDurable. With the active
// segment poisoned and the replacement open failing, a writeMessage call
// would append to the DAMAGED segment — the exact divergence the poison state
// exists to prevent.

// TestSegment_WriteMessage_RefusesPoisonedSegment (TDD, review finding 3).
func TestSegment_WriteMessage_RefusesPoisonedSegment(t *testing.T) {
	dir := t.TempDir()
	sm, err := NewSegmentManager(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sm.Close() })

	// A healthy active segment.
	require.NoError(t, sm.CheckpointBatch("q", segFaultRecs(1)))
	qs := queueSegmentsOf(t, sm, "q")
	qs.mutex.Lock()
	damaged := qs.currentSegment
	damagedPath := damaged.path
	qs.mutex.Unlock()

	// Poison the active segment (short write + failed truncate) and keep the
	// replacement from opening, so the damaged segment stays current and
	// poisoned — the state where writing to it must be refused.
	var failOpen atomic.Bool
	failOpen.Store(true)
	openRestore := SetSegmentOpenFileForTest(func(name string, flag int, perm os.FileMode) (*os.File, error) {
		if failOpen.Load() && strings.HasPrefix(name, dir) {
			return nil, syscall.ENFILE
		}
		return os.OpenFile(name, flag, perm)
	})
	defer openRestore()

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

	require.Error(t, sm.CheckpointBatch("q", segFaultRecs(2)), "the batch path poisons the segment")
	wRestore()
	tRestore()

	qs.mutex.Lock()
	poisoned := qs.segPoisoned
	stillCurrent := qs.currentSegment == damaged
	qs.mutex.Unlock()
	require.NotNil(t, poisoned, "the active segment must be poisoned")
	require.True(t, stillCurrent, "with the replacement open failing, the damaged segment stays current")

	st, serr := os.Stat(damagedPath)
	require.NoError(t, serr)
	damagedSize := st.Size()

	// RED pre-fix: writeMessage appends to the damaged segment (returns nil,
	// bytes land at wrong positions). The gate must refuse instead.
	err = sm.Write("q", segFaultMessage(3, segFaultBody), 3)
	require.Error(t, err, "writeMessage must refuse a poisoned active segment, exactly like writeMessageBatchDurable")

	st, serr = os.Stat(damagedPath)
	require.NoError(t, serr)
	require.Equal(t, damagedSize, st.Size(), "no byte may be appended to the damaged segment")

	// Once the replacement can open again, writeMessage heals onto a fresh
	// segment and the damaged one is left untouched.
	failOpen.Store(false)
	require.NoError(t, sm.Write("q", segFaultMessage(4, segFaultBody), 4))
	st, serr = os.Stat(damagedPath)
	require.NoError(t, serr)
	require.Equal(t, damagedSize, st.Size(), "the retired damaged segment is never written again")

	msg, rerr := sm.Read("q", 4)
	require.NoError(t, rerr)
	require.Equal(t, segFaultBody, string(msg.Body))
}
