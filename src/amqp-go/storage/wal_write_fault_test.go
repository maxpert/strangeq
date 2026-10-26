package storage

import (
	"errors"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// WS-C C1: a failed WAL write or fsync used to leave the partial bytes on disk
// with a stale in-memory fileOffset. The next batch then indexed positions
// that did not match the file (live reads broke), and at restart the scan saw
// a CRC failure with real data after it — interior corruption — and refused
// the boot. Even a "clean" leftover (whole batch written, fsync failed)
// resurrected records whose publisher was nacked, breaking tx atomicity.
//
// The fix truncates back to the pre-write offset (fdatasync + Stat check);
// if that repair fails it poisons the file (never written again) and rolls.

// walFaultBody is large enough that a 100-byte partial write can never
// complete a record, so the damaged-tail classification is deterministic.
const walFaultBody = "wal-write-fault-body-with-plenty-of-padding-padding-padding-padding"

// walFaultMessage builds a durable message for offset off.
func walFaultMessage(off uint64, body string) *protocol.Message {
	return &protocol.Message{
		Exchange:     "amq.direct",
		RoutingKey:   "q",
		Body:         []byte(body),
		DeliveryMode: 2,
		DeliveryTag:  off,
	}
}

// walFileSize returns the size of the WAL's current file, read under
// fileMutex so it cannot race a concurrent flush or roll.
func walFileSize(t testing.TB, wm *WALManager) int64 {
	t.Helper()
	sw := wm.sharedWAL
	require.NotNil(t, sw)
	sw.fileMutex.Lock()
	defer sw.fileMutex.Unlock()
	require.NotNil(t, sw.currentFile)
	st, err := sw.currentFile.Stat()
	require.NoError(t, err)
	return st.Size()
}

// installOneShotWriteHook installs a WAL write hook that, exactly once for a
// file under dir, writes the first 100 bytes for real and then reports
// ENOSPC. Everything else passes through.
func installOneShotWriteHook(t *testing.T, dir string, realBytes int) (restore func()) {
	t.Helper()
	var fired atomic.Int32
	return SetWALWriteForTest(func(f *os.File, b []byte) (int, error) {
		if fired.Add(1) == 1 && strings.HasPrefix(f.Name(), dir) {
			if realBytes > len(b) {
				realBytes = len(b)
			}
			n, err := f.Write(b[:realBytes])
			if err != nil {
				return n, err
			}
			return n, syscall.ENOSPC
		}
		return f.Write(b)
	})
}

// requireWALWriteError asserts err is a *WALWriteError with the given op and
// repaired state (unwrapping the ErrWALUnavailable wrapper if present).
func requireWALWriteError(t *testing.T, err error, op string, repaired bool) {
	t.Helper()
	require.Error(t, err)
	var werr *WALWriteError
	require.True(t, errors.As(err, &werr), "error must be a *WALWriteError, got %T: %v", err, err)
	assert.Equal(t, op, werr.Op)
	assert.Equal(t, repaired, werr.Repaired)
}

// recoveredOffsets collects the offsets a RecoverFromWAL scan returned.
func recoveredOffsetSet(t *testing.T, recovered []*RecoveryMessage) map[uint64]bool {
	t.Helper()
	got := make(map[uint64]bool, len(recovered))
	for _, rm := range recovered {
		require.False(t, got[rm.Offset], "offset %d recovered twice", rm.Offset)
		got[rm.Offset] = true
	}
	return got
}

// TestWAL_PartialWriteThenRecovery_NoInteriorCorruption (TDD test 1).
func TestWAL_PartialWriteThenRecovery_NoInteriorCorruption(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, DefaultWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() }) // session 1 is abandoned, not closed

	for off := uint64(1); off <= 5; off++ {
		require.NoError(t, wm.Write("q", walFaultMessage(off, walFaultBody), off))
	}
	pre := walFileSize(t, wm)

	restore := installOneShotWriteHook(t, dir, 100)
	err = wm.Write("q", walFaultMessage(6, walFaultBody), 6)
	restore()
	requireWALWriteError(t, err, "write", true)

	// The file is exactly what it was before the failed batch.
	require.Equal(t, pre, walFileSize(t, wm))
	require.Equal(t, pre, wm.sharedWAL.fileOffset.Load())

	// Subsequent writes succeed and are readable IN-SESSION (today the stale
	// fileOffset makes these reads fail with offset mismatch / CRC errors).
	for off := uint64(7); off <= 10; off++ {
		require.NoError(t, wm.Write("q", walFaultMessage(off, walFaultBody), off))
	}
	for off := uint64(7); off <= 10; off++ {
		msg, rerr := wm.Read("q", off)
		require.NoError(t, rerr, "in-session read of offset %d", off)
		require.Equal(t, walFaultBody, string(msg.Body))
	}

	// Session 2 constructs without refusal and recovers exactly 1-5, 7-10.
	wm2, cerr := NewWALManager(dir)
	require.NoError(t, cerr, "session 2 must not be refused (interior corruption)")
	defer wm2.Close()
	recovered, cerr := wm2.RecoverFromWAL()
	require.NoError(t, cerr)
	got := recoveredOffsetSet(t, recovered)
	want := map[uint64]bool{}
	for off := uint64(1); off <= 5; off++ {
		want[off] = true
	}
	for off := uint64(7); off <= 10; off++ {
		want[off] = true
	}
	require.Equal(t, want, got, "offset 6 (nacked, repaired) must be absent; all others present")
}

// TestWAL_FsyncErrorTruncatesBatch (TDD test 2).
func TestWAL_FsyncErrorTruncatesBatch(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, DefaultWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	require.NoError(t, wm.Write("q", walFaultMessage(1, walFaultBody), 1))
	sizeAfter1 := walFileSize(t, wm)

	var calls atomic.Int32
	restore := SetWALGroupCommitFsyncForTest(func(f *os.File) error {
		if calls.Add(1) == 1 {
			return syscall.EIO
		}
		return fdatasyncFile(f)
	})
	defer restore()

	err = wm.Write("q", walFaultMessage(2, walFaultBody), 2)
	requireWALWriteError(t, err, "fsync", true)
	require.Equal(t, sizeAfter1, walFileSize(t, wm),
		"an fsync-failed batch's bytes must be truncated back (fsyncgate: nobody knows they reached the media)")

	require.NoError(t, wm.Write("q", walFaultMessage(3, walFaultBody), 3))
	_, rerr := wm.Read("q", 3)
	require.NoError(t, rerr, "in-session read after a repaired fsync failure")

	wm2, cerr := NewWALManager(dir)
	require.NoError(t, cerr)
	defer wm2.Close()
	recovered, cerr := wm2.RecoverFromWAL()
	require.NoError(t, cerr)
	got := recoveredOffsetSet(t, recovered)
	require.Equal(t, map[uint64]bool{1: true, 3: true}, got,
		"the nacked record 2 must not be resurrected at restart")
}

// TestWALTx_FsyncError_LeavesNoPartialTx (TDD test 3).
func TestWALTx_FsyncError_LeavesNoPartialTx(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, DefaultWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	var fired atomic.Int32
	restore := SetWALTxFsyncForTest(func(f *os.File) error {
		if fired.Add(1) == 1 {
			return syscall.EIO
		}
		return fdatasyncFile(f)
	})
	defer restore()

	pre := walFileSize(t, wm)

	tx := func(offsets ...uint64) []*RecoveryMessage {
		recs := make([]*RecoveryMessage, 0, len(offsets))
		for _, off := range offsets {
			recs = append(recs, &RecoveryMessage{
				QueueName: "q",
				Offset:    off,
				Message:   walFaultMessage(off, walFaultBody),
			})
		}
		return recs
	}

	err = wm.WriteTxAtomic(tx(10, 11, 12))
	requireWALWriteError(t, err, "tx-fsync", true)
	require.Equal(t, pre, walFileSize(t, wm), "the whole tx unit must be gone")

	require.NoError(t, wm.WriteTxAtomic(tx(20, 21)))
	msg, rerr := wm.Read("q", 20)
	require.NoError(t, rerr)
	require.Equal(t, walFaultBody, string(msg.Body))

	wm2, cerr := NewWALManager(dir)
	require.NoError(t, cerr)
	defer wm2.Close()
	recovered, cerr := wm2.RecoverFromWAL()
	require.NoError(t, cerr)
	got := recoveredOffsetSet(t, recovered)
	require.Equal(t, map[uint64]bool{20: true, 21: true}, got,
		"a tx the client was told FAILED (10-12) must never appear after restart")
}

// TestWALTx_ShortWrite_LeavesNoPartialTx (TDD test 4).
func TestWALTx_ShortWrite_LeavesNoPartialTx(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, DefaultWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	var fired atomic.Int32
	restore := SetWALWriteForTest(func(f *os.File, b []byte) (int, error) {
		if fired.Add(1) == 1 && strings.HasPrefix(f.Name(), dir) {
			n, werr := f.Write(b[:len(b)/2])
			if werr != nil {
				return n, werr
			}
			return n, syscall.ENOSPC
		}
		return f.Write(b)
	})
	defer restore()

	pre := walFileSize(t, wm)

	recs := []*RecoveryMessage{
		{QueueName: "q", Offset: 10, Message: walFaultMessage(10, walFaultBody)},
		{QueueName: "q", Offset: 11, Message: walFaultMessage(11, walFaultBody)},
		{QueueName: "q", Offset: 12, Message: walFaultMessage(12, walFaultBody)},
	}
	err = wm.WriteTxAtomic(recs)
	requireWALWriteError(t, err, "tx-write", true)
	require.Equal(t, pre, walFileSize(t, wm), "one truncate to startPos removes the whole contiguous tx unit")

	require.NoError(t, wm.WriteTxAtomic([]*RecoveryMessage{
		{QueueName: "q", Offset: 20, Message: walFaultMessage(20, walFaultBody)},
		{QueueName: "q", Offset: 21, Message: walFaultMessage(21, walFaultBody)},
	}))
	_, rerr := wm.Read("q", 20)
	require.NoError(t, rerr)

	wm2, cerr := NewWALManager(dir)
	require.NoError(t, cerr, "session 2 must not be refused (interior corruption from the partial tx)")
	defer wm2.Close()
	recovered, cerr := wm2.RecoverFromWAL()
	require.NoError(t, cerr)
	got := recoveredOffsetSet(t, recovered)
	require.Equal(t, map[uint64]bool{20: true, 21: true}, got)
}

// TestWAL_TruncateFails_PoisonsAndRolls (TDD test 5): when the repair's own
// truncate fails, the file is poisoned (never written again), retired with a
// damagedTail marker, and replaced immediately; the WAL stays healthy.
func TestWAL_TruncateFails_PoisonsAndRolls(t *testing.T) {
	t.Run("with checkpoint: damaged file unlinked, record served from segment", func(t *testing.T) {
		t.Skip("checkpoint copying removed in segstore v1")
		dir := t.TempDir()
		wm, err := NewWALManagerWithConfig(dir, coldTailWALConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = wm.Close() })
		sw := wm.sharedWAL

		require.NoError(t, wm.Write("q", walFaultMessage(1, walFaultBody), 1))
		pre := walFileSize(t, wm)
		startNum := sw.fileNum.Load()
		walPath := sw.currentFile.Name()

		var truncFired atomic.Int32
		truncRestore := SetWALTruncateForTest(func(f *os.File, size int64) error {
			if truncFired.Add(1) == 1 && strings.HasPrefix(f.Name(), dir) {
				return syscall.EIO
			}
			return walTruncateDefault(f, size, true)
		})
		writeRestore := installOneShotWriteHook(t, dir, 100)

		err = wm.Write("q", walFaultMessage(2, walFaultBody), 2)
		writeRestore()
		truncRestore()
		requireWALWriteError(t, err, "write", false)

		// The diverged file was rolled away immediately: fileNum advanced and
		// the old file is retired with damagedTail + durableSize == pre.
		require.Equal(t, startNum+1, sw.fileNum.Load())
		sw.oldFilesMutex.RLock()
		info := sw.oldFiles[startNum]
		sw.oldFilesMutex.RUnlock()
		require.NotNil(t, info)
		require.True(t, info.damagedTail)
		require.Equal(t, pre, info.durableSize)

		// Rolled + writable again => healthy.
		require.NoError(t, wm.Health())

		// Writes continue into the new file.
		require.NoError(t, wm.Write("q", walFaultMessage(3, walFaultBody), 3))

		// The damaged file is checkpointed by scanning ONLY [0, durableSize):
		// record 1 reaches the segment, the garbage tail never does, and the
		// file is unlinked afterwards.
		sm, serr := NewSegmentManager(dir)
		require.NoError(t, serr)
		t.Cleanup(func() { _ = sm.Close() })
		wm.SetSegmentManager(sm)
		sw.performCheckpoint()

		_, serr = os.Stat(walPath)
		require.True(t, os.IsNotExist(serr), "the damaged WAL file must be unlinked after its durable prefix is checkpointed")
		msg, serr := sm.Read("q", 1)
		require.NoError(t, serr, "record 1 must be served from the segment")
		require.Equal(t, walFaultBody, string(msg.Body))
	})

	t.Run("without checkpoint: session 2 recovers the durable records", func(t *testing.T) {
		dir := t.TempDir()
		wm, err := NewWALManagerWithConfig(dir, coldTailWALConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = wm.Close() })
		sw := wm.sharedWAL

		require.NoError(t, wm.Write("q", walFaultMessage(1, walFaultBody), 1))
		startNum := sw.fileNum.Load()

		var truncFired atomic.Int32
		truncRestore := SetWALTruncateForTest(func(f *os.File, size int64) error {
			if truncFired.Add(1) == 1 && strings.HasPrefix(f.Name(), dir) {
				return syscall.EIO
			}
			return walTruncateDefault(f, size, true)
		})
		writeRestore := installOneShotWriteHook(t, dir, 100)

		err = wm.Write("q", walFaultMessage(2, walFaultBody), 2)
		writeRestore()
		truncRestore()
		requireWALWriteError(t, err, "write", false)
		require.NoError(t, wm.Write("q", walFaultMessage(3, walFaultBody), 3))

		wm2, cerr := NewWALManager(dir)
		require.NoError(t, cerr, "the damaged file's torn tail is benign at boot")
		defer wm2.Close()
		recovered, cerr := wm2.RecoverFromWAL()
		require.NoError(t, cerr)
		got := recoveredOffsetSet(t, recovered)
		require.True(t, got[1], "durable record 1 must be recovered")
		require.True(t, got[3], "durable record 3 must be recovered")
		require.False(t, got[2], "the nacked partial record 2 must not be recovered")
		require.Equal(t, startNum+1, sw.fileNum.Load())
	})
}

// TestWAL_PoisonedFile_RollFails_NacksThenRecovers (TDD test 6): while the
// poisoned file cannot be rolled away (open failures), writes fail fast with
// ErrWALUnavailable and Health() reports the degradation; once the fault
// clears, the next write self-heals.
func TestWAL_PoisonedFile_RollFails_NacksThenRecovers(t *testing.T) {
	walRollBackoffMin = time.Millisecond
	walRollBackoffMax = 5 * time.Millisecond
	t.Cleanup(func() {
		walRollBackoffMin = 10 * time.Millisecond
		walRollBackoffMax = 1 * time.Second
	})

	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, coldTailWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })
	sw := wm.sharedWAL

	require.NoError(t, wm.Write("q", walFaultMessage(1, walFaultBody), 1))
	startNum := sw.fileNum.Load()

	var failOpen atomic.Bool
	failOpen.Store(true)
	openRestore := SetWALOpenFileForTest(func(name string, flag int, perm os.FileMode) (*os.File, error) {
		if failOpen.Load() && strings.HasPrefix(name, dir) {
			return nil, syscall.ENFILE
		}
		return os.OpenFile(name, flag, perm)
	})
	defer openRestore()

	var truncFired atomic.Int32
	truncRestore := SetWALTruncateForTest(func(f *os.File, size int64) error {
		if truncFired.Add(1) == 1 && strings.HasPrefix(f.Name(), dir) {
			return syscall.EIO
		}
		return walTruncateDefault(f, size, true)
	})
	defer truncRestore()
	writeRestore := installOneShotWriteHook(t, dir, 100)
	defer writeRestore()

	// Poison: short write + failed truncate + failed roll.
	err = wm.Write("q", walFaultMessage(2, walFaultBody), 2)
	requireWALWriteError(t, err, "write", false)

	// While the poisoned file cannot be rolled away, every write fails fast
	// with ErrWALUnavailable (no disk I/O on the diverged file).
	for off := uint64(3); off <= 22; off++ {
		werr := wm.Write("q", walFaultMessage(off, walFaultBody), off)
		require.True(t, errors.Is(werr, ErrWALUnavailable),
			"write %d while poisoned must fail with ErrWALUnavailable, got %v", off, werr)
	}
	require.Error(t, wm.Health(), "Health must report the poisoned/degraded WAL")

	// Clear the fault: the next write's ensureWritableLocked rolls the poisoned
	// file away (backoff <= 5ms) and succeeds — the WAL self-heals.
	failOpen.Store(false)
	nextOff := uint64(22)
	var healedOffset uint64
	require.Eventually(t, func() bool {
		nextOff++
		if werr := wm.Write("q", walFaultMessage(nextOff, walFaultBody), nextOff); werr == nil {
			healedOffset = nextOff
			return true
		}
		return false
	}, 1*time.Second, time.Millisecond, "a write must succeed once the roll fault clears")
	require.NoError(t, wm.Health())
	require.Greater(t, sw.fileNum.Load(), startNum, "the poisoned file must have been rolled away")

	// Session 2 recovers every SUCCESSFUL write (1 and the healed one) and
	// nothing else: the nacked record 2 never became durable.
	wm2, cerr := NewWALManager(dir)
	require.NoError(t, cerr)
	defer wm2.Close()
	recovered, cerr := wm2.RecoverFromWAL()
	require.NoError(t, cerr)
	got := recoveredOffsetSet(t, recovered)
	require.True(t, got[1])
	require.True(t, got[healedOffset], "healed offset %d must be recovered", healedOffset)
	require.False(t, got[2], "the nacked record 2 must not be recovered")
	require.Len(t, got, 2, "exactly the successful writes must be recovered")
}

// TestWAL_OffsetInvariantHoldsUnderFaultMix (TDD test 7): 200 iterations of a
// seeded random mix of OK / short write / write error / fsync error / tx
// variants. The runtime I3 assertion (armed package-wide) must never panic,
// and session 2 must recover exactly the offsets whose write returned nil.
func TestWAL_OffsetInvariantHoldsUnderFaultMix(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, DefaultWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	var mu sync.Mutex
	nextWriteFault := "" // "", "short", "err"
	nextFsyncFault := false
	nextTxFsyncFault := false

	writeRestore := SetWALWriteForTest(func(f *os.File, b []byte) (int, error) {
		mu.Lock()
		fault := nextWriteFault
		nextWriteFault = ""
		mu.Unlock()
		switch fault {
		case "short":
			n, werr := f.Write(b[:len(b)/2])
			if werr != nil {
				return n, werr
			}
			return n, syscall.ENOSPC
		case "err":
			return 0, syscall.EIO
		default:
			return f.Write(b)
		}
	})
	defer writeRestore()
	fsyncRestore := SetWALGroupCommitFsyncForTest(func(f *os.File) error {
		mu.Lock()
		fault := nextFsyncFault
		nextFsyncFault = false
		mu.Unlock()
		if fault {
			return syscall.EIO
		}
		return fdatasyncFile(f)
	})
	defer fsyncRestore()
	txFsyncRestore := SetWALTxFsyncForTest(func(f *os.File) error {
		mu.Lock()
		fault := nextTxFsyncFault
		nextTxFsyncFault = false
		mu.Unlock()
		if fault {
			return syscall.EIO
		}
		return fdatasyncFile(f)
	})
	defer txFsyncRestore()

	rng := rand.New(rand.NewSource(42)) // logged via the expected-set dump on failure
	expected := map[uint64]bool{}

	nextOff := uint64(0)
	newMsg := func() (*protocol.Message, uint64) {
		nextOff++
		return walFaultMessage(nextOff, walFaultBody), nextOff
	}

	for iter := 0; iter < 200; iter++ {
		isTx := rng.Intn(3) == 0
		fault := rng.Intn(6) // 0 = none, 1 = short write, 2 = write err, 3 = fsync err

		var offsets []uint64
		var opErr error
		if isTx {
			var recs []*RecoveryMessage
			for i := 0; i < 2+rng.Intn(2); i++ {
				msg, off := newMsg()
				offsets = append(offsets, off)
				recs = append(recs, &RecoveryMessage{QueueName: "q", Offset: off, Message: msg})
			}
			switch fault {
			case 1:
				mu.Lock()
				nextWriteFault = "short"
				mu.Unlock()
			case 2:
				mu.Lock()
				nextWriteFault = "err"
				mu.Unlock()
			case 3:
				mu.Lock()
				nextTxFsyncFault = true
				mu.Unlock()
			}
			opErr = wm.WriteTxAtomic(recs)
		} else {
			msg, off := newMsg()
			offsets = []uint64{off}
			switch fault {
			case 1:
				mu.Lock()
				nextWriteFault = "short"
				mu.Unlock()
			case 2:
				mu.Lock()
				nextWriteFault = "err"
				mu.Unlock()
			case 3:
				mu.Lock()
				nextFsyncFault = true
				mu.Unlock()
			}
			opErr = wm.Write("q", msg, off)
		}

		if opErr == nil {
			for _, off := range offsets {
				expected[off] = true
			}
		}
		// A failed op's offsets are simply absent from `expected`; the repair
		// removed their bytes, and an error return means nobody was told they
		// were stored.
	}

	// Session 2 recovers exactly the successful set. (Session 1 is abandoned;
	// abandoned-session goroutines are quiescent under the 1h-interval config
	// except DefaultWALConfig's intervals — the cleanup Close drains them.)
	wm2, cerr := NewWALManager(dir)
	require.NoError(t, cerr)
	defer wm2.Close()
	recovered, cerr := wm2.RecoverFromWAL()
	require.NoError(t, cerr)
	got := recoveredOffsetSet(t, recovered)
	if !assert.Equal(t, expected, got) {
		var wantOnly, gotOnly []uint64
		for off := range expected {
			if !got[off] {
				wantOnly = append(wantOnly, off)
			}
		}
		for off := range got {
			if !expected[off] {
				gotOnly = append(gotOnly, off)
			}
		}
		t.Fatalf("seeded mix (source 42) diverged: missing=%v unexpected=%v", wantOnly, gotOnly)
	}
}
