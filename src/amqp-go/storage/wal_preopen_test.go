package storage

// W8: the eager next-file pre-open.
//
// The roll used to run its retired-file directory F_FULLFSYNC inline under
// fileMutex on the commit path. Under crash_model: barrier nothing else flushes
// the drive cache, so that call paid the whole previous file's backlog (~109 ms
// for a 512 MiB file) while the committer held fileMutex — the F3 confirm tail.
//
// W8 moves the CREATE off the commit path into a helper goroutine
// (preopenNext): step 2 creates + opens + directory-syncs the next file and
// parks it. rollFile then only swaps the parked handle. Under crash_model:
// barrier step 1 (the retired file's tail flush) is DROPPED entirely — the
// frontier recovery rule no longer depends on it and nothing defers media
// writes any more; force_flush keeps it as shipped. These tests pin the control
// flow with the syncDir / walOpenFile seams and the pre-open completion hook,
// never with sleeps.

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// preopenMsg is a small durable publish for the live-WAL fixtures.
func preopenMsg(tag uint64) *protocol.Message {
	return &protocol.Message{
		Body:         []byte(fmt.Sprintf("preopen-record-%08d", tag)),
		RoutingKey:   "q",
		DeliveryMode: 2,
		DeliveryTag:  tag,
	}
}

// noopGroupCommit removes the real (slow) confirm barrier from the live-WAL
// W8 fixtures; these tests exercise CONTROL FLOW, not durability.
func noopGroupCommit(t *testing.T) {
	t.Helper()
	restore := SetWALGroupCommitFsyncForTest(func(*os.File) error { return nil })
	t.Cleanup(restore)
}

// waitPreopenDone waits for `want` pre-open completions.
func waitPreopenDone(t *testing.T, ch <-chan struct{}, want int) {
	t.Helper()
	for i := 0; i < want; i++ {
		select {
		case <-ch:
		case <-time.After(10 * time.Second):
			t.Fatalf("pre-open did not complete (wanted %d completions)", want)
		}
	}
}

// writeUntilFileNum writes records until the shared WAL has rolled to (at
// least) file `target`, returning nothing. It never sleeps.
func writeUntilFileNum(t *testing.T, wm *WALManager, sw *QueueWAL, target uint64) {
	t.Helper()
	for i := uint64(1); i < 500000; i++ {
		require.NoError(t, wm.Write("q", preopenMsg(i), i), "write %d", i)
		if sw.fileNum.Load() >= target {
			return
		}
	}
	t.Fatalf("the WAL never rolled to file %d", target)
}

// walFileExists reports whether the shared WAL directory holds file `num`.
func walFileExists(t *testing.T, sw *QueueWAL, num uint64) bool {
	t.Helper()
	_, err := os.Stat(filepath.Join(sw.dataDir, fmt.Sprintf("%020d%s", num, WALFileExtension)))
	return err == nil
}

// TestWALPreopen_RollTakesParkedFileWithoutBlockingOnTheFlush is THE decisive
// W8 test (T-W8-1). With a file parked and the helper's directory flush
// deliberately BLOCKED, a roll on the commit path must still complete AND must
// issue neither an open nor a directory flush of its own: it only swaps the
// parked handle. If it fell back to the inline open it would call walOpenFile
// (and, for a fresh file, syncDir) — both observed here.
func TestWALPreopen_RollTakesParkedFileWithoutBlockingOnTheFlush(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, coldTailWALConfig())
	require.NoError(t, err)
	defer func() { _ = wm.Close() }()
	sw := wm.sharedWAL
	noopGroupCommit(t)

	done := make(chan struct{}, 32)
	restoreDone := SetWALPreopenDoneForTest(func() {
		select {
		case done <- struct{}{}:
		default:
		}
	})
	defer restoreDone()

	// Count every commit-path open and directory flush. The helper's own
	// directory barrier is gated (blocked) once armed, so any delta during the
	// armed roll is the roll's.
	var mu sync.Mutex
	opens, syncs := 0, 0
	armed := false
	gate := make(chan struct{})
	restoreOpen := SetWALOpenFileForTest(func(name string, flag int, perm os.FileMode) (*os.File, error) {
		mu.Lock()
		opens++
		mu.Unlock()
		return os.OpenFile(name, flag, perm)
	})
	defer restoreOpen()
	restoreSync := SetSyncDirForTest(func(string) error {
		mu.Lock()
		isArmed := armed
		mu.Unlock()
		if isArmed {
			// Block EVERY armed directory fsync so a roll that fell back and
			// tried to create a file would land here and hang.
			<-gate
		}
		mu.Lock()
		syncs++
		mu.Unlock()
		return nil
	})
	defer restoreSync()
	defer close(gate) // release the helper before Close(), whatever happens

	// Roll once (inline: nothing is parked yet), then wait for the helper to
	// park the next file.
	writeUntilFileNum(t, wm, sw, 2)
	waitPreopenDone(t, done, 1)
	parked := sw.fileNum.Load() + 1
	require.True(t, walFileExists(t, sw, parked),
		"the helper must have created the next WAL file (%d) off the commit path", parked)

	// Arm the counters.
	mu.Lock()
	opensBefore, syncsBefore := opens, syncs
	beforeNum := sw.fileNum.Load()
	armed = true
	mu.Unlock()

	// The next roll must consume the parked file — no open, no flush. Run it
	// off-goroutine so a blocked commit path fails the test instead of hanging
	// it.
	rolled := make(chan error, 1)
	go func() {
		for i := uint64(100000); i < 400000; i++ {
			if werr := wm.Write("q", preopenMsg(i), i); werr != nil {
				rolled <- werr
				return
			}
			if sw.fileNum.Load() > beforeNum {
				rolled <- nil
				return
			}
		}
		rolled <- fmt.Errorf("never rolled past file %d", beforeNum)
	}()

	select {
	case werr := <-rolled:
		require.NoError(t, werr)
	case <-time.After(10 * time.Second):
		t.Fatalf("the roll blocked on an open/directory flush: it did not take the parked file")
	}

	mu.Lock()
	opensAfter, syncsAfter := opens, syncs
	mu.Unlock()
	require.Equal(t, opensBefore, opensAfter,
		"the roll must issue NO open on the commit path (it took the parked handle)")
	require.Equal(t, syncsBefore, syncsAfter,
		"the roll must issue NO directory fsync on the commit path")
	require.Equal(t, parked, sw.fileNum.Load(),
		"the roll must have swapped in the parked file")
}

// TestWALPreopen_NoParkFallsBackToInlineOpen pins the degradation path: with
// the helper disabled, every roll takes the pre-W8 inline open and a fresh file
// is created. This is the baseline behaviour W8 must never regress.
func TestWALPreopen_NoParkFallsBackToInlineOpen(t *testing.T) {
	t.Cleanup(SetWALPreopenEnabledForTest(false))

	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, coldTailWALConfig())
	require.NoError(t, err)
	defer func() { _ = wm.Close() }()
	sw := wm.sharedWAL
	noopGroupCommit(t)

	writeUntilFileNum(t, wm, sw, 3)
	require.True(t, walFileExists(t, sw, sw.fileNum.Load()),
		"the active file must exist on disk")
}

// TestWALPreopen_FailedPreopenFallsBackToInlineOpen pins T-W8-2: when the
// helper's open of the next file fails, nothing is parked and the roll falls
// back to the inline open, which succeeds. Only the FIRST open of the target is
// faulted, so the helper's attempt fails and the roll's inline attempt wins.
func TestWALPreopen_FailedPreopenFallsBackToInlineOpen(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, coldTailWALConfig())
	require.NoError(t, err)
	defer func() { _ = wm.Close() }()
	sw := wm.sharedWAL
	noopGroupCommit(t)

	done := make(chan struct{}, 32)
	restoreDone := SetWALPreopenDoneForTest(func() {
		select {
		case done <- struct{}{}:
		default:
		}
	})
	defer restoreDone()

	// Fault only the FIRST open of file 3 (the helper's pre-open); the roll's
	// inline retry must be allowed through.
	target := fmt.Sprintf("%020d%s", uint64(3), WALFileExtension)
	var mu sync.Mutex
	opens := 0
	failed := false
	restoreOpen := SetWALOpenFileForTest(func(name string, flag int, perm os.FileMode) (*os.File, error) {
		if filepath.Base(name) == target {
			mu.Lock()
			opens++
			first := opens == 1
			if first {
				failed = true
			}
			mu.Unlock()
			if first {
				return nil, syscall.EIO
			}
		}
		return os.OpenFile(name, flag, perm)
	})
	defer restoreOpen()

	// Roll to file 2 (inline), then let the helper's pre-open of file 3 fail.
	writeUntilFileNum(t, wm, sw, 2)
	waitPreopenDone(t, done, 1)
	mu.Lock()
	sawFailure := failed
	mu.Unlock()
	require.True(t, sawFailure, "the helper's pre-open of the next file must have been attempted and failed")

	// Now fill file 2: the roll to file 3 has no parked file, so it opens
	// inline and succeeds.
	writeUntilFileNum(t, wm, sw, 3)
	require.Equal(t, uint64(3), sw.fileNum.Load(),
		"a failed pre-open must not stop the roll: the inline open is the fallback")
}

// TestWALPreopen_RollWaitsForInFlightOpen pins T-W8-5: a roll that needs the
// exact file the helper is still opening WAITS for that result rather than
// opening the same path a second time. The helper's second directory sync (the
// creation sync inside its step 2 under barrier mode) is gated, so the helper
// is stuck mid-open while the roll arrives; the roll must block until the gate
// is released.
func TestWALPreopen_RollWaitsForInFlightOpen(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, coldTailWALConfig())
	require.NoError(t, err)
	defer func() { _ = wm.Close() }()
	sw := wm.sharedWAL
	noopGroupCommit(t)

	var mu sync.Mutex
	n := 0
	gate := make(chan struct{})
	var gateCloseOnce sync.Once
	releaseGate := func() { gateCloseOnce.Do(func() { close(gate) }) }
	restoreSync := SetSyncDirForTest(func(string) error {
		mu.Lock()
		n++
		cur := n
		mu.Unlock()
		if cur == 2 {
			// The helper's creation directory sync (step 2 under barrier mode):
			// block it so it is mid-open when the roll arrives.
			<-gate
		}
		return nil
	})
	defer restoreSync()
	defer releaseGate()

	// Roll to file 2 (inline: this is dir sync #1). The helper then begins
	// opening file 3 and blocks in step 1 (dir sync #2).
	writeUntilFileNum(t, wm, sw, 2)
	deadline := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		reached := n >= 2
		mu.Unlock()
		if reached {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the helper never reached its gated step-1 flush")
		}
		time.Sleep(time.Millisecond)
	}

	// Fill file 2: the roll wants file 3, which the helper is opening — it must
	// wait, so the write must NOT complete while the gate is held.
	before := sw.fileNum.Load()
	rolled := make(chan error, 1)
	go func() {
		for i := uint64(200000); i < 500000; i++ {
			if werr := wm.Write("q", preopenMsg(i), i); werr != nil {
				rolled <- werr
				return
			}
			if sw.fileNum.Load() > before {
				rolled <- nil
				return
			}
		}
		rolled <- fmt.Errorf("never rolled past file %d", before)
	}()

	select {
	case werr := <-rolled:
		t.Fatalf("the roll must WAIT for the helper's in-flight open of the next file, "+
			"but it completed early (err=%v)", werr)
	case <-time.After(300 * time.Millisecond):
		// Expected: the roll is parked on the in-flight pre-open.
	}

	releaseGate() // let the helper finish; the waiting roll then proceeds

	select {
	case werr := <-rolled:
		require.NoError(t, werr, "the roll must complete once the in-flight open finishes")
	case <-time.After(10 * time.Second):
		t.Fatalf("the roll never completed after the in-flight pre-open finished")
	}
	require.Equal(t, before+1, sw.fileNum.Load(),
		"the roll must have advanced exactly one file")
	require.True(t, walFileExists(t, sw, sw.fileNum.Load()))
}

// TestWALPreopen_CloseDiscardsParkedFileAndRebootIsClean pins T-W8-4: a parked
// (unused) header-only file is removed on a clean close, and a reboot is clean.
func TestWALPreopen_CloseDiscardsParkedFileAndRebootIsClean(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, coldTailWALConfig())
	require.NoError(t, err)
	sw := wm.sharedWAL
	noopGroupCommit(t)

	done := make(chan struct{}, 32)
	restoreDone := SetWALPreopenDoneForTest(func() {
		select {
		case done <- struct{}{}:
		default:
		}
	})
	defer restoreDone()

	writeUntilFileNum(t, wm, sw, 2)
	waitPreopenDone(t, done, 1)
	parked := sw.fileNum.Load() + 1
	require.True(t, walFileExists(t, sw, parked), "the helper must have parked the next file")

	require.NoError(t, wm.Close())
	require.False(t, walFileExists(t, sw, parked),
		"a clean close must discard the parked, still-unused header-only file")

	// Reboot: recovery must be clean and every durable record recoverable.
	wm2, err := NewWALManagerWithConfig(dir, coldTailWALConfig())
	require.NoError(t, err)
	defer func() { _ = wm2.Close() }()
	msgs, rerr := wm2.RecoverFromWAL()
	require.NoError(t, rerr, "a reboot after a clean close with a parked file must be clean")
	require.NotEmpty(t, msgs, "the durable records must still be recoverable")
}

// TestWALPreopen_LeftoverParkedFileIsBenignOnBoot pins the crash case the
// diagnosis names: a pre-opened (header-only, 0-record) file left on disk after
// a crash must not affect recovery — the newest file is empty, and the records
// in the files below it are recovered.
func TestWALPreopen_LeftoverParkedFileIsBenignOnBoot(t *testing.T) {
	dir := t.TempDir()

	buf1 := walHeaderBytes()
	for off := uint64(1); off <= 5; off++ {
		buf1 = appendHoleRecord(t, buf1, off)
	}
	writeSharedWALFile(t, dir, 1, buf1)

	buf2 := walHeaderBytes()
	for off := uint64(11); off <= 15; off++ {
		buf2 = appendHoleRecord(t, buf2, off)
	}
	writeSharedWALFile(t, dir, 2, buf2)

	// The leftover pre-opened file: header only, never used.
	writeSharedWALFile(t, dir, 3, walHeaderBytes())

	st, err := NewStorageEngineWithConfig(dir, interfaces.EngineConfig{})
	require.NoError(t, err, "a leftover pre-opened (header-only) WAL file must not refuse the boot")
	defer st.Close()

	msgs, rerr := st.GetRecoverableMessages()
	require.NoError(t, rerr)
	require.Len(t, msgs["q"], 10,
		"every record in the two real files must be recovered; the empty newest file contributes nothing")
}

// TestWALPreopen_RetiredFileTailDefectIsBenign is the crash-after-swap recovery
// test, UPDATED FOR THE NO-FLUSH REALITY.
//
// W8 no longer defers a retired-file tail flush: under barrier mode step 1 is
// DROPPED entirely, and nothing defers media writes any more. But a crash can
// still leave the just-retired file (F-) with a bounded tail defect while the
// pre-opened next file (F) exists as a header-only, data-empty stub. The
// frontier rule (see walTailRule) must treat a bounded defect on F- as benign,
// exactly as R-TAIL-HOLE did for the second-newest file, and force_flush keeps
// the two-newest rule.
//
// Fixture: file 1 clean, file 2 = [records][bounded hole][records], file 3 = the
// pre-opened header-only file (newest). Boot must succeed and recover file 1
// plus file 2's contiguous prefix, in BOTH modes.
func TestWALPreopen_RetiredFileTailDefectIsBenign(t *testing.T) {
	for _, mode := range []struct {
		name      string
		engineCfg interfaces.EngineConfig
	}{
		{"barrier_frontier", interfaces.EngineConfig{CrashModel: interfaces.CrashModelBarrier}},
		{"force_flush_two_newest", interfaces.EngineConfig{CrashModel: interfaces.CrashModelForceFlush}},
	} {
		t.Run(mode.name, func(t *testing.T) {
			dir := t.TempDir()

			clean := walHeaderBytes()
			for off := uint64(1); off <= 5; off++ {
				clean = appendHoleRecord(t, clean, off)
			}
			writeSharedWALFile(t, dir, 1, clean)

			second := walHeaderBytes()
			for off := uint64(11); off <= 15; off++ {
				second = appendHoleRecord(t, second, off)
			}
			second = append(second, make([]byte, 64*1024)...) // the bounded tail defect
			for off := uint64(16); off <= 18; off++ {
				second = appendHoleRecord(t, second, off)
			}
			writeSharedWALFile(t, dir, 2, second)

			// The pre-opened next file exists and is newest (data-empty), so
			// file 2 is F- (resp. second-newest): its bounded hole must be
			// tolerated, never fatal.
			writeSharedWALFile(t, dir, 3, walHeaderBytes())

			st, err := NewStorageEngineWithConfig(dir, mode.engineCfg)
			require.NoError(t, err,
				"a bounded tail defect on the just-retired file must not refuse the boot")
			defer st.Close()

			msgs, rerr := st.GetRecoverableMessages()
			require.NoError(t, rerr)
			require.Len(t, msgs["q"], 10,
				"file 1 (5 records) plus file 2's contiguous prefix (5 records) must be recovered; "+
					"replay stops at the first defect")
			for _, m := range msgs["q"] {
				require.True(t, m.DeliveryTag <= 5 || (m.DeliveryTag >= 11 && m.DeliveryTag <= 15),
					"only the prefix before the defect may be recovered, got tag %d", m.DeliveryTag)
			}
		})
	}
}
