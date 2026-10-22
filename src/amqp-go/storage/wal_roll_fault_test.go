package storage

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// WS-C C3: rollFile used to close the current write handle BEFORE opening the
// next file, and discarded the open error. A failed open left fileNum advanced
// and currentFile set to a closed handle, so every following Write failed with
// "file already closed" until restart.

// walRollTestBody keeps record bodies big enough that a 100-byte partial write
// can never complete a record (used by the C1 fault tests that share this
// file's helpers).
const walRollTestBody = "roll-fault-test-body-padding-padding-padding-padding-padding"

// rollFaultMessage builds a durable message for offset off.
func rollFaultMessage(off uint64, body string) *protocol.Message {
	return &protocol.Message{
		Exchange:     "amq.direct",
		RoutingKey:   "q",
		Body:         []byte(body),
		DeliveryMode: 2,
		DeliveryTag:  off,
	}
}

// TestWAL_RollFile_OpenFails_WritesRecoverAfterRetry (TDD test 14).
//
// The open hook returns EMFILE for the first 3 roll attempts, then passes
// through. Every Write must still return nil (a failed roll never nacks a
// durable batch), the roll must succeed once the fault clears (backoff
// 1ms/5ms), the old file must be retired into oldFiles, all records must
// stay readable in-session, and session 2 must recover every offset.
func TestWAL_RollFile_OpenFails_WritesRecoverAfterRetry(t *testing.T) {
	// Shrink the roll backoff so retries land inside the test's lifetime.
	// Mutated before the first write: the values are read under fileMutex by
	// the (idle-at-this-point) batch writer, so there is no race.
	walRollBackoffMin = time.Millisecond
	walRollBackoffMax = 5 * time.Millisecond
	t.Cleanup(func() {
		walRollBackoffMin = 10 * time.Millisecond
		walRollBackoffMax = 1 * time.Second
	})

	dir := t.TempDir()
	cfg := coldTailWALConfig()
	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() }) // abandoned, not closed, before the reopen below
	sw := wm.sharedWAL

	var opens atomic.Int32
	restore := SetWALOpenFileForTest(func(name string, flag int, perm os.FileMode) (*os.File, error) {
		if opens.Add(1) <= 3 && strings.HasPrefix(name, dir) {
			return nil, syscall.ENFILE
		}
		return os.OpenFile(name, flag, perm)
	})
	t.Cleanup(restore)

	startNum := sw.fileNum.Load()

	// Write until the file crosses FileSize, then 20 more (sleeping briefly so
	// the roll backoff windows expire and the retry succeeds).
	passedThreshold := false
	extra := 0
	var written []uint64
	for off := uint64(1); off <= 500; off++ {
		require.NoError(t, wm.Write("q", rollFaultMessage(off, walRollTestBody), off),
			"a failed roll must never fail a durable batch (write %d)", off)
		written = append(written, off)
		if !passedThreshold && sw.fileOffset.Load() >= cfg.FileSize {
			passedThreshold = true
		}
		if passedThreshold {
			extra++
			time.Sleep(time.Millisecond) // let the backoff window expire
			if extra >= 20 {
				break
			}
		}
	}
	require.True(t, passedThreshold, "test bug: never crossed FileSize")
	require.GreaterOrEqual(t, extra, 20, "test bug: fewer than 20 writes past the threshold")

	require.Greater(t, sw.fileNum.Load(), startNum,
		"the roll must succeed once the open fault clears")

	sw.oldFilesMutex.RLock()
	_, retired := sw.oldFiles[startNum]
	sw.oldFilesMutex.RUnlock()
	require.True(t, retired, "the rolled-from file must be retired into oldFiles")

	// Every record stays readable in-session (old file via the read-handle
	// cache, new file via currentReadFile).
	for _, off := range written {
		msg, rerr := wm.Read("q", off)
		require.NoError(t, rerr, "read of offset %d after roll retry", off)
		require.Equal(t, walRollTestBody, string(msg.Body), "offset %d body", off)
	}

	// Session 2 recovers every offset from both files.
	wm2, rerr := NewWALManager(dir)
	require.NoError(t, rerr, "session 2 must construct without refusal")
	defer wm2.Close()
	recovered, rerr := wm2.RecoverFromWAL()
	require.NoError(t, rerr)
	got := map[uint64]bool{}
	for _, rm := range recovered {
		got[rm.Offset] = true
	}
	for _, off := range written {
		require.True(t, got[off], "session 2 must recover offset %d", off)
	}
}

// TestWAL_RollFile_HeaderWriteFails_NoPoisonedStub (TDD test 15).
//
// The open hook returns a read-only handle for the first open, so the new
// file's header write fails with EBADF. The roll must fail WITHOUT leaving a
// 1–5 byte stub file behind (the "refuse foreign header forever" trap), the
// WAL must stay writable on the current file, and the retry must succeed.
func TestWAL_RollFile_HeaderWriteFails_NoPoisonedStub(t *testing.T) {
	dir := t.TempDir()
	cfg := coldTailWALConfig()
	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })
	sw := wm.sharedWAL

	require.NoError(t, wm.Write("q", rollFaultMessage(1, walRollTestBody), 1))
	preRoll := sw.fileOffset.Load()

	var opens atomic.Int32
	restore := SetWALOpenFileForTest(func(name string, flag int, perm os.FileMode) (*os.File, error) {
		if opens.Add(1) == 1 && strings.HasPrefix(name, dir) {
			// Create the real file, then hand back a READ-ONLY handle so the
			// header write fails with EBADF.
			if f, cerr := os.OpenFile(name, flag, perm); cerr == nil {
				_ = f.Close()
			}
			return os.Open(name)
		}
		return os.OpenFile(name, flag, perm)
	})
	defer restore()

	sw.fileMutex.Lock()
	rerr := sw.rollFile()
	sw.fileMutex.Unlock()
	require.Error(t, rerr, "the roll must fail when the header write fails")

	// The WAL is untouched and still writable: offset invariant holds and a
	// write succeeds into the CURRENT (oversized) file.
	require.Equal(t, preRoll, sw.fileOffset.Load())
	require.NoError(t, wm.Write("q", rollFaultMessage(2, walRollTestBody), 2))
	_, rerr = wm.Read("q", 2)
	require.NoError(t, rerr)

	// Retry (hook one-shot consumed) succeeds.
	sw.fileMutex.Lock()
	rerr = sw.rollFile()
	sw.fileMutex.Unlock()
	require.NoError(t, rerr)

	// No stub shorter than a header may exist anywhere in the directory.
	sharedDir := filepath.Join(dir, "wal", "shared")
	entries, derr := os.ReadDir(sharedDir)
	require.NoError(t, derr)
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), WALFileExtension) {
			continue
		}
		info, ierr := e.Info()
		require.NoError(t, ierr)
		require.GreaterOrEqual(t, info.Size(), int64(WALHeaderSize),
			"stub WAL file %s (%d bytes) must not survive a failed header write", e.Name(), info.Size())
	}

	// Writes continue into the new file and session 2 constructs cleanly.
	require.NoError(t, wm.Write("q", rollFaultMessage(3, walRollTestBody), 3))

	wm2, cerr := NewWALManager(dir)
	require.NoError(t, cerr, "session 2 must construct without refusal")
	defer wm2.Close()
	recovered, cerr := wm2.RecoverFromWAL()
	require.NoError(t, cerr)
	for _, off := range []uint64{1, 2, 3} {
		found := false
		for _, rm := range recovered {
			if rm.Offset == off {
				found = true
			}
		}
		require.True(t, found, "session 2 must recover offset %d", off)
	}
}

// errSimulatedCrash is the error the checkpoint-unlink hook returns to
// simulate dying between the segment fsync and the WAL unlink (used by the
// C8 test in this suite).
var errSimulatedCrash = errors.New("simulated crash between segment fsync and WAL unlink")
