package storage

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// WS-C C4: a directory entry must be durable before anything relies on that
// file for durability. The sites: WAL/segment file creation (sync the dir when
// the file is created), queue-dir creation (sync the parent), boot dirs
// (mkdirAllDurable), and post-unlink (sync once per pass).

// dirSyncRecorder records the durability-barrier events of one scenario, in
// order, through the syncDir / segment-fsync / checkpoint-unlink / WAL-open
// hooks, passing every call through to the real implementation.
type dirSyncRecorder struct {
	mu     sync.Mutex
	events []string
}

func (r *dirSyncRecorder) record(ev string) {
	r.mu.Lock()
	r.events = append(r.events, ev)
	r.mu.Unlock()
}

func (r *dirSyncRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

// idx returns the index of the first occurrence of ev, or -1.
func (r *dirSyncRecorder) idx(ev string) int {
	for i, e := range r.snapshot() {
		if e == ev {
			return i
		}
	}
	return -1
}

// lastIdx returns the index of the last occurrence of ev, or -1.
func (r *dirSyncRecorder) lastIdx(ev string) int {
	last := -1
	for i, e := range r.snapshot() {
		if e == ev {
			last = i
		}
	}
	return last
}

// install feeds the recorder through the durability hooks.
func (r *dirSyncRecorder) install(t *testing.T) {
	t.Helper()
	syncRestore := SetSyncDirForTest(func(dir string) error {
		r.record("syncdir:" + dir)
		return syncDirPlatform(dir)
	})
	t.Cleanup(syncRestore)
	segFsyncRestore := SetSegmentFsyncForTest(func(f *os.File) error {
		r.record("segfsync:" + f.Name())
		return fdatasyncFile(f)
	})
	t.Cleanup(segFsyncRestore)
	unlinkRestore := SetCheckpointUnlinkForTest(func(path string) error {
		r.record("unlink:" + path)
		return os.Remove(path)
	})
	t.Cleanup(unlinkRestore)
	openRestore := SetWALOpenFileForTest(func(name string, flag int, perm os.FileMode) (*os.File, error) {
		r.record("walopen:" + name)
		return os.OpenFile(name, flag, perm)
	})
	t.Cleanup(openRestore)
}

// TestWAL_Checkpoint_DirSyncOrdering (TDD test 16) asserts the durability
// barrier ordering end-to-end: every directory entry a checkpoint relies on is
// durable (synced at creation) BEFORE the segment bytes are fdatasynced and
// the WAL file is unlinked, and the unlink itself is followed by a directory
// fsync.
func TestWAL_Checkpoint_DirSyncOrdering(t *testing.T) {
	t.Skip("checkpoint copying removed in segstore v1")
	dir := t.TempDir()
	rec := &dirSyncRecorder{}
	rec.install(t)

	wm, err := NewWALManagerWithConfig(dir, coldTailWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })
	sm, err := NewSegmentManager(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sm.Close() })
	wm.SetSegmentManager(sm)

	require.NoError(t, wm.Write("q", &protocol.Message{
		Exchange: "amq.direct", RoutingKey: "q", Body: []byte("dir-sync-body"), DeliveryMode: 2, DeliveryTag: 1,
	}, 1))

	segmentsDir := filepath.Join(dir, "segments")
	queueDir := filepath.Join(segmentsDir, "q") // marker-encoded name is identity for plain "q"
	sharedDir := filepath.Join(dir, "wal", "shared")

	sw := wm.sharedWAL
	sw.fileMutex.Lock()
	walPath := sw.currentFile.Name()
	require.NoError(t, sw.rollFile())
	sw.fileMutex.Unlock()

	// The roll's new WAL file: its creation is IMMEDIATELY followed by the
	// directory fsync of wal/shared (C4: the directory entry is durable before
	// the first confirm of a record in it).
	events := rec.snapshot()
	lastOpen := -1
	for i, ev := range events {
		if strings.HasPrefix(ev, "walopen:") {
			lastOpen = i
		}
	}
	require.GreaterOrEqual(t, lastOpen, 0, "the roll must open a new WAL file")
	require.Less(t, lastOpen+1, len(events), "event after the roll's WAL open")
	require.Equal(t, "syncdir:"+sharedDir, events[lastOpen+1],
		"syncDir(wal/shared) must fire immediately after the new WAL file is created during the roll")

	// The checkpoint moves the record to a segment and unlinks the WAL file.
	sw.performCheckpoint()

	segFsyncIdx := rec.idx("segfsync:" + queueSegPath(t, sm, "q"))
	require.GreaterOrEqual(t, segFsyncIdx, 0, "the segment fsync must be recorded")
	unlinkIdx := rec.idx("unlink:" + walPath)
	require.GreaterOrEqual(t, unlinkIdx, 0, "the WAL unlink must be recorded")

	// Queue-dir creation syncs the PARENT segments/ before anything is
	// checkpointed into it.
	queueDirSync := rec.idx("syncdir:" + segmentsDir)
	require.GreaterOrEqual(t, queueDirSync, 0, "queue-dir creation must syncDir the parent segments/ dir")
	require.Less(t, queueDirSync, unlinkIdx,
		"syncdir(segments/) at queue-dir creation must precede the WAL unlink")

	// The segment file's own directory entry is durable before its bytes are
	// fdatasynced, and both precede the WAL unlink.
	segCreateSync := rec.lastIdx("syncdir:" + queueDir)
	require.GreaterOrEqual(t, segCreateSync, 0, "segment creation must syncDir the queue dir")
	require.Less(t, segCreateSync, segFsyncIdx, "syncdir(queue dir) at segment creation must precede the segment fsync")
	require.Less(t, segFsyncIdx, unlinkIdx, "the segment fsync must precede the WAL unlink")

	// The unlink is followed by a directory fsync of wal/shared.
	require.Less(t, unlinkIdx, rec.lastIdx("syncdir:"+sharedDir),
		"the WAL unlink must be followed by syncDir(wal/shared)")
}

// queueSegPath returns the active segment file path for a queue.
func queueSegPath(t *testing.T, sm *SegmentManager, queue string) string {
	t.Helper()
	val, ok := sm.queueSegments.Load(queue)
	require.True(t, ok, "no QueueSegments for %q", queue)
	qs := val.(*QueueSegments)
	qs.mutex.Lock()
	defer qs.mutex.Unlock()
	require.NotNil(t, qs.currentSegment)
	return qs.currentSegment.path
}

// TestSyncDir_FailureOnCreate_FailsRollAndSegmentCreate (TDD test 17): a failed
// directory fsync at file creation fails the operation (and removes the file)
// rather than proceeding with a possibly-non-durable directory entry.
//
// SEGSTORE V1 RE-POINT. The failure used to be produced by the checkpoint: the
// checkpoint copied WAL records into a segment, and a failed dir fsync at that
// creation aborted the copy. Checkpoint copying is gone — the WAL tier owns
// durability and nothing moves records out of it — so the guard is now driven
// through the path that still CREATES segment files: opening a queue's
// segments. That path is live at boot (recovering a legacy queue's directory
// opens its first segment) and it is the same createSegmentFileLocked call the
// checkpoint used to reach, with the same contract: a failed dir fsync removes
// the half-created file and fails the operation.
//
// The WAL is still the tier that owns the durable copy, so "the WAL is kept"
// is now the statement that a failed segment creation costs the data nothing.
func TestSyncDir_FailureOnCreate_FailsRollAndSegmentCreate(t *testing.T) {
	t.Run("segment create: dir fsync fails, the create fails, WAL kept, no .seg left", func(t *testing.T) {
		dir := t.TempDir()
		wm, err := NewWALManagerWithConfig(dir, coldTailWALConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = wm.Close() })
		sm, err := NewSegmentManager(dir)
		require.NoError(t, err)
		t.Cleanup(func() { _ = sm.Close() })

		require.NoError(t, wm.Write("q", &protocol.Message{
			Exchange: "amq.direct", RoutingKey: "q", Body: []byte("dirsync-fail"), DeliveryMode: 2, DeliveryTag: 1,
		}, 1))
		sw := wm.sharedWAL
		sw.fileMutex.Lock()
		walPath := sw.currentFile.Name()
		require.NoError(t, sw.rollFile())
		sw.fileMutex.Unlock()

		// The queue's segment directory and its ownership marker are put in
		// place BEFORE the fault is armed, so the failure lands exactly where
		// the guard is: the SEGMENT FILE's creation. The marker's own atomic
		// write syncs the same directory, so arming the fault earlier would
		// fail the MARKER write instead and never reach the segment create.
		queueDir := filepath.Join(dir, "segments", "q")
		require.NoError(t, os.MkdirAll(queueDir, 0755))
		require.NoError(t, writeSegmentDirMarker(queueDir, "q"))

		restore := SetSyncDirForTest(func(d string) error {
			if d == queueDir {
				return syscall.EIO
			}
			return syncDirPlatform(d)
		})
		defer restore()

		// Opening the queue's segments creates its first segment file, and the
		// directory fsync of that creation fails.
		_, err = sm.getOrCreateQueueSegments("q")
		require.Error(t, err,
			"a failed directory fsync at segment creation must fail the operation instead of "+
				"proceeding with a possibly-non-durable directory entry")
		// GUARD THE GUARD: the failure must have landed on the SEGMENT FILE's
		// creation — the site this test is about — and not on some other step of
		// opening the queue. createSegmentFileLocked reports exactly this.
		require.Contains(t, err.Error(), "segment dir fsync",
			"the failure did not land on the segment file's creation: %v", err)

		// The segment create failed: the WAL is kept, and no .seg file was left
		// behind in the queue dir.
		_, serr := os.Stat(walPath)
		require.False(t, os.IsNotExist(serr), "the WAL file must be kept when the segment dir fsync fails")
		entries, derr := os.ReadDir(queueDir)
		require.NoError(t, derr)
		for _, e := range entries {
			require.False(t, strings.HasSuffix(e.Name(), SegmentFileExtension),
				"no .seg file may survive a failed dir fsync (found %s)", e.Name())
		}

		// Once the fault clears, the retry succeeds end-to-end: the segment
		// file is created and its directory entry made durable.
		restore()
		qs, err := sm.getOrCreateQueueSegments("q")
		require.NoError(t, err, "the retry must succeed once the fault clears")
		require.NotNil(t, qs)
		entries, derr = os.ReadDir(queueDir)
		require.NoError(t, derr)
		segFound := false
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), SegmentFileExtension) {
				segFound = true
			}
		}
		require.True(t, segFound, "the retry must create the segment file")
	})

	t.Run("WAL roll: roll fails, WAL stays writable, retry succeeds", func(t *testing.T) {
		dir := t.TempDir()
		wm, err := NewWALManagerWithConfig(dir, coldTailWALConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = wm.Close() })
		sw := wm.sharedWAL

		require.NoError(t, wm.Write("q", &protocol.Message{
			Exchange: "amq.direct", RoutingKey: "q", Body: []byte("dirsync-roll"), DeliveryMode: 2, DeliveryTag: 1,
		}, 1))
		preRoll := sw.fileOffset.Load()

		sharedDir := filepath.Join(dir, "wal", "shared")
		hookActive := true
		restore := SetSyncDirForTest(func(d string) error {
			if hookActive && d == sharedDir {
				return syscall.EIO
			}
			return syncDirPlatform(d)
		})
		defer restore()

		sw.fileMutex.Lock()
		rerr := sw.rollFile()
		sw.fileMutex.Unlock()
		require.Error(t, rerr, "the roll must fail when the WAL dir fsync fails")

		// Nothing changed: the current file stays writable.
		require.Equal(t, preRoll, sw.fileOffset.Load())
		require.NoError(t, wm.Write("q", &protocol.Message{
			Exchange: "amq.direct", RoutingKey: "q", Body: []byte("dirsync-roll-2"), DeliveryMode: 2, DeliveryTag: 2,
		}, 2))

		// The retry succeeds once the fault clears.
		hookActive = false
		sw.fileMutex.Lock()
		rerr = sw.rollFile()
		sw.fileMutex.Unlock()
		require.NoError(t, rerr)
	})
}

// TestSyncDir_UnsupportedTreatedAsSuccess (TDD test 18): the errno class that
// means "this filesystem cannot fsync directory handles" (EINVAL/EBADF/
// ENOTSUP) is mapped to success rather than blocking every roll forever.
func TestSyncDir_UnsupportedTreatedAsSuccess(t *testing.T) {
	if syncDirPlatformSync == nil {
		t.Skip("platform has no injectable directory Sync")
	}
	prev := syncDirPlatformSync
	defer func() { syncDirPlatformSync = prev }()

	for _, errno := range []error{syscall.EINVAL, syscall.EBADF, syscall.ENOTSUP, syscall.EOPNOTSUPP} {
		syncDirPlatformSync = func(f *os.File) error { return errno }
		require.NoError(t, syncDir(t.TempDir()),
			"%v from a directory Sync means 'unsupported', not 'failed'", errno)
	}

	// A real failure is NOT tolerated.
	syncDirPlatformSync = func(f *os.File) error { return syscall.EIO }
	require.Error(t, syncDir(t.TempDir()), "EIO from a directory Sync is a real failure")
}
