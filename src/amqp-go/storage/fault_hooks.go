package storage

import (
	"fmt"
	"os"
	"sync/atomic"
)

// WS-C (C10) TEST FAULT HOOKS.
//
// Every hook in this file follows the pattern walGroupCommitFsync set
// (wal_manager.go): an atomic.Pointer to a function type, a wrapper that
// loads-and-calls the hook or runs the default, and a SetXForTest setter that
// returns a restore function. nil = default path; the cost when unset is one
// atomic pointer load per call site.
//
// ALL of these are TEST-ONLY injection points. Production code never calls a
// setter. They exist so the durability tests (and the later crash-recovery
// harness wave) can inject write, fsync, truncate, open, unlink and
// directory-fsync faults deterministically at exactly the syscall the
// production path performs, instead of simulating damage by editing files
// behind the broker's back.
//
// Usage rules (see the WS-C spec §12):
//   - Hooks are package-global. Tests that install one must NOT be parallel,
//     and every hook fn filters by path prefix and passes everything else to
//     the default, so unrelated I/O from other tests is unaffected.
//   - Always defer the returned restore func.
//   - Counter-based one-shot faults use atomic.Int32/Bool.

// --- WAL write ------------------------------------------------------------

// walWriteFn is the signature of the WAL record-append performed by
// commitBatch's group-commit batch, writeTxAtomic's single-shot unit, and the
// WAL compaction's copy of a record into its temporary file.
type walWriteFn func(f *os.File, b []byte) (int, error)

var walWriteHook atomic.Pointer[walWriteFn]

// walWrite appends b to the WAL file f through the hookable seam. Default:
// f.Write(b).
func walWrite(f *os.File, b []byte) (int, error) {
	if p := walWriteHook.Load(); p != nil {
		return (*p)(f, b)
	}
	return f.Write(b)
}

// SetWALWriteForTest installs fn as the write used by the shared-WAL
// group-commit batch writer, the transaction slow path, and the WAL
// compaction's copy of a record, and returns a restore function. TEST-ONLY: it
// lets tests inject a short write (n < len(b) with an error) or a hard write
// error deterministically. Production never calls it.
func SetWALWriteForTest(fn func(f *os.File, b []byte) (int, error)) (restore func()) {
	prev := walWriteHook.Load()
	f := walWriteFn(fn)
	walWriteHook.Store(&f)
	return func() { walWriteHook.Store(prev) }
}

// --- WAL stager (WS-F phase 1) ----------------------------------------------

// walStageHookFn observes the stager's progress. It is invoked after every
// request the stager serializes onto its current batch, with the cumulative
// number of requests staged so far. TEST-ONLY.
type walStageHookFn func(stagedRecords int)

var walStageHook atomic.Pointer[walStageHookFn]

// walStageHookFire reports one stager add to the installed hook. A nil hook
// costs one atomic load, the same pattern as every seam in this file; the
// production call sites use this wrapper so the hook check never branches on
// anything but the atomic load.
func walStageHookFire(stagedRecords int) {
	if p := walStageHook.Load(); p != nil {
		(*p)(stagedRecords)
	}
}

// SetWALStageHookForTest installs fn as the stager observer and returns a
// restore function. TEST-ONLY: the WS-F phase 1 pipeline tests use it to
// prove the NEXT batch is staged while the current batch's fsync is still in
// flight, and — by blocking inside the hook on the first add — to make
// first-batch membership deterministic. Never called by production code; nil
// = default (no observation).
func SetWALStageHookForTest(fn func(stagedRecords int)) (restore func()) {
	prev := walStageHook.Load()
	f := walStageHookFn(fn)
	walStageHook.Store(&f)
	return func() { walStageHook.Store(prev) }
}

// --- WAL transaction fsync ------------------------------------------------

// walTxFsyncHook installs over the walFsyncFn type declared alongside the
// group-commit fsync hook (wal_manager.go).
var walTxFsyncHook atomic.Pointer[walFsyncFn]

// walTxFsync is the fdatasync performed by writeTxAtomic. Default:
// fdatasyncFile — identical to the pre-hook behaviour, which called
// fdatasyncFile directly. It is deliberately NOT routed through
// walGroupCommitFsync: tests that fault the group-commit barrier must not
// break transaction commits (and vice versa).
func walTxFsync(f *os.File) error {
	if p := walTxFsyncHook.Load(); p != nil {
		return (*p)(f)
	}
	return fdatasyncFile(f)
}

// SetWALTxFsyncForTest installs fn as the fsync used by the transaction slow
// path (WriteTxAtomic). TEST-ONLY.
func SetWALTxFsyncForTest(fn func(*os.File) error) (restore func()) {
	prev := walTxFsyncHook.Load()
	f := walFsyncFn(fn)
	walTxFsyncHook.Store(&f)
	return func() { walTxFsyncHook.Store(prev) }
}

// --- WAL truncate (write-error repair) ------------------------------------

// walTruncateFn is the signature of the truncate-back-to-pre-write-offset
// performed by repairAfterFailedWriteLocked. The hook REPLACES the truncate
// AND its fsync (see walTruncateDefault).
type walTruncateFn func(f *os.File, size int64) error

var walTruncateHook atomic.Pointer[walTruncateFn]

// walTruncateDefault is the unhooked truncate: ftruncate (which works on
// O_WRONLY|O_APPEND descriptors) followed, when sync is set, by fdatasync.
// fdatasync flushes the i_size change from ftruncate (POSIX: metadata needed
// to retrieve the data), so it is sufficient to make the truncation durable.
//
// It deliberately does NOT route the fsync through walGroupCommitFsync:
// existing tests install an always-failing group-commit fsync hook and expect
// "batch nacked, WAL healthy, next batch OK" — if the repair's fsync went
// through that hook, every such test would poison the WAL instead.
func walTruncateDefault(f *os.File, size int64, sync bool) error {
	if err := f.Truncate(size); err != nil {
		return err
	}
	if sync {
		return fdatasyncFile(f)
	}
	return nil
}

// walTruncate truncates the WAL file f to size. sync reports whether the
// truncation itself must be made durable (!SyncDisabled); with fsync disabled
// the truncation is still performed (it restores the offset invariant) but
// nothing is fsynced, which matches that config's contract.
func walTruncate(f *os.File, size int64, sync bool) error {
	if p := walTruncateHook.Load(); p != nil {
		return (*p)(f, size)
	}
	return walTruncateDefault(f, size, sync)
}

// SetWALTruncateForTest installs fn as the truncate (including its fsync)
// used by the WAL write-error repair. TEST-ONLY: returning an error from it
// drives the poison-and-roll path.
func SetWALTruncateForTest(fn func(f *os.File, size int64) error) (restore func()) {
	prev := walTruncateHook.Load()
	f := walTruncateFn(fn)
	walTruncateHook.Store(&f)
	return func() { walTruncateHook.Store(prev) }
}

// --- WAL file open ---------------------------------------------------------

// walOpenFileFn is the signature of the WAL write-handle open.
type walOpenFileFn func(name string, flag int, perm os.FileMode) (*os.File, error)

var walOpenFileHook atomic.Pointer[walOpenFileFn]

// walOpenFile opens a WAL file through the hookable seam. Default:
// os.OpenFile.
func walOpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	if p := walOpenFileHook.Load(); p != nil {
		return (*p)(name, flag, perm)
	}
	return os.OpenFile(name, flag, perm)
}

// SetWALOpenFileForTest installs fn as the open used for WAL write handles
// (file creation at boot and at roll). TEST-ONLY: returning EMFILE simulates
// descriptor exhaustion; returning a read-only handle forces the file-header
// write to fail.
func SetWALOpenFileForTest(fn func(name string, flag int, perm os.FileMode) (*os.File, error)) (restore func()) {
	prev := walOpenFileHook.Load()
	f := walOpenFileFn(fn)
	walOpenFileHook.Store(&f)
	return func() { walOpenFileHook.Store(prev) }
}

// --- segment write ---------------------------------------------------------

// segmentWriteFn is the signature of the segment record-append.
type segmentWriteFn func(f *os.File, b []byte) (int, error)

var segmentWriteHook atomic.Pointer[segmentWriteFn]

// segmentWrite appends b to the segment file f through the hookable seam.
// Default: f.Write(b).
func segmentWrite(f *os.File, b []byte) (int, error) {
	if p := segmentWriteHook.Load(); p != nil {
		return (*p)(f, b)
	}
	return f.Write(b)
}

// SetSegmentWriteForTest installs fn as the write used by the segment
// checkpoint batch path. TEST-ONLY: it lets tests inject a short write
// (ENOSPC after N bytes) deterministically.
func SetSegmentWriteForTest(fn func(f *os.File, b []byte) (int, error)) (restore func()) {
	prev := segmentWriteHook.Load()
	f := segmentWriteFn(fn)
	segmentWriteHook.Store(&f)
	return func() { segmentWriteHook.Store(prev) }
}

// --- segment fsync ---------------------------------------------------------

var segmentFsyncHook atomic.Pointer[walFsyncFn]

// segmentFsync is the fdatasync performed after a durable segment batch
// write. Default: fdatasyncFile.
func segmentFsync(f *os.File) error {
	if p := segmentFsyncHook.Load(); p != nil {
		return (*p)(f)
	}
	return fdatasyncFile(f)
}

// SetSegmentFsyncForTest installs fn as the fsync used by the segment
// checkpoint batch path. TEST-ONLY.
func SetSegmentFsyncForTest(fn func(*os.File) error) (restore func()) {
	prev := segmentFsyncHook.Load()
	f := walFsyncFn(fn)
	segmentFsyncHook.Store(&f)
	return func() { segmentFsyncHook.Store(prev) }
}

// --- segment truncate ------------------------------------------------------

// segmentTruncateFn is the signature of the truncate-back-to-pre-batch-size
// performed by repairSegmentLocked. The default (below) includes the
// Stat==size check, so a hook that returns nil without truncating is caught
// by the caller's invariant assertion rather than silently diverging.
type segmentTruncateFn func(f *os.File, size int64) error

var segmentTruncateHook atomic.Pointer[segmentTruncateFn]

// segmentTruncateDefault truncates the segment file f to size and makes the
// truncation durable, then verifies the file really is size bytes.
func segmentTruncateDefault(f *os.File, size int64) error {
	if err := walTruncateDefault(f, size, true); err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat after segment truncate: %w", err)
	}
	if st.Size() != size {
		return fmt.Errorf("segment size %d after truncate to %d", st.Size(), size)
	}
	return nil
}

// segmentTruncate truncates the segment file f to size through the hookable
// seam.
func segmentTruncate(f *os.File, size int64) error {
	if p := segmentTruncateHook.Load(); p != nil {
		return (*p)(f, size)
	}
	return segmentTruncateDefault(f, size)
}

// SetSegmentTruncateForTest installs fn as the truncate (including its fsync
// and Stat check) used by the segment write-error repair. TEST-ONLY:
// returning an error drives the retire-segment path.
func SetSegmentTruncateForTest(fn func(f *os.File, size int64) error) (restore func()) {
	prev := segmentTruncateHook.Load()
	f := segmentTruncateFn(fn)
	segmentTruncateHook.Store(&f)
	return func() { segmentTruncateHook.Store(prev) }
}

// --- segment file open -----------------------------------------------------

// segmentOpenFileFn is the signature of the segment file open.
type segmentOpenFileFn func(name string, flag int, perm os.FileMode) (*os.File, error)

var segmentOpenFileHook atomic.Pointer[segmentOpenFileFn]

// segmentOpenFile opens a segment file through the hookable seam. Default:
// os.OpenFile.
func segmentOpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	if p := segmentOpenFileHook.Load(); p != nil {
		return (*p)(name, flag, perm)
	}
	return os.OpenFile(name, flag, perm)
}

// SetSegmentOpenFileForTest installs fn as the open used to create segment
// files. TEST-ONLY: returning EMFILE simulates a failed segment roll.
func SetSegmentOpenFileForTest(fn func(name string, flag int, perm os.FileMode) (*os.File, error)) (restore func()) {
	prev := segmentOpenFileHook.Load()
	f := segmentOpenFileFn(fn)
	segmentOpenFileHook.Store(&f)
	return func() { segmentOpenFileHook.Store(prev) }
}

// --- directory fsync -------------------------------------------------------

// syncDirFn is the signature of the directory fsync.
type syncDirFn func(dir string) error

var syncDirHook atomic.Pointer[syncDirFn]

// SetSyncDirForTest installs fn as the directory fsync used at every
// durability-relevant directory mutation (WAL/segment file creation, queue
// directory creation, WAL unlink). TEST-ONLY: tests use it to observe the
// ordering of durability barriers or to inject a directory-fsync failure.
func SetSyncDirForTest(fn func(dir string) error) (restore func()) {
	prev := syncDirHook.Load()
	f := syncDirFn(fn)
	syncDirHook.Store(&f)
	return func() { syncDirHook.Store(prev) }
}

// --- WAL unlink ------------------------------------------------------------

// walUnlinkFn is the signature of the WAL file unlink performed by the
// reclaim sweep, once every record the file holds is acknowledged.
type walUnlinkFn func(path string) error

var walUnlinkHook atomic.Pointer[walUnlinkFn]

// walUnlink unlinks a fully acknowledged WAL file through the hookable seam.
// Default: os.Remove.
func walUnlink(path string) error {
	if p := walUnlinkHook.Load(); p != nil {
		return (*p)(path)
	}
	return os.Remove(path)
}

// SetCheckpointUnlinkForTest installs fn as the unlink performed on a WAL
// file whose every record is acknowledged. TEST-ONLY: the hook runs before
// the unlink, so a test can block in it (to inspect state) or return an error
// to simulate an unlink failure at exactly that point.
//
// THE NAME IS LEGACY: it says "checkpoint" because the unlink used to
// follow a checkpoint copy into the segments. Checkpoint copying was removed
// in segstore v1 (WI8) and the unlink now follows the reclaim sweep.
// RENAME IT WHEN THE CHECKPOINT-ERA TESTS ARE DELETED.
func SetCheckpointUnlinkForTest(fn func(path string) error) (restore func()) {
	prev := walUnlinkHook.Load()
	f := walUnlinkFn(fn)
	walUnlinkHook.Store(&f)
	return func() { walUnlinkHook.Store(prev) }
}

// --- WAL compaction fsync -------------------------------------------------

// walCompactionFsyncHook installs over the walFsyncFn type: the fsync applied
// to a WAL compaction's temporary file before it is renamed over the
// original.
var walCompactionFsyncHook atomic.Pointer[walFsyncFn]

// walCompactionFsync is the fsync performed on a WAL compaction's temporary
// file. Default: fdatasyncFile — identical to the group-commit fsync, so a
// compacted file is as durable as a written one.
func walCompactionFsync(f *os.File) error {
	if p := walCompactionFsyncHook.Load(); p != nil {
		return (*p)(f)
	}
	return fdatasyncFile(f)
}

// SetWALCompactionFsyncForTest installs fn as the fsync used by the WAL
// compaction before it renames its temporary file over the original.
// TEST-ONLY: returning an error aborts the compaction before the rename, so
// the original bytes and index stay untouched.
func SetWALCompactionFsyncForTest(fn func(*os.File) error) (restore func()) {
	prev := walCompactionFsyncHook.Load()
	f := walFsyncFn(fn)
	walCompactionFsyncHook.Store(&f)
	return func() { walCompactionFsyncHook.Store(prev) }
}

// --- WAL compaction before install ----------------------------------------

// walCompactionBeforeInstallHook observes the moment a WAL compaction has
// finished writing its temporary file and is about to install it.
var walCompactionBeforeInstallHook atomic.Pointer[walCompactionBeforeInstallFn]

type walCompactionBeforeInstallFn func(fileNum uint64)

// walCompactionBeforeInstall fires the before-install hook. TEST-ONLY
// observability: it lets a test land an acknowledgement at the exact point
// between "the copy is written" and "the copy is installed", with no timing.
func walCompactionBeforeInstall(fileNum uint64) {
	if p := walCompactionBeforeInstallHook.Load(); p != nil {
		(*p)(fileNum)
	}
}

// SetWALCompactionBeforeInstallForTest installs fn as the before-install
// observer of the WAL compaction. TEST-ONLY.
func SetWALCompactionBeforeInstallForTest(fn func(fileNum uint64)) (restore func()) {
	prev := walCompactionBeforeInstallHook.Load()
	f := walCompactionBeforeInstallFn(fn)
	walCompactionBeforeInstallHook.Store(&f)
	return func() { walCompactionBeforeInstallHook.Store(prev) }
}

// --- WAL rename -----------------------------------------------------------

// walRenameHook installs over the rename performed by the WAL compaction.
var walRenameHook atomic.Pointer[walRenameFn]

type walRenameFn func(oldPath, newPath string) error

// walRename renames oldPath to newPath. Default: os.Rename — atomic on every
// supported platform, so a reader never observes a half-installed file.
func walRename(oldPath, newPath string) error {
	if p := walRenameHook.Load(); p != nil {
		return (*p)(oldPath, newPath)
	}
	return os.Rename(oldPath, newPath)
}

// SetWALRenameForTest installs fn as the rename used by the WAL compaction.
// TEST-ONLY: returning an error aborts the compaction BEFORE the rename, so
// the original bytes and the original index are left exactly as they were.
func SetWALRenameForTest(fn func(oldPath, newPath string) error) (restore func()) {
	prev := walRenameHook.Load()
	f := walRenameFn(fn)
	walRenameHook.Store(&f)
	return func() { walRenameHook.Store(prev) }
}

// --- WAL ReadAt -----------------------------------------------------------

// walReadAtHook installs over the positional read used by the WAL delivery
// path.
var walReadAtHook atomic.Pointer[walReadAtFn]

type walReadAtFn func(f *os.File, b []byte, off int64) (int, error)

// walReadAt reads len(b) bytes at off. Default: os.File.ReadAt.
func walReadAt(f *os.File, b []byte, off int64) (int, error) {
	if p := walReadAtHook.Load(); p != nil {
		return (*p)(f, b, off)
	}
	return f.ReadAt(b, off)
}

// SetWALReadAtForTest installs fn as the positional read used by the WAL
// delivery path (record parsing, body-block reads and read-ahead). TEST-ONLY:
// it lets a test observe WHICH offsets the delivery path reads, and assert
// the locking discipline that read runs under.
func SetWALReadAtForTest(fn func(f *os.File, b []byte, off int64) (int, error)) (restore func()) {
	prev := walReadAtHook.Load()
	f := walReadAtFn(fn)
	walReadAtHook.Store(&f)
	return func() { walReadAtHook.Store(prev) }
}
