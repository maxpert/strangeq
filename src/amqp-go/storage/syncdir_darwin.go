//go:build darwin

package storage

import "os"

// syncDirPlatformSync is the FULL directory fsync on macOS (fullFsyncDir: the
// preemptible F_FULLFSYNC, labelled for the full-flush counter) for the same
// reason the WAL file fsync is: os.File.Sync on darwin reaches F_FULLFSYNC
// through runtime.fcntl, which never enters the syscall and therefore holds the
// P for the whole flush (see the long comment on fdatasyncFile in
// fdatasync_darwin.go).
//
// It is the FULL primitive (force_flush and boot paths). The load-coupled fence
// under crash_model: barrier is syncDirPlatformFenceSync below; in barrier mode
// no site that can fire while serving traffic reaches this variable.
//
// This is an explicit per-platform definition rather than an init() override of
// the shared variable: only one platform file defines syncDirPlatformSync per
// GOOS, so nothing can clobber another platform's choice, and SetSyncDirForTest
// still swaps this same variable.
var syncDirPlatformSync = fullFsyncDir

// fullFsyncDir is the darwin directory full flush: F_FULLFSYNC through the
// preemptible x/sys path, labelled for the full-flush counter. It is the FULL
// (force_flush / boot) directory primitive; the load-coupled fence is
// syncDirPlatformFenceSync below.
func fullFsyncDir(f *os.File) error { return fdatasyncFileSite(f, siteDirFull) }

// syncDirPlatformFenceSync is the crash_model: barrier directory primitive on
// macOS: fcntl(F_BARRIERFSYNC) on the directory fd, with a PLAIN-fsync fallback
// (never F_FULLFSYNC — see barrierDirFsyncFD). Every directory site that can
// fire under load uses this in barrier mode; only boot paths keep the full
// flush above.
var syncDirPlatformFenceSync = barrierDirSyncFile
