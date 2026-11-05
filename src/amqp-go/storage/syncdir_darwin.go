//go:build darwin

package storage

// syncDirPlatformSync is the directory fsync on macOS. It is fdatasyncFile —
// the preemptible F_FULLFSYNC — for the same reason the WAL file fsync is:
// os.File.Sync on darwin reaches F_FULLFSYNC through runtime.fcntl, which never
// enters the syscall and therefore holds the P for the whole flush (see the long
// comment on fdatasyncFile in fdatasync_darwin.go). A WAL roll or reclaim syncs
// the directory on every call, so leaving os.File.Sync here would park the
// committer's P on every roll exactly as before.
//
// This is an explicit per-platform definition rather than an init() override of
// the shared variable: only one platform file defines syncDirPlatformSync per
// GOOS, so nothing can clobber another platform's choice, and SetSyncDirForTest
// still swaps this same variable.
var syncDirPlatformSync = fdatasyncFile
