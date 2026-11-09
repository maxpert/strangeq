//go:build linux

package storage

import (
	"os"
	"syscall"
)

// fdatasyncFile flushes file data to disk without flushing metadata (access
// time, etc.) on Linux. This is significantly faster than fsync for WAL and
// segment files where only data integrity matters, not metadata.
// Note: f.Fd() sets the file to blocking mode on Unix, which is safe for
// regular files (Go's poll package doesn't use epoll for them).
func fdatasyncFile(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}

// fdatasyncFileSite is fdatasyncFile. Linux has no F_FULLFSYNC, so there is
// nothing to count and crash_model is inert here; the site is ignored.
func fdatasyncFileSite(f *os.File, _ string) error {
	return fdatasyncFile(f)
}

// barrierSyncFileSite issues the crash_model: barrier confirm primitive; the
// site is ignored on Linux.
func barrierSyncFileSite(f *os.File, _ string) error {
	return fdatasyncFile(f)
}

// ConfirmFenceOrdersOnly reports whether the crash_model: barrier confirm
// primitive is a genuine ORDERING-ONLY fence (an fsync plus an ordering
// barrier, WITHOUT a drive write-cache flush). Linux has no cheaper ordered
// primitive than fdatasync(2), and on a volatile-cache device fdatasync
// already flushes the drive cache — so the barrier IS a full flush here and
// this is false, not true.
const ConfirmFenceOrdersOnly = false
