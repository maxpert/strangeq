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
