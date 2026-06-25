//go:build !linux

package storage

import "os"

// fdatasyncFile falls back to file.Sync() on non-Linux platforms (macOS,
// Windows, etc.). On Linux, fdatasync(2) is used for better performance.
func fdatasyncFile(f *os.File) error {
	return f.Sync()
}
