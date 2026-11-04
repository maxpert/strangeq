//go:build !linux && !darwin

package storage

import "os"

// fdatasyncFile falls back to file.Sync() on the platforms that are neither
// Linux nor macOS (Windows, the BSDs, etc.). On Linux, fdatasync(2) is used for
// better performance; on macOS the F_FULLFSYNC barrier goes through the
// preemptible path in fdatasync_darwin.go.
func fdatasyncFile(f *os.File) error {
	return f.Sync()
}
