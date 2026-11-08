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

// fdatasyncFileSite is fdatasyncFile; there is no F_FULLFSYNC on these
// platforms, so the site is ignored and crash_model is inert.
func fdatasyncFileSite(f *os.File, _ string) error {
	return f.Sync()
}

// barrierSyncFile mirrors fdatasyncFile on the platforms that are neither Linux
// nor macOS: os.File.Sync. There is no cheaper ordered primitive here, so
// crash_model has no effect on these platforms.
func barrierSyncFile(f *os.File) error {
	return f.Sync()
}

// barrierSyncFileSite is barrierSyncFile; the site is ignored here.
func barrierSyncFileSite(f *os.File, _ string) error {
	return f.Sync()
}

// ConfirmFenceOrdersOnly reports whether the crash_model: barrier confirm
// primitive is a genuine ORDERING-ONLY fence (an fsync plus an ordering
// barrier, WITHOUT a drive write-cache flush). This platform has no cheaper
// ordered primitive than os.File.Sync — a full flush — so the barrier is a
// full flush and this is false.
const ConfirmFenceOrdersOnly = false
