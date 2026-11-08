//go:build darwin

package storage

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// D1 (F3/F4 root cause, 2026-11-03). On darwin os.File.Sync reaches
// fcntl(F_FULLFSYNC) through runtime.fcntl — a bare libcCall with NO
// entersyscall — so the flushing goroutine keeps its P for the whole 5–20 ms
// flush and every GC stop-the-world waits for it. fdatasyncFile issues the SAME
// barrier through x/sys/unix, which enters the syscall.
//
// A UNIT TEST CANNOT PROVE PREEMPTIBILITY. Preemptibility is a property of the
// calling goroutine's runtime state (does it hand its P off?) and of the Go
// scheduler's stop-the-world machinery — not of any return value this package
// can observe. What these tests CAN prove is (a) the barrier still succeeds on a
// real file, (b) the error path is sane, and (c) the directory fsync is wired to
// the new call. The ACCEPTANCE EVIDENCE is the execution trace:
// `tools/stwstat` over an F3 fsync profile must show the long STWs
// (≥ 0.5 ms; 22.5 % of wall before the fix) collapse to ~0. The lead re-runs it.

// TestFdatasyncFile_Darwin checks the barrier's success and error paths.
func TestFdatasyncFile_Darwin(t *testing.T) {
	t.Run("succeeds on a real file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "wal-d1")
		// Match production's WAL open flags (wal_manager.go openWALFile):
		// O_CREATE|O_WRONLY|O_APPEND, so the test exercises the same fd.
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		require.NoError(t, err)
		t.Cleanup(func() { _ = f.Close() })
		_, err = f.Write([]byte("durable bytes"))
		require.NoError(t, err)

		require.NoError(t, fdatasyncFile(f),
			"F_FULLFSYNC on a real file must succeed; the call is the same barrier os.File.Sync issues")
	})

	t.Run("closed file returns an error", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "wal-d1-closed")
		// Match production's WAL open flags (wal_manager.go openWALFile).
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		require.NoError(t, err)
		require.NoError(t, f.Close())

		require.Error(t, fdatasyncFile(f),
			"syncing a closed file must report an error, never silently succeed")
	})
}

// TestSyncDirPlatformSync_DarwinWiring pins the platform split: on darwin the
// directory fsync must be fdatasyncFile (the entersyscall F_FULLFSYNC), not
// os.File.Sync. This is a regression guard against "simplifying" the split away;
// the behavioural evidence for the property is the STW trace, not this test.
func TestSyncDirPlatformSync_DarwinWiring(t *testing.T) {
	require.NotNil(t, syncDirPlatformSync, "darwin must install a directory Sync")
	require.Equal(t,
		reflect.ValueOf(fullFsyncDir).Pointer(),
		reflect.ValueOf(syncDirPlatformSync).Pointer(),
		"the darwin directory full flush must route through fullFsyncDir (the preemptible F_FULLFSYNC, "+
			"labelled for the full-flush counter), not os.File.Sync")
	require.NotNil(t, syncDirPlatformFenceSync, "darwin must install a directory fence")
	require.Equal(t,
		reflect.ValueOf(barrierDirSyncFile).Pointer(),
		reflect.ValueOf(syncDirPlatformFenceSync).Pointer(),
		"the darwin directory fence must route through barrierDirSyncFile (F_BARRIERFSYNC with a plain-fsync fallback)")
}
