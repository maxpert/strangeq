package storage

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// D2 (F3/F4 root cause, 2026-11-03). With storage.fsync disabled the WAL file's
// CONTENTS are not durable, so making its DIRECTORY ENTRY durable buys nothing —
// and on APFS a directory F_FULLFSYNC flushes the volume's whole dirty set
// (measured 16–128 ms per 512 MiB roll with ~2 GB/s of un-fsynced WAL writes
// pending). The roll path (openWALFile, C4) and the reclaim path
// (tryReclaimOldFiles) must therefore skip their directory fsync when
// SyncDisabled is set, and behave exactly as before when it is not.
//
// These tests use the SetSyncDirForTest seam to observe whether syncDir was
// called at all; the call itself is passed through to the real platform
// implementation so the non-disabled case is a real durability barrier.

// dirSyncCounter records every syncDir call made through the test seam.
type dirSyncCounter struct {
	mu   sync.Mutex
	dirs []string
}

func (c *dirSyncCounter) record(dir string) error {
	c.mu.Lock()
	c.dirs = append(c.dirs, dir)
	c.mu.Unlock()
	return syncDirPlatform(dir)
}

func (c *dirSyncCounter) snapshot() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.dirs...)
}

// TestWAL_Roll_DirSyncHonorsSyncDisabled asserts that the directory fsync the
// WAL roll performs at file creation (openWALFile C4) is skipped when
// SyncDisabled is true and present when it is false. The file creation itself
// and the roll must succeed in both modes.
func TestWAL_Roll_DirSyncHonorsSyncDisabled(t *testing.T) {
	sharedDir := func(dir string) string { return filepath.Join(dir, "wal", "shared") }

	cases := []struct {
		name         string
		syncDisabled bool
		wantSyncs    int
	}{
		{"fsync enabled: the new file's directory entry is made durable", false, 1},
		{"fsync disabled: no directory barrier on the roll", true, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := coldTailWALConfig()
			cfg.SyncDisabled = tc.syncDisabled
			wm, err := NewWALManagerWithConfig(dir, cfg)
			require.NoError(t, err)
			t.Cleanup(func() { _ = wm.Close() })

			// Installed AFTER boot: boot's own first-file creation already
			// synced the directory and is not the event under test.
			counter := &dirSyncCounter{}
			restore := SetSyncDirForTest(counter.record)
			t.Cleanup(restore)

			sw := wm.sharedWAL
			sw.fileMutex.Lock()
			require.NoError(t, sw.rollFile())
			sw.fileMutex.Unlock()

			got := counter.snapshot()
			require.Len(t, got, tc.wantSyncs,
				"directory fsyncs during a roll must be %d when SyncDisabled=%v, got %v",
				tc.wantSyncs, tc.syncDisabled, got)
			for _, d := range got {
				require.Equal(t, sharedDir(dir), d)
			}
		})
	}
}

// TestWAL_Reclaim_DirSyncHonorsSyncDisabled asserts the reclaim sweep's
// post-unlink directory fsync is skipped when SyncDisabled is true and present
// when it is false. The UNLINK itself is independent of the fsync gate: a fully
// acknowledged file is removed in both modes.
func TestWAL_Reclaim_DirSyncHonorsSyncDisabled(t *testing.T) {
	sharedDir := func(dir string) string { return filepath.Join(dir, "wal", "shared") }

	cases := []struct {
		name         string
		syncDisabled bool
		wantSyncs    int
	}{
		{"fsync enabled: the unlink is made durable", false, 1},
		{"fsync disabled: no directory barrier after the unlink", true, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := coldTailWALConfig()
			cfg.SyncDisabled = tc.syncDisabled
			wm, err := NewWALManagerWithConfig(dir, cfg)
			require.NoError(t, err)
			t.Cleanup(func() { _ = wm.Close() })

			writeSegstoreReclaimRecord(t, wm, "q", 1, "d2-reclaim")
			_, path := rollSegstoreReclaimWAL(t, wm)
			wm.Acknowledge("q", 1)

			// Installed after the roll so only the reclaim sweep is observed.
			counter := &dirSyncCounter{}
			restore := SetSyncDirForTest(counter.record)
			t.Cleanup(restore)

			wm.sharedWAL.tryReclaimOldFiles()

			// The fully acknowledged file is reclaimed regardless of the sync
			// gate: skipping the barrier must not skip the unlink.
			_, serr := os.Stat(path)
			require.True(t, errors.Is(serr, fs.ErrNotExist),
				"a fully acknowledged sealed file must be unlinked in both modes: %v", serr)

			got := counter.snapshot()
			require.Len(t, got, tc.wantSyncs,
				"directory fsyncs during reclaim must be %d when SyncDisabled=%v, got %v",
				tc.wantSyncs, tc.syncDisabled, got)
			for _, d := range got {
				require.Equal(t, sharedDir(dir), d)
			}
		})
	}
}
