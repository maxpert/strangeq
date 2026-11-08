package storage

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
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
	// W8 pre-creates the next WAL file; these tests count directory fsyncs
	// exactly, so pin the inline-roll path (no background pre-open).
	t.Cleanup(SetWALPreopenEnabledForTest(false))
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
	// W8 pre-creates the next WAL file; these tests count directory fsyncs
	// exactly, so pin the inline-roll path (no background pre-open).
	t.Cleanup(SetWALPreopenEnabledForTest(false))
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

// txDirSyncWALConfig returns a config with production FileSize (so no
// incidental roll happens mid-transaction) and long background intervals (so
// the cleanup/compaction goroutines never add an unrelated directory sync).
func txDirSyncWALConfig(syncDisabled bool) WALConfig {
	cfg := DefaultWALConfig()
	cfg.SyncDisabled = syncDisabled
	cfg.CleanupInterval = time.Hour
	cfg.CompactionInterval = time.Hour
	return cfg
}

// txDirSyncRecord builds one transaction record with a body large enough to be
// unambiguously a record, but far below the production FileSize.
func txDirSyncRecord(queue string, tag uint64) *RecoveryMessage {
	return &RecoveryMessage{
		QueueName: queue,
		Offset:    tag,
		Message: &protocol.Message{
			Body:         []byte("tx-dir-sync"),
			DeliveryMode: 2,
			DeliveryTag:  tag,
		},
	}
}

// TestWAL_Tx_DirSyncWhenFsyncDisabled (finding 1, D2 amendment) pins the
// WriteTxAtomic "always durable" contract under storage.fsync:false. A roll
// creates the file with no directory barrier (D2 preserves the publish-path
// behaviour), so the FIRST transaction into that file must issue the barrier
// itself before writing; the flag then latches, a second transaction adds no
// sync, and a roll resets it so the next transaction syncs once more.
func TestWAL_Tx_DirSyncWhenFsyncDisabled(t *testing.T) {
	dir := t.TempDir()
	sharedDir := filepath.Join(dir, "wal", "shared")

	wm, err := NewWALManagerWithConfig(dir, txDirSyncWALConfig(true))
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	counter := &dirSyncCounter{}
	restore := SetSyncDirForTest(counter.record)
	t.Cleanup(restore)

	sw := wm.sharedWAL

	// 1. A roll under SyncDisabled creates the new file with NO barrier.
	sw.fileMutex.Lock()
	require.NoError(t, sw.rollFile())
	sw.fileMutex.Unlock()
	require.Len(t, counter.snapshot(), 0,
		"a roll must not sync the directory when SyncDisabled is set (D2)")

	// 2. The first transaction makes the entry durable: exactly one sync.
	require.NoError(t, wm.WriteTxAtomic([]*RecoveryMessage{txDirSyncRecord("tx-q", 1)}))
	got := counter.snapshot()
	require.Len(t, got, 1,
		"the first transaction into a file with a non-durable entry must sync the directory exactly once, got %v", got)
	require.Equal(t, sharedDir, got[0])

	// 3. The second transaction reuses the latched durable entry: no re-sync.
	require.NoError(t, wm.WriteTxAtomic([]*RecoveryMessage{txDirSyncRecord("tx-q", 2)}))
	require.Len(t, counter.snapshot(), 1,
		"a second transaction into the same file must not re-sync the directory")

	// 4. A roll resets the flag for the fresh file.
	sw.fileMutex.Lock()
	require.NoError(t, sw.rollFile())
	sw.fileMutex.Unlock()
	require.Len(t, counter.snapshot(), 1,
		"a roll must not itself sync under SyncDisabled")

	// 5. The next transaction syncs the new file's entry: two total.
	require.NoError(t, wm.WriteTxAtomic([]*RecoveryMessage{txDirSyncRecord("tx-q", 3)}))
	require.Len(t, counter.snapshot(), 2,
		"after a roll the next transaction must sync the new file's entry")
}

// TestWAL_Tx_DirSyncFailureAbortsTx (finding 1, D2 amendment) pins the failure
// semantics: a directory barrier that returns an error must abort the
// transaction BEFORE any byte is written — the record is not indexed, the file
// offset does not move, and a restart sees no Begin marker.
func TestWAL_Tx_DirSyncFailureAbortsTx(t *testing.T) {
	dir := t.TempDir()
	cfg := txDirSyncWALConfig(true)

	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)

	sw := wm.sharedWAL

	// Roll so the active file was created with the barrier skipped, i.e. its
	// directory entry is NOT durable.
	sw.fileMutex.Lock()
	require.NoError(t, sw.rollFile())
	sw.fileMutex.Unlock()

	beforeOffset := sw.fileOffset.Load()

	restore := SetSyncDirForTest(func(string) error { return syscall.EIO })
	t.Cleanup(restore) // E6: restore the global hook even on an early return
	terr := wm.WriteTxAtomic([]*RecoveryMessage{txDirSyncRecord("fail-q", 1)})
	restore()

	require.Error(t, terr, "a failed directory barrier must abort the transaction")
	require.Equal(t, beforeOffset, sw.fileOffset.Load(),
		"no bytes may be written when the directory barrier fails")
	_, indexed := walIndexEntry(sw, recordKey{queueName: "fail-q", deliveryTag: 1})
	require.False(t, indexed, "an aborted transaction must not be indexed")

	require.NoError(t, wm.Close())

	// A restart must not see the aborted transaction's Begin marker.
	wm2, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm2.Close() })
	recovered, err := wm2.RecoverFromWAL()
	require.NoError(t, err)
	for _, rm := range recovered {
		require.NotEqual(t, "fail-q", rm.QueueName,
			"an aborted transaction must leave no Begin marker on disk")
	}
}

// TestWAL_Tx_NoExtraSyncWhenFsyncEnabled (finding 1 control) proves the new
// barrier is not paid when fsync is on: the roll already made the file's
// directory entry durable, so WriteTxAtomic must add no directory sync.
func TestWAL_Tx_NoExtraSyncWhenFsyncEnabled(t *testing.T) {
	// W8 pre-creates the next WAL file; these tests count directory fsyncs
	// exactly, so pin the inline-roll path (no background pre-open).
	t.Cleanup(SetWALPreopenEnabledForTest(false))
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, txDirSyncWALConfig(false))
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	counter := &dirSyncCounter{}
	restore := SetSyncDirForTest(counter.record)
	t.Cleanup(restore)

	sw := wm.sharedWAL
	sw.fileMutex.Lock()
	require.NoError(t, sw.rollFile())
	sw.fileMutex.Unlock()

	afterRoll := counter.snapshot()
	require.Len(t, afterRoll, 1,
		"with fsync enabled a roll must make the new file's directory entry durable once")

	require.NoError(t, wm.WriteTxAtomic([]*RecoveryMessage{txDirSyncRecord("ctrl-q", 1)}))
	require.Len(t, counter.snapshot(), len(afterRoll),
		"a transaction must add no directory sync when the file's entry is already durable")
}
