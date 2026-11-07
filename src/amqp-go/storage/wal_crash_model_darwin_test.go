//go:build darwin

package storage

// crash_model (F_BARRIERFSYNC) site pins.
//
// The switch is darwin-only: on Linux and every other platform barrierSyncFile
// IS fdatasyncFile, so there is no command to distinguish and crash_model has
// no effect. These tests run on darwin, where the two primitives are the two
// distinct fcntl commands F_BARRIERFSYNC (85) and F_FULLFSYNC (51).
//
// THEY OBSERVE THE PRIMITIVE, NOT THE HOOK THAT REPLACES IT. The existing
// walGroupCommitFsync / walTxFsync hooks substitute the whole fsync, so a test
// that installs one cannot tell which primitive the production path would have
// issued. SetWALSyncCmdForTest instead intercepts the raw fcntl command both
// primitives issue, so it can attribute a command to a site (by driving one
// site at a time) and can fault either command.

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

const (
	cmBarrier = unix.F_BARRIERFSYNC // 85
	cmFull    = unix.F_FULLFSYNC    // 51
)

// syncCmdRecorder records the fcntl command each durability site issues. It
// returns nil for every command: the test observes WHICH command a site issues;
// it does not need the real (slow) flush.
type syncCmdRecorder struct {
	mu   sync.Mutex
	cmds []int
}

func (r *syncCmdRecorder) record(_ uintptr, cmd int) error {
	r.mu.Lock()
	r.cmds = append(r.cmds, cmd)
	r.mu.Unlock()
	return nil
}

func (r *syncCmdRecorder) snapshot() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.cmds...)
}

func (r *syncCmdRecorder) reset() {
	r.mu.Lock()
	r.cmds = nil
	r.mu.Unlock()
}

func (r *syncCmdRecorder) install(t *testing.T) {
	t.Helper()
	restore := SetWALSyncCmdForTest(r.record)
	t.Cleanup(restore)
}

// crashModelTestConfig is DefaultWALConfig with the maintenance loops pushed a
// test-length away, so no background sweep adds an unrelated fsync, plus the
// requested confirm mode.
func crashModelTestConfig(force bool) WALConfig {
	cfg := DefaultWALConfig()
	cfg.CleanupInterval = time.Hour
	cfg.CompactionInterval = time.Hour
	if force {
		cfg.ConfirmSync = walConfirmForceFlush
	}
	return cfg
}

func crashModelTestMessage(queue string, tag uint64) *protocol.Message {
	return &protocol.Message{Body: []byte("crash-model"), RoutingKey: queue, DeliveryMode: 2, DeliveryTag: tag}
}

// TestCrashModel_GroupCommitConfirmPrimitive pins the FIRST confirm site: the
// group-commit batch barrier (commitBatch -> walGroupCommitFsync). The default
// (barrier) issues F_BARRIERFSYNC; force_flush issues F_FULLFSYNC.
func TestCrashModel_GroupCommitConfirmPrimitive(t *testing.T) {
	cases := []struct {
		name  string
		force bool
		want  int
	}{
		{"barrier_default", false, cmBarrier},
		{"force_flush", true, cmFull},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &syncCmdRecorder{}
			rec.install(t)

			wm, err := NewWALManagerWithConfig(t.TempDir(), crashModelTestConfig(tc.force))
			require.NoError(t, err)
			t.Cleanup(func() { _ = wm.Close() })

			// Boot's file creation synced the directory; observe only the confirm.
			rec.reset()
			require.NoError(t, wm.Write("q", crashModelTestMessage("q", 1), 1))

			require.Equal(t, []int{tc.want}, rec.snapshot(),
				"the group-commit confirm barrier must issue exactly cmd %d", tc.want)
		})
	}
}

// TestCrashModel_TxConfirmPrimitive pins the SECOND confirm site: the
// transaction slow-path barrier (writeTxAtomic -> walTxFsync). It stays
// always-durable regardless of SyncDisabled, and it follows crash_model just
// like the group commit.
func TestCrashModel_TxConfirmPrimitive(t *testing.T) {
	cases := []struct {
		name  string
		force bool
		want  int
	}{
		{"barrier_default", false, cmBarrier},
		{"force_flush", true, cmFull},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &syncCmdRecorder{}
			rec.install(t)

			wm, err := NewWALManagerWithConfig(t.TempDir(), crashModelTestConfig(tc.force))
			require.NoError(t, err)
			t.Cleanup(func() { _ = wm.Close() })

			rec.reset()
			require.NoError(t, wm.WriteTxAtomic([]*RecoveryMessage{{
				QueueName: "txq",
				Offset:    1,
				Message:   crashModelTestMessage("txq", 1),
			}}))

			require.Equal(t, []int{tc.want}, rec.snapshot(),
				"the transaction confirm barrier must issue exactly cmd %d", tc.want)
		})
	}
}

// TestCrashModel_BarrierFallsBackToFullFlush pins the fallback: a drive that
// rejects F_BARRIERFSYNC must fall back to F_FULLFSYNC (strictly stronger),
// NEVER to plain fsync, which has no ordering barrier.
func TestCrashModel_BarrierFallsBackToFullFlush(t *testing.T) {
	var rec syncCmdRecorder
	restore := SetWALSyncCmdForTest(func(_ uintptr, cmd int) error {
		rec.mu.Lock()
		rec.cmds = append(rec.cmds, cmd)
		rec.mu.Unlock()
		if cmd == cmBarrier {
			return unix.ENOTSUP
		}
		return nil
	})
	t.Cleanup(restore)

	wm, err := NewWALManagerWithConfig(t.TempDir(), crashModelTestConfig(false))
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	rec.reset()
	require.NoError(t, wm.Write("q", crashModelTestMessage("q", 1), 1))

	require.Equal(t, []int{cmBarrier, cmFull}, rec.snapshot(),
		"an ENOTSUP F_BARRIERFSYNC must fall back to F_FULLFSYNC, never to plain fsync")
}

// TestCrashModel_RetainedFullFlushSitesUnchanged pins every RETAINED
// F_FULLFSYNC site that routes through fdatasyncFile: in BOTH modes it must
// still issue F_FULLFSYNC (51) and never F_BARRIERFSYNC (85). A regression that
// routes roll / reclaim / repair / compaction / tail-truncate through the fence
// fails here.
func TestCrashModel_RetainedFullFlushSitesUnchanged(t *testing.T) {
	for _, mode := range []struct {
		name  string
		force bool
	}{
		{"barrier_default", false},
		{"force_flush", true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			// (1) The roll / creation directory barrier (openWALFile C4 -> syncDir).
			t.Run("roll_dir_barrier", func(t *testing.T) {
				rec := &syncCmdRecorder{}
				rec.install(t)
				wm, err := NewWALManagerWithConfig(t.TempDir(), crashModelTestConfig(mode.force))
				require.NoError(t, err)
				t.Cleanup(func() { _ = wm.Close() })

				rec.reset()
				sw := wm.sharedWAL
				sw.fileMutex.Lock()
				require.NoError(t, sw.rollFile())
				sw.fileMutex.Unlock()

				got := rec.snapshot()
				require.Contains(t, got, cmFull, "the roll directory barrier must stay F_FULLFSYNC")
				require.NotContains(t, got, cmBarrier, "the roll directory barrier must not use the fence")
			})

			// (2) The reclaim sweep's post-unlink directory barrier.
			t.Run("reclaim_dir_barrier", func(t *testing.T) {
				rec := &syncCmdRecorder{}
				rec.install(t)
				wm, err := NewWALManagerWithConfig(t.TempDir(), crashModelTestConfig(mode.force))
				require.NoError(t, err)
				t.Cleanup(func() { _ = wm.Close() })

				writeSegstoreReclaimRecord(t, wm, "q", 1, "reclaim-crash-model")
				rollSegstoreReclaimWAL(t, wm)
				wm.Acknowledge("q", 1)

				rec.reset()
				wm.sharedWAL.tryReclaimOldFiles()

				got := rec.snapshot()
				require.Contains(t, got, cmFull, "the reclaim directory barrier must stay F_FULLFSYNC")
				require.NotContains(t, got, cmBarrier, "the reclaim directory barrier must not use the fence")
			})

			// (3) The write-error repair truncate (walTruncateDefault).
			t.Run("repair_truncate", func(t *testing.T) {
				rec := &syncCmdRecorder{}
				rec.install(t)
				f, err := os.CreateTemp(t.TempDir(), "repair")
				require.NoError(t, err)
				t.Cleanup(func() { _ = f.Close() })
				_, err = f.Write([]byte("repair payload"))
				require.NoError(t, err)

				rec.reset()
				require.NoError(t, walTruncate(f, 0, true))
				require.Equal(t, []int{cmFull}, rec.snapshot(),
					"the repair truncate barrier must stay F_FULLFSYNC")
			})

			// (4) The WAL compaction's fsync before the rename.
			t.Run("compaction_fsync", func(t *testing.T) {
				rec := &syncCmdRecorder{}
				rec.install(t)
				f, err := os.CreateTemp(t.TempDir(), "compact")
				require.NoError(t, err)
				t.Cleanup(func() { _ = f.Close() })

				rec.reset()
				require.NoError(t, walCompactionFsync(f))
				require.Equal(t, []int{cmFull}, rec.snapshot(),
					"the compaction fsync must stay F_FULLFSYNC")
			})

			// (5) The ported R-TAIL-HOLE boot truncate.
			t.Run("tail_truncate", func(t *testing.T) {
				rec := &syncCmdRecorder{}
				rec.install(t)
				path := filepath.Join(t.TempDir(), "tail.wal")
				require.NoError(t, os.WriteFile(path, []byte("0123456789"), 0644))

				rec.reset()
				require.NoError(t, walTruncateTailDurable(path, 4))
				require.Equal(t, []int{cmFull}, rec.snapshot(),
					"the tail truncate durability barrier must stay F_FULLFSYNC")
			})
		})
	}
}

// TestCrashModel_TxDirBarrierStaysFullFlush pins the transaction DIRECTORY
// barrier distinctly from the transaction confirm barrier: within one tx the
// directory entry must be made durable with F_FULLFSYNC even when the confirm
// primitive is the fence. It rolls under SyncDisabled (so the file entry is not
// durable yet) and then issues one transaction.
func TestCrashModel_TxDirBarrierStaysFullFlush(t *testing.T) {
	rec := &syncCmdRecorder{}
	rec.install(t)

	cfg := crashModelTestConfig(false)
	cfg.SyncDisabled = true
	wm, err := NewWALManagerWithConfig(t.TempDir(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	sw := wm.sharedWAL
	sw.fileMutex.Lock()
	require.NoError(t, sw.rollFile())
	sw.fileMutex.Unlock()

	rec.reset()
	require.NoError(t, wm.WriteTxAtomic([]*RecoveryMessage{{
		QueueName: "txdir",
		Offset:    1,
		Message:   crashModelTestMessage("txdir", 1),
	}}))

	got := rec.snapshot()
	require.Contains(t, got, cmFull,
		"the transaction directory barrier must stay F_FULLFSYNC")
	require.Contains(t, got, cmBarrier,
		"the transaction confirm barrier must be the fence")
}

// TestCrashModel_CloseStaysFullFlush pins the clean close: QueueWAL.close calls
// os.File.Sync (F_FULLFSYNC on darwin), which is NOT the fence seam. If close
// were ever routed through barrierSyncFile the recorder would see 85 here.
func TestCrashModel_CloseStaysFullFlush(t *testing.T) {
	rec := &syncCmdRecorder{}
	rec.install(t)

	wm, err := NewWALManagerWithConfig(t.TempDir(), crashModelTestConfig(false))
	require.NoError(t, err)
	require.NoError(t, wm.Write("q", crashModelTestMessage("q", 1), 1))

	rec.reset()
	require.NoError(t, wm.Close())

	require.NotContains(t, rec.snapshot(), cmBarrier,
		"close must keep os.File.Sync (F_FULLFSYNC), never route through the fence")
}

// TestCrashModel_SegmentSealStaysFullFlush pins the segment seal: sealSegment
// calls file.Sync directly (os.File.Sync, F_FULLFSYNC), so it must never reach
// the fence seam even under the default crash_model.
func TestCrashModel_SegmentSealStaysFullFlush(t *testing.T) {
	rec := &syncCmdRecorder{}
	rec.install(t)

	sm, err := NewSegmentManagerWithConfig(t.TempDir(), DefaultSegmentConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = sm.Close() })

	require.NoError(t, writeLegacyForTest(sm, "segpin", crashModelTestMessage("segpin", 1), 1))
	val, ok := sm.queueSegments.Load("segpin")
	require.True(t, ok)
	qs := val.(*QueueSegments)

	rec.reset()
	qs.mutex.Lock()
	err = qs.sealSegment()
	qs.mutex.Unlock()
	require.NoError(t, err)

	require.Empty(t, rec.snapshot(),
		"the segment seal must keep os.File.Sync (F_FULLFSYNC), never route through the fence seam")
}
