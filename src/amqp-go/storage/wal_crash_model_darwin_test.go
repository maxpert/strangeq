//go:build darwin

package storage

// crash_model (F_BARRIERFSYNC) site pins — BARRIER-EVERYWHERE.
//
// The switch is darwin-only: on Linux and every other platform barrierSyncFile
// IS fdatasyncFile, so there is no command to distinguish and crash_model has
// no effect. These tests run on darwin, where the two primitives are the two
// distinct fcntl commands F_BARRIERFSYNC (85) and F_FULLFSYNC (51).
//
// THE RULE (spec §1): in barrier mode EVERY sync that can fire while the broker
// is serving traffic uses the fence (85; a directory falls back to PLAIN fsync,
// never 51). F_FULLFSYNC (51) survives only at the one-shot lifecycle points —
// clean close, boot, the I/O-error repair path — and in force_flush mode at
// every site.
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

// TestCrashModel_BarrierFallsBackToFullFlush pins the FILE fallback: a drive
// that rejects F_BARRIERFSYNC must fall back to F_FULLFSYNC (strictly
// stronger), NEVER to plain fsync, which has no ordering barrier.
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

// TestCrashModel_DirFenceFallbackUsesPlainFsyncNeverFull is T-BE-3: a directory
// fd that rejects F_BARRIERFSYNC falls back to PLAIN fsync, NEVER to
// F_FULLFSYNC (a full flush here would silently reintroduce the per-roll
// whole-drive-cache drain). The hook sees the 85 attempt and no 51.
func TestCrashModel_DirFenceFallbackUsesPlainFsyncNeverFull(t *testing.T) {
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

	dir := t.TempDir()

	rec.reset()
	require.NoError(t, syncDirFence(dir),
		"an ENOTSUP directory fence must fall back to plain fsync, not fail")
	got := rec.snapshot()
	require.Equal(t, []int{cmBarrier}, got,
		"the directory fence must attempt 85 then fall back to plain fsync: never 51")
	require.NotContains(t, got, cmFull)
}

// TestCrashModel_SitePins is T-BE-1: per site, per mode, the primitive.
//
//	barrier  -> 85 (fence) at every load-coupled site; 51 only at repair,
//	            boot-tail, boot-mkdir and close.
//	force    -> 51 everywhere.
func TestCrashModel_SitePins(t *testing.T) {
	for _, mode := range []struct {
		name  string
		force bool
	}{
		{"barrier_default", false},
		{"force_flush", true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			fence := !mode.force // darwin: barrier == fence

			// roll / creation directory barrier (openWALFile C4 -> policy.dir).
			t.Run("roll_creation_dir", func(t *testing.T) {
				t.Cleanup(SetWALPreopenEnabledForTest(false))
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

				assertSitePrimitive(t, rec.snapshot(), fence, "roll/creation dir")
			})

			// reclaim directory barrier.
			t.Run("reclaim_dir", func(t *testing.T) {
				t.Cleanup(SetWALPreopenEnabledForTest(false))
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

				assertSitePrimitive(t, rec.snapshot(), fence, "reclaim dir")
			})

			// transaction directory barrier (writeTxAtomic under SyncDisabled).
			t.Run("tx_dir", func(t *testing.T) {
				rec := &syncCmdRecorder{}
				rec.install(t)
				cfg := crashModelTestConfig(mode.force)
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
					QueueName: "txdir", Offset: 1, Message: crashModelTestMessage("txdir", 1),
				}}))
				got := rec.snapshot()
				// tx dir + tx confirm, both the same primitive under either mode.
				require.NotEmpty(t, got, "the tx path must issue its barriers")
				for _, c := range got {
					require.Equal(t, wantPrimitive(fence), c,
						"every tx barrier in %s must be cmd %d, got %v", mode.name, wantPrimitive(fence), got)
				}
			})

			// compaction file fsync + dir barrier. Driven directly with the
			// production helper and the instance policy (the maintenance
			// goroutine is not part of the site pin).
			t.Run("compaction_file", func(t *testing.T) {
				rec := &syncCmdRecorder{}
				rec.install(t)
				wm, err := NewWALManagerWithConfig(t.TempDir(), crashModelTestConfig(mode.force))
				require.NoError(t, err)
				t.Cleanup(func() { _ = wm.Close() })

				f, err := os.CreateTemp(t.TempDir(), "compact")
				require.NoError(t, err)
				t.Cleanup(func() { _ = f.Close() })

				rec.reset()
				require.NoError(t, walCompactionFsync(f, wm.sharedWAL.policy(siteCompactionFile)))
				assertSitePrimitive(t, rec.snapshot(), fence, "compaction file fsync")
			})

			// repair truncate: ALWAYS the full flush in both modes.
			t.Run("repair_truncate_stays_full", func(t *testing.T) {
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
					"the repair truncate barrier is the full flush in BOTH modes")
			})

			// boot tail truncation: ALWAYS the full flush.
			t.Run("boot_tail_stays_full", func(t *testing.T) {
				rec := &syncCmdRecorder{}
				rec.install(t)
				path := filepath.Join(t.TempDir(), "tail.wal")
				require.NoError(t, os.WriteFile(path, []byte("0123456789"), 0644))

				rec.reset()
				require.NoError(t, walTruncateTailDurable(path, 4))
				require.Equal(t, []int{cmFull}, rec.snapshot(),
					"the boot tail truncate is the full flush in BOTH modes")
			})

			// clean close: ALWAYS the full flush (via fdatasyncFile, not the fence).
			t.Run("close_stays_full", func(t *testing.T) {
				rec := &syncCmdRecorder{}
				rec.install(t)
				wm, err := NewWALManagerWithConfig(t.TempDir(), crashModelTestConfig(mode.force))
				require.NoError(t, err)
				require.NoError(t, wm.Write("q", crashModelTestMessage("q", 1), 1))

				rec.reset()
				require.NoError(t, wm.Close())
				require.Contains(t, rec.snapshot(), cmFull,
					"a clean close must issue the full flush")
				require.NotContains(t, rec.snapshot(), cmBarrier,
					"a clean close must never route through the fence")
			})
		})
	}
}

// wantPrimitive maps fence-ness to the expected fcntl command.
func wantPrimitive(fence bool) int {
	if fence {
		return cmBarrier
	}
	return cmFull
}

// assertSitePrimitive asserts the recorded commands are the expected primitive
// and nothing else (a non-empty snapshot of exactly want, or empty only when
// the site legitimately issued nothing).
func assertSitePrimitive(t *testing.T, got []int, fence bool, site string) {
	t.Helper()
	want := wantPrimitive(fence)
	require.NotEmpty(t, got, "%s: expected cmd %d", site, want)
	for _, c := range got {
		require.Equal(t, want, c, "%s: expected cmd %d, got %v", site, want, got)
	}
}

// TestCrashModel_SegmentSealAndAtomicWrite pins the segment seal and the
// metadata atomic write: fence under barrier, full under force_flush.
func TestCrashModel_SegmentSealAndAtomicWrite(t *testing.T) {
	for _, force := range []bool{false, true} {
		name := "barrier_default"
		if force {
			name = "force_flush"
		}
		want := cmBarrier
		if force {
			want = cmFull
		}
		t.Run(name, func(t *testing.T) {
			rec := &syncCmdRecorder{}
			rec.install(t)

			cfg := DefaultSegmentConfig()
			if force {
				cfg.ConfirmSync = walConfirmForceFlush
			}
			sm, err := NewSegmentManagerWithConfig(t.TempDir(), cfg)
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
			require.Equal(t, []int{want}, rec.snapshot(),
				"the segment seal must issue exactly cmd %d", want)

			// The metadata atomic write: one file fsync + one dir sync, both
			// the same primitive.
			rec.reset()
			path := filepath.Join(t.TempDir(), "meta.cbor")
			pol := newSyncPolicy(walConfirmBarrier)
			if force {
				pol = newSyncPolicy(walConfirmForceFlush)
			}
			require.NoError(t, atomicWriteFileWithPolicy(path, []byte("x"), 0644, pol.withSite(siteMetadataFile)))
			got := rec.snapshot()
			require.Len(t, got, 2, "atomic write is one file fsync + one dir sync, got %v", got)
			for _, c := range got {
				require.Equal(t, want, c, "atomic write barrier must be cmd %d, got %v", want, got)
			}
		})
	}
}

// TestCrashModel_W8Step1 pins the W8 adjustment: in barrier mode the pre-open
// helper DROPS its retired-file flush step; in force_flush it keeps it. Step 2
// (creation dir barrier) runs in both, with the mode's primitive.
func TestCrashModel_W8Step1(t *testing.T) {
	cases := []struct {
		name     string
		force    bool
		wantBar  int
		wantFull int
		min      int
	}{
		// barrier: exactly one dir barrier (the creation), the fence.
		{name: "barrier_default", force: false, min: 1},
		// force_flush: step 1 + step 2, both full.
		{name: "force_flush", force: true, min: 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &syncCmdRecorder{}
			rec.install(t)
			wm, err := NewWALManagerWithConfig(t.TempDir(), crashModelTestConfig(tc.force))
			require.NoError(t, err)
			t.Cleanup(func() { _ = wm.Close() })

			sw := wm.sharedWAL
			rec.reset()
			sw.preopenNext()

			got := rec.snapshot()
			want := wantPrimitive(!tc.force)
			for _, c := range got {
				require.Equal(t, want, c,
					"every pre-open directory barrier in %s must be cmd %d, got %v", tc.name, want, got)
			}
			if tc.force {
				require.GreaterOrEqual(t, len(got), tc.min,
					"force_flush keeps BOTH pre-open directory barriers (step 1 + step 2)")
			} else {
				require.Equal(t, tc.min, len(got),
					"barrier mode DROPS the retired-file flush: only the creation barrier fires")
			}
		})
	}
}
