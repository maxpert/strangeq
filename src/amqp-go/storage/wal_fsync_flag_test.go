package storage

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// F0 — the WAL group-commit fsync must honor the SyncDisabled flag, and every
// zero-value path must default to fsync ON (durable). Transactions
// (WriteTxAtomic) are intentionally out of scope: they always fdatasync.

// TestWALFsyncFlag_ZeroValueDefaultsDurable pins the safe-by-default contract at
// the storage layer: neither the zero-value WALConfig nor DefaultWALConfig()
// disables fsync.
func TestWALFsyncFlag_ZeroValueDefaultsDurable(t *testing.T) {
	assert.False(t, WALConfig{}.SyncDisabled, "zero-value WALConfig must fsync (SyncDisabled=false)")
	assert.False(t, DefaultWALConfig().SyncDisabled, "DefaultWALConfig must fsync (SyncDisabled=false)")
}

// TestWALFsyncFlag_Honored proves the flag is honored both ways: with fsync
// enabled (default) the group-commit fsync hook fires and wal_fsync_total
// advances; with SyncDisabled the hook never fires and the metric stays flat,
// yet records are still durable-enough (page-cache visible) so completions fire
// and Read succeeds.
func TestWALFsyncFlag_Honored(t *testing.T) {
	run := func(t *testing.T, syncDisabled bool) (hookCalls, metricFsyncs int64) {
		var hook atomic.Int64
		restore := SetWALGroupCommitFsyncForTest(func(f *os.File) error {
			hook.Add(1)
			return f.Sync()
		})
		defer restore()

		cfg := DefaultWALConfig()
		cfg.SyncDisabled = syncDisabled
		wm, err := NewWALManagerWithConfig(t.TempDir(), cfg)
		require.NoError(t, err)
		defer wm.Close()

		metrics := &countingWALMetrics{}
		wm.SetMetrics(metrics)

		const n = 32
		done := make(chan error, n)
		for i := 1; i <= n; i++ {
			off := uint64(i)
			msg := &protocol.Message{Body: []byte("body"), RoutingKey: "q", DeliveryMode: 2, DeliveryTag: off}
			wm.WriteAsync("q", msg, off, func(cerr error) { done <- cerr })
		}
		deadline := time.After(10 * time.Second)
		for i := 0; i < n; i++ {
			select {
			case err := <-done:
				require.NoError(t, err, "completion must succeed even when fsync is skipped")
			case <-deadline:
				t.Fatalf("only %d/%d completions fired", i, n)
			}
		}
		// The record must be readable regardless of fsync (index update happens
		// on the durable AND the sync-disabled path).
		got, rerr := wm.Read("q", 1)
		require.NoError(t, rerr, "record must be readable after completion")
		require.Equal(t, []byte("body"), got.Body)
		return hook.Load(), metrics.fsyncs.Load()
	}

	t.Run("enabled_fsyncs", func(t *testing.T) {
		hook, metric := run(t, false)
		assert.Greater(t, hook, int64(0), "fsync hook must fire when sync enabled")
		assert.Greater(t, metric, int64(0), "wal_fsync_total must advance when sync enabled")
	})
	t.Run("disabled_skips", func(t *testing.T) {
		hook, metric := run(t, true)
		assert.Equal(t, int64(0), hook, "fsync hook must NOT fire when sync disabled")
		assert.Equal(t, int64(0), metric, "wal_fsync_total must stay flat when sync disabled")
	})
}
