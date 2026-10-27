package storage

import (
	"errors"
	"io/fs"
	"os"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/stretchr/testify/require"
)

// The RECLAIM and the COMPACTION are two maintenance passes with opposite
// cost profiles, and they used to share one tick.
//
//   - The RECLAIM unlinks a WAL file whose records are all acknowledged. It
//     is cheap, but its LATENCY IS A MEMORY BOUND: a file that is not
//     reclaimed keeps its per-file inventory alive, and at 64 KiB bodies the
//     WAL rolls ~28 times a second.
//
//   - The COMPACTION REWRITES A WHOLE FILE. At 64 KiB bodies a rolled file is
//     mostly dead within a second, so a compaction pass that runs as often as
//     the reclaim rewrites every such file -- tens of gigabytes of extra I/O
//     per second -- and that I/O competes with the group-commit flush the
//     publisher's confirm waits for.
//
// These tests pin that they are SEPARATE cadences, and that each one runs on
// its own.

func TestWALMaintenance_ReclaimAndCompactionHaveSeparateCadences(t *testing.T) {
	def := DefaultWALConfig()
	require.Greater(t, def.CompactionInterval, def.CleanupInterval,
		"the compaction must run far less often than the reclaim: it rewrites a whole file")
	require.Equal(t, 30*time.Minute, def.CompactionInterval)

	ec := interfaces.EngineConfig{
		WALCleanupCheckIntervalMS: 5000,
		CompactionIntervalMS:      1800000,
	}
	cfg := WALConfigFromEngine(ec)
	require.Equal(t, 5*time.Second, cfg.CleanupInterval)
	require.Equal(t, 30*time.Minute, cfg.CompactionInterval)

	// An UNSET compaction interval must not fall back to the reclaim's
	// cadence: the zero value means "use the default", not "compact as often
	// as the reclaim".
	unset := WALConfigFromEngine(interfaces.EngineConfig{})
	require.Equal(t, DefaultWALCompactionInterval, unset.CompactionInterval)
}

// TestWALMaintenance_ReclaimRunsOnTheCleanupTick is the behavioural half: a
// rolled WAL file whose records are all acknowledged is unlinked by the
// BACKGROUND maintenance, on the cleanup cadence, with the compaction
// effectively disabled.
func TestWALMaintenance_ReclaimRunsOnTheCleanupTick(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultWALConfig()
	cfg.CleanupInterval = 20 * time.Millisecond
	cfg.CompactionInterval = time.Hour // the compaction must NOT be what unlinks it

	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	writeSegstoreReclaimRecord(t, wm, "q", 7, "reclaim-me")
	_, path := rollSegstoreReclaimWAL(t, wm)
	wm.Acknowledge("q", 7)

	require.Eventually(t, func() bool {
		_, err := os.Stat(path)
		return errors.Is(err, fs.ErrNotExist)
	}, 5*time.Second, 20*time.Millisecond,
		"the background reclaim must unlink the fully acknowledged WAL file on the cleanup tick")
}
