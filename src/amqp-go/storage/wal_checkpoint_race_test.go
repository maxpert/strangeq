package storage

import (
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// WS-C review finding 1: performCheckpoint read walFileInfo.checkpointed in
// its loop body WITHOUT oldFilesMutex, while tryDeleteOldFiles (the cleanup
// goroutine) writes it under oldFilesMutex.Lock — and performCheckpoint's own
// write is under the same Lock. checkpointLoop and cleanupLoop are
// independent goroutines, so the unlocked read is a Go-memory-model data
// race: benign in effect (the flag only ever flips false→true) but real, and
// -race flags it.
//
// The fix snapshots the mutable flag under the same oldFilesMutex.RLock that
// snapshots the map, so the loop body never touches a field another
// goroutine may be mutating.

// TestCheckpoint_CheckpointedFlag_NoRaceBetweenCheckpointAndRetention races
// performCheckpoint (checkpointLoop's body) against tryDeleteOldFiles
// (cleanupLoop's body) the way production does, in the interleaving that
// actually exposes the race:
//
//   - every unlink FAILS, so a checkpointed file stays registered forever.
//     In that steady state performCheckpoint's pass is a pure READ pass —
//     snapshot RLock, then per file only the unlocked `if !checkpointed`
//     read and a failed unlink — it never takes oldFilesMutex again. That is
//     exactly the production shape for any file whose unlink keeps failing.
//   - a producer keeps rolling FRESH files; each one is expired the instant
//     it rolls, so tryDeleteOldFiles' force-checkpoint succeeds and WRITES
//     its flag under oldFilesMutex.Lock while the checkpoint pass is sailing
//     through its unprotected reads.
//
// Under -race this is RED before the snapshot fix (DATA RACE on the
// checkpointed field) and GREEN after.
func TestCheckpoint_CheckpointedFlag_NoRaceBetweenCheckpointAndRetention(t *testing.T) {
	dir := t.TempDir()
	cfg := coldTailWALConfig()
	cfg.RetentionPeriod = time.Nanosecond // every file is expired the instant it rolls
	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })
	sm, err := NewSegmentManager(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sm.Close() })
	wm.SetSegmentManager(sm)
	sw := wm.sharedWAL

	var rolled atomic.Int64
	roll := func(off uint64) {
		require.NoError(t, wm.Write("q", segFaultMessage(off, segFaultBody), off))
		sw.fileMutex.Lock()
		require.NoError(t, sw.rollFile())
		sw.fileMutex.Unlock()
		rolled.Add(1)
	}

	// Every unlink fails, so checkpointed files stay registered and both
	// passes keep running against them.
	restore := SetCheckpointUnlinkForTest(func(path string) error {
		if strings.HasPrefix(path, dir) {
			return syscall.EIO
		}
		return os.Remove(path)
	})
	defer restore()

	// Warm-up: roll a batch and let both passes run until every flag is true.
	// From here on performCheckpoint's passes are the lock-free read shape.
	for i := 0; i < 6; i++ {
		roll(uint64(i + 1))
	}
	deadline := time.Now().Add(250 * time.Millisecond)
	var warm sync.WaitGroup
	warm.Add(2)
	go func() {
		defer warm.Done()
		for time.Now().Before(deadline) {
			sw.performCheckpoint()
		}
	}()
	go func() {
		defer warm.Done()
		for time.Now().Before(deadline) {
			sw.tryDeleteOldFiles()
		}
	}()
	warm.Wait()

	// Race window: drip fresh files (each mints a checkpointed=false that the
	// retention pass writes under oldFilesMutex) while the checkpoint pass
	// loops over its lock-free reads.
	var wg sync.WaitGroup
	stop := time.Now().Add(400 * time.Millisecond)
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			if time.Now().After(stop) {
				return
			}
			roll(uint64(100 + i))
			time.Sleep(25 * time.Millisecond)
		}
	}()
	go func() {
		defer wg.Done()
		for time.Now().Before(stop) {
			sw.performCheckpoint()
		}
	}()
	go func() {
		defer wg.Done()
		for time.Now().Before(stop) {
			sw.tryDeleteOldFiles()
		}
	}()
	wg.Wait()

	// The unlinks all failed, so EVERY rolled file must still be registered —
	// the loops above really ran against live entries.
	sw.oldFilesMutex.RLock()
	registered := len(sw.oldFiles)
	sw.oldFilesMutex.RUnlock()
	require.Equal(t, int(rolled.Load()), registered,
		"the unlink fault keeps every rolled file registered; the raced code paths ran against live entries")
}
