package storage

// TWO-PHASE RECLAIM (2026-11-07).
//
// The reclaim sweep used to hold EVERY index shard's write lock while it
// performed the file's os.Remove. On APFS each unlink of a fully-fenced WAL file
// is a large extent-free transaction that competes with the group-commit's fence
// transactions, so the committer -- which needs ONE shard lock for indexRecord
// while holding fileMutex -- stalled inside the sweep's lock envelope. Every
// confirm and durable delivery behind it froze with it: 516 ms of syscall delay
// per 6 s window, confirm p99.9 431 ms.
//
// The fix splits the sweep: phase A (under the locks) unregisters and unindexes,
// phase B (after EVERY lock is released) unlinks. These tests pin (1) that a
// commit completes while a reclaim's unlink is parked, (2) the failure/orphan
// semantics, (3) that boot tolerates the crash-between-phases orphan, and (4)
// that concurrent sweeps never unlink one path twice.

import (
	"errors"
	"io/fs"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// TestReclaimUnlinkDoesNotBlockCommitter is the fix's core assertion: while the
// reclaim's DELETION syscall is parked, a NEW durable publish must complete end
// to end (write, fence, index, readable). Before the fix the sweep held every
// shard's write lock across the unlink, so the committer's indexRecord blocked
// while it held fileMutex and the publish never returned -- RED.
func TestReclaimUnlinkDoesNotBlockCommitter(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, segstoreReclaimWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	writeSegstoreReclaimRecord(t, wm, "sealed-q", 1, "acked-in-sealed-file")
	_, path := rollSegstoreReclaimWAL(t, wm)
	wm.Acknowledge("sealed-q", 1)

	entered := make(chan struct{})
	release := make(chan struct{})
	var enterOnce sync.Once
	restore := SetCheckpointUnlinkForTest(func(candidate string) error {
		if candidate != path {
			return os.Remove(candidate)
		}
		enterOnce.Do(func() { close(entered) })
		<-release
		return os.Remove(candidate)
	})
	t.Cleanup(restore)

	sweepDone := make(chan struct{})
	go func() {
		defer close(sweepDone)
		wm.sharedWAL.tryReclaimOldFiles()
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("reclaim never reached the deletion syscall")
	}

	// The deletion is parked. A durable publish on ANOTHER tag must complete:
	// the committer takes ONE shard lock for indexRecord while holding
	// fileMutex, and the parked sweep must hold no shard lock.
	published := make(chan error, 1)
	go func() {
		published <- wm.Write("live-q", &protocol.Message{
			RoutingKey:   "live-q",
			Body:         []byte("live-commit-during-unlink"),
			DeliveryMode: 2,
			DeliveryTag:  42,
		}, 42)
	}()

	select {
	case werr := <-published:
		require.NoError(t, werr,
			"a durable publish must complete while a reclaim deletion is in flight: "+
				"the indexRecord must not wait on the unlink")
	case <-time.After(5 * time.Second):
		t.Fatal("the committer was blocked by the in-flight reclaim deletion " +
			"(indexRecord could not take a shard lock) -- the unlink is still inside the lock envelope")
	}

	// And a read of the just-committed record must return (its index entry is
	// live on a shard the parked sweep cannot be holding).
	msg, rerr := wm.Read("live-q", 42)
	require.NoError(t, rerr, "the committed record must be readable during the parked deletion")
	require.Equal(t, []byte("live-commit-during-unlink"), msg.Body)

	close(release)
	select {
	case <-sweepDone:
	case <-time.After(5 * time.Second):
		t.Fatal("reclaim sweep did not finish after the deletion was released")
	}
	_, serr := os.Stat(path)
	require.True(t, errors.Is(serr, fs.ErrNotExist),
		"the reclaimed file must be removed once the deletion returns: %v", serr)
}

// TestReclaimUnlinkFailureOrphansFileAndRetries pins the new failure semantics:
// phase A unregisters and unindexes BEFORE the deletion, so a failed deletion
// leaves the fully-acknowledged file ON DISK with no in-memory reference, its
// path recorded for retry; the next sweep retries and succeeds. (Before the fix
// the file stayed registered and indexed; see the package comment on
// reclaimUnlinkPass.)
func TestReclaimUnlinkFailureOrphansFileAndRetries(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, segstoreReclaimWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	writeSegstoreReclaimRecord(t, wm, "q", 7, "retry-me")
	fileNum, path := rollSegstoreReclaimWAL(t, wm)
	wm.Acknowledge("q", 7)
	qw := wm.sharedWAL

	restore := SetCheckpointUnlinkForTest(func(candidate string) error {
		if candidate == path {
			return syscall.EACCES
		}
		return os.Remove(candidate)
	})
	qw.tryReclaimOldFiles()
	restore()

	// Phase A completed: unregistered and unindexed.
	qw.oldFilesMutex.RLock()
	_, registered := qw.oldFiles[fileNum]
	pending := append([]string(nil), qw.pendingUnlink...)
	qw.oldFilesMutex.RUnlock()
	require.False(t, registered, "phase A must unregister the file even when the deletion fails")
	require.False(t, walIndexHasCopy(qw, recordKey{queueName: "q", deliveryTag: 7}),
		"phase A must unindex the file's records even when the deletion fails")

	// The failed deletion left the orphan on disk and queued for retry.
	_, serr := os.Stat(path)
	require.NoError(t, serr, "a failed deletion must leave the file on disk")
	require.Contains(t, pending, path, "the failed path must be recorded for the next sweep")

	// The next sweep retries it (no fault now) and it is gone.
	qw.tryReclaimOldFiles()
	_, serr = os.Stat(path)
	require.True(t, errors.Is(serr, fs.ErrNotExist),
		"the next sweep must retry the pending deletion and remove the file: %v", serr)
	qw.oldFilesMutex.RLock()
	require.Empty(t, qw.pendingUnlink, "a successful retry must clear the pending ledger")
	qw.oldFilesMutex.RUnlock()
}

// TestReclaimCrashBetweenPhasesOrphanToleratedAtBoot models a crash between the
// two phases: phase A unregisters and unindexes the file, then the process dies
// before the deletion. The state left on disk is an unreferenced <digits>.wal.
// Boot must TOLERATE it -- rebuildBootState re-enumerates and re-indexes it --
// and the all-acknowledged records become readable (and redelivered) again, the
// same at-least-once duplicate a crash before a single-phase unlink produced.
func TestReclaimCrashBetweenPhasesOrphanToleratedAtBoot(t *testing.T) {
	dir := t.TempDir()
	cfg := segstoreReclaimWALConfig()
	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)

	writeSegstoreReclaimRecord(t, wm, "q", 5, "orphan-body")
	fileNum, path := rollSegstoreReclaimWAL(t, wm)
	wm.Acknowledge("q", 5)
	qw := wm.sharedWAL

	// CRASH BETWEEN THE PHASES: phase A runs (unregister + unindex), then the
	// deletion never happens (EIO models "the process died before the syscall").
	restore := SetCheckpointUnlinkForTest(func(candidate string) error {
		if candidate == path {
			return syscall.EIO
		}
		return os.Remove(candidate)
	})
	qw.tryReclaimOldFiles()
	restore()

	qw.oldFilesMutex.RLock()
	_, registered := qw.oldFiles[fileNum]
	qw.oldFilesMutex.RUnlock()
	require.False(t, registered, "premise: the crash state must leave the file unreferenced")
	_, serr := os.Stat(path)
	require.NoError(t, serr, "premise: the orphan must still be on disk")

	// The process (and its volatile ack state) is gone.
	require.NoError(t, wm.Close())

	// BOOT TOLERATES THE ORPHAN.
	reopened, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err,
		"an unreferenced <digits>.wal left between the reclaim phases must not refuse the boot")
	t.Cleanup(func() { _ = reopened.Close() })

	msg, rerr := reopened.Read("q", 5)
	require.NoError(t, rerr, "the orphan's records must be re-indexed at boot")
	require.Equal(t, []byte("orphan-body"), msg.Body)

	reopened.sharedWAL.oldFilesMutex.RLock()
	_, reindexed := reopened.sharedWAL.oldFiles[fileNum]
	reopened.sharedWAL.oldFilesMutex.RUnlock()
	require.True(t, reindexed, "boot must re-register the orphan file in oldFiles")
}

// TestReclaimConcurrentSweepsDeleteEachPathOnce drives several reclaim sweeps at
// once and asserts that no path is unlinked more than once. The two-phase split
// serializes phase A on the all-shard lock (so only one sweep ever collects a
// given file into its to-unlink list) and guards the pending ledger with
// oldFilesMutex. Run under -race, this also proves the ledger is race-free.
func TestReclaimConcurrentSweepsDeleteEachPathOnce(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, segstoreReclaimWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	writeSegstoreReclaimRecord(t, wm, "q", 1, "first")
	_, pathA := rollSegstoreReclaimWAL(t, wm)
	writeSegstoreReclaimRecord(t, wm, "q", 2, "second")
	_, pathB := rollSegstoreReclaimWAL(t, wm)
	wm.Acknowledge("q", 1)
	wm.Acknowledge("q", 2)

	var mu sync.Mutex
	counts := map[string]int{}
	restore := SetCheckpointUnlinkForTest(func(candidate string) error {
		mu.Lock()
		counts[candidate]++
		mu.Unlock()
		return os.Remove(candidate)
	})
	t.Cleanup(restore)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wm.sharedWAL.tryReclaimOldFiles()
		}()
	}
	wg.Wait()

	mu.Lock()
	got := map[string]int{pathA: counts[pathA], pathB: counts[pathB]}
	mu.Unlock()
	for _, p := range []string{pathA, pathB} {
		require.LessOrEqual(t, got[p], 1,
			"concurrent sweeps must unlink %s at most once (got %d)", p, got[p])
		_, serr := os.Stat(p)
		require.True(t, errors.Is(serr, fs.ErrNotExist),
			"a fully-acknowledged sealed file must be gone: %s: %v", p, serr)
	}

	qw := wm.sharedWAL
	qw.oldFilesMutex.RLock()
	pending := append([]string(nil), qw.pendingUnlink...)
	qw.oldFilesMutex.RUnlock()
	require.Empty(t, pending, "no path may be left pending when every deletion succeeded")
}
