package storage

// F1 — the lock cycle between oldFileCacheMutex and bitmapMutex.
//
// THE CYCLE. Three edges:
//
//	QueueWAL.readMessageBatch acquires its file handle from getOldFileHandle.
//	    While that function returned an unlock func() alongside the handle, its
//	    CACHE-HIT path returned qw.oldFileCacheMutex.RUnlock as that unlock — so
//	    oldFileCacheMutex.RLock was held for the whole rest of the function, and
//	    the acknowledgement filter at the end of it takes qw.bitmapMutex.RLock()
//	    inside that scope.
//	        edge: oldFileCacheMutex -> bitmapMutex
//
//	QueueWAL.tryReclaimOldFiles holds bitmapMutex.RLock via defer and calls
//	    closeOldFileHandle, which takes oldFileCacheMutex.Lock() unconditionally —
//	    whether or not that fileNum is actually cached.
//	        edge: bitmapMutex -> oldFileCacheMutex
//
//	WALManager.Acknowledge takes bitmapMutex.Lock() on the CALLER's goroutine, so
//	    a pending writer can coexist with tryReclaimOldFiles' read lock. This is
//	    the leg that closes the cycle.
//
// THE INTERLEAVING:
//
//	G_read   holds oldFileCacheMutex.RLock, has not yet reached the bitmap filter
//	G_clean  holds oldFilesMutex + bitmapMutex.RLock, BLOCKS on oldFileCacheMutex.Lock
//	G_ack    BLOCKS on bitmapMutex.Lock                      (writer now pending)
//	G_read   BLOCKS on bitmapMutex.RLock  -- sync.RWMutex blocks new readers
//	                                         once a writer is waiting
//
//	G_read -> G_ack -> G_clean -> G_read.  Permanent.
//
// HOW THIS FIXTURE DRIVES IT. Nothing is manufactured and no production code is
// touched; every goroutine calls a shipped public method:
//
//	readers  wm.ReadBatch on offsets in a rolled file that is NEVER acked, in a
//	         loop, so after the first (cache-miss) read every subsequent one
//	         takes getOldFileHandle's cache-HIT path. That is the window.
//	ackers   wm.Acknowledge in a loop over a DISJOINT set of offsets, supplying
//	         both the bitmapMutex.Lock writer traffic and, as whole files become
//	         fully acknowledged, the allAcked reclaims that make
//	         tryReclaimOldFiles reach closeOldFileHandle.
//	cleaner  the shipped cleanupLoop, at CleanupInterval=1ms.
//
// The read set and the ack set are disjoint BY FILE and this is load-bearing:
// an acknowledged offset has its offsetIndex entry deleted by cleanupLoop, and
// readMessageBatch errors out without one — so acking a read target would
// silently stop exercising the cache-hit path.
//
// THE DETECTOR IS ZERO PROGRESS, NOT A DEADLINE — and that distinction is the
// whole reason this fixture is allowed to exist under a no-flaky-tests rule.
// A loaded machine still makes progress; a deadlock makes EXACTLY ZERO. That is
// a step function, not a threshold: it cannot be tuned, it cannot drift with the
// hardware, and it cannot be relaxed a second time. The workers keep atomic
// counters; the fixture samples them twice, a grace apart, and declares a wedge
// only when the two samples are IDENTICAL and the workers have not drained.
// For scale, and these are MEASURED on this fixture rather than estimated —
// they are the numbers to judge an observed count against, so a wrong one here
// would mislead exactly the reader who needs them: a healthy 10 s storm runs
// 563774 ReadBatch and 8863334 Acknowledge calls (276088 / 1095616 under
// -race). A wedged tree stops at single- or double-digit ReadBatch and tens to
// low hundreds of Acknowledge calls — 7/36, 12/59 and 29/381 across separate
// runs — because the cycle forms within the first few milliseconds. Five orders
// of magnitude separate the two, so "the counters did not move at all" is not a
// quantity any amount of load can produce.
//
// ON FAILURE THIS FIXTURE FAILS IN-PROCESS WITH ITS OWN GOROUTINE DUMP. It does
// not hang, and it must not be run with a harness -timeout as its detector: the
// dump is captured by runtime.Stack(buf, true) here, logged inline and written to
// a file, which survives any -timeout setting and names the fixture that produced
// it. Read the dump for the three expected frames:
//
//	readMessageBatch  blocked in  sync.(*RWMutex).RLock   (bitmapMutex)
//	Acknowledge       blocked in  sync.(*RWMutex).Lock    (bitmapMutex)
//	tryReclaimOldFiles blocked in  sync.(*RWMutex).Lock    (oldFileCacheMutex,
//	                                                       via closeOldFileHandle)

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/maxpert/amqp-go/protocol"
)

const (
	deadlockQueue = "deadlock.q"
	// Small files so a few hundred records produce many rolled files: every
	// reclaim of a fully-acked one is another chance for tryReclaimOldFiles to
	// reach closeOldFileHandle while a reader holds oldFileCacheMutex.RLock.
	deadlockWALFileSize = 4 * 1024
	deadlockRecords     = 2000
	deadlockBodySize    = 256
	deadlockReaders     = 8
	deadlockAckers      = 4
	// deadlockStormDuration is how long the three-way traffic runs. It bounds
	// the SUCCESS path; on the failure path the in-storm zero-progress sampler
	// ends the run sooner.
	//
	// 10 s rather than the 25 s this fixture originally used, and the cut is
	// measured rather than guessed: on the tree carrying the cycle the wedge
	// forms within the first few milliseconds of traffic, after single- or
	// double-digit ReadBatch calls. A healthy 10 s storm runs 563774 ReadBatch
	// and 8863334 Acknowledge calls (see the header), so the storm exceeds the
	// traffic needed to form the cycle by five orders of magnitude.
	//
	// 10 s IS SUFFICIENT AND THAT IS MEASURED, NOT ASSUMED — read this before
	// "restoring" 25 s. Phase 1 needs ~9 s of this window: prev is sampled
	// before the workers wedge, so the first sample is non-zero and the verdict
	// comes from the zeros at t=6 and t=9. That leaves one grace period of
	// margin, which is thin in the direction that matters — a window too tight
	// yields a false GREEN, not a false red. So phase 2 was verified to redden
	// ALONE: with phase 1 made structurally incapable of firing
	// (deadlockZeroSamplesToWedge = 1<<30, so its guard is always true and its
	// fire condition always false), the unmodified tree still FAILED at 10 s
	// under -race — storm elapsed 10.001090333s, phase 2's assertion, full F1
	// signature. Detection does not depend on the phase-1 margin. Raising this
	// back to 25 s is not the careful choice; it is not what makes this fixture
	// work.
	deadlockStormDuration = 10 * time.Second
	// deadlockProgressGrace separates the two counter samples. It is not a
	// threshold on how SLOW the system may be — the comparison is for equality,
	// so any forward motion at all clears it. It only sets how long a wedge must
	// persist before it is named.
	deadlockProgressGrace = 3 * time.Second
	// deadlockZeroSamplesToWedge is how many consecutive all-zero deltas end the
	// storm early. A deadlock's zero is permanent, so requiring it twice costs
	// one grace on the failure path and removes any residual doubt about a
	// scheduling artifact. The post-storm check below has no such knob and is
	// the backstop: a wedge cannot be tuned into a pass.
	deadlockZeroSamplesToWedge = 2
	// deadlockMinSuccessfulReads is the driver floor. reads counts ATTEMPTS and
	// increments even when ReadBatch errors, so it cannot witness that the
	// cache-hit path was ever taken; readOK counts SUCCESSES and can. Without
	// this floor a build where every ReadBatch failed would spin the loop, drain
	// cleanly and pass while never entering the code under test.
	deadlockMinSuccessfulReads = 1000
	deadlockMinAcks            = 1000
)

// captureAllStacks returns a runtime.Stack dump of every goroutine, growing the
// buffer until the dump fits. A truncated dump is worse than none: it silently
// drops the very frames this fixture exists to name.
func captureAllStacks() string {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return string(buf[:n])
		}
		buf = make([]byte, 2*len(buf))
	}
}

// countBlockedIn reports how many goroutine blocks in a runtime.Stack dump
// contain fn and are parked inside sync.RWMutex. Counting blocks rather than
// substring hits is deliberate: one goroutine mentioning a symbol twice must not
// read as two wedged goroutines.
func countBlockedIn(dump, fn string) int {
	n := 0
	for _, g := range strings.Split(dump, "\n\n") {
		if strings.Contains(g, fn) && strings.Contains(g, "sync.(*RWMutex).") {
			n++
		}
	}
	return n
}

func TestWALAckReadDeadlock_ColdReadVersusAckVersusReclaim(t *testing.T) {
	dir := t.TempDir()

	cfg := DefaultWALConfig()
	cfg.FileSize = deadlockWALFileSize
	// The cleanup ticker is the cycle's third goroutine. At the 5s default it
	// fires ~5 times in the storm; at 1ms it fires continuously.
	cfg.CleanupInterval = time.Millisecond
	// Keep performCheckpoint out of it. No segment manager is ever set on this
	// WALManager, so performCheckpoint and forceCheckpointFile both return
	// immediately anyway — this makes that explicit rather than incidental.
	cfg.CheckpointInterval = time.Hour
	cfg.RetentionPeriod = 0 // reclaim via the allAcked path, not by expiry

	// wedged gates the deferred Close below and must be declared before it.
	var wedged atomic.Bool

	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	defer func() {
		if wedged.Load() {
			// WALManager.Close holds wm.mu across qw.close() -> qw.wg.Wait(),
			// and cleanupLoop is one of the wedged goroutines — so on the
			// failure path this close can NEVER return, and because it would
			// never release wm.mu every later WALManager call in this test
			// binary would block too. Skipping it is what stops one wedged
			// fixture from taking the rest of the package with it. This is
			// cleanup, not assertion: it runs only on the branch we hope not to
			// take, which is exactly why it is written explicitly.
			t.Log("SKIPPING wm.Close(): the storm did not drain, so Close would block " +
				"forever inside wg.Wait() while holding wm.mu")
			return
		}
		_ = wm.Close()
	}()

	body := make([]byte, deadlockBodySize)
	for i := range body {
		body[i] = byte('a' + i%26)
	}

	base := uint64(1) << 44
	for i := 0; i < deadlockRecords; i++ {
		msg := &protocol.Message{
			RoutingKey:   deadlockQueue,
			Body:         body,
			DeliveryMode: 2,
			DeliveryTag:  base + uint64(i),
			MessageID:    fmt.Sprintf("dl-%d", i),
		}
		require.NoError(t, wm.Write(deadlockQueue, msg, base+uint64(i)))
	}

	sw := wm.sharedWAL
	require.NotNil(t, sw, "PREMISE BROKEN: no shared WAL was constructed")

	// ---- PREMISE: the WAL rolled into many files, and we can split them ------
	// readOffsets come from the HIGHEST-numbered rolled file and are never
	// acked, so their offsetIndex entries survive and every ReadBatch on them
	// keeps taking the cache-hit path. ackOffsets are every other rolled file's
	// offsets, so whole files reach allAcked and get reclaimed during the storm.
	sw.oldFilesMutex.RLock()
	rolledFiles := len(sw.oldFiles)
	var highest uint64
	for fileNum := range sw.oldFiles {
		if fileNum > highest {
			highest = fileNum
		}
	}
	var readOffsets, ackOffsets []uint64
	for fileNum, info := range sw.oldFiles {
		if info.offsets == nil {
			continue
		}
		it := info.offsets.Iterator()
		for it.HasNext() {
			off := it.Next()
			if fileNum == highest {
				readOffsets = append(readOffsets, off)
			} else {
				ackOffsets = append(ackOffsets, off)
			}
		}
	}
	sw.oldFilesMutex.RUnlock()

	require.GreaterOrEqual(t, rolledFiles, 10,
		"PREMISE BROKEN: only %d rolled WAL file(s) at FileSize=%d after %d records. The cycle "+
			"needs tryReclaimOldFiles to actually RECLAIM files (that is the only path to "+
			"closeOldFileHandle), and reclaims require whole files to become fully acknowledged",
		rolledFiles, deadlockWALFileSize, deadlockRecords)
	require.NotEmpty(t, readOffsets,
		"PREMISE BROKEN: no offsets in the highest rolled file, so there is no never-acked cold "+
			"read target and readMessageBatch's cache-hit path is never exercised")
	require.NotEmpty(t, ackOffsets,
		"PREMISE BROKEN: no offsets outside the highest rolled file, so no file can become "+
			"fully acknowledged and tryReclaimOldFiles never reaches closeOldFileHandle")

	// ---- PREMISE: a cold read of that file genuinely SUCCEEDS and populates
	//      the handle cache, so subsequent reads take the cache-HIT path.
	_, batchErr := wm.ReadBatch(deadlockQueue, readOffsets[0])
	require.NoError(t, batchErr,
		"PREMISE BROKEN: ReadBatch failed for offset %d before the storm began. ReadBatch is the "+
			"only caller that reaches bitmapMutex through the old-file handle cache; if it cannot "+
			"succeed here, the cycle's first edge is never taken", readOffsets[0])

	sw.oldFileCacheMutex.RLock()
	cached := len(sw.oldFileCache)
	sw.oldFileCacheMutex.RUnlock()
	require.NotZero(t, cached,
		"PREMISE BROKEN: the old-file handle cache is empty after a successful cold ReadBatch, so "+
			"getOldFileHandle never takes its cache-HIT return — the path that formed edge 4")

	t.Logf("PREMISE: %d rolled files; %d never-acked cold-read offsets (file %d); %d ack offsets; "+
		"handle cache populated (%d entry/entries)",
		rolledFiles, len(readOffsets), highest, len(ackOffsets), cached)

	// ---- THE STORM ----------------------------------------------------------
	var reads, readOK, acks atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup

	for r := 0; r < deadlockReaders; r++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			i := seed
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := wm.ReadBatch(deadlockQueue, readOffsets[i%len(readOffsets)]); err == nil {
					readOK.Add(1)
				}
				reads.Add(1)
				i++
			}
		}(r)
	}

	for a := 0; a < deadlockAckers; a++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			// Each acker walks the whole ack set. Re-acking an already-acked
			// offset is a no-op for correctness and still supplies the
			// bitmapMutex.Lock traffic that makes a writer pending.
			i := seed
			for {
				select {
				case <-stop:
					return
				default:
				}
				wm.Acknowledge(deadlockQueue, ackOffsets[i%len(ackOffsets)])
				acks.Add(1)
				i++
			}
		}(a)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	// ---- DETECTOR, PHASE 1: zero progress DURING the storm ------------------
	// The workers cannot have drained here (stop is still open), so the second
	// conjunct of the wedge verdict holds by construction and equality of two
	// samples is the entire test.
	stormStart := time.Now()
	prev := reads.Load() + acks.Load()
	zeroRuns := 0
	for time.Since(stormStart) < deadlockStormDuration && zeroRuns < deadlockZeroSamplesToWedge {
		remaining := deadlockStormDuration - time.Since(stormStart)
		if remaining > deadlockProgressGrace {
			remaining = deadlockProgressGrace
		}
		time.Sleep(remaining)
		cur := reads.Load() + acks.Load()
		if cur == prev {
			zeroRuns++
		} else {
			zeroRuns = 0
		}
		prev = cur
	}
	stormElapsed := time.Since(stormStart)

	if zeroRuns >= deadlockZeroSamplesToWedge {
		failWedged(t, &wedged, stormElapsed, reads.Load(), readOK.Load(), acks.Load(),
			fmt.Sprintf("the worker counters did not advance across %d consecutive %s samples "+
				"while the storm was still running", zeroRuns, deadlockProgressGrace))
		return
	}

	close(stop)

	// ---- DETECTOR, PHASE 2: drain, sampling for zero progress ---------------
	// A partial wedge — some readers blocked, others still running — makes
	// progress during phase 1 and is caught only here: once stop is closed the
	// live workers return within one loop iteration, the counters freeze, and
	// two identical samples with the drain outstanding is a wedge.
	//
	// This loop terminates unconditionally: every worker rechecks stop each
	// iteration, so a live one exits and a wedged one never moves the counters
	// again — there is no state in which it can sample forever.
	prevOps := reads.Load() + acks.Load()
	for drained := false; !drained; {
		select {
		case <-done:
			drained = true
		case <-time.After(deadlockProgressGrace):
			curOps := reads.Load() + acks.Load()
			if curOps == prevOps {
				failWedged(t, &wedged, stormElapsed, reads.Load(), readOK.Load(), acks.Load(),
					fmt.Sprintf("the workers did not drain after close(stop) and the counters were "+
						"IDENTICAL across two samples %s apart (%d ops both times)",
						deadlockProgressGrace, curOps))
				return
			}
			t.Logf("workers had not drained but the counters ADVANCED (%d -> %d ops); "+
				"not a wedge, still waiting for the drain", prevOps, curOps)
			prevOps = curOps
		}
	}

	sw.oldFilesMutex.RLock()
	remaining := len(sw.oldFiles)
	sw.oldFilesMutex.RUnlock()
	t.Logf("NO DEADLOCK OBSERVED: %d ReadBatch attempts (%d succeeded), %d Acknowledge calls, "+
		"%d/%d rolled files reclaimed during the storm",
		reads.Load(), readOK.Load(), acks.Load(), rolledFiles-remaining, rolledFiles)

	// Driver floors. These are what stop a green from being vacuous: a build
	// where ReadBatch always errors, or where the ack loop never ran, drains
	// perfectly and proves nothing.
	require.Greater(t, readOK.Load(), int64(deadlockMinSuccessfulReads),
		"DETECTOR DEGRADED: only %d of %d ReadBatch calls SUCCEEDED during the storm. The "+
			"cycle's first edge is formed inside a successful cold read; a storm of failing "+
			"reads never enters it, so this run says nothing about the deadlock",
		readOK.Load(), reads.Load())
	require.Greater(t, acks.Load(), int64(deadlockMinAcks),
		"DETECTOR DEGRADED: only %d Acknowledge calls during the storm, so the pending-writer "+
			"leg that closes the cycle was barely driven", acks.Load())
	require.Less(t, remaining, rolledFiles,
		"PREMISE BROKEN: no rolled WAL file was reclaimed during the storm, so "+
			"tryReclaimOldFiles never reached closeOldFileHandle and the cycle's second edge "+
			"was never taken. A green here says nothing about the deadlock")
}

// failWedged captures every goroutine's stack, records it both inline and in a
// file that outlives the test binary, and fails. It is the whole reason this
// fixture no longer relies on a harness -timeout panic for its evidence.
func failWedged(t *testing.T, wedged *atomic.Bool, elapsed time.Duration, reads, readOK, acks int64, why string) {
	t.Helper()
	// Set before failing: the deferred wm.Close() reads this, and Close cannot
	// return while a WAL goroutine is wedged.
	wedged.Store(true)

	dump := captureAllStacks()
	dumpPath := "<not written>"
	if f, err := os.CreateTemp("", "f1-deadlock-dump-*.txt"); err == nil {
		if _, werr := f.WriteString(dump); werr == nil {
			dumpPath = f.Name()
		}
		_ = f.Close()
	}

	readBatch := countBlockedIn(dump, "readMessageBatch")
	acknowledge := countBlockedIn(dump, "Acknowledge")
	reclaim := countBlockedIn(dump, "tryReclaimOldFiles")

	signature := "F1 SIGNATURE MATCHED"
	if readBatch == 0 || acknowledge == 0 || reclaim == 0 {
		signature = "WEDGED, BUT NOT THE F1 SIGNATURE — read the dump before attributing this"
	}

	t.Logf("GOROUTINE DUMP (also written to %s):\n%s", dumpPath, dump)
	t.Fatalf("DEADLOCK: %s.\n"+
		"  storm elapsed: %s; ReadBatch attempts %d (%d succeeded); Acknowledge %d\n"+
		"  %s: goroutines blocked in sync.RWMutex — readMessageBatch=%d, Acknowledge=%d, "+
		"tryReclaimOldFiles=%d\n"+
		"  Expected F1 frames: readMessageBatch on bitmapMutex.RLock, Acknowledge on "+
		"bitmapMutex.Lock, tryReclaimOldFiles on oldFileCacheMutex.Lock via closeOldFileHandle.\n"+
		"  Full dump: %s",
		why, elapsed, reads, readOK, acks, signature, readBatch, acknowledge, reclaim, dumpPath)
}
