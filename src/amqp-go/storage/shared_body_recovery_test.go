package storage

import (
	"encoding/binary"
	"io"
	"os"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// coldTailWALConfig is a config tuned for shared-body GC tests: a tiny FileSize
// so a single 8 KB shared unit rolls its file immediately, and long background
// intervals so cleanup/checkpoint only run when the test calls them.
func coldTailWALConfig() WALConfig {
	cfg := DefaultWALConfig()
	cfg.FileSize = 4096
	cfg.CleanupInterval = time.Hour
	cfg.CheckpointInterval = time.Hour
	return cfg
}

// ackAndWait acknowledges every physical queue/tag copy and waits for the
// queue-scoped read gates. Production acknowledgement is synchronous; the
// physical lookup keeps this shared-body fixture from using a global tag gate.
func ackAndWait(t testing.TB, wm *WALManager, offsets []uint64) {
	t.Helper()
	for _, off := range offsets {
		wm.sharedWAL.offsetIndexMutex.RLock()
		queues := make([]string, 0, 1)
		for key := range wm.sharedWAL.recordCopies {
			if key.deliveryTag == off {
				queues = append(queues, key.queueName)
			}
		}
		wm.sharedWAL.offsetIndexMutex.RUnlock()
		require.NotEmpty(t, queues, "no physical record for tag %d", off)
		for _, queueName := range queues {
			wm.Acknowledge(queueName, off)
		}
	}
	for _, off := range offsets {
		wm.sharedWAL.offsetIndexMutex.RLock()
		for key, copies := range wm.sharedWAL.recordCopies {
			if key.deliveryTag != off {
				continue
			}
			require.True(t, wm.IsAcknowledged(key.queueName, off))
			for _, r := range copies {
				require.True(t, r.acked.Load())
			}
		}
		wm.sharedWAL.offsetIndexMutex.RUnlock()
	}
}

// walRecordBoundaries returns the file-relative start offset of every record in
// filePath (skipping the header), plus a trailing entry for EOF.
func walRecordBoundaries(t testing.TB, filePath string) []int64 {
	t.Helper()
	f, err := os.Open(filePath)
	require.NoError(t, err)
	defer f.Close()
	dataStart, err := walFileDataStart(f)
	require.NoError(t, err)
	if _, err := f.Seek(dataStart, io.SeekStart); err != nil {
		require.NoError(t, err)
	}
	bounds := []int64{dataStart}
	pos := dataStart
	for {
		hdr := make([]byte, 8)
		if _, err := io.ReadFull(f, hdr); err != nil {
			break
		}
		dataLen := binary.BigEndian.Uint32(hdr[4:8])
		skip := make([]byte, dataLen)
		if _, err := io.ReadFull(f, skip); err != nil {
			break
		}
		pos += 8 + int64(dataLen)
		bounds = append(bounds, pos)
	}
	return bounds
}

// queuesN returns N distinct queue names.
func queuesN(n int) []string {
	qs := make([]string, n)
	for i := range qs {
		qs[i] = "shq_" + string(rune('a'+i))
	}
	return qs
}

// TestSharedBody_RefcountZeroReclaimsSpace (spec test #2, CEO #3): publish a
// shared body to K queues, ack ALL K, run cleanup; the whole file (BodyBlock
// included) is unlinked — implicit refcount reached zero.
func TestSharedBody_RefcountZeroReclaimsSpace(t *testing.T) {
	tmpDir := t.TempDir()
	wm, err := NewWALManagerWithConfig(tmpDir, coldTailWALConfig())
	require.NoError(t, err)
	defer wm.Close()

	const K = 5
	body := sharedTestBody(8192)
	queues := queuesN(K)
	require.NoError(t, writeSharedSync(t, wm, body, queues, 1))

	sharedFile := firstWALFile(t, tmpDir) // the rolled shared-body file
	_, err = os.Stat(sharedFile)
	require.NoError(t, err, "shared file must exist before reclaim")

	offsets := make([]uint64, K)
	for i := range offsets {
		offsets[i] = uint64(1 + i)
	}
	ackAndWait(t, wm, offsets)
	wm.sharedWAL.tryReclaimOldFiles()

	_, err = os.Stat(sharedFile)
	assert.True(t, os.IsNotExist(err), "the shared-body file must be unlinked once all refs are acked")
}

// TestSharedBody_PartialAckRetainsBody (spec test #3): with K-1 refs acked the
// file is retained and the last consumer still resolves the correct body (no
// premature free); acking the last ref then reclaims the file.
func TestSharedBody_PartialAckRetainsBody(t *testing.T) {
	tmpDir := t.TempDir()
	wm, err := NewWALManagerWithConfig(tmpDir, coldTailWALConfig())
	require.NoError(t, err)
	defer wm.Close()

	const K = 4
	body := sharedTestBody(8192)
	queues := queuesN(K)
	require.NoError(t, writeSharedSync(t, wm, body, queues, 1))
	sharedFile := firstWALFile(t, tmpDir)

	// Ack the first K-1; leave the last unacked.
	ackAndWait(t, wm, []uint64{1, 2, 3})
	wm.sharedWAL.tryReclaimOldFiles()

	_, err = os.Stat(sharedFile)
	require.NoError(t, err, "file must NOT be deleted while one ref is unacked (no premature free)")

	// The last consumer still resolves the correct body.
	m, err := wm.Read(queues[K-1], uint64(K))
	require.NoError(t, err)
	assert.Equal(t, body, m.Body)

	// Ack the last ref -> reclaim.
	ackAndWait(t, wm, []uint64{uint64(K)})
	wm.sharedWAL.tryReclaimOldFiles()
	_, err = os.Stat(sharedFile)
	assert.True(t, os.IsNotExist(err), "file reclaimed after the final ref is acked")
}

// TestSharedBody_CrashMidFanoutRecoversRefcount (spec test #4): after a crash
// (empty ackBitmap on reopen) all K references recover, ALL resolving to the
// SAME body — refcount reconstructed as K by construction — and the file is
// reclaimed only after all K are re-acked. Torn-tail subcases included.
func TestSharedBody_CrashMidFanoutRecoversRefcount(t *testing.T) {
	t.Run("all-refs-recover-to-same-body", func(t *testing.T) {
		tmpDir := t.TempDir()
		wm, err := NewWALManagerWithConfig(tmpDir, coldTailWALConfig())
		require.NoError(t, err)

		const K = 6
		body := sharedTestBody(8192)
		queues := queuesN(K)
		require.NoError(t, writeSharedSync(t, wm, body, queues, 1))
		require.NoError(t, wm.Close())

		// Reopen (fresh, empty ackBitmap == the post-crash state).
		wm2, err := NewWALManagerWithConfig(tmpDir, coldTailWALConfig())
		require.NoError(t, err)
		defer wm2.Close()

		recovered, err := wm2.RecoverFromWAL()
		require.NoError(t, err)
		require.Len(t, recovered, K, "all K references recover (refcount == K)")
		for _, rm := range recovered {
			assert.Equal(t, body, rm.Message.Body, "every recovered ref resolves to the shared body")
			assert.Nil(t, rm.Message.BodyRef, "recovery clears BodyRef (resolved to inline)")
		}
	})

	t.Run("torn-tail-drops-last-ref-no-dangle", func(t *testing.T) {
		tmpDir := t.TempDir()
		wm, err := NewWALManagerWithConfig(tmpDir, coldTailWALConfig())
		require.NoError(t, err)

		const K = 4
		body := sharedTestBody(8192)
		queues := queuesN(K)
		require.NoError(t, writeSharedSync(t, wm, body, queues, 1))
		sharedFile := firstWALFile(t, tmpDir)
		require.NoError(t, wm.Close())

		// Truncate between refs: keep block + (K-1) refs, drop the last ref.
		bounds := walRecordBoundaries(t, sharedFile)
		require.GreaterOrEqual(t, len(bounds), K+2)            // header start + block + K refs + EOF
		require.NoError(t, os.Truncate(sharedFile, bounds[K])) // drop the final ref record

		wm2, err := NewWALManagerWithConfig(tmpDir, coldTailWALConfig())
		require.NoError(t, err)
		defer wm2.Close()
		recovered, err := wm2.RecoverFromWAL()
		require.NoError(t, err)
		assert.Len(t, recovered, K-1, "surviving refs recover; the torn ref is simply absent")
		for _, rm := range recovered {
			assert.Equal(t, body, rm.Message.Body, "no dangling body: surviving refs still resolve")
		}
	})

	t.Run("torn-tail-block-only-is-orphan", func(t *testing.T) {
		tmpDir := t.TempDir()
		wm, err := NewWALManagerWithConfig(tmpDir, coldTailWALConfig())
		require.NoError(t, err)

		const K = 3
		body := sharedTestBody(8192)
		queues := queuesN(K)
		require.NoError(t, writeSharedSync(t, wm, body, queues, 1))
		sharedFile := firstWALFile(t, tmpDir)
		require.NoError(t, wm.Close())

		// Truncate after the block only: no refs survive.
		bounds := walRecordBoundaries(t, sharedFile)
		require.GreaterOrEqual(t, len(bounds), 2)
		require.NoError(t, os.Truncate(sharedFile, bounds[1])) // keep only the block record

		wm2, err := NewWALManagerWithConfig(tmpDir, coldTailWALConfig())
		require.NoError(t, err)
		defer wm2.Close()
		recovered, err := wm2.RecoverFromWAL()
		require.NoError(t, err)
		assert.Len(t, recovered, 0, "an orphan block references nothing and is reclaimed with its file")
	})
}

// TestSharedBody_GCDoesNotFreeInflightBody (spec test #5, run with -race): a
// reader resolves an unacked ref's body while the other refs are acked and
// cleanup runs. The file is never deleted while a ref is unacked, so the reader
// always sees correct bytes; no race, no short read.
func TestSharedBody_GCDoesNotFreeInflightBody(t *testing.T) {
	tmpDir := t.TempDir()
	wm, err := NewWALManagerWithConfig(tmpDir, coldTailWALConfig())
	require.NoError(t, err)
	defer wm.Close()

	const K = 6
	body := sharedTestBody(8192)
	queues := queuesN(K)
	require.NoError(t, writeSharedSync(t, wm, body, queues, 1))

	// Force a second file so the shared file is a rolled/old file (read path goes
	// through the old-file handle cache — the GC-vs-reader surface §5.3).
	extra := sharedTestBody(64)
	msg := &protocol.Message{Body: extra, DeliveryMode: 2, DeliveryTag: 100}
	require.NoError(t, wm.Write(queues[0], msg, 100))
	require.NoError(t, waitDurable(wm, queues[0], 100))

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 500; i++ {
			m, rerr := wm.Read(queues[K-1], uint64(K)) // the ref we keep unacked
			if rerr == nil {
				assert.Equal(t, body, m.Body)
			}
		}
	}()

	// Concurrently ack the OTHER refs and run cleanup repeatedly.
	acked := make([]uint64, 0, K-1)
	for i := 0; i < K-1; i++ {
		acked = append(acked, uint64(1+i))
	}
	ackAndWait(t, wm, acked)
	for i := 0; i < 200; i++ {
		wm.sharedWAL.tryReclaimOldFiles()
	}
	<-done

	// The kept ref is still resolvable — never freed while unacked.
	m, rerr := wm.Read(queues[K-1], uint64(K))
	require.NoError(t, rerr)
	assert.Equal(t, body, m.Body)
}

// TestSharedBody_CompactionKeepsOneBodyCopy (spec test #6, re-pointed).
//
// SEGSTORE V1 RE-POINT. This test used to force the age backstop and assert
// that the CHECKPOINT re-inlined the shared body: the WAL file was deleted and
// each queue held its own INLINE copy — the N-copy blowup, documented as the
// backstop's degradation. Checkpoint copying is gone (nothing moves records out
// of the WAL any more), so the re-inline path does not exist and the degradation
// it documented cannot be produced.
//
// The rewrite path that IS live is WAL COMPACTION (WI7): it rewrites a sealed
// WAL file with only its committed, unacknowledged records, and it is the one
// place where a shared body could still blow up into N copies. So the guard
// this test inherits is the same one — a shared body stays ONE copy on disk —
// asserted against the mechanism that could break it, and with a STRONGER
// outcome than the old test could assert: the block is copied once, every
// surviving reference still resolves it, and no inline copy is produced.
func TestSharedBody_CompactionKeepsOneBodyCopy(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := coldTailWALConfig()
	// A tiny threshold so that acknowledging a few references makes the sealed
	// file eligible for the rewrite. The shared body dominates the file's bytes,
	// so the references alone can never reach a half-dead ratio; the threshold
	// only decides WHEN the rewrite happens, never what it does.
	cfg.CompactionThreshold = 0.001
	wm, err := NewWALManagerWithConfig(tmpDir, cfg)
	require.NoError(t, err)
	defer wm.Close()

	const K = 4
	body := sharedTestBody(8192)
	queues := queuesN(K)
	require.NoError(t, writeSharedSync(t, wm, body, queues, 1))
	sharedFile := firstWALFile(t, tmpDir) // the rolled shared-body file

	before, err := WALRecordCountsForTest(tmpDir)
	require.NoError(t, err)
	require.Equal(t, 1, before.BodyBlocks, "the fan-out must start with exactly one body copy")
	require.Equal(t, K, before.RefBodies, "the fan-out must start with K references")
	require.Equal(t, 0, before.InlineBodies, "the fan-out must start with no inline copy")

	// Acknowledge HALF of the references; TWO stay live. Two, not one: the
	// N-copy blowup this test guards is "one body copy per surviving
	// reference", so a single survivor could not see it.
	acked := []uint64{1, 2}
	ackAndWait(t, wm, acked)

	// ACT: the rewrite.
	wm.sharedWAL.tryCompactOldFiles()

	// The rewrite must NOT have blown the body up into N copies.
	after, err := WALRecordCountsForTest(tmpDir)
	require.NoError(t, err)
	assert.Equal(t, 1, after.BodyBlocks,
		"the shared body must survive the rewrite as ONE copy, not one copy per surviving reference")
	assert.Equal(t, 0, after.InlineBodies,
		"the rewrite must not inline the body into every surviving reference")
	assert.Equal(t, 2, after.RefBodies,
		"both unacknowledged references must survive the rewrite (the rewrite did not happen otherwise)")

	// Every surviving reference still resolves the shared body: the block was
	// copied once and each locator remapped to its new position.
	for _, i := range []int{2, 3} {
		m, rerr := wm.Read(queues[i], uint64(1+i))
		require.NoError(t, rerr, "queue %s must still be readable after the rewrite", queues[i])
		assert.Equal(t, body, m.Body, "the surviving reference must still resolve the shared body")
	}

	// The rewritten file is still on disk: the rewrite reclaims space inside the
	// file, it does not drop the live record.
	_, serr := os.Stat(sharedFile)
	require.NoError(t, serr, "the rewritten WAL file must still exist")
}

// TestSharedBody_ColdTailStaysOneCopyAcrossMaintenance (spec test #12, CEO #1,
// re-pointed): a shared fan-out whose references are all unacked holds the
// body as ONE copy in the WAL — never N copies — and the file is reclaimed
// whole once every reference is acknowledged.
//
// SEGSTORE V1 RE-POINT. The checkpoint is gone, so the maintenance this test
// exercises is the WAL tier's own: the compaction sweep that rewrites a sealed
// file and the reclaim sweep that unlinks a fully acknowledged one. The
// property under test is unchanged and still live: an unacknowledged
// reference PINS the shared body (and the file that holds it), and the body is
// never multiplied.
func TestSharedBody_ColdTailStaysOneCopyAcrossMaintenance(t *testing.T) {
	t.Run("unacked-ref-pins-one-copy-across-maintenance", func(t *testing.T) {
		tmpDir := t.TempDir()
		cfg := coldTailWALConfig()
		// A tiny threshold so that the acknowledged references make the sealed
		// file eligible for the rewrite (the shared body dominates the file's
		// bytes, so the references alone can never reach a half-dead ratio).
		cfg.CompactionThreshold = 0.001
		wm, err := NewWALManagerWithConfig(tmpDir, cfg)
		require.NoError(t, err)
		defer wm.Close()

		sm, err := NewSegmentManager(tmpDir)
		require.NoError(t, err)
		defer sm.Close()
		wm.SetSegmentManager(sm)

		const K = 4
		body := sharedTestBody(8192)
		queues := queuesN(K)
		require.NoError(t, writeSharedSync(t, wm, body, queues, 1))
		sharedFile := firstWALFile(t, tmpDir)

		// Acknowledge HALF the references; two stay live and pin the file.
		ackAndWait(t, wm, []uint64{1, 2})

		// MAINTENANCE: the rewrite sweep and the reclaim sweep both run.
		wm.sharedWAL.tryCompactOldFiles()
		wm.sharedWAL.tryReclaimOldFiles()

		// (a) the file is NOT reclaimed — a live reference still needs it,
		// (b) the body is still exactly ONE copy, (c) no copy per reference.
		_, err = os.Stat(sharedFile)
		require.NoError(t, err, "a shared-body file with a live reference must not be reclaimed")
		counts, err := WALRecordCountsForTest(tmpDir)
		require.NoError(t, err)
		assert.Equal(t, 1, counts.BodyBlocks, "still exactly one body copy on disk")
		assert.Equal(t, 0, counts.InlineBodies, "no N-copy blowup happened")

		// The live references still resolve the shared body.
		for _, i := range []int{2, 3} {
			m, rerr := wm.Read(queues[i], uint64(1+i))
			require.NoError(t, rerr, "queue %s must still resolve the shared body", queues[i])
			assert.Equal(t, body, m.Body)
		}

		// (d) the WAL tier never copies records into the legacy segment tier.
		for i, q := range queues {
			_, rerr := sm.Read(q, uint64(1+i))
			assert.Error(t, rerr, "no segment record must be written for %s", q)
		}

		// (e) ack the LIVE references -> whole-file reclaim. The two already
		// acknowledged ones are gone from the index: the rewrite dropped them,
		// which is why only the live ones are acknowledged here.
		ackAndWait(t, wm, []uint64{3, 4})
		wm.sharedWAL.tryReclaimOldFiles()
		_, err = os.Stat(sharedFile)
		assert.True(t, os.IsNotExist(err), "reclaimed whole once all refs ack")
	})

	t.Run("backstop-degrades-to-n-inline-copies", func(t *testing.T) {
		// The age backstop re-inlined the shared body into N inline copies
		// through the checkpoint path, and this subtest documented that
		// degradation. That path is gone: nothing copies WAL records into
		// segments any more, so the degradation cannot be produced. The
		// guarantee that SURVIVED — a shared body is never multiplied — is
		// asserted against the live rewrite path in
		// TestSharedBody_CompactionKeepsOneBodyCopy.
		t.Skip("checkpoint copying removed in segstore v1")
	})
}
