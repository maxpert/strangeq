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

// ackAndWait acks each offset and blocks until the WAL's ackBitmap reflects them.
func ackAndWait(t testing.TB, wm *WALManager, offsets []uint64) {
	t.Helper()
	for _, off := range offsets {
		wm.Acknowledge("", off)
	}
	require.Eventually(t, func() bool {
		wm.sharedWAL.bitmapMutex.RLock()
		defer wm.sharedWAL.bitmapMutex.RUnlock()
		for _, off := range offsets {
			if !wm.sharedWAL.ackBitmap.Contains(off) {
				return false
			}
		}
		return true
	}, 3*time.Second, 2*time.Millisecond, "ack bitmap did not reflect acks")
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
	wm.sharedWAL.tryDeleteOldFiles()

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
	wm.sharedWAL.tryDeleteOldFiles()

	_, err = os.Stat(sharedFile)
	require.NoError(t, err, "file must NOT be deleted while one ref is unacked (no premature free)")

	// The last consumer still resolves the correct body.
	m, err := wm.Read(queues[K-1], uint64(K))
	require.NoError(t, err)
	assert.Equal(t, body, m.Body)

	// Ack the last ref -> reclaim.
	ackAndWait(t, wm, []uint64{uint64(K)})
	wm.sharedWAL.tryDeleteOldFiles()
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
		wm.sharedWAL.tryDeleteOldFiles()
	}
	<-done

	// The kept ref is still resolvable — never freed while unacked.
	m, rerr := wm.Read(queues[K-1], uint64(K))
	require.NoError(t, rerr)
	assert.Equal(t, body, m.Body)
}

// TestSharedBody_CheckpointReInlinesBody (spec test #6): with the age backstop
// forced, a shared fan-out whose refs are all unacked is checkpointed — the WAL
// file is deleted and each queue holds a self-contained INLINE segment record
// (no reference), body readable from segments.
func TestSharedBody_CheckpointReInlinesBody(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := coldTailWALConfig()
	cfg.SharedBodyMaxPinAge = time.Nanosecond // backstop fires immediately -> re-inline
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

	time.Sleep(2 * time.Millisecond) // ensure age > SharedBodyMaxPinAge
	wm.sharedWAL.performCheckpoint()

	_, err = os.Stat(sharedFile)
	assert.True(t, os.IsNotExist(err), "backstop re-inlines then deletes the WAL file")

	// Each queue now has an INLINE segment record readable from segments.
	for i, q := range queues {
		m, rerr := sm.Read(q, uint64(1+i))
		require.NoError(t, rerr, "queue %s must have a re-inlined segment record", q)
		assert.Equal(t, body, m.Body)
	}
}

// TestSharedBody_ColdTailStaysOneCopyAcrossCheckpoint (spec test #12, CEO #1):
// with the hardening ON (no age backstop), an all-unacked shared fan-out is NOT
// checkpointed — the body stays as ONE copy in the WAL. Acking all refs then
// reclaims the whole file. A second subtest forces the backstop and documents
// the honest N-copy re-inline degradation.
func TestSharedBody_ColdTailStaysOneCopyAcrossCheckpoint(t *testing.T) {
	t.Run("hardening-on-body-survives-as-one-copy", func(t *testing.T) {
		tmpDir := t.TempDir()
		cfg := coldTailWALConfig() // SharedBodyMaxPinAge == 0 -> skip forever
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

		// Ack NONE; checkpoint several times.
		for i := 0; i < 3; i++ {
			wm.sharedWAL.performCheckpoint()
		}

		// (a) file NOT deleted, (b) NO segment records written, (c) still exactly
		// ONE body copy (the BodyBlock).
		_, err = os.Stat(sharedFile)
		require.NoError(t, err, "a shared-body file must be skipped by checkpoint (§2)")
		for i, q := range queues {
			_, rerr := sm.Read(q, uint64(1+i))
			assert.Error(t, rerr, "no segment record must be written for %s", q)
		}
		counts, err := WALRecordCountsForTest(tmpDir)
		require.NoError(t, err)
		assert.Equal(t, 1, counts.BodyBlocks, "still exactly one body copy on disk")
		assert.Equal(t, 0, counts.InlineBodies, "no N-copy re-inline happened")

		// (d) ack all K -> whole-file reclaim.
		offs := []uint64{1, 2, 3, 4}
		ackAndWait(t, wm, offs)
		wm.sharedWAL.tryDeleteOldFiles()
		_, err = os.Stat(sharedFile)
		assert.True(t, os.IsNotExist(err), "reclaimed whole once all refs ack")
	})

	t.Run("backstop-degrades-to-n-inline-copies", func(t *testing.T) {
		tmpDir := t.TempDir()
		cfg := coldTailWALConfig()
		cfg.SharedBodyMaxPinAge = time.Nanosecond
		wm, err := NewWALManagerWithConfig(tmpDir, cfg)
		require.NoError(t, err)
		defer wm.Close()

		sm, err := NewSegmentManager(tmpDir)
		require.NoError(t, err)
		defer sm.Close()
		wm.SetSegmentManager(sm)

		const K = 3
		body := sharedTestBody(8192)
		queues := queuesN(K)
		require.NoError(t, writeSharedSync(t, wm, body, queues, 1))
		sharedFile := firstWALFile(t, tmpDir)

		time.Sleep(2 * time.Millisecond)
		wm.sharedWAL.performCheckpoint()

		_, err = os.Stat(sharedFile)
		assert.True(t, os.IsNotExist(err), "backstop re-inlines + deletes the WAL file")
		for i, q := range queues {
			m, rerr := sm.Read(q, uint64(1+i))
			require.NoError(t, rerr)
			assert.Equal(t, body, m.Body, "each queue now holds its own inline copy (documented degradation)")
		}
	})
}
