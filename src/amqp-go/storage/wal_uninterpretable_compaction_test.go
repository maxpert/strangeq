package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io/fs"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// FINDING A — WAL COMPACTION MUST NEVER DELETE A RECORD THIS BUILD CANNOT
// INTERPRET.
//
// A v4 message record whose payload sets the reserved CONTINUATION bit (or
// whose body kind this build does not know) is DURABLE DATA THIS BUILD CANNOT
// INTERPRET. The boot index (indexWALFile) deliberately does NOT index it:
// nothing can serve it. What it does instead is count its bytes as LIVE
// (inv.unacked), so the file can never be reclaimed while they are on disk.
//
// "COUNTED AS LIVE" IS NOT, BY ITSELF, ENOUGH. The compaction rewrites a file
// with only the records the inventory HOLDS, and the uninterpretable record is
// not one of them. The reclaim predicate reads inv.unacked (so the file is
// pinned against UNLINKING), but the compaction's own eligibility test was
// "is the file fully acknowledged?" -- a question the uninterpretable bytes
// answer NO to, and a NO means "go ahead and rewrite the file". The rewrite
// therefore DELETED the uninterpretable record's bytes: a silent loss of
// confirmed durable data that a newer build could still have read.
//
// THE INVARIANT THIS FILE PINS: a WAL file that holds ANY byte this build
// cannot interpret is NEVER compacted and NEVER reclaimed. Its bytes stay on
// disk, byte for byte, until a build that CAN interpret them (or an operator)
// deals with them.
//
// WHAT A MUTATION WOULD BREAK: if the guard that skips such a file were
// removed -- if compactOldFile went back to selecting files by the dead-byte
// ratio alone -- the sweep below would rewrite the file, and the assertion
// that the file is BYTE-IDENTICAL afterwards would fail, because the
// uninterpretable record's bytes would be gone.

// TestWALCompactionPreservesUninterpretableRecords is the reproduction.
//
// The fixture is a HAND-BUILT v4 WAL file:
//
//   - one message record whose flags word carries the reserved continuation
//     bit (msgFlagContinuation) -- the uninterpretable record;
//   - eight ordinary message records, seven of which are acknowledged, so
//     the file's dead bytes are strictly more than half of its durable bytes
//     and the compaction's selection predicate is satisfied.
//
// The eighth ordinary record stays UNACKNOWLEDGED on purpose: it is what makes
// the rewrite non-empty, so without the guard the sweep really rewrites the
// file rather than finding nothing to copy.
func TestWALCompactionPreservesUninterpretableRecords(t *testing.T) {
	dir := t.TempDir()

	const (
		liveQueue   = "q-live"
		futureQueue = "q-future"
	)
	futureBody := []byte("a body only a newer build can read")
	records := [][]byte{
		grammarWALRecord(WALRecordTypeMessage,
			grammarMessagePayload(futureQueue, 900, futureBody, msgFlagContinuation)),
	}
	for i := 1; i <= 8; i++ {
		records = append(records, grammarWALRecord(WALRecordTypeMessage,
			grammarMessagePayload(liveQueue, uint64(i), []byte(strings.Repeat("x", 512)), 0)))
	}
	path := writeGrammarWALFile(t, dir, "00000000000000000001.wal", records)

	cfg := segstoreReclaimWALConfig()
	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })
	qw := wm.sharedWAL

	// THE PREMISE. The boot index registered the hand-built file and counted
	// the uninterpretable record's bytes as LIVE.
	qw.oldFilesMutex.RLock()
	info, registered := qw.oldFiles[1]
	qw.oldFilesMutex.RUnlock()
	require.True(t, registered, "the hand-built WAL file must be registered as an old file")
	inv := info.inv
	require.Positive(t, inv.uninterpretableBytes.Load(),
		"the boot index must count the uninterpretable record's bytes explicitly")
	require.Positive(t, inv.unacked.Load(),
		"the uninterpretable record's bytes must be counted as LIVE")

	// Acknowledge seven of the eight ordinary records: the file's dead bytes
	// are now strictly more than half of its durable bytes, which is the
	// compaction's selection predicate.
	for i := 1; i <= 7; i++ {
		wm.Acknowledge(liveQueue, uint64(i))
	}
	require.Greater(t, float64(inv.deadBytes.Load()), 0.5*float64(inv.durableBytes.Load()),
		"the fixture must put the file strictly above the compaction threshold (dead=%d durable=%d)",
		inv.deadBytes.Load(), inv.durableBytes.Load())

	before, err := os.ReadFile(path)
	require.NoError(t, err)
	require.True(t, bytes.Contains(before, futureBody),
		"the fixture must actually hold the uninterpretable record's body")

	qw.tryCompactOldFiles()

	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after,
		"a file holding a record this build cannot interpret must not be compacted: "+
			"the uninterpretable record's bytes were deleted")
	require.True(t, bytes.Contains(after, futureBody),
		"the uninterpretable record must still be on disk after the compaction sweep")
	require.True(t, bytes.Contains(after, []byte(strings.Repeat("x", 512))),
		"the acknowledged records must still be on disk too: the whole file must be left alone")

	_, err = os.Stat(path + walCompactSuffix)
	require.True(t, errors.Is(err, fs.ErrNotExist),
		"a skipped compaction must not leave a temp file behind: %v", err)
}

// TestWALCompactionPreservesUninterpretableRecordsInATransaction is the same
// invariant on the IN-TRANSACTION branch of the boot index.
//
// A continuation-flagged record inside a COMMITTED transaction is durable data
// a newer build could still read: the transaction's commit makes it durable,
// so its bytes are protected exactly like an out-of-transaction record's.
// (Inside a DISCARDED transaction the bytes are garbage -- the transaction
// never committed -- and the file can be compacted, which is the correct
// behaviour and is asserted below too.)
func TestWALCompactionPreservesUninterpretableRecordsInATransaction(t *testing.T) {
	const epoch = uint64(42)
	begin := grammarWALRecord(WALRecordTypeTxBoundary,
		append([]byte{WALTxMarkerBegin}, binary.BigEndian.AppendUint64(nil, epoch)...))
	commit := grammarWALRecord(WALRecordTypeTxBoundary,
		append([]byte{WALTxMarkerCommit}, binary.BigEndian.AppendUint64(nil, epoch)...))
	// A COMMIT FOR A DIFFERENT EPOCH: the open transaction is DISCARDED (its
	// records were never committed), and the records that follow are outside
	// any transaction.
	mismatchedCommit := grammarWALRecord(WALRecordTypeTxBoundary,
		append([]byte{WALTxMarkerCommit}, binary.BigEndian.AppendUint64(nil, epoch+1)...))

	t.Run("a committed transaction protects the record", func(t *testing.T) {
		dir := t.TempDir()
		const (
			liveQueue   = "q-live"
			futureQueue = "q-future"
		)
		futureBody := []byte("a body only a newer build can read")
		records := [][]byte{begin}
		records = append(records, grammarWALRecord(WALRecordTypeMessage,
			grammarMessagePayload(futureQueue, 900, futureBody, msgFlagContinuation)))
		records = append(records, commit)
		for i := 1; i <= 8; i++ {
			records = append(records, grammarWALRecord(WALRecordTypeMessage,
				grammarMessagePayload(liveQueue, uint64(i), []byte(strings.Repeat("x", 512)), 0)))
		}
		path := writeGrammarWALFile(t, dir, "00000000000000000001.wal", records)

		cfg := segstoreReclaimWALConfig()
		wm, err := NewWALManagerWithConfig(dir, cfg)
		require.NoError(t, err)
		t.Cleanup(func() { _ = wm.Close() })
		qw := wm.sharedWAL

		qw.oldFilesMutex.RLock()
		info, registered := qw.oldFiles[1]
		qw.oldFilesMutex.RUnlock()
		require.True(t, registered)
		inv := info.inv
		require.Positive(t, inv.uninterpretableBytes.Load(),
			"the committed transaction's uninterpretable bytes must be counted explicitly")
		require.Positive(t, inv.unacked.Load(),
			"the committed transaction's uninterpretable bytes must be counted as LIVE")

		for i := 1; i <= 7; i++ {
			wm.Acknowledge(liveQueue, uint64(i))
		}
		require.Greater(t, float64(inv.deadBytes.Load()), 0.5*float64(inv.durableBytes.Load()),
			"the fixture must put the file strictly above the compaction threshold")

		before, err := os.ReadFile(path)
		require.NoError(t, err)

		qw.tryCompactOldFiles()

		after, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, before, after,
			"a file holding a committed transaction's uninterpretable record must not be compacted")
		require.True(t, bytes.Contains(after, futureBody),
			"the uninterpretable record must still be on disk after the compaction sweep")
	})

	t.Run("a discarded transaction's record is garbage", func(t *testing.T) {
		dir := t.TempDir()
		const (
			liveQueue   = "q-live"
			futureQueue = "q-future"
		)
		futureBody := []byte("a body only a newer build can read")
		records := [][]byte{begin}
		records = append(records, grammarWALRecord(WALRecordTypeMessage,
			grammarMessagePayload(futureQueue, 900, futureBody, msgFlagContinuation)))
		// The commit names a DIFFERENT epoch: the transaction is discarded,
		// so its bytes are garbage.
		records = append(records, mismatchedCommit)
		for i := 1; i <= 8; i++ {
			records = append(records, grammarWALRecord(WALRecordTypeMessage,
				grammarMessagePayload(liveQueue, uint64(i), []byte(strings.Repeat("x", 512)), 0)))
		}
		path := writeGrammarWALFile(t, dir, "00000000000000000001.wal", records)

		cfg := segstoreReclaimWALConfig()
		wm, err := NewWALManagerWithConfig(dir, cfg)
		require.NoError(t, err)
		t.Cleanup(func() { _ = wm.Close() })
		qw := wm.sharedWAL

		qw.oldFilesMutex.RLock()
		info, registered := qw.oldFiles[1]
		qw.oldFilesMutex.RUnlock()
		require.True(t, registered)
		inv := info.inv
		require.Zero(t, inv.uninterpretableBytes.Load(),
			"a discarded transaction's bytes are garbage, not protected durable data")

		for i := 1; i <= 7; i++ {
			wm.Acknowledge(liveQueue, uint64(i))
		}
		require.Greater(t, float64(inv.deadBytes.Load()), 0.5*float64(inv.durableBytes.Load()),
			"the fixture must put the file strictly above the compaction threshold")

		qw.tryCompactOldFiles()

		after, err := os.ReadFile(path)
		require.NoError(t, err)
		require.False(t, bytes.Contains(after, futureBody),
			"a discarded transaction's record is garbage and may be compacted away")
	})
}

// TestWALReclaimPreservesUninterpretableRecords pins the OTHER half of the
// invariant: a file whose records are all acknowledged but which also holds
// bytes this build cannot interpret is never UNLINKED either.
//
// WHAT A MUTATION WOULD BREAK: if the reclaim predicate stopped consulting the
// uninterpretable bytes, the sweep would unlink the file and the durable bytes
// would be gone -- and the assertion that the file still exists would fail.
func TestWALReclaimPreservesUninterpretableRecords(t *testing.T) {
	dir := t.TempDir()

	const (
		liveQueue   = "q-live"
		futureQueue = "q-future"
	)
	futureBody := []byte("a body only a newer build can read")
	records := [][]byte{
		grammarWALRecord(WALRecordTypeMessage,
			grammarMessagePayload(futureQueue, 900, futureBody, msgFlagContinuation)),
	}
	for i := 1; i <= 3; i++ {
		records = append(records, grammarWALRecord(WALRecordTypeMessage,
			grammarMessagePayload(liveQueue, uint64(i), []byte(strings.Repeat("x", 512)), 0)))
	}
	path := writeGrammarWALFile(t, dir, "00000000000000000001.wal", records)

	cfg := segstoreReclaimWALConfig()
	wm, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })
	qw := wm.sharedWAL

	// Every INTERPRETABLE record is acknowledged, so the file holds nothing
	// this build can serve. The uninterpretable record's bytes are still
	// durable data.
	for i := 1; i <= 3; i++ {
		wm.Acknowledge(liveQueue, uint64(i))
	}

	qw.tryReclaimOldFiles()

	_, err = os.Stat(path)
	require.NoError(t, err,
		"a file holding bytes this build cannot interpret must never be unlinked")
	qw.oldFilesMutex.RLock()
	_, registered := qw.oldFiles[1]
	qw.oldFilesMutex.RUnlock()
	require.True(t, registered, "the file must stay registered so a later sweep retries it")
}
