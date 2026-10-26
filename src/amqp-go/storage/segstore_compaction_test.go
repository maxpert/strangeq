package storage

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// WI7 — byte-ratio WAL compaction.
//
// The WAL tier keeps every acknowledged record's bytes on disk until the file
// is reclaimed whole (WI6). A file whose acknowledged bytes are the majority
// of its durable bytes is mostly garbage; compaction rewrites it with only the
// committed, unacknowledged records.
//
// The invariants these tests pin:
//
//   - Selection is by BYTE RATIO and the comparison is STRICT: exactly 50%
//     dead does not compact, more than 50% does.
//   - A compaction never loses a live record, and never resurrects an
//     acknowledged one.
//   - Every failure before the rename leaves the old bytes AND the old index
//     exactly as they were.
//   - An acknowledgement that lands WHILE a compaction is running settles the
//     copy that the compaction carries over, and leaves unrelated records
//     live.
//   - After a compaction the read path uses the NEW layout: the dead records
//     are gone from the file and from the index.
//   - A shared body and a committed transaction survive a compaction and a
//     restart: exactly the live committed messages come back.

func compactionFileSize(t *testing.T, path string) int64 {
	t.Helper()
	st, err := os.Stat(path)
	require.NoError(t, err)
	return st.Size()
}

func compactionReadFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return b
}

// TestSegstoreCompactionThresholdStrict pins the selection predicate: the
// comparison is `deadBytes > threshold * durableBytes`, so exactly 50% dead
// does NOT compact and any ratio above it does.
//
// The fixture computes the body sizes it needs from the MEASURED per-record
// overhead, so the test does not hard-code the on-disk framing.
func TestSegstoreCompactionThresholdStrict(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, segstoreReclaimWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })
	qw := wm.sharedWAL

	// One zero-body record establishes the per-record overhead R0.
	writeSegstoreReclaimRecord(t, wm, "q", 1, "")
	r0 := compactionFileSize(t, qw.currentFile.Name()) - int64(WALHeaderSize)
	require.Greater(t, r0, int64(0), "a record must occupy more than the file header")

	// Records 2..5 are sized so that acknowledging 2 and 5 makes EXACTLY half
	// of the file's durable bytes dead:
	//
	//   durable = header + 5*R0 + 2*b1 + b2
	//   dead    = (R0+b1) + (R0+b2)
	//   dead*2 == durable  <=>  b2 == header + R0
	const b1 = 10
	b2 := int64(WALHeaderSize) + r0
	writeSegstoreReclaimRecord(t, wm, "q", 2, strings.Repeat("b", b1))
	writeSegstoreReclaimRecord(t, wm, "q", 3, strings.Repeat("b", b1))
	writeSegstoreReclaimRecord(t, wm, "q", 4, "")
	writeSegstoreReclaimRecord(t, wm, "q", 5, strings.Repeat("c", int(b2)))

	fileNum, path := rollSegstoreReclaimWAL(t, wm)
	wm.Acknowledge("q", 2)
	wm.Acknowledge("q", 5)

	inv := qw.oldFiles[fileNum].inv
	dead := inv.deadBytes.Load()
	durable := inv.durableBytes
	require.Equal(t, 2*dead, durable,
		"the fixture must put exactly 50%% of the file's durable bytes dead (dead=%d durable=%d)", dead, durable)

	before := compactionReadFile(t, path)
	qw.tryCompactOldFiles()
	after := compactionReadFile(t, path)
	require.Equal(t, before, after, "exactly 50%% dead must NOT compact")
	_, err = os.Stat(path + ".compact")
	require.True(t, errors.Is(err, fs.ErrNotExist), "a non-compacting sweep must not leave a temp file behind")

	// One more acknowledgement pushes the file strictly above the threshold.
	wm.Acknowledge("q", 3)
	inv = qw.oldFiles[fileNum].inv
	require.Greater(t, float64(inv.deadBytes.Load()), 0.5*float64(inv.durableBytes),
		"the fixture must now be strictly above the threshold")

	qw.tryCompactOldFiles()
	compacted := compactionReadFile(t, path)
	require.Less(t, len(compacted), len(after), "a file above the threshold must be compacted")

	// The live records survive with their bodies intact.
	msg, err := wm.Read("q", 1)
	require.NoError(t, err)
	require.Equal(t, "", string(msg.Body))
	msg, err = wm.Read("q", 4)
	require.NoError(t, err)
	require.Equal(t, "", string(msg.Body))

	// The acknowledged records are gone from the index and from the file.
	for _, tag := range []uint64{2, 3, 5} {
		_, err = wm.Read("q", tag)
		require.Error(t, err, "acknowledged tag %d must not be readable after compaction", tag)
	}
	qw.offsetIndexMutex.RLock()
	for _, tag := range []uint64{2, 3, 5} {
		_, ok := qw.offsetIndex[recordKey{queueName: "q", deliveryTag: tag}]
		require.False(t, ok, "acknowledged tag %d must not stay in the index", tag)
	}
	qw.offsetIndexMutex.RUnlock()
}

// TestSegstoreCompactionPreservesLive proves a compaction keeps every
// committed, unacknowledged record (and its body) and drops only the
// acknowledged ones.
func TestSegstoreCompactionPreservesLive(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, segstoreReclaimWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })
	qw := wm.sharedWAL

	writeSegstoreReclaimRecord(t, wm, "live", 1, "one")
	writeSegstoreReclaimRecord(t, wm, "live", 2, "two")
	writeSegstoreReclaimRecord(t, wm, "live", 3, "three")
	writeSegstoreReclaimRecord(t, wm, "dead", 1, strings.Repeat("x", 4096))

	_, path := rollSegstoreReclaimWAL(t, wm)
	wm.Acknowledge("dead", 1)

	before := compactionFileSize(t, path)
	qw.tryCompactOldFiles()
	after := compactionFileSize(t, path)
	require.Less(t, after, before, "compaction must reclaim the acknowledged bytes")

	for tag, body := range map[uint64]string{1: "one", 2: "two", 3: "three"} {
		msg, err := wm.Read("live", tag)
		require.NoError(t, err, "live tag %d must survive compaction", tag)
		require.Equal(t, body, string(msg.Body))
	}

	qw.offsetIndexMutex.RLock()
	_, ok := qw.offsetIndex[recordKey{queueName: "dead", deliveryTag: 1}]
	qw.offsetIndexMutex.RUnlock()
	require.False(t, ok, "the acknowledged record must be dropped from the index")

	require.NotContains(t, string(compactionReadFile(t, path)), strings.Repeat("x", 64),
		"the acknowledged body must be gone from the compacted file")
}

// TestSegstoreCompactionWriteFsyncRenameFaults proves every failure BEFORE
// the rename leaves the old bytes and the old index untouched.
func TestSegstoreCompactionWriteFsyncRenameFaults(t *testing.T) {
	newFixture := func(t *testing.T) (*WALManager, string) {
		t.Helper()
		dir := t.TempDir()
		wm, err := NewWALManagerWithConfig(dir, segstoreReclaimWALConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = wm.Close() })
		writeSegstoreReclaimRecord(t, wm, "live", 1, "live-body")
		writeSegstoreReclaimRecord(t, wm, "dead", 1, strings.Repeat("d", 4096))
		_, path := rollSegstoreReclaimWAL(t, wm)
		wm.Acknowledge("dead", 1)
		return wm, path
	}

	// The control: with no fault injected, the SAME fixture IS compacted. It
	// is what makes the three fault cases meaningful — "the bytes did not
	// change" is only evidence if "the bytes would have changed" is true.
	t.Run("control", func(t *testing.T) {
		wm, path := newFixture(t)
		qw := wm.sharedWAL
		before := compactionFileSize(t, path)

		qw.tryCompactOldFiles()

		after := compactionFileSize(t, path)
		require.Less(t, after, before, "the control fixture must be compacted without a fault")
		msg, err := wm.Read("live", 1)
		require.NoError(t, err)
		require.Equal(t, "live-body", string(msg.Body))
	})

	t.Run("write fault", func(t *testing.T) {
		wm, path := newFixture(t)
		qw := wm.sharedWAL
		before := compactionReadFile(t, path)

		restore := SetWALWriteForTest(func(f *os.File, b []byte) (int, error) {
			if strings.HasSuffix(f.Name(), ".compact") {
				return 0, errors.New("injected compaction write fault")
			}
			return f.Write(b)
		})
		qw.tryCompactOldFiles()
		restore()

		require.Equal(t, before, compactionReadFile(t, path), "a write fault must leave the old bytes untouched")
		msg, err := wm.Read("live", 1)
		require.NoError(t, err, "a write fault must leave the old index untouched")
		require.Equal(t, "live-body", string(msg.Body))
		_, err = os.Stat(path + ".compact")
		require.True(t, errors.Is(err, fs.ErrNotExist), "an aborted compaction must not leave a temp file behind")
	})

	t.Run("fsync fault", func(t *testing.T) {
		wm, path := newFixture(t)
		qw := wm.sharedWAL
		before := compactionReadFile(t, path)

		restore := SetWALCompactionFsyncForTest(func(*os.File) error {
			return errors.New("injected compaction fsync fault")
		})
		qw.tryCompactOldFiles()
		restore()

		require.Equal(t, before, compactionReadFile(t, path), "an fsync fault must leave the old bytes untouched")
		msg, err := wm.Read("live", 1)
		require.NoError(t, err, "an fsync fault must leave the old index untouched")
		require.Equal(t, "live-body", string(msg.Body))
	})

	t.Run("rename fault", func(t *testing.T) {
		wm, path := newFixture(t)
		qw := wm.sharedWAL
		before := compactionReadFile(t, path)

		restore := SetWALRenameForTest(func(oldPath, newPath string) error {
			// The install holds the index WRITE lock, so neither a writer
			// nor a reader can take the lock while the rename happens.
			require.False(t, qw.offsetIndexMutex.TryLock(),
				"the rename must happen under the index write lock")
			require.False(t, qw.offsetIndexMutex.TryRLock(),
				"the rename must happen under the index write lock")
			return errors.New("injected compaction rename fault")
		})
		qw.tryCompactOldFiles()
		restore()

		require.Equal(t, before, compactionReadFile(t, path), "a rename fault must leave the old bytes untouched")
		msg, err := wm.Read("live", 1)
		require.NoError(t, err, "a rename fault must leave the old index untouched")
		require.Equal(t, "live-body", string(msg.Body))
	})
}

// TestSegmentAckDuringCompaction proves an acknowledgement that lands WHILE
// a compaction is running settles the copy the compaction carries over, and
// leaves the unrelated records live.
//
// The acknowledgement is delivered through the before-install hook and a
// channel pair, so the ordering is deterministic and needs no timing.
func TestSegmentAckDuringCompaction(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, segstoreReclaimWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })
	qw := wm.sharedWAL

	writeSegstoreReclaimRecord(t, wm, "q", 1, "one")
	writeSegstoreReclaimRecord(t, wm, "q", 2, "two")
	writeSegstoreReclaimRecord(t, wm, "q", 3, "three")
	writeSegstoreReclaimRecord(t, wm, "dead", 1, strings.Repeat("d", 4096))
	_, path := rollSegstoreReclaimWAL(t, wm)
	wm.Acknowledge("dead", 1)

	reached := make(chan struct{})
	proceed := make(chan struct{})
	var once sync.Once
	restore := SetWALCompactionBeforeInstallForTest(func(fileNum uint64) {
		once.Do(func() {
			close(reached)
			<-proceed
		})
	})
	defer restore()

	done := make(chan struct{})
	go func() {
		defer close(done)
		qw.tryCompactOldFiles()
	}()

	<-reached
	// The acknowledgement lands while the candidate file is already written.
	wm.Acknowledge("q", 2)
	close(proceed)
	<-done

	// The late-acknowledged record must NOT be resurrected as live.
	_, err = wm.Read("q", 2)
	require.Error(t, err, "a record acknowledged during compaction must not be delivered")

	// The unrelated records stay live.
	for tag, body := range map[uint64]string{1: "one", 3: "three"} {
		msg, rerr := wm.Read("q", tag)
		require.NoError(t, rerr, "unrelated tag %d must stay live", tag)
		require.Equal(t, body, string(msg.Body))
	}

	// The compacted file exists and no temp file is left behind.
	_, err = os.Stat(path)
	require.NoError(t, err)
	_, err = os.Stat(path + ".compact")
	require.True(t, errors.Is(err, fs.ErrNotExist))
}

// TestSegstoreCompactionReadLayoutExclusion proves the read path uses the NEW
// layout after a compaction: the dead records are gone from the file and from
// the index, the live ones are read from their new positions, and the read
// happens under the index read lock (so a compaction cannot swap the layout
// underneath a reader).
func TestSegstoreCompactionReadLayoutExclusion(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, segstoreReclaimWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })
	qw := wm.sharedWAL

	// The dead record is written FIRST, so dropping it shifts every live
	// record to a new position: the read layout must change.
	writeSegstoreReclaimRecord(t, wm, "dead", 1, strings.Repeat("d", 4096))
	writeSegstoreReclaimRecord(t, wm, "live", 1, "one")
	writeSegstoreReclaimRecord(t, wm, "live", 2, "two")
	_, path := rollSegstoreReclaimWAL(t, wm)
	wm.Acknowledge("dead", 1)

	qw.offsetIndexMutex.RLock()
	oldPosition := qw.offsetIndex[recordKey{queueName: "live", deliveryTag: 1}].pos.position
	qw.offsetIndexMutex.RUnlock()
	qw.tryCompactOldFiles()

	qw.offsetIndexMutex.RLock()
	newPosition := qw.offsetIndex[recordKey{queueName: "live", deliveryTag: 1}].pos.position
	qw.offsetIndexMutex.RUnlock()
	require.NotEqual(t, oldPosition, newPosition, "the compacted layout must differ from the old one")

	// The read must hit the NEW position, under the index read lock.
	var seen []int64
	restore := SetWALReadAtForTest(func(f *os.File, b []byte, off int64) (int, error) {
		seen = append(seen, off)
		require.False(t, qw.offsetIndexMutex.TryLock(),
			"a delivery read must hold the index read lock, so a compaction cannot swap the layout under it")
		return f.ReadAt(b, off)
	})
	msg, err := wm.Read("live", 1)
	restore()
	require.NoError(t, err)
	require.Equal(t, "one", string(msg.Body))
	require.Contains(t, seen, newPosition, "the read must use the compacted layout")
	require.NotContains(t, seen, oldPosition, "the read must not use the pre-compaction layout")

	// The dead record is excluded from the read layout entirely.
	_, err = wm.Read("dead", 1)
	require.Error(t, err)
	qw.offsetIndexMutex.RLock()
	_, ok := qw.offsetIndex[recordKey{queueName: "dead", deliveryTag: 1}]
	qw.offsetIndexMutex.RUnlock()
	require.False(t, ok)
	require.NotContains(t, string(compactionReadFile(t, path)), strings.Repeat("d", 64),
		"the excluded record's bytes must be gone from the file")
}

// TestSegstoreCompactionSharedBodyTxRestart proves a shared body and a
// committed transaction survive a compaction AND a restart: exactly the live
// committed messages come back, with their bodies intact.
func TestSegstoreCompactionSharedBodyTxRestart(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, segstoreReclaimWALConfig())
	require.NoError(t, err)
	qw := wm.sharedWAL

	queues := []string{"shared-a", "shared-b", "shared-c"}
	body := sharedTestBody(4096)
	require.NoError(t, writeSharedSync(t, wm, body, queues, 1))

	// A committed transaction (Begin + records + Commit) in the same file.
	require.NoError(t, wm.WriteTxAtomic([]*RecoveryMessage{
		{QueueName: "tx", Offset: 100, Message: &protocol.Message{DeliveryMode: 2, Body: []byte("tx-1")}},
		{QueueName: "tx", Offset: 101, Message: &protocol.Message{DeliveryMode: 2, Body: []byte("tx-2")}},
	}))

	// Acknowledged bulk so the file is above the compaction threshold.
	writeSegstoreReclaimRecord(t, wm, "dead", 1, strings.Repeat("d", 8192))
	_, path := rollSegstoreReclaimWAL(t, wm)
	wm.Acknowledge("dead", 1)

	qw.tryCompactOldFiles()
	compacted := compactionReadFile(t, path)
	require.NotContains(t, string(compacted), strings.Repeat("d", 64),
		"the acknowledged bulk must be gone from the compacted file")

	require.NoError(t, wm.Close())

	// Restart: the WAL is rebuilt from the compacted file.
	reopened, err := NewWALManagerWithConfig(dir, segstoreReclaimWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	recovered, err := reopened.RecoverFromWAL()
	require.NoError(t, err)

	got := map[string]string{}
	for _, rm := range recovered {
		got[rm.QueueName+"/"+strconv.FormatUint(rm.Offset, 10)] = string(rm.Message.Body)
	}
	require.Equal(t, map[string]string{
		"shared-a/1": string(body),
		"shared-b/2": string(body),
		"shared-c/3": string(body),
		"tx/100":     "tx-1",
		"tx/101":     "tx-2",
	}, got, "exactly the live committed messages must come back after a restart")
}

// TestSegstoreCompactionSharedBodyLiveRead proves a shared body survives a
// compaction ON THE LIVE PATH: the record's body locator is remapped to the
// block's new position, and the delivery path resolves the body through that
// new locator.
//
// The restart path is covered by TestSegstoreCompactionSharedBodyTxRestart;
// this one covers the read that happens WITHOUT a restart, which is the path
// that resolves a BodyBlock by absolute offset.
func TestSegstoreCompactionSharedBodyLiveRead(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, segstoreReclaimWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })
	qw := wm.sharedWAL

	// The acknowledged bulk is written FIRST, so dropping it moves the shared
	// BodyBlock and the remapping of the locator is observable.
	writeSegstoreReclaimRecord(t, wm, "dead", 1, strings.Repeat("d", 8192))

	queues := []string{"live-a", "live-b", "live-c"}
	body := sharedTestBody(4096)
	require.NoError(t, writeSharedSync(t, wm, body, queues, 1))

	_, path := rollSegstoreReclaimWAL(t, wm)
	wm.Acknowledge("dead", 1)

	qw.offsetIndexMutex.RLock()
	oldRef := qw.offsetIndex[recordKey{queueName: "live-a", deliveryTag: 1}].bodyRef
	qw.offsetIndexMutex.RUnlock()
	require.GreaterOrEqual(t, oldRef, int64(0), "the shared-body record must carry a body reference")

	qw.tryCompactOldFiles()
	compacted := compactionReadFile(t, path)
	require.NotContains(t, string(compacted), strings.Repeat("d", 64),
		"the acknowledged bulk must be gone from the compacted file")

	qw.offsetIndexMutex.RLock()
	newRef := qw.offsetIndex[recordKey{queueName: "live-a", deliveryTag: 1}].bodyRef
	qw.offsetIndexMutex.RUnlock()
	require.NotEqual(t, oldRef, newRef, "the body locator must be remapped to the block's new position")

	// Every reference to the shared body still resolves to the same bytes.
	for i, q := range queues {
		msg, rerr := wm.Read(q, uint64(1+i))
		require.NoError(t, rerr, "shared-body record for queue %s must survive compaction", q)
		require.Equal(t, body, msg.Body, "the shared body must be intact for queue %s", q)
	}
}

// TestSegstoreCompactionBootRemovesOrphans proves a crash between "the
// compacted copy is written" and "the compacted copy is installed" does not
// leak a file forever: the boot removes the orphan.
//
// The orphan is a PARTIAL DUPLICATE of a file that still holds every record,
// so removing it loses nothing.
func TestSegstoreCompactionBootRemovesOrphans(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, segstoreReclaimWALConfig())
	require.NoError(t, err)
	writeSegstoreReclaimRecord(t, wm, "q", 1, "one")
	fileNum, path := rollSegstoreReclaimWAL(t, wm)
	require.NoError(t, wm.Close())

	// The orphan a crashed compaction leaves behind.
	orphan := path + ".compact"
	require.NoError(t, os.WriteFile(orphan, []byte("partial copy"), 0644))
	// A file that merely LOOKS similar is not an orphan and must survive.
	foreign := filepath.Join(filepath.Dir(path), "not-a-number.wal.compact")
	require.NoError(t, os.WriteFile(foreign, []byte("foreign"), 0644))

	reopened, err := NewWALManagerWithConfig(dir, segstoreReclaimWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })

	_, err = os.Stat(orphan)
	require.True(t, errors.Is(err, fs.ErrNotExist), "the boot must remove a numeric .wal.compact orphan")
	_, err = os.Stat(foreign)
	require.NoError(t, err, "a file that is not a recognized orphan must not be removed")

	// The real WAL file is untouched and still recovers its record.
	recovered, err := reopened.RecoverFromWAL()
	require.NoError(t, err)
	require.Len(t, recovered, 1)
	require.Equal(t, uint64(1), recovered[0].Offset)
	require.Equal(t, "one", string(recovered[0].Message.Body))
	_ = fileNum
}
