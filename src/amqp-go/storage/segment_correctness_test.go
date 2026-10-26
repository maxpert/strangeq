package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Shared fixture helpers
//
// Every test in this file asserts its own premise at runtime (canon rule 11):
// a compaction fixture that never compacts, or a corruption fixture whose
// corruption lands outside the record it meant to damage, is a vacuous pass.
// ---------------------------------------------------------------------------

// segTestConfig is deliberately tiny so that a handful of records rolls the
// active segment into sealedSegments — compaction only ever considers SEALED
// segments, so a fixture at the 1 GB default would never compact anything.
// CompactionInterval is an hour so the background compactionLoop cannot race
// the explicit compactSegment call each test makes.
func segTestConfig() SegmentConfig {
	return SegmentConfig{
		SegmentSize:         512,
		CompactionThreshold: 0.5,
		CompactionInterval:  time.Hour,
		CheckpointInterval:  time.Hour,
	}
}

func segTestMessages(queueName string, offsets []uint64) []*RecoveryMessage {
	out := make([]*RecoveryMessage, 0, len(offsets))
	for _, o := range offsets {
		out = append(out, &RecoveryMessage{
			QueueName: queueName,
			Offset:    o,
			Message: &protocol.Message{
				Exchange:     "amq.direct",
				RoutingKey:   queueName,
				Body:         []byte(fmt.Sprintf("segbody-%06d-end", o)),
				DeliveryMode: 2,
				DeliveryTag:  o,
			},
		})
	}
	return out
}

func segBodyMarker(offset uint64) []byte {
	return []byte(fmt.Sprintf("segbody-%06d-end", offset))
}

// buildSealedSegment writes `count` records for `queueName` and asserts that
// EXACTLY ONE sealed segment resulted holding EVERY record. Both halves are
// premises the compaction tests depend on; neither is documented, both are
// checked.
func buildSealedSegment(t *testing.T, sm *SegmentManager, queueName string, count int) (*QueueSegments, *SegmentFile) {
	t.Helper()

	offsets := make([]uint64, 0, count)
	for i := 1; i <= count; i++ {
		offsets = append(offsets, uint64(i))
	}
	// ONE batch: writeMessageBatch appends every record and only then re-checks
	// the roll condition, so the whole set lands in a single segment which is
	// then sealed. Writing record-by-record would seal a PREFIX and leave the
	// rest in the active segment, where compaction never looks — the fixture
	// would sit outside the regime the defect lives in.
	require.NoError(t, writeLegacyBatchForTest(sm, queueName, segTestMessages(queueName, offsets)))

	val, ok := sm.queueSegments.Load(queueName)
	require.True(t, ok, "PREMISE: no QueueSegments was created for %q", queueName)
	qs := val.(*QueueSegments)

	qs.sealedMutex.RLock()
	sealed := make([]*SegmentFile, 0, len(qs.sealedSegments))
	for _, s := range qs.sealedSegments {
		sealed = append(sealed, s)
	}
	qs.sealedMutex.RUnlock()

	require.Len(t, sealed, 1,
		"PREMISE: the fixture must produce exactly ONE sealed segment; compaction only considers sealed segments, so a fixture with zero of them asserts nothing")
	seg := sealed[0]

	seg.mutex.RLock()
	indexed := len(seg.index)
	seg.mutex.RUnlock()
	require.Equal(t, count, indexed,
		"PREMISE: the sealed segment must hold every record the fixture wrote")

	return qs, seg
}

// ackMost acknowledges the odd offsets plus offset 2, so the sealed segment
// crosses the 0.5 compaction threshold (exactly half would NOT: tryCompaction
// uses a strict >), and asserts that it really did. The survivors stay
// INTERLEAVED with the acked records, which is what makes a compacted layout
// differ from the original at every surviving position.
func ackMost(t *testing.T, qs *QueueSegments, seg *SegmentFile, count int) (acked, unacked []uint64) {
	t.Helper()
	for i := 1; i <= count; i++ {
		if i%2 == 1 || i == 2 {
			qs.applyAck(uint64(i))
			acked = append(acked, uint64(i))
		} else {
			unacked = append(unacked, uint64(i))
		}
	}
	ratio := float64(seg.deletedCount.Load()) / float64(seg.messageCount.Load())
	require.Greater(t, ratio, qs.cfg.CompactionThreshold,
		"PREMISE: the segment must be over the compaction threshold, or compaction would never select it (deleted=%d total=%d)",
		seg.deletedCount.Load(), seg.messageCount.Load())
	return acked, unacked
}

func readFileBytes(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return b
}

// ---------------------------------------------------------------------------
// S-7 — queue names are client-controlled and reach the filesystem
// ---------------------------------------------------------------------------

func TestSegmentPath_ClientControlledQueueNameCannotEscapeTheDataDirectory(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	require.NoError(t, os.MkdirAll(dataDir, 0o755))

	// A canary the traversal would land next to. Its directory is the parent of
	// the data directory, i.e. OUTSIDE everything the broker owns.
	canaryDir := filepath.Join(root, "outside")
	require.NoError(t, os.MkdirAll(canaryDir, 0o755))

	sm, err := NewSegmentManagerWithConfig(dataDir, segTestConfig())
	require.NoError(t, err)

	// Exactly what a client can send in queue.declare: an AMQP shortstr. Nothing
	// between the wire and filepath.Join(sm.dataDir, queueName) inspects it.
	hostile := []string{
		"../../outside/pwned",
		"../escaped",
		"..",
		"a/b/c",
	}

	for _, name := range hostile {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, writeLegacyBatchForTest(sm, name, segTestMessages(name, []uint64{1, 2, 3})))

			// (a) nothing may exist outside the segments directory
			entries, err := os.ReadDir(canaryDir)
			require.NoError(t, err)
			require.Empty(t, entries,
				"ESCAPE: queue name %q created %d entries in %s, outside the data directory",
				name, len(entries), canaryDir)

			escaped, err := filepath.Glob(filepath.Join(root, "*", "*"+SegmentFileExtension))
			require.NoError(t, err)
			require.Empty(t, escaped,
				"ESCAPE: queue name %q wrote segment files outside <data>/segments: %v", name, escaped)

			// (b) every path the manager touched must stay under sm.dataDir
			val, ok := sm.queueSegments.Load(name)
			require.True(t, ok, "no QueueSegments for %q", name)
			qs := val.(*QueueSegments)
			rel, err := filepath.Rel(sm.dataDir, qs.dataDir)
			require.NoError(t, err)
			require.False(t, strings.HasPrefix(rel, ".."),
				"ESCAPE: queue %q resolved to %q which is outside %q", name, qs.dataDir, sm.dataDir)

			// (c) the data must still be there, under the ORIGINAL queue name
			msg, err := sm.Read(name, 2)
			require.NoError(t, err, "queue %q: message 2 must still be readable", name)
			require.Equal(t, segBodyMarker(2), msg.Body)
		})
	}

	// (d) a fresh manager over the same directory must map every directory back
	//     to the queue name that created it — otherwise the checkpointed copy is
	//     unreachable and performCheckpoint has already unlinked the WAL file.
	require.NoError(t, sm.Close())
	sm2, err := NewSegmentManagerWithConfig(dataDir, segTestConfig())
	require.NoError(t, err)
	defer sm2.Close()

	recovered, err := sm2.RecoverFromSegments()
	require.NoError(t, err)
	for _, name := range hostile {
		msgs, ok := recovered[name]
		require.True(t, ok, "queue %q did not survive a restart of the segment manager; recovered keys: %v",
			name, keysOf(recovered))
		require.Len(t, msgs, 3, "queue %q: wrong record count after restart", name)
	}
}

// Found while building the S-7 fixture: SegmentManager.Close is reachable
// twice on a real shutdown path (DisruptorStorage.Close calls it, and a
// deferred Close in any caller calls it again) and the second call panics on
// `close of closed channel` — a panic during shutdown, on the tier that holds
// the only copy of checkpointed data.
func TestSegmentManager_CloseIsIdempotent(t *testing.T) {
	dataDir := t.TempDir()
	sm, err := NewSegmentManagerWithConfig(dataDir, segTestConfig())
	require.NoError(t, err)
	require.NoError(t, writeLegacyBatchForTest(sm, "idem", segTestMessages("idem", []uint64{1, 2})))

	require.NoError(t, sm.Close())
	require.NotPanics(t, func() { _ = sm.Close() },
		"a second Close must not panic: DisruptorStorage.Close and any deferred Close both reach it")
}

func keysOf(m map[string][]*RecoveryMessage) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// A queue name that is already a safe single path element must keep the EXACT
// directory it has today. This is what makes the S-7 fix migration-free: an
// existing data directory's segment folders are untouched.
func TestSegmentPath_OrdinaryQueueNamesKeepTheirExistingDirectory(t *testing.T) {
	dataDir := t.TempDir()
	sm, err := NewSegmentManagerWithConfig(dataDir, segTestConfig())
	require.NoError(t, err)
	defer sm.Close()

	ordinary := []string{
		"orders",
		"my.queue",
		"amq.gen-JzTY6a2Cs0M1F0J2vTvKLg",
		"orders queue",
		"queue:with:colons",
		"héllo-ünicode",
		"50%off",
	}
	for _, name := range ordinary {
		require.NoError(t, writeLegacyBatchForTest(sm, name, segTestMessages(name, []uint64{1})))
		want := filepath.Join(sm.dataDir, name)
		st, err := os.Stat(want)
		require.NoError(t, err, "queue %q must still use the directory it uses today: %s", name, want)
		require.True(t, st.IsDir())
	}
}

// ---------------------------------------------------------------------------
// S-1 — compactSegment destroys records it cannot read or write
// ---------------------------------------------------------------------------

func TestSegmentCompaction_ReadFailureMustNotDestroyTheRecord(t *testing.T) {
	dataDir := t.TempDir()
	sm, err := NewSegmentManagerWithConfig(dataDir, segTestConfig())
	require.NoError(t, err)
	defer sm.Close()

	const count = 12
	const queueName = "read-fault-queue"
	qs, seg := buildSealedSegment(t, sm, queueName, count)
	_, unacked := ackMost(t, qs, seg, count)

	// Corrupt the CRC field of ONE unacked record, in the file, with real bytes.
	// This is not a hook: readSegmentMessageAt fails on it exactly as it would
	// on a real bit-rot or torn write.
	victim := unacked[0]
	seg.mutex.RLock()
	pos, ok := seg.index[victim]
	seg.mutex.RUnlock()
	require.True(t, ok, "PREMISE: victim offset %d must be indexed", victim)

	f, err := os.OpenFile(seg.path, os.O_WRONLY, 0o644)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{0xDE, 0xAD, 0xBE, 0xEF}, pos)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// PREMISE: the corruption must actually make the read fail, and must not
	// have damaged any other record.
	_, rerr := readSegmentMessageAt(seg.file, pos)
	require.Error(t, rerr, "PREMISE: the corrupted record must be unreadable")
	for _, o := range unacked[1:] {
		seg.mutex.RLock()
		p := seg.index[o]
		seg.mutex.RUnlock()
		m, err := readSegmentMessageAt(seg.file, p)
		require.NoError(t, err, "PREMISE: record %d must be undamaged", o)
		require.Equal(t, segBodyMarker(o), m.Body)
	}

	before := readFileBytes(t, seg.path)
	require.Contains(t, string(before), string(segBodyMarker(victim)),
		"PREMISE: the victim's bytes must be in the file before compaction")

	err = qs.compactSegment(seg)
	assert.Error(t, err,
		"compactSegment must FAIL when a record cannot be read: it is about to rename a rewritten file over the only copy of that record")

	after := readFileBytes(t, seg.path)
	assert.Contains(t, string(after), string(segBodyMarker(victim)),
		"DESTROYED: the record that could not be read is gone from the segment file — compaction dropped it and renamed anyway")
	assert.Equal(t, before, after,
		"the segment file must be byte-identical after a failed compaction")

	// Every other unacked record must still be readable through the segment.
	for _, o := range unacked[1:] {
		m, err := qs.readMessage(o)
		if !assert.NoError(t, err, "record %d must survive an aborted compaction", o) {
			continue
		}
		assert.Equal(t, o, m.DeliveryTag)
	}
}

// The control arm. Same fixture, NO fault injected: compaction must run,
// succeed, and drop exactly the acked records.
func TestSegmentCompaction_ControlArmWithNoFaultCompactsSuccessfully(t *testing.T) {
	dataDir := t.TempDir()
	sm, err := NewSegmentManagerWithConfig(dataDir, segTestConfig())
	require.NoError(t, err)
	defer sm.Close()

	const count = 12
	const queueName = "control-queue"
	qs, seg := buildSealedSegment(t, sm, queueName, count)
	acked, unacked := ackMost(t, qs, seg, count)

	before := readFileBytes(t, seg.path)

	require.NoError(t, qs.compactSegment(seg), "the control arm must compact cleanly")

	after := readFileBytes(t, seg.path)
	require.NotEqual(t, before, after,
		"PREMISE: the control arm must actually REWRITE the segment, or this fixture proves nothing about compaction")
	require.Less(t, len(after), len(before), "compaction must shrink the segment")

	for _, o := range acked {
		require.NotContains(t, string(after), string(segBodyMarker(o)),
			"acked record %d should have been compacted away", o)
	}
	for _, o := range unacked {
		require.Contains(t, string(after), string(segBodyMarker(o)),
			"unacked record %d must survive compaction", o)
		m, err := qs.readMessage(o)
		require.NoError(t, err)
		require.Equal(t, o, m.DeliveryTag,
			"after compaction, offset %d must still resolve to ITS OWN record", o)
	}
}

// A write failure inside compactSegment, driven by a REAL kernel-enforced
// condition (RLIMIT_FSIZE -> EFBIG on write), not by a production hook. The
// limit is process-wide, so the experiment runs in a re-exec of this same test
// binary; the parent only asserts the child's exit status.
func TestSegmentCompaction_WriteFailureMustNotDestroyRecords(t *testing.T) {
	if os.Getenv("SQ_SEGMENT_WRITE_FAULT_CHILD") == "1" {
		segmentWriteFaultChild(t)
		return
	}
	cmd := exec.Command(os.Args[0],
		"-test.run=^TestSegmentCompaction_WriteFailureMustNotDestroyRecords$",
		"-test.v", "-test.count=1")
	cmd.Env = append(os.Environ(), "SQ_SEGMENT_WRITE_FAULT_CHILD=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("child process (RLIMIT_FSIZE write-fault arm) failed: %v\n%s", err, out)
	}
}

func segmentWriteFaultChild(t *testing.T) {
	dataDir := t.TempDir()
	sm, err := NewSegmentManagerWithConfig(dataDir, segTestConfig())
	require.NoError(t, err)
	defer sm.Close()

	const count = 12
	const queueName = "write-fault-queue"
	qs, seg := buildSealedSegment(t, sm, queueName, count)
	_, unacked := ackMost(t, qs, seg, count)

	before := readFileBytes(t, seg.path)

	// Cap any file at 32 bytes. Every segment record is larger than that, so the
	// FIRST write into the compaction temp file returns EFBIG from the kernel.
	var saved syscall.Rlimit
	require.NoError(t, syscall.Getrlimit(syscall.RLIMIT_FSIZE, &saved))
	limited := syscall.Rlimit{Cur: 32, Max: saved.Max}
	require.NoError(t, syscall.Setrlimit(syscall.RLIMIT_FSIZE, &limited))

	// PREMISE: the limit must really be in force, or this fixture is vacuous.
	probe := filepath.Join(dataDir, "rlimit-probe")
	perr := os.WriteFile(probe, make([]byte, 4096), 0o644)
	_ = os.Remove(probe)

	cerr := qs.compactSegment(seg)

	require.NoError(t, syscall.Setrlimit(syscall.RLIMIT_FSIZE, &saved))
	require.Error(t, perr, "PREMISE: RLIMIT_FSIZE did not take effect; the write-fault arm would be vacuous")
	assert.Error(t, cerr,
		"compactSegment must FAIL when a record cannot be written: it is about to rename a truncated file over the only copy of the data")

	after := readFileBytes(t, seg.path)
	assert.Equal(t, before, after,
		"the segment file must be byte-identical after a failed compaction")
	for _, o := range unacked {
		assert.Contains(t, string(after), string(segBodyMarker(o)),
			"DESTROYED: unacked record %d is gone after a write failure during compaction", o)
		m, err := qs.readMessage(o)
		if !assert.NoError(t, err, "record %d must survive an aborted compaction", o) {
			continue
		}
		assert.Equal(t, o, m.DeliveryTag)
	}

	orphans, err := filepath.Glob(filepath.Join(qs.dataDir, "*"+segmentCompactSuffix))
	require.NoError(t, err)
	require.Empty(t, orphans, "an aborted compaction must not leave its temp file behind")
}

// ---------------------------------------------------------------------------
// S-2 — a failed os.Rename installs the new index over the old file
// ---------------------------------------------------------------------------

func TestSegmentCompaction_RenameFailureMustNotInstallTheNewIndex(t *testing.T) {
	dataDir := t.TempDir()
	sm, err := NewSegmentManagerWithConfig(dataDir, segTestConfig())
	require.NoError(t, err)
	defer sm.Close()

	const count = 12
	const queueName = "rename-fault-queue"
	qs, seg := buildSealedSegment(t, sm, queueName, count)
	_, unacked := ackMost(t, qs, seg, count)

	before := readFileBytes(t, seg.path)

	// REAL filesystem condition, no hooks: pre-create the compaction temp file,
	// then make its DIRECTORY read-only. O_CREATE on an existing writable file
	// still succeeds and writes still succeed (no directory entry is created),
	// but os.Rename needs write permission on the directory and gets EACCES.
	tempPath := seg.path + segmentCompactSuffix
	require.NoError(t, os.WriteFile(tempPath, nil, 0o644))
	require.NoError(t, os.Chmod(qs.dataDir, 0o500))
	t.Cleanup(func() { _ = os.Chmod(qs.dataDir, 0o755) })

	// PREMISE: the directory must really be un-writable, or the rename would
	// succeed and this fixture would assert nothing.
	probe := filepath.Join(qs.dataDir, "rename-probe")
	if err := os.WriteFile(probe, []byte("x"), 0o644); err == nil {
		_ = os.Remove(probe)
		t.Skip("SKIPPING LOUDLY: this process can still create files in a 0500 directory (running as root?); the rename-failure arm cannot be driven here")
	}

	cerr := qs.compactSegment(seg)
	assert.Error(t, cerr,
		"compactSegment must FAIL when os.Rename fails: the old file is still on disk, so installing the compacted index over it pairs new-layout positions with old-layout bytes")

	after := readFileBytes(t, seg.path)
	assert.Equal(t, before, after, "the segment file must be unchanged when the rename failed")

	// The killer assertion: every offset must still resolve to ITS OWN record.
	// With the new index installed over the old file, offset N returns some
	// other (frequently already-acked) record's bytes, silently.
	for _, o := range unacked {
		m, err := qs.readMessage(o)
		if !assert.NoError(t, err, "SILENT CORRUPTION: offset %d became unreadable after a failed rename", o) {
			continue
		}
		assert.Equal(t, o, m.DeliveryTag,
			"SILENT CORRUPTION: Read(offset=%d) returned a record whose DeliveryTag is %d", o, m.DeliveryTag)
		assert.Equal(t, segBodyMarker(o), m.Body)
	}
}

// ---------------------------------------------------------------------------
// S-4 / S-5 — segment scan classification
// ---------------------------------------------------------------------------

// segRecordBounds walks a segment file with an INDEPENDENT implementation of
// the framing (so the code under test is never the authority on where its own
// records are) and returns each record's [start,end).
func segRecordBounds(t *testing.T, path string) [][2]int64 {
	t.Helper()
	raw := readFileBytes(t, path)
	var out [][2]int64
	var pos int64
	for pos+8 <= int64(len(raw)) {
		dataLen := int64(binary.BigEndian.Uint32(raw[pos+4 : pos+8]))
		end := pos + 8 + dataLen
		if end > int64(len(raw)) {
			break
		}
		out = append(out, [2]int64{pos, end})
		pos = end
	}
	return out
}

func seedQueueSegment(t *testing.T, dataDir, queueName string, count int) string {
	t.Helper()
	sm, err := NewSegmentManagerWithConfig(dataDir, segTestConfig())
	require.NoError(t, err)
	offsets := make([]uint64, 0, count)
	for i := 1; i <= count; i++ {
		offsets = append(offsets, uint64(i))
	}
	require.NoError(t, writeLegacyBatchForTest(sm, queueName, segTestMessages(queueName, offsets)))
	require.NoError(t, sm.Close())

	segDir := filepath.Join(dataDir, "segments", queueName)
	entries, err := os.ReadDir(segDir)
	require.NoError(t, err)
	// The batch seals its segment and opens a fresh (empty) active one, so pick
	// the file that actually holds the records.
	var segPath string
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), SegmentFileExtension) {
			continue
		}
		info, ierr := e.Info()
		require.NoError(t, ierr)
		if info.Size() > 0 {
			require.Empty(t, segPath, "PREMISE: the fixture must produce exactly one non-empty segment file")
			segPath = filepath.Join(segDir, e.Name())
		}
	}
	require.NotEmpty(t, segPath, "PREMISE: the fixture wrote no segment file")
	require.Len(t, segRecordBounds(t, segPath), count,
		"PREMISE: the independent framing walk must find exactly the records the fixture wrote")
	return segPath
}

func reopenAndRecover(t *testing.T, dataDir string) (map[string][]*RecoveryMessage, error) {
	t.Helper()
	sm, err := NewSegmentManagerWithConfig(dataDir, segTestConfig())
	require.NoError(t, err)
	defer sm.Close()
	return sm.RecoverFromSegments()
}

func TestSegmentScan_InteriorCRCFailureIsFatalAndDoesNotTruncate(t *testing.T) {
	dataDir := t.TempDir()
	const damaged = "damaged-queue"
	const healthy = "healthy-queue"
	segPath := seedQueueSegment(t, dataDir, damaged, 10)
	_ = seedQueueSegment(t, dataDir, healthy, 6)

	bounds := segRecordBounds(t, segPath)
	victim := bounds[3]

	f, err := os.OpenFile(segPath, os.O_WRONLY, 0o644)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{0x00, 0x00, 0x00, 0x01}, victim[0])
	require.NoError(t, err)
	require.NoError(t, f.Close())

	sizeBefore := int64(len(readFileBytes(t, segPath)))
	// PREMISE: real data must follow the damaged record, or this is a tail case,
	// not an interior one.
	raw := readFileBytes(t, segPath)
	allZero := true
	for _, b := range raw[victim[1]:] {
		if b != 0 {
			allZero = false
			break
		}
	}
	require.False(t, allZero, "PREMISE: real data must follow the damaged record")

	recovered, err := reopenAndRecover(t, dataDir)
	require.Error(t, err, "an interior CRC failure in the only copy of the data must gate the boot")
	require.Equal(t, interfaces.RecoveryFatal, interfaces.OutcomeOf(err),
		"interior segment corruption must classify Fatal, got %v", interfaces.OutcomeOf(err))

	faults := interfaces.FaultsOf(err)
	require.NotEmpty(t, faults)
	var named bool
	for _, fl := range faults {
		if fl.Artifact == segPath {
			named = true
			require.True(t, filepath.IsAbs(fl.Artifact), "the fault must name an ABSOLUTE path")
			require.Equal(t, "segment-scan", fl.Stage)
		}
	}
	require.True(t, named, "the fault must name the damaged segment by absolute path; got %v", faults)

	require.Equal(t, sizeBefore, int64(len(readFileBytes(t, segPath))),
		"TRUNCATED: recovery destroyed the valid records after the damaged one; the segment is the only copy")

	// Quarantine: one damaged segment must not abandon the other queue.
	require.Len(t, recovered[healthy], 6,
		"a fatal fault on one queue's segment abandoned an undamaged queue's segments")
}

func TestSegmentScan_TornTailShapesAreBenign(t *testing.T) {
	t.Run("truncated_mid_record", func(t *testing.T) {
		dataDir := t.TempDir()
		segPath := seedQueueSegment(t, dataDir, "torn", 10)
		bounds := segRecordBounds(t, segPath)
		cut := bounds[9][0] + 4
		require.NoError(t, os.Truncate(segPath, cut))

		recovered, err := reopenAndRecover(t, dataDir)
		require.NoError(t, err, "a torn tail was never fsynced, so it must not gate the boot; got %v", err)
		require.Len(t, recovered["torn"], 9)
		st, serr := os.Stat(segPath)
		require.NoError(t, serr)
		require.Equal(t, cut, st.Size(), "recovery must not rewrite the file")
	})

	t.Run("zero_filled_tail_on_a_record_boundary", func(t *testing.T) {
		dataDir := t.TempDir()
		segPath := seedQueueSegment(t, dataDir, "zerotail", 10)
		bounds := segRecordBounds(t, segPath)
		start := bounds[8][0]
		f, err := os.OpenFile(segPath, os.O_WRONLY, 0o644)
		require.NoError(t, err)
		size := int64(len(readFileBytes(t, segPath)))
		_, err = f.WriteAt(make([]byte, size-start), start)
		require.NoError(t, err)
		require.NoError(t, f.Close())

		recovered, err := reopenAndRecover(t, dataDir)
		require.NoError(t, err,
			"a zero-filled tail starting on a record boundary must be a reported torn tail, not a fatal fault; got %v", err)
		require.Len(t, recovered["zerotail"], 8)
		st, serr := os.Stat(segPath)
		require.NoError(t, serr)
		require.Equal(t, size, st.Size(), "recovery must not rewrite the file")
	})

	t.Run("crc_failure_on_the_last_record", func(t *testing.T) {
		dataDir := t.TempDir()
		segPath := seedQueueSegment(t, dataDir, "tailcrc", 10)
		bounds := segRecordBounds(t, segPath)
		last := bounds[9]
		f, err := os.OpenFile(segPath, os.O_WRONLY, 0o644)
		require.NoError(t, err)
		_, err = f.WriteAt([]byte{0x00, 0x00, 0x00, 0x01}, last[0])
		require.NoError(t, err)
		require.NoError(t, f.Close())

		size := int64(len(readFileBytes(t, segPath)))
		recovered, err := reopenAndRecover(t, dataDir)
		require.NoError(t, err, "a CRC failure on the LAST record is a torn write; got %v", err)
		require.Len(t, recovered["tailcrc"], 9)
		st, serr := os.Stat(segPath)
		require.NoError(t, serr)
		require.Equal(t, size, st.Size(), "recovery must not rewrite the file")
	})
}

func TestSegmentScan_ZeroRegionFollowedByDataIsFatal(t *testing.T) {
	dataDir := t.TempDir()
	segPath := seedQueueSegment(t, dataDir, "zeromid", 10)
	bounds := segRecordBounds(t, segPath)
	start, end := bounds[4][0], bounds[6][1]

	f, err := os.OpenFile(segPath, os.O_WRONLY, 0o644)
	require.NoError(t, err)
	_, err = f.WriteAt(make([]byte, end-start), start)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = reopenAndRecover(t, dataDir)
	require.Error(t, err, "a zero-filled region FOLLOWED BY DATA cannot be a torn tail")
	require.Equal(t, interfaces.RecoveryFatal, interfaces.OutcomeOf(err))
}

func TestSegmentScan_UnreadableSegmentFileIsDegradedAndQuarantined(t *testing.T) {
	dataDir := t.TempDir()
	segPath := seedQueueSegment(t, dataDir, "unreadable", 10)
	_ = seedQueueSegment(t, dataDir, "sibling", 4)
	require.NoError(t, os.Chmod(segPath, 0o000))
	t.Cleanup(func() { _ = os.Chmod(segPath, 0o644) })

	if f, err := os.Open(segPath); err == nil {
		_ = f.Close()
		t.Skip("SKIPPING LOUDLY: chmod 000 did not make the segment unreadable (running as root?); the degraded arm cannot be driven here")
	}

	recovered, err := reopenAndRecover(t, dataDir)
	require.Error(t, err, "an unreadable segment file abandons confirmed durable messages and must gate the boot")
	require.Equal(t, interfaces.RecoveryDegraded, interfaces.OutcomeOf(err),
		"an unreadable segment file must classify Degraded, got %v", interfaces.OutcomeOf(err))

	var named bool
	for _, fl := range interfaces.FaultsOf(err) {
		if fl.Artifact == segPath {
			named = true
			require.True(t, filepath.IsAbs(fl.Artifact))
			require.Equal(t, "segment-open", fl.Stage)
		}
	}
	require.True(t, named, "the fault must name the unreadable segment by absolute path")
	require.Len(t, recovered["sibling"], 4, "one unreadable file must not abandon the whole segments directory")
}

// The premise walBlankRecordHeader rests on, asserted at runtime for the
// SEGMENT write path rather than documented: no record this build writes can
// have a zero length, so an all-zero header is a zero region and never data.
func TestSegmentScan_TheWritePathNeverProducesAZeroLengthRecord(t *testing.T) {
	dataDir := t.TempDir()
	segPath := seedQueueSegment(t, dataDir, "nonzero", 8)
	bounds := segRecordBounds(t, segPath)
	require.Len(t, bounds, 8)
	raw := readFileBytes(t, segPath)
	for _, b := range bounds {
		dataLen := binary.BigEndian.Uint32(raw[b[0]+4 : b[0]+8])
		require.NotZero(t, dataLen, "a segment record at offset %d has dataLen 0", b[0])
	}
	// An empty body is the smallest thing the write path can emit; it must still
	// carry a non-zero payload.
	bytes, err := serializeSegmentMessage(&protocol.Message{DeliveryMode: 2}, 1, false)
	require.NoError(t, err)
	require.NotZero(t, binary.BigEndian.Uint32(bytes[4:8]),
		"an empty-body message must still serialize to a non-zero-length segment record")
}

// ---------------------------------------------------------------------------
// S-8 — .seg.compact orphans
// ---------------------------------------------------------------------------

func TestSegmentLoad_CompactionOrphansAreReclaimed(t *testing.T) {
	dataDir := t.TempDir()
	segPath := seedQueueSegment(t, dataDir, "orphan-queue", 6)

	// What a crash mid-compaction leaves behind.
	orphan := segPath + segmentCompactSuffix
	require.NoError(t, os.WriteFile(orphan, []byte("half-written"), 0o644))

	recovered, err := reopenAndRecover(t, dataDir)
	require.NoError(t, err)
	require.Len(t, recovered["orphan-queue"], 6)

	require.NoFileExists(t, orphan,
		"a crash-orphaned %s file is never reclaimed and accumulates forever", segmentCompactSuffix)
}

// ---------------------------------------------------------------------------
// The compaction/read race in the same swap S-2 is about
// ---------------------------------------------------------------------------

// compactSegment replaces segment.file AND segment.index together, but
// readMessage takes segment.mutex only for the index lookup and then reads
// segment.file OUTSIDE it. A reader caught in that window pairs a position
// from one layout with a handle to the other — the same wrong-record outcome
// S-2 produces through a failed rename, reached instead by a plain data race
// on segment.file (which -race reports).
func TestSegmentCompaction_ConcurrentReadsNeverSeeAMismatchedFileAndIndex(t *testing.T) {
	dataDir := t.TempDir()
	sm, err := NewSegmentManagerWithConfig(dataDir, segTestConfig())
	require.NoError(t, err)
	defer sm.Close()

	const count = 12
	const queueName = "compact-race-queue"
	qs, seg := buildSealedSegment(t, sm, queueName, count)
	_, unacked := ackMost(t, qs, seg, count)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var reads, hits atomic.Int64
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, o := range unacked {
					reads.Add(1)
					m, rerr := qs.readMessage(o)
					if rerr != nil {
						continue
					}
					hits.Add(1)
					assert.Equal(t, o, m.DeliveryTag,
						"MISMATCH: Read(offset=%d) returned a record whose DeliveryTag is %d", o, m.DeliveryTag)
				}
			}
		}()
	}

	for i := 0; i < 200; i++ {
		require.NoError(t, qs.compactSegment(seg))
	}
	close(stop)
	wg.Wait()

	require.Positive(t, hits.Load(),
		"PREMISE: the readers must have completed at least one successful read concurrently with the compactions (reads=%d)", reads.Load())
	for _, o := range unacked {
		m, rerr := qs.readMessage(o)
		require.NoError(t, rerr)
		require.Equal(t, o, m.DeliveryTag)
	}
}
