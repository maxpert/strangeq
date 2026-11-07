package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

// Step 3, storage half.
//
// Two properties are pinned here:
//
//  1. A WAL file whose framing version this build cannot parse is FATAL at
//     storage construction, and the broker never appends v4 records after a
//     foreign header. Before Step 3, rebuildBootState swallowed the version
//     error per-file and construction succeeded.
//  2. Ra's three-way CRC split (ref-rabbitmq.md §1.3): a CRC failure on the
//     LAST record is a torn tail and is survivable; a CRC failure on an
//     INTERIOR record means the file cannot be interpreted and is fatal.

// plantForeignWAL writes <dir>/wal/shared/00000000000000000001.wal carrying a
// SQWAL magic and a version byte this build rejects, asserting that premise.
func plantForeignWAL(t *testing.T, dataDir string) string {
	t.Helper()
	const plantedVersion = byte(1)
	if plantedVersion == WALVersion4 {
		t.Fatalf("fixture is out of range of its own defect: planted version %d IS WALVersion4",
			plantedVersion)
	}
	sharedDir := filepath.Join(dataDir, "wal", "shared")
	if err := os.MkdirAll(sharedDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	p := filepath.Join(sharedDir, "00000000000000000001.wal")
	if err := os.WriteFile(p, append([]byte(WALMagic), plantedVersion), 0o644); err != nil {
		t.Fatalf("plant: %v", err)
	}
	return p
}

func TestWALVersion_ForeignHeaderIsFatalAtConstruction(t *testing.T) {
	dataDir := t.TempDir()
	walPath := plantForeignWAL(t, dataDir)

	st, err := NewDisruptorStorageWithEngineConfig(dataDir, interfaces.EngineConfig{})
	if err == nil {
		if st != nil {
			_ = st.Close()
		}
		t.Fatalf("storage construction SUCCEEDED on a WAL directory holding a file this build "+
			"cannot parse (%s); every durable publish accepted afterwards is unrecoverable", walPath)
	}
	if st != nil {
		t.Fatalf("construction returned a non-nil store alongside an error")
	}
	msg := err.Error()
	for _, want := range []string{walPath, "format version", "--unsafe-recovery"} {
		if !strings.Contains(msg, want) {
			t.Errorf("construction refusal is missing %q: %s", want, msg)
		}
	}
}

func TestWALVersion_ForeignHeaderIsNeverAppendedTo(t *testing.T) {
	dataDir := t.TempDir()
	walPath := plantForeignWAL(t, dataDir)

	before, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if before.Size() != int64(len(WALMagic)+1) {
		t.Fatalf("fixture premise broken: planted file is %d bytes", before.Size())
	}

	// --unsafe-recovery must let the broker start; it must NOT make it write
	// v4 records after a foreign header. That is the self-perpetuating poison:
	// every boot deepens the damage and nothing can ever read the file again.
	st, cerr := NewDisruptorStorageWithEngineConfig(dataDir, interfaces.EngineConfig{UnsafeRecovery: true})
	if cerr != nil {
		t.Fatalf("--unsafe-recovery must permit construction, got: %v", cerr)
	}

	q := &protocol.Queue{Name: "q", Durable: true, Ordinal: 1}
	if err := st.StoreQueue(q); err != nil {
		t.Fatalf("store queue: %v", err)
	}
	for i := 0; i < 8; i++ {
		if err := st.StoreMessage("q", &protocol.Message{
			DeliveryTag:  uint64(1)<<44 | uint64(i),
			Body:         []byte(fmt.Sprintf("payload-%d", i)),
			DeliveryMode: 2,
		}); err != nil {
			t.Fatalf("store message %d: %v", i, err)
		}
	}
	_ = st.Close()

	after, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("stat after: %v", err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("the foreign-header WAL file grew from %d to %d bytes: the broker appended v4 "+
			"records after a header it cannot parse, deepening the damage on every boot",
			before.Size(), after.Size())
	}
}

// TestOpenNextFile_RefusesToAppendAfterAForeignHeader is the direct detector
// for the self-perpetuating poison, independent of rebuildBootState.
//
// openNextFile wrote the v4 header only when `size == 0`. A pre-existing file
// with a foreign 6-byte header is not empty, so no header was written and the
// broker appended v4 records after it — forever, deepening the damage on every
// boot with no operator signal. Step 1's rebuildBootState now resumes fileNum
// past the highest existing stem, which incidentally stops openNextFile ever
// TARGETING such a file; this test bypasses that so the refusal is asserted
// where the invariant actually lives rather than as a side effect of a
// different fix that a future refactor could remove.
func TestOpenNextFile_RefusesToAppendAfterAForeignHeader(t *testing.T) {
	dir := t.TempDir()
	const plantedVersion = byte(1)
	if plantedVersion == WALVersion4 {
		t.Fatalf("fixture is out of range of its own defect: planted version IS WALVersion4")
	}
	planted := filepath.Join(dir, "00000000000000000001.wal")
	if err := os.WriteFile(planted, append([]byte(WALMagic), plantedVersion), 0o644); err != nil {
		t.Fatalf("plant: %v", err)
	}
	before, err := os.Stat(planted)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}

	qw := &QueueWAL{name: "shared", dataDir: dir, cfg: DefaultWALConfig()}
	// Premise, checked: fileNum is 0, so the very next Add(1) targets exactly
	// the planted file. If that ever stops being true this test silently stops
	// testing anything.
	if qw.fileNum.Load() != 0 {
		t.Fatalf("fixture premise broken: fileNum starts at %d, so openNextFile would not "+
			"target the planted file", qw.fileNum.Load())
	}

	oerr := qw.openNextFile()
	if qw.currentFile != nil {
		_ = qw.currentFile.Close()
	}
	if qw.currentReadFile != nil {
		_ = qw.currentReadFile.Close()
	}
	if oerr == nil {
		t.Fatalf("openNextFile opened a file carrying a foreign header for O_APPEND writing; "+
			"every record written after it is unrecoverable and the damage compounds on each boot (%s)",
			planted)
	}
	if !errors.Is(oerr, ErrUnsupportedWALVersion) {
		t.Errorf("want ErrUnsupportedWALVersion, got %v", oerr)
	}
	after, err := os.Stat(planted)
	if err != nil {
		t.Fatalf("stat after: %v", err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("planted file grew from %d to %d bytes", before.Size(), after.Size())
	}
}

// --- Ra's three-way CRC split ------------------------------------------------

// walRecordSpan is one physical record's byte range inside a WAL file.
type walRecordSpan struct {
	headerAt int64 // offset of the 4-byte CRC
	dataAt   int64 // offset of the record payload
	dataLen  int
}

// walkWALRecords returns the physical span of every record in a v4 WAL file.
// It is a deliberate second implementation of the framing walk: a test that
// located records with the code under test could not detect that code being
// wrong about them.
func walkWALRecords(t *testing.T, path string) []walRecordSpan {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()

	start, err := walFileDataStart(f)
	if err != nil {
		t.Fatalf("walFileDataStart(%s): %v", path, err)
	}
	if _, err := f.Seek(start, io.SeekStart); err != nil {
		t.Fatalf("seek: %v", err)
	}

	var spans []walRecordSpan
	pos := start
	hdr := make([]byte, 8)
	for {
		if _, err := io.ReadFull(f, hdr); err != nil {
			break
		}
		crc := binary.BigEndian.Uint32(hdr[0:4])
		dataLen := int(binary.BigEndian.Uint32(hdr[4:8]))
		// A BLANK HEADER IS THE END OF THE DATA, not a run of zero-length
		// records. A sparse zero tail (a preallocated file, or a barrier tail
		// the drive has not materialised) would otherwise be read as millions
		// of zero-length records. This mirrors walBlankRecordHeader, the
		// production rule (canon rule 12).
		if crc == 0 && dataLen == 0 {
			break
		}
		if _, err := f.Seek(int64(dataLen), io.SeekCurrent); err != nil {
			break
		}
		spans = append(spans, walRecordSpan{headerAt: pos, dataAt: pos + 8, dataLen: dataLen})
		pos += 8 + int64(dataLen)
	}
	return spans
}

// setWALTailBoundForTest shrinks the R-TAIL-HOLE unconfirmed bound for a test
// and returns a restore function. It exists so the FATAL rule ("real interior
// corruption with an out-of-bound suffix") can be pinned deterministically
// without writing 256 MiB of suffix. TEST-ONLY: the bound vars are
// package-global, so a test using this must not be parallel; restore via
// t.Cleanup.
func setWALTailBoundForTest(unconfirmed, frame int64) func() {
	prevU, prevF := walMaxUnconfirmedBytes, walMaxFrameBytes
	walMaxUnconfirmedBytes, walMaxFrameBytes = unconfirmed, frame
	return func() {
		walMaxUnconfirmedBytes, walMaxFrameBytes = prevU, prevF
	}
}

// seedWAL writes n durable records through the real write path and returns the
// single WAL file they landed in.
func seedWAL(t *testing.T, dataDir string, n int) string {
	t.Helper()
	st, err := NewDisruptorStorageWithEngineConfig(dataDir, interfaces.EngineConfig{})
	if err != nil {
		t.Fatalf("seed storage: %v", err)
	}
	if err := st.StoreQueue(&protocol.Queue{Name: "q", Durable: true, Ordinal: 1}); err != nil {
		t.Fatalf("seed queue: %v", err)
	}
	for i := 0; i < n; i++ {
		if err := st.StoreMessage("q", &protocol.Message{
			DeliveryTag:  uint64(1)<<44 | uint64(i),
			Body:         []byte(fmt.Sprintf("record-%03d", i)),
			DeliveryMode: 2,
		}); err != nil {
			t.Fatalf("seed message %d: %v", i, err)
		}
	}
	_ = st.Close()

	sharedDir := filepath.Join(dataDir, "wal", "shared")
	entries, err := os.ReadDir(sharedDir)
	if err != nil {
		t.Fatalf("read shared dir: %v", err)
	}
	var found []string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == WALFileExtension {
			found = append(found, filepath.Join(sharedDir, e.Name()))
		}
	}
	if len(found) != 1 {
		t.Fatalf("fixture premise broken: expected exactly one WAL file, got %d (%v); a roll "+
			"would put the corrupted record in a different file from the one under test",
			len(found), found)
	}
	return found[0]
}

// corruptRecordPayload flips a bit inside record `idx`'s payload and asserts
// the CRC really does now mismatch — otherwise the test would be measuring
// nothing.
func corruptRecordPayload(t *testing.T, path string, idx int) {
	t.Helper()
	spans := walkWALRecords(t, path)
	if idx < 0 || idx >= len(spans) {
		t.Fatalf("record index %d out of range (file holds %d records)", idx, len(spans))
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open rw: %v", err)
	}
	defer f.Close()

	sp := spans[idx]
	one := make([]byte, 1)
	if _, err := f.ReadAt(one, sp.dataAt+int64(sp.dataLen)/2); err != nil {
		t.Fatalf("read payload byte: %v", err)
	}
	one[0] ^= 0x01
	if _, err := f.WriteAt(one, sp.dataAt+int64(sp.dataLen)/2); err != nil {
		t.Fatalf("write payload byte: %v", err)
	}

	// Premise, checked: this record's stored CRC no longer matches its bytes.
	hdr := make([]byte, 8)
	if _, err := f.ReadAt(hdr, sp.headerAt); err != nil {
		t.Fatalf("re-read header: %v", err)
	}
	stored := binary.BigEndian.Uint32(hdr[0:4])
	if stored == 0 {
		t.Fatalf("fixture is out of range of its own defect: record %d was written with CRC "+
			"disabled (stored CRC is 0), so the scanner skips verification entirely", idx)
	}
	data := make([]byte, sp.dataLen)
	if _, err := f.ReadAt(data, sp.dataAt); err != nil {
		t.Fatalf("re-read data: %v", err)
	}
	h := crc32.NewIEEE()
	h.Write(hdr[4:8])
	h.Write(data)
	if h.Sum32() == stored {
		t.Fatalf("fixture is out of range of its own defect: after the bit flip the CRC of "+
			"record %d STILL matches", idx)
	}
}

func TestWALCRC_CleanFileIsBenignControl(t *testing.T) {
	dataDir := t.TempDir()
	path := seedWAL(t, dataDir, 5)
	if got := len(walkWALRecords(t, path)); got != 5 {
		t.Fatalf("control premise broken: expected 5 physical records, walked %d", got)
	}

	st, err := NewDisruptorStorageWithEngineConfig(dataDir, interfaces.EngineConfig{})
	if err != nil {
		t.Fatalf("uncorrupted directory must construct: %v", err)
	}
	defer st.Close()
	msgs, rerr := st.GetRecoverableMessages()
	if rerr != nil {
		t.Fatalf("uncorrupted directory must recover cleanly, got: %v", rerr)
	}
	if len(msgs["q"]) != 5 {
		t.Fatalf("control: expected 5 recoverable records, got %d", len(msgs["q"]))
	}
}

func TestWALCRC_InteriorFailureIsFatal(t *testing.T) {
	// R-TAIL-HOLE SUPERSEDES the unconditional form of this assertion: on one
	// of the two newest WAL files a bounded non-zero suffix after a CRC failure
	// is a benign unconfirmed tail (replay stops at the defect). seedWAL
	// produces a single file, which IS the newest, so the bound is shrunk to
	// zero here to keep pinning the FATAL rule for a suffix that is out of
	// bound. The benign arm for a newest file is pinned by
	// TestWALTail_ZeroFilledTailBoundaryShapes and TestWALTail_BoundedHole*.
	t.Cleanup(setWALTailBoundForTest(0, 0))

	dataDir := t.TempDir()
	path := seedWAL(t, dataDir, 5)
	spans := walkWALRecords(t, path)
	if len(spans) != 5 {
		t.Fatalf("fixture premise broken: expected 5 records, walked %d", len(spans))
	}
	const interior = 2
	if interior >= len(spans)-1 {
		t.Fatalf("fixture is out of range of its own defect: record %d is the LAST record, so "+
			"the tail rule would apply and the interior rule would never be reached", interior)
	}
	corruptRecordPayload(t, path, interior)

	st, err := NewDisruptorStorageWithEngineConfig(dataDir, interfaces.EngineConfig{})
	if err == nil {
		defer st.Close()
		_, rerr := st.GetRecoverableMessages()
		if rerr == nil {
			t.Fatalf("an INTERIOR CRC failure was silently skipped: recovery reported success "+
				"while dropping a confirmed durable record from %s", path)
		}
		if interfaces.OutcomeOf(rerr) != interfaces.RecoveryFatal {
			t.Fatalf("interior CRC failure classified %v, want RecoveryFatal: %v",
				interfaces.OutcomeOf(rerr), rerr)
		}
		return
	}
	if interfaces.OutcomeOf(err) != interfaces.RecoveryFatal {
		t.Fatalf("interior CRC failure classified %v, want RecoveryFatal: %v",
			interfaces.OutcomeOf(err), err)
	}
}

// TestWALCRC_TailFailureIsBenignNotFatal.
//
// CANON RULE 1 RECORD — this test previously asserted RecoveryDegraded, which
// GATES the boot, and the assertion was changed rather than softened. The
// written justification, ruled by step3-fix's mandate on review-3's H-1:
//
//   - A torn trailing record was never fsynced, therefore never confirmed,
//     therefore no durability promise is broken by dropping it. Degraded means
//     "data that WAS confirmed has been abandoned", which is false here.
//   - No broker in the reference study asks for an operator flag on a torn
//     tail: Ra drops the last record and continues, Kafka truncates and WARNs
//     with no config, NATS truncates and reports, Osiris/Artemis/CQv2 truncate
//     (ref-rabbitmq.md §1.6, and §0: "Nobody defaults to refuse-on-corruption").
//   - Requiring --unsafe-recovery after a power loss makes the safe path the
//     annoying one, which is how operators end up running with the data-loss
//     flag permanently set — the exact failure mode Step 3 exists to prevent.
//
// The assertions are STRICTLY STRONGER than the ones they replace: the file
// must recover WITHOUT --unsafe-recovery (it previously needed the flag), the
// fault must still be produced and named by path (a benign fault is filtered
// out of the package's error returns, so it is checked at the site), and the
// four intact records must still come back. A "fix" that made a torn tail
// silent, fatal, or file-discarding fails it.
func TestWALCRC_TailFailureIsBenignNotFatal(t *testing.T) {
	dataDir := t.TempDir()
	path := seedWAL(t, dataDir, 5)
	spans := walkWALRecords(t, path)
	last := len(spans) - 1
	if last != 4 {
		t.Fatalf("fixture premise broken: expected 5 records, walked %d", len(spans))
	}
	corruptRecordPayload(t, path, last)

	// The fault is still PRODUCED, still classified, and still names the file.
	// joinFaults deliberately withholds benign faults from the package's error
	// returns, so this is asserted where the classification is made.
	qw := &QueueWAL{name: "shared", dataDir: filepath.Dir(path), cfg: DefaultWALConfig()}
	msgs, serr := qw.scanWALFile(path)
	if serr == nil {
		t.Fatalf("a torn-tail CRC failure was silently swallowed; nothing reported it")
	}
	faults := interfaces.FaultsOf(serr)
	if len(faults) != 1 {
		t.Fatalf("want exactly one classified fault, got %d: %v", len(faults), serr)
	}
	if faults[0].Outcome != interfaces.RecoveryBenign {
		t.Fatalf("tail CRC failure classified %v, want RecoveryBenign: %v", faults[0].Outcome, serr)
	}
	if faults[0].Artifact != path {
		t.Fatalf("fault names %q, not the file under test %q", faults[0].Artifact, path)
	}
	if len(msgs) != 4 {
		t.Fatalf("scanWALFile kept %d of the 4 records that precede the torn tail", len(msgs))
	}

	// And the boot is NOT gated: no flag, four records back.
	st, err := NewDisruptorStorageWithEngineConfig(dataDir, interfaces.EngineConfig{})
	if err != nil {
		t.Fatalf("a torn tail must not require --unsafe-recovery: %v", err)
	}
	defer st.Close()
	got, rerr := st.GetRecoverableMessages()
	if rerr != nil {
		t.Fatalf("a torn tail must not gate the boot, got %v: %v", interfaces.OutcomeOf(rerr), rerr)
	}
	if len(got["q"]) != 4 {
		t.Fatalf("expected the 4 records before the torn tail to survive, got %d", len(got["q"]))
	}
}

// TestStoreQueue_UnreadableExistingRecordFailsClosed pins recon-recovery Item 3.
//
// existingOrdinalLocked returned 0 for BOTH "no record" and "record present but
// unreadable". StoreQueue then wrote a NEW ordinal over the unreadable record,
// and at the next recovery every one of that queue's confirmed durable records
// took the Case B "dead incarnation" branch and was silently discarded.
func TestStoreQueue_UnreadableExistingRecordFailsClosed(t *testing.T) {
	dir := t.TempDir()
	pm, err := NewPersistentMetadataStore(dir)
	if err != nil {
		t.Fatalf("metadata store: %v", err)
	}

	const name = "victim"
	if err := pm.StoreQueue(&protocol.Queue{Name: name, Durable: true, Ordinal: 7}); err != nil {
		t.Fatalf("seed store: %v", err)
	}

	// Corrupt the record on disk and evict it from the cache, which is exactly
	// what loadCacheFromDisk's `if err == nil` skip produces at boot.
	path := filepath.Join(dir, MetadataDir, QueuesDir, name+FileExtension)
	if err := os.WriteFile(path, []byte{0xff, 0xff, 0xff, 0xff}, 0o644); err != nil {
		t.Fatalf("corrupt record: %v", err)
	}
	pm.queueCache.Delete(name)

	// Premise, checked: the record is present-but-unreadable, not absent.
	if _, serr := os.Stat(path); serr != nil {
		t.Fatalf("fixture premise broken: record must still EXIST to distinguish "+
			"'unreadable' from 'absent': %v", serr)
	}

	fresh := &protocol.Queue{Name: name, Durable: true, Ordinal: 99}
	err = pm.StoreQueue(fresh)
	if err == nil {
		t.Fatalf("StoreQueue silently reassigned ordinal %d over an unreadable existing record; "+
			"every confirmed durable record under the old ordinal is now a 'dead incarnation' "+
			"and is discarded at the next recovery", fresh.Ordinal)
	}
	if !errors.Is(err, ErrQueueRecordUnreadable) {
		t.Errorf("want ErrQueueRecordUnreadable, got %v", err)
	}
}
