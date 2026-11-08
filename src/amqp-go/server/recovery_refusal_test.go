package server

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
)

// Step 3 — the recovery default is inverted: a data directory whose recovery
// cannot be completed safely REFUSES the boot, and --unsafe-recovery is the
// only way past it.
//
// Before this step server/builder.go refused on exactly two sentinel errors
// (ErrLegacyDataDirectory, ErrOrdinalMismatch) and every other recovery error
// logged "Recovery failed" and fell through to `return server, nil`. The
// measured consequence: recovery aborts on `unsupported WAL file format
// version: got 1, want 4`, a durable StoreMessage then returns OK (which is
// where the publisher confirm is sent), the bytes land on disk, and the next
// boot recovers zero.

// foreignWALFixture plants a WAL file this build cannot parse and returns the
// data directory plus the absolute path of the planted file.
//
// It asserts its own premise (canon rule 11) rather than documenting it: the
// planted version byte must genuinely not be the version this build accepts,
// otherwise the fixture is out of range of the defect it exists to trip and
// every test built on it is vacuous.
func foreignWALFixture(t *testing.T) (dataDir string, walPath string) {
	t.Helper()

	const plantedVersion = byte(1)
	if plantedVersion == storage.WALVersion4 {
		t.Fatalf("fixture is out of range of its own defect: planted WAL version byte %d "+
			"IS this build's accepted version (storage.WALVersion4=%d), so walFileDataStart "+
			"would accept the file and nothing under test would ever be reached",
			plantedVersion, storage.WALVersion4)
	}

	dataDir = t.TempDir()
	sharedDir := filepath.Join(dataDir, "wal", "shared")
	if err := os.MkdirAll(sharedDir, 0o755); err != nil {
		t.Fatalf("mkdir shared WAL dir: %v", err)
	}
	walPath = filepath.Join(sharedDir, "00000000000000000001.wal")
	body := append([]byte(storage.WALMagic), plantedVersion)
	if err := os.WriteFile(walPath, body, 0o644); err != nil {
		t.Fatalf("plant foreign WAL file: %v", err)
	}

	// Premise, checked: the bytes on disk are exactly a SQWAL header carrying a
	// version this build rejects. A short/garbled write would be parsed as an
	// empty file instead and the test would pass for the wrong reason.
	got, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read back planted WAL file: %v", err)
	}
	if len(got) != len(storage.WALMagic)+1 ||
		string(got[:len(storage.WALMagic)]) != storage.WALMagic ||
		got[len(storage.WALMagic)] != plantedVersion {
		t.Fatalf("planted WAL file is not a foreign-version header: %q", got)
	}
	return dataDir, walPath
}

func refusalTestConfig(t *testing.T, dataDir string) *config.AMQPConfig {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Storage.Path = dataDir
	cfg.Server.LogLevel = "error"
	return cfg
}

// TestRecovery_ForeignWALVersionRefusesToBoot is the headline assertion of
// Step 3: the boot fails instead of silently starting empty.
func TestRecovery_ForeignWALVersionRefusesToBoot(t *testing.T) {
	dataDir, walPath := foreignWALFixture(t)

	srv, err := NewServerBuilderWithConfig(refusalTestConfig(t, dataDir)).Build()
	if err == nil {
		if srv != nil {
			t.Fatalf("Build() SILENTLY SUCCEEDED on a data directory whose WAL carries an "+
				"unparseable framing version (%s); this broker would now accept and confirm "+
				"durable publishes it can never recover", walPath)
		}
		t.Fatalf("Build() returned (nil, nil)")
	}
	if srv != nil {
		t.Fatalf("Build() returned a non-nil server alongside an error: %v", err)
	}

	// A refusal that does not say how to proceed is an outage with no exit.
	// These four parts are the feature, and they regress like one.
	msg := err.Error()
	for _, want := range []string{
		walPath,             // WHICH file
		"format version",    // WHAT happened
		"--unsafe-recovery", // HOW to proceed
		"discard",           // what proceeding COSTS
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal message is missing the %q part an operator needs.\nmessage: %s", want, msg)
		}
	}
}

// TestRecovery_UnsafeRecoveryBootsAndNamesEveryDiscardedArtifact asserts the
// escape hatch exists, works, and is loud — the boot proceeds, and the log
// carries the discarded artifact BY PATH rather than a single generic line.
func TestRecovery_UnsafeRecoveryBootsAndNamesEveryDiscardedArtifact(t *testing.T) {
	dataDir, walPath := foreignWALFixture(t)

	logPath := filepath.Join(t.TempDir(), "boot.log")
	cfg := refusalTestConfig(t, dataDir)
	cfg.Storage.UnsafeRecovery = true
	cfg.Server.LogLevel = "info"
	cfg.Server.LogFile = logPath

	srv, err := NewServerBuilderWithConfig(cfg).Build()
	if err != nil {
		t.Fatalf("--unsafe-recovery must let the broker boot, got: %v", err)
	}
	if srv == nil {
		t.Fatalf("Build() returned (nil, nil) with --unsafe-recovery set")
	}

	// The in-process form of the pinned metric: every discarded artifact is
	// enumerable for the lifetime of the process, not just at boot.
	faults := srv.UnsafeRecoveryFaults()
	if len(faults) == 0 {
		t.Fatalf("the broker booted degraded but reports no discarded artifacts; nothing can " +
			"tell an operator, a dashboard or a later reader that this process is running on " +
			"data it could not recover")
	}
	var named bool
	for _, f := range faults {
		if f.Artifact == walPath {
			named = true
		}
		if !f.Outcome.GatesBoot() {
			t.Errorf("a benign fault was recorded as a discarded artifact: %v", f)
		}
	}
	if !named {
		t.Errorf("UnsafeRecoveryFaults() does not name %s; artifacts must be identified by path", walPath)
	}

	raw, rerr := os.ReadFile(logPath)
	if rerr != nil {
		t.Fatalf("reading the broker's own log file %s: %v", logPath, rerr)
	}
	logged := string(raw)
	if strings.TrimSpace(logged) == "" {
		t.Fatalf("broker log at %s is empty; this test cannot observe anything", logPath)
	}
	for _, want := range []string{
		walPath,           // every discarded artifact, BY PATH
		"unsafe-recovery", // and why it was discarded rather than recovered
	} {
		if !strings.Contains(logged, want) {
			t.Errorf("boot log does not contain %q; a degraded boot must name what it dropped.\nlog:\n%s", want, logged)
		}
	}
}

// TestRecovery_UnreadableQueueRecordRefusesToBoot covers the genuinely
// dangerous half of recovery Case C.
//
// "No metadata record for this queue name" was INFERRED to mean "the queue was
// deleted", and every one of that queue's confirmed durable records was
// discarded at Warn level with a successful boot reported. Absence is what
// queue.delete produces, so that inference is right for a delete — but a
// metadata record that is PRESENT AND UNREADABLE produces the same absence:
// ListQueues skips it (`continue // Skip corrupted files`) and GetQueue fails
// on it. The two demand opposite answers, and the broker could not tell them
// apart. It can now, and the unreadable case is fatal.
//
// The DELETED case is deliberately NOT fatal — see step3.md. Two existing
// tests assert it must still boot, and they are right: DeleteQueue never
// purges the shared WAL, so leftover records are the norm after deleting a
// durable queue with a backlog.
func TestRecovery_UnreadableQueueRecordRefusesToBoot(t *testing.T) {
	dataDir := t.TempDir()

	// Boot 1: a real durable queue with a real confirmed durable message.
	cfg := refusalTestConfig(t, dataDir)
	st, err := storage.NewDisruptorStorageWithEngineConfig(dataDir, cfg.GetEngine())
	if err != nil {
		t.Fatalf("boot 1 storage: %v", err)
	}
	b := broker.NewStorageBroker(st, cfg.GetEngine())
	if _, err := b.DeclareQueue("orphan", true, false, false, nil); err != nil {
		t.Fatalf("declare: %v", err)
	}
	if err := b.PublishMessage("", "orphan", &protocol.Message{
		Body:         []byte("confirmed"),
		DeliveryMode: 2,
		Exchange:     "",
		RoutingKey:   "orphan",
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	b.Close()
	if err := st.Close(); err != nil {
		t.Fatalf("boot 1 close: %v", err)
	}

	// Corrupt (do NOT remove) the metadata record. Removing it is a delete;
	// corrupting it is a lost record, and only the second is a defect.
	metaPath := filepath.Join(dataDir, "metadata", "queues", "orphan.cbor")
	if _, serr := os.Stat(metaPath); serr != nil {
		t.Fatalf("fixture premise broken: expected a queue metadata record at %s: %v", metaPath, serr)
	}
	if err := os.WriteFile(metaPath, []byte{0xff, 0xff, 0xff, 0xff}, 0o644); err != nil {
		t.Fatalf("corrupt metadata record: %v", err)
	}

	// Premise, checked: the WAL record really is still physically present, so
	// recovery has something to be wrong about.
	sharedDir := filepath.Join(dataDir, "wal", "shared")
	entries, derr := os.ReadDir(sharedDir)
	if derr != nil {
		t.Fatalf("read shared WAL dir: %v", derr)
	}
	var walBytes int64
	for _, e := range entries {
		if info, ierr := e.Info(); ierr == nil && filepath.Ext(e.Name()) == ".wal" {
			walBytes += info.Size()
		}
	}
	if walBytes <= int64(len(storage.WALMagic)+1) {
		t.Fatalf("fixture is out of range of its own defect: the shared WAL holds %d bytes, "+
			"i.e. header only — no durable record survived boot 1, so nothing can be misattributed", walBytes)
	}
	// Premise, checked: the record is PRESENT, not absent. If it were absent
	// this test would be asserting the delete case, which must boot.
	if _, serr := os.Stat(metaPath); serr != nil {
		t.Fatalf("fixture premise broken: the metadata record must still EXIST: %v", serr)
	}

	// Boot 2 must refuse.
	srv, err := NewServerBuilderWithConfig(refusalTestConfig(t, dataDir)).Build()
	if err == nil {
		t.Fatalf("Build() succeeded while silently discarding confirmed durable records for a "+
			"queue whose metadata record is present but unreadable (srv=%v)", srv != nil)
	}
	if !strings.Contains(err.Error(), "orphan") {
		t.Errorf("refusal message does not name the queue whose records were at risk: %v", err)
	}
	if !strings.Contains(err.Error(), "--unsafe-recovery") {
		t.Errorf("refusal message does not tell the operator how to proceed: %v", err)
	}
}

// TestRecovery_DeletedQueueLeftoversStillBoot is the CONTROL for the test
// above, and the reason Case C is not blanket-fatal. Deleting a durable queue
// that still has a backlog leaves its records in the shared WAL forever
// (DeleteQueue tears down the ring, never the WAL). If that refused the boot,
// an ordinary AMQP operation would brick the next restart.
func TestRecovery_DeletedQueueLeftoversStillBoot(t *testing.T) {
	dataDir := t.TempDir()

	cfg := refusalTestConfig(t, dataDir)
	st, err := storage.NewDisruptorStorageWithEngineConfig(dataDir, cfg.GetEngine())
	if err != nil {
		t.Fatalf("boot 1 storage: %v", err)
	}
	b := broker.NewStorageBroker(st, cfg.GetEngine())
	if _, err := b.DeclareQueue("gone", true, false, false, nil); err != nil {
		t.Fatalf("declare: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := b.PublishMessage("", "gone", &protocol.Message{
			Body:         []byte("confirmed"),
			DeliveryMode: 2,
			Exchange:     "",
			RoutingKey:   "gone",
		}); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	if _, err := b.DeleteQueue("gone", false, false); err != nil {
		t.Fatalf("delete: %v", err)
	}
	b.Close()
	if err := st.Close(); err != nil {
		t.Fatalf("boot 1 close: %v", err)
	}

	// Premise, checked: the delete really did leave durable records behind and
	// really did remove the metadata record — otherwise this control is out of
	// range of the case it controls for.
	metaPath := filepath.Join(dataDir, "metadata", "queues", "gone.cbor")
	if _, serr := os.Stat(metaPath); serr == nil {
		t.Fatalf("fixture premise broken: the metadata record still exists after DeleteQueue")
	}
	sharedDir := filepath.Join(dataDir, "wal", "shared")
	entries, derr := os.ReadDir(sharedDir)
	if derr != nil {
		t.Fatalf("read shared WAL dir: %v", derr)
	}
	var walBytes int64
	for _, e := range entries {
		if info, ierr := e.Info(); ierr == nil && filepath.Ext(e.Name()) == ".wal" {
			walBytes += info.Size()
		}
	}
	if walBytes <= int64(len(storage.WALMagic)+1) {
		t.Fatalf("fixture is out of range of its own defect: the shared WAL holds only %d bytes, "+
			"so no leftover record survived the delete and this control proves nothing", walBytes)
	}

	srv, err := NewServerBuilderWithConfig(refusalTestConfig(t, dataDir)).Build()
	if err != nil {
		t.Fatalf("deleting a durable queue with a backlog and restarting must still boot; got: %v", err)
	}
	if srv == nil {
		t.Fatalf("Build() returned (nil, nil)")
	}
}

// TestRecovery_LegacyOrdinalZeroRecordStillRefuses is a REGRESSION GUARD, not a
// new behaviour: ErrLegacyDataDirectory was already fatal before Step 3 and
// must stay fatal after the classifier replaces the two-sentinel allow-list.
// It is the control that proves the rewrite did not lose the cases the
// allow-list did get right.
func TestRecovery_LegacyOrdinalZeroRecordStillRefuses(t *testing.T) {
	dataDir := t.TempDir()

	cfg := refusalTestConfig(t, dataDir)
	st, err := storage.NewDisruptorStorageWithEngineConfig(dataDir, cfg.GetEngine())
	if err != nil {
		t.Fatalf("boot 1 storage: %v", err)
	}
	b := broker.NewStorageBroker(st, cfg.GetEngine())
	if _, err := b.DeclareQueue("legacy", true, false, false, nil); err != nil {
		t.Fatalf("declare: %v", err)
	}
	if err := b.PublishMessage("", "legacy", &protocol.Message{
		Body:         []byte("confirmed"),
		DeliveryMode: 2,
		Exchange:     "",
		RoutingKey:   "legacy",
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	b.Close()
	if err := st.Close(); err != nil {
		t.Fatalf("boot 1 close: %v", err)
	}

	// Rewrite the metadata record with Ordinal==0 — a pre-packing directory.
	metaPath := filepath.Join(dataDir, "metadata", "queues", "legacy.cbor")
	raw, rerr := os.ReadFile(metaPath)
	if rerr != nil {
		t.Fatalf("read queue record: %v", rerr)
	}
	var q protocol.Queue
	if err := cbor.Unmarshal(raw, &q); err != nil {
		t.Fatalf("decode queue record: %v", err)
	}
	if q.Ordinal == 0 {
		t.Fatalf("fixture is out of range of its own defect: the queue record already carries " +
			"Ordinal==0 before the test rewrites it, so the rewrite proves nothing")
	}
	q.Ordinal = 0
	out, merr := cbor.Marshal(&q)
	if merr != nil {
		t.Fatalf("encode queue record: %v", merr)
	}
	if err := os.WriteFile(metaPath, out, 0o644); err != nil {
		t.Fatalf("rewrite queue record: %v", err)
	}

	srv, err := NewServerBuilderWithConfig(refusalTestConfig(t, dataDir)).Build()
	if err == nil {
		t.Fatalf("Build() succeeded on a legacy (Ordinal==0) data directory holding confirmed "+
			"durable records (srv=%v)", srv != nil)
	}
	if !strings.Contains(err.Error(), "--unsafe-recovery") {
		t.Errorf("refusal message does not tell the operator how to proceed: %v", err)
	}
}

// --- Step 3-fix: review-3 B-1, the six-file fixture through the real Build() --

// walRecordCount walks a v4 WAL file's framing and counts its physical records.
// It is a third, deliberately independent implementation of the walk (the
// storage package has its own test walker): a test that asked the code under
// test how much its own quarantine cost could not detect that code being wrong.
//
// A BLANK RECORD HEADER (CRC == 0 AND length == 0) ENDS THE DATA, it is not a
// run of zero-length records. This is the production rule
// (storage.walBlankRecordHeader, canon rule 12) and it is load-bearing here: a
// still-open WAL file can carry a sparse zero tail, so a whole-file read walks
// the entire zero tail counting it as millions of records. Before this stop the
// seeding loop below read a live file's zero tail, concluded the just-created
// last file already held enough records, and broke BEFORE writing any record
// into it — leaving an empty file that the post-Close premise rejected.
func walRecordCount(t *testing.T, path string) int {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()

	hdrLen := len(storage.WALMagic) + 1
	hdr := make([]byte, hdrLen)
	if _, err := io.ReadFull(f, hdr); err != nil {
		return 0
	}
	if string(hdr[:len(storage.WALMagic)]) != storage.WALMagic {
		return 0
	}

	n := 0
	rec := make([]byte, 8)
	for {
		if _, err := io.ReadFull(f, rec); err != nil {
			break
		}
		crc := binary.BigEndian.Uint32(rec[0:4])
		dataLen := int(binary.BigEndian.Uint32(rec[4:8]))
		if crc == 0 && dataLen == 0 {
			break // blank header: end of data (sparse zero tail)
		}
		if _, err := f.Seek(int64(dataLen), io.SeekCurrent); err != nil {
			break
		}
		n++
	}
	return n
}

// seedSixFileWAL builds a data directory holding exactly `wantFiles` shared-WAL
// files of confirmed durable records for one durable queue, plus that queue's
// metadata record. Seeded through storage.WALManager rather than
// DisruptorStorage because the latter's Close() runs a final checkpoint that
// unlinks every rolled WAL file.
func seedSixFileWAL(t *testing.T, dataDir string, wantFiles int) []string {
	t.Helper()

	// W8 pre-creates the next WAL file off the commit path; this fixture counts
	// the .wal files on disk EXACTLY, so pin the inline-roll path (no
	// background pre-open) for determinism.
	t.Cleanup(storage.SetWALPreopenEnabledForTest(false))

	pm, err := storage.NewPersistentMetadataStore(dataDir)
	if err != nil {
		t.Fatalf("metadata store: %v", err)
	}
	if err := pm.StoreQueue(&protocol.Queue{Name: "q", Durable: true, Ordinal: 1}); err != nil {
		t.Fatalf("store queue record: %v", err)
	}

	cfg := storage.DefaultWALConfig()
	cfg.FileSize = 2048
	wm, err := storage.NewWALManagerWithConfig(dataDir, cfg)
	if err != nil {
		t.Fatalf("seed WAL manager: %v", err)
	}
	sharedDir := filepath.Join(dataDir, "wal", "shared")
	for i := 1; i <= 5000; i++ {
		// The WAL persists the OFFSET as the record's identity and recovery
		// reads the delivery tag back out of it, so the offset must be the
		// composite tag under the queue's ordinal (1) or every record recovers
		// as a dead incarnation and messages_recovered is 0 for a reason that
		// has nothing to do with the defect under test.
		tag := uint64(1)<<44 | uint64(i)
		if err := wm.Write("q", &protocol.Message{
			DeliveryTag:  tag,
			Body:         []byte(strings.Repeat("x", 40)),
			DeliveryMode: 2,
		}, tag); err != nil {
			t.Fatalf("seed message %d: %v", i, err)
		}
		if len(walFilesInDir(t, sharedDir)) == wantFiles &&
			walRecordCount(t, walFilesInDir(t, sharedDir)[wantFiles-1]) >= 3 {
			break
		}
	}
	_ = wm.Close()

	files := walFilesInDir(t, sharedDir)
	if len(files) != wantFiles {
		t.Fatalf("fixture premise broken: wanted exactly %d WAL files, got %d", wantFiles, len(files))
	}
	for _, f := range files {
		if walRecordCount(t, f) == 0 {
			t.Fatalf("fixture premise broken: %s holds 0 records", f)
		}
	}
	return files
}

func walFilesInDir(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".wal" {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(out)
	return out
}

// TestRecovery_UnsafeRecoveryConfinesLossToTheNamedFile is review-3's B-1
// acceptance test, driven through the real ServerBuilder.Build().
//
// MEASURED against the real amqp-server binary before this fix: six WAL files,
// five intact holding 271,080 confirmed durable messages, one damaged;
// `--unsafe-recovery` boot reported `"messages_recovered":0` and
// `"discarded_artifacts":1` while telling the operator that "every durable
// message in THIS WAL file is abandoned". Both claims were false: one 6-byte
// file was listed, five healthy 59 MB files were discarded.
//
// The assertion is the COUNT, not that the broker booted.
func TestRecovery_UnsafeRecoveryConfinesLossToTheNamedFile(t *testing.T) {
	dataDir := t.TempDir()
	files := seedSixFileWAL(t, dataDir, 6)

	const damaged = 3 // file 4 of 6, exactly review-3's producer
	survivors := 0
	for i, f := range files {
		if i == damaged {
			continue
		}
		survivors += walRecordCount(t, f)
	}
	if survivors == 0 {
		t.Fatalf("fixture premise broken: the five undamaged files hold 0 records")
	}
	if err := os.WriteFile(files[damaged], append([]byte(storage.WALMagic), byte(1)), 0o644); err != nil {
		t.Fatalf("plant foreign header: %v", err)
	}

	// Without the flag the boot is still refused, and the refusal names the
	// damaged file and only the damaged file.
	if srv, err := NewServerBuilderWithConfig(refusalTestConfig(t, dataDir)).Build(); err == nil {
		t.Fatalf("Build() succeeded on a damaged WAL directory without --unsafe-recovery (srv=%v)", srv != nil)
	} else {
		if !strings.Contains(err.Error(), files[damaged]) {
			t.Errorf("refusal does not name the damaged file %s: %v", files[damaged], err)
		}
		for i, f := range files {
			if i == damaged {
				continue
			}
			if strings.Contains(err.Error(), f) {
				t.Errorf("refusal names an UNDAMAGED file %s as an abandoned artifact: %v", f, err)
			}
		}
	}

	// With the flag, the five undamaged files' confirmed durable messages must
	// still be recovered.
	logPath := filepath.Join(t.TempDir(), "boot.log")
	cfg := refusalTestConfig(t, dataDir)
	cfg.Storage.UnsafeRecovery = true
	cfg.Server.LogLevel = "info"
	cfg.Server.LogFile = logPath

	srv, err := NewServerBuilderWithConfig(cfg).Build()
	if err != nil {
		t.Fatalf("--unsafe-recovery must let the broker boot: %v", err)
	}
	if srv == nil {
		t.Fatalf("Build() returned (nil, nil)")
	}
	raw, rerr := os.ReadFile(logPath)
	if rerr != nil {
		t.Fatalf("read boot log: %v", rerr)
	}
	// review-3 H-2: the same damaged file is raised TWICE — once by
	// initOrdinalAllocator (which reads GetRecoverableMessages) and once by
	// PerformRecovery. Both are now marked and measured, and the artifact is
	// counted ONCE. A gauge reading 2 for one discarded file is a cost
	// statement that overstates, which is exactly as wrong as one that
	// understates.
	marked := srv.UnsafeRecoveryFaults()
	if len(marked) != 1 || marked[0].Artifact != files[damaged] {
		got := make([]string, 0, len(marked))
		for _, f := range marked {
			got = append(got, f.Artifact)
		}
		t.Fatalf("UnsafeRecoveryFaults() = %v, want exactly [%s]", got, files[damaged])
	}

	want := fmt.Sprintf("\"messages_recovered\":%d", survivors)
	if !strings.Contains(string(raw), want) {
		t.Fatalf("QUARANTINE FAILED: boot log does not report %s. A fatal fault on ONE file "+
			"abandoned the whole directory while the operator-facing cost statement named that one "+
			"file.\nlog:\n%s", want, raw)
	}
}

// TestRecovery_DurableBindingsSurviveRestartWithoutBindingRecovery is the guard
// that makes deleting RecoveryManager.recoverBindings and
// protocol.DurableEntityMetadata.Bindings safe (canon rule 6).
//
// The deleted code never ran: GetDurableEntityMetadata initialised Bindings to
// an empty slice and never appended, so the loop body executed zero times.
// Durable bindings nonetheless survive a restart, because the ROUTING path
// re-reads them from storage on every publish rather than from the in-memory
// exchange. Wiring the dead loop would have created a second source of truth
// for a fact that is already correct; this test pins the mechanism that makes
// it correct, so a future change to the routing path cannot silently remove
// binding durability now that nothing else re-establishes it.
func TestRecovery_DurableBindingsSurviveRestartWithoutBindingRecovery(t *testing.T) {
	dataDir := t.TempDir()
	cfg := refusalTestConfig(t, dataDir)

	st1, err := storage.NewDisruptorStorageWithEngineConfig(dataDir, cfg.GetEngine())
	if err != nil {
		t.Fatalf("boot 1 storage: %v", err)
	}
	b1 := broker.NewStorageBroker(st1, cfg.GetEngine())
	if err := b1.DeclareExchange("bex", "direct", true, false, false, nil); err != nil {
		t.Fatalf("declare exchange: %v", err)
	}
	if _, err := b1.DeclareQueue("bq", true, false, false, nil); err != nil {
		t.Fatalf("declare queue: %v", err)
	}
	if err := b1.BindQueue("bq", "bex", "bk", nil); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if err := b1.PublishMessage("bex", "bk", &protocol.Message{
		Body: []byte("before-restart"), DeliveryMode: 2, Exchange: "bex", RoutingKey: "bk",
	}); err != nil {
		t.Fatalf("publish 1: %v", err)
	}
	// Premise, checked: the binding really did route before the restart.
	if got := b1.GetQueueReadyCount("bq"); got != 1 {
		t.Fatalf("fixture premise broken: %d ready before restart, want 1", got)
	}
	b1.Close()
	if err := st1.Close(); err != nil {
		t.Fatalf("boot 1 close: %v", err)
	}

	// Boot 2 through the real builder, then publish WITHOUT redeclaring
	// anything. Nothing in recovery re-establishes the binding.
	srv, err := NewServerBuilderWithConfig(refusalTestConfig(t, dataDir)).Build()
	if err != nil {
		t.Fatalf("boot 2: %v", err)
	}
	if srv == nil {
		t.Fatalf("Build() returned (nil, nil)")
	}
	if err := srv.Broker.PublishMessage("bex", "bk", &protocol.Message{
		Body: []byte("after-restart"), DeliveryMode: 2, Exchange: "bex", RoutingKey: "bk",
	}); err != nil {
		t.Fatalf("publish 2: %v", err)
	}
	// Drain the queue: the pre-restart message (recovered) and the post-restart
	// one (which can only be there if the binding still routes).
	var bodies []string
	for i := 0; i < 4; i++ {
		msg, _, _, gerr := srv.Broker.GetMessageForGet("bq", true)
		if gerr != nil {
			t.Fatalf("basic.get: %v", gerr)
		}
		if msg == nil {
			break
		}
		bodies = append(bodies, string(msg.Body))
	}
	var routed bool
	for _, b := range bodies {
		if b == "after-restart" {
			routed = true
		}
	}
	if !routed {
		t.Fatalf("a durable binding did NOT survive the restart: publishing to bex/bk with no "+
			"redeclare produced %v. Binding durability rests entirely on the routing path "+
			"re-reading bindings from storage; nothing re-establishes them at boot", bodies)
	}
}

// TestRecovery_BindingDurabilityIsTheStorageRecord is the NEGATIVE CONTROL for
// the test above, and it is what gives that test power in both directions.
//
// Without it, "the message routed after a restart" could be true for a reason
// that has nothing to do with the on-disk binding record — and the obvious
// mutation (making the metadata store stop serving bindings) kills the fixture
// at its own pre-restart premise check rather than isolating the post-restart
// assertion. Removing the binding RECORD between boots isolates it exactly:
// the same publish must now fail to route.
func TestRecovery_BindingDurabilityIsTheStorageRecord(t *testing.T) {
	dataDir := t.TempDir()
	cfg := refusalTestConfig(t, dataDir)

	st1, err := storage.NewDisruptorStorageWithEngineConfig(dataDir, cfg.GetEngine())
	if err != nil {
		t.Fatalf("boot 1 storage: %v", err)
	}
	b1 := broker.NewStorageBroker(st1, cfg.GetEngine())
	if err := b1.DeclareExchange("bex", "direct", true, false, false, nil); err != nil {
		t.Fatalf("declare exchange: %v", err)
	}
	if _, err := b1.DeclareQueue("bq", true, false, false, nil); err != nil {
		t.Fatalf("declare queue: %v", err)
	}
	if err := b1.BindQueue("bq", "bex", "bk", nil); err != nil {
		t.Fatalf("bind: %v", err)
	}
	b1.Close()
	if err := st1.Close(); err != nil {
		t.Fatalf("boot 1 close: %v", err)
	}

	// Premise, checked: a binding record really is on disk, so removing it is a
	// real intervention rather than a no-op.
	bindingsDir := filepath.Join(dataDir, "metadata", "bindings")
	entries, derr := os.ReadDir(bindingsDir)
	if derr != nil || len(entries) == 0 {
		t.Fatalf("fixture premise broken: no binding record under %s (%v)", bindingsDir, derr)
	}
	if err := os.RemoveAll(bindingsDir); err != nil {
		t.Fatalf("remove binding records: %v", err)
	}

	srv, err := NewServerBuilderWithConfig(refusalTestConfig(t, dataDir)).Build()
	if err != nil {
		t.Fatalf("boot 2: %v", err)
	}
	if err := srv.Broker.PublishMessage("bex", "bk", &protocol.Message{
		Body: []byte("after-restart"), DeliveryMode: 2, Exchange: "bex", RoutingKey: "bk",
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	msg, _, _, gerr := srv.Broker.GetMessageForGet("bq", true)
	if gerr != nil {
		t.Fatalf("basic.get: %v", gerr)
	}
	if msg != nil {
		t.Fatalf("a publish routed to bq after its binding RECORD was deleted (%q). Binding "+
			"durability is supposed to be the storage record and nothing else; if this routes, "+
			"the positive test above is not measuring what it claims to", msg.Body)
	}
}

// TestBuild_RefusalGuardsAndArtifactCounting pins the two structural halves of
// review-3 H-2 at the unit level, because neither is reachable end to end
// today and "unreachable today" is exactly how this class survives.
func TestBuild_RefusalGuardsAndArtifactCounting(t *testing.T) {
	t.Run("a_benign_only_error_produces_no_gating_faults", func(t *testing.T) {
		// The InitError branch used to gate on `ierr != nil` rather than on
		// `len(gating) > 0`. If InitError ever carries a benign-only error the
		// operator gets a refusal with NO artifact list at all — the header and
		// the flag paragraph with nothing in between.
		benign := interfaces.BenignFault("ordinal-allocator", "queue q", "nothing was lost")
		gating := gatingFaultList(interfaces.ExplainedFaults("ordinal-allocator", benign))
		if len(gating) != 0 {
			t.Fatalf("a benign-only error produced %d gating faults", len(gating))
		}
		msg := interfaces.RecoveryRefusalMessage("delivery-tag ordinal recovery", gating)
		if strings.Contains(msg, "[fatal]") || strings.Contains(msg, "[degraded]") {
			t.Fatalf("an artifact-less refusal somehow listed one: %s", msg)
		}
		// The hazard, stated as an assertion: this message names nothing an
		// operator can act on, which is why the branch must not emit it.
		if strings.Contains(msg, "queue q") {
			t.Fatalf("unexpected artifact in an empty refusal: %s", msg)
		}
	})

	t.Run("the_same_artifact_raised_twice_is_counted_once", func(t *testing.T) {
		a := interfaces.FatalFault("wal-scan", "/data/wal/shared/0004.wal", "d", "c", nil)
		b := interfaces.FatalFault("wal-scan", "/data/wal/shared/0004.wal", "d", "c", nil)
		c := interfaces.FatalFault("wal-scan", "/data/wal/shared/0009.wal", "d", "c", nil)
		got := dedupeFaultsByArtifact([]*interfaces.RecoveryFault{a}, []*interfaces.RecoveryFault{b, c})
		if len(got) != 2 || got[0] != a || got[1] != c {
			t.Fatalf("dedupeFaultsByArtifact returned %d faults, want [0004, 0009] in order", len(got))
		}
	})
}
