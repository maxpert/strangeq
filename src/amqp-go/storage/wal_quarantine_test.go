package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

// Step 3-fix, storage half. Two blocking defects from review-3 are pinned here.
//
// BLOCKING-1: a FATAL fault on ONE WAL file returned from inside
// RecoverFromWAL's file loop, so every later file was never opened, never
// scanned and never enumerated as a fault, and GetRecoverableMessages then
// discarded the records already read from the EARLIER files. Measured against
// the real binary: a six-file directory holding 271,080 confirmed durable
// messages recovered ZERO with --unsafe-recovery, from a single flipped bit,
// while the operator-facing message said the loss was confined to one file.
// The property asserted here is CONFINEMENT: a damaged file is quarantined by
// name, every other file is still scanned, and the surviving records come back.
//
// BLOCKING-2: "is this the last record" was `recStart < fileSize`, an OFFSET
// comparison, which a zero-filled tail (the ordinary shape of a power loss)
// defeats by construction. Ra's is_last_record/3 — which this code cites as its
// authority — inspects the remaining BYTES. The four boundary shapes are
// asserted below, including the two the reviewer flagged as most likely to be
// wrong: zero-fill starting mid-record and zero-fill starting exactly on a
// record boundary.

// seedMultiFileWAL writes durable records through the REAL write path with a
// small WAL file size until the shared directory holds exactly `wantFiles`
// files, the last of which carries at least `minTail` records. It returns the
// file paths in numeric order.
//
// The loop is bounded and the postconditions are asserted at runtime rather
// than assumed: a fixture that produced five files, or one file, would make
// every "quarantine confines to one file" assertion below vacuous.
func seedMultiFileWAL(t *testing.T, dataDir string, wantFiles, minTail int) []string {
	t.Helper()

	// Seeded through WALManager rather than DisruptorStorage on purpose: a
	// DisruptorStorage has a segment manager attached, and its Close() runs a
	// final performCheckpoint that UNLINKS every rolled WAL file — which would
	// leave a one-file directory and make every quarantine assertion vacuous.
	cfg := DefaultWALConfig()
	cfg.FileSize = 2048
	wm, err := NewWALManagerWithConfig(dataDir, cfg)
	if err != nil {
		t.Fatalf("seed WAL manager: %v", err)
	}

	sharedDir := filepath.Join(dataDir, "wal", "shared")
	const maxRecords = 5000
	written := 0
	for i := 1; i <= maxRecords; i++ {
		// The WAL persists the OFFSET as the record's identity and recovery
		// reads the delivery tag back out of it, so the offset must be the
		// composite tag (ordinal 1) or every record recovers as a dead
		// incarnation and the fixture measures nothing.
		tag := uint64(1)<<44 | uint64(i)
		if err := wm.Write("q", &protocol.Message{
			DeliveryTag:  tag,
			Body:         []byte(fmt.Sprintf("multi-file-record-%05d", i)),
			DeliveryMode: 2,
		}, tag); err != nil {
			t.Fatalf("seed message %d: %v", i, err)
		}
		written++
		files := walFilesIn(t, sharedDir)
		if len(files) > wantFiles {
			t.Fatalf("fixture overshot: %d WAL files after %d records, wanted %d",
				len(files), written, wantFiles)
		}
		if len(files) == wantFiles && len(walkWALRecords(t, files[wantFiles-1])) >= minTail {
			break
		}
	}
	_ = wm.Close()

	files := walFilesIn(t, sharedDir)
	if len(files) != wantFiles {
		t.Fatalf("fixture premise broken: wanted exactly %d WAL files, got %d (%v)",
			wantFiles, len(files), files)
	}
	for _, f := range files {
		if n := len(walkWALRecords(t, f)); n == 0 {
			t.Fatalf("fixture premise broken: %s holds 0 records, so damaging it would cost nothing", f)
		}
	}
	return files
}

// walFilesIn lists the .wal files of a directory in numeric-stem order.
func walFilesIn(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if _, ok := parseWALFileNum(e.Name()); ok {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(out)
	return out
}

// recordsIn counts the physical records of one WAL file with the SECOND
// implementation of the framing walk, so the code under test cannot be the
// authority on how much its own quarantine cost.
func recordsIn(t *testing.T, path string) int {
	t.Helper()
	return len(walkWALRecords(t, path))
}

// recoverAll opens the directory with --unsafe-recovery and returns the
// recovered records for queue "q" plus the classified fault.
func recoverAll(t *testing.T, dataDir string) (int, error) {
	t.Helper()
	st, err := NewDisruptorStorageWithEngineConfig(dataDir, interfaces.EngineConfig{
		WALFileSize:    2048,
		UnsafeRecovery: true,
	})
	if err != nil {
		t.Fatalf("--unsafe-recovery must permit construction: %v", err)
	}
	defer st.Close()
	msgs, rerr := st.GetRecoverableMessages()
	return len(msgs["q"]), rerr
}

// plantForeignHeaderOver replaces an existing WAL file with a 6-byte foreign
// header, the exact damage review-3 injected as file 4 of 6.
func plantForeignHeaderOver(t *testing.T, path string) {
	t.Helper()
	const plantedVersion = byte(1)
	if plantedVersion == WALVersion4 {
		t.Fatalf("fixture is out of range of its own defect: planted version IS WALVersion4")
	}
	if err := os.WriteFile(path, append([]byte(WALMagic), plantedVersion), 0o644); err != nil {
		t.Fatalf("plant over %s: %v", path, err)
	}
}

// zeroTail zeroes the last n bytes of a file IN PLACE, leaving its size
// unchanged. This is the power-loss shape: the tail blocks never reached the
// platter but the size metadata was journaled.
func zeroTail(t *testing.T, path string, n int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open rw: %v", err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if st.Size() <= n {
		t.Fatalf("fixture premise broken: %s is %d bytes, cannot zero a %d-byte tail",
			path, st.Size(), n)
	}
	if _, err := f.WriteAt(make([]byte, n), st.Size()-n); err != nil {
		t.Fatalf("zero tail: %v", err)
	}
}

// zeroFromRecordBoundary zeroes from the start of record idx to EOF, in place.
// This is the asymmetry review-3 named: today the same zero region is FATAL
// when it starts mid-record and reports NO FAULT AT ALL when it happens to
// start on a record boundary.
func zeroFromRecordBoundary(t *testing.T, path string, idx int) {
	t.Helper()
	spans := walkWALRecords(t, path)
	if idx < 0 || idx >= len(spans) {
		t.Fatalf("record index %d out of range (%d records)", idx, len(spans))
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatalf("open rw: %v", err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	from := spans[idx].headerAt
	if st.Size()-from < 16 {
		t.Fatalf("fixture premise broken: only %d bytes follow the boundary", st.Size()-from)
	}
	// Trim the file so the zero region is an EXACT multiple of the 8-byte
	// record header. Otherwise the walk's final partial header short-reads and
	// the scanner reports a torn tail by accident — the test would then pass
	// without the zero region ever having been recognised as anything, which is
	// precisely the vacuous pass this shape exists to rule out.
	end := from + (st.Size()-from)/8*8
	if err := f.Truncate(end); err != nil {
		t.Fatalf("truncate to 8-byte multiple: %v", err)
	}
	if _, err := f.WriteAt(make([]byte, end-from), from); err != nil {
		t.Fatalf("zero from boundary: %v", err)
	}
}

// artifacts returns the artifact of every fault reachable from err.
func artifacts(err error) []string {
	var out []string
	for _, f := range interfaces.FaultsOf(err) {
		out = append(out, f.Artifact)
	}
	return out
}

func containsArtifact(err error, want string) bool {
	for _, a := range artifacts(err) {
		if a == want {
			return true
		}
	}
	return false
}

// --- BLOCKING-1 -------------------------------------------------------------

// TestWALQuarantine_SixFileFixtureRecoversEveryUndamagedFile is the acceptance
// test for review-3's B-1, reproduced at fixture scale. The real-binary
// measurement was: five intact 59 MB files holding 271,080 confirmed durable
// messages, one damaged file, `--unsafe-recovery` boot => messages_recovered 0.
func TestWALQuarantine_SixFileFixtureRecoversEveryUndamagedFile(t *testing.T) {
	for _, tc := range []struct {
		name    string
		damaged int // index into files
		damage  func(t *testing.T, path string)
	}{
		{"foreign_header_planted_as_file_4_of_6", 3, plantForeignHeaderOver},
		{"one_flipped_bit_in_file_2_of_6", 1, func(t *testing.T, path string) {
			spans := walkWALRecords(t, path)
			if len(spans) < 3 {
				t.Fatalf("fixture premise broken: %d records, need an interior one", len(spans))
			}
			corruptRecordPayload(t, path, 0)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			files := seedMultiFileWAL(t, dataDir, 6, 3)

			// The control number, computed by the independent framing walk
			// BEFORE any damage: what the five surviving files hold.
			survivors := 0
			for i, f := range files {
				if i == tc.damaged {
					continue
				}
				survivors += recordsIn(t, f)
			}
			if survivors == 0 {
				t.Fatalf("fixture premise broken: the undamaged files hold 0 records")
			}

			tc.damage(t, files[tc.damaged])

			got, rerr := recoverAll(t, dataDir)
			if rerr == nil {
				t.Fatalf("damaging %s produced no fault at all", files[tc.damaged])
			}
			if !interfaces.OutcomeOf(rerr).GatesBoot() {
				t.Fatalf("damaging %s must gate the boot, got %v", files[tc.damaged], interfaces.OutcomeOf(rerr))
			}
			if !containsArtifact(rerr, files[tc.damaged]) {
				t.Fatalf("the fault list does not name the damaged file %s; artifacts=%v",
					files[tc.damaged], artifacts(rerr))
			}
			for i, f := range files {
				if i == tc.damaged {
					continue
				}
				if containsArtifact(rerr, f) {
					t.Fatalf("undamaged file %s was reported as an abandoned artifact; artifacts=%v",
						f, artifacts(rerr))
				}
			}
			if got != survivors {
				t.Fatalf("QUARANTINE FAILED: recovered %d of %d confirmed durable records held by the "+
					"FIVE UNDAMAGED files; a fatal fault on %s abandoned the whole directory while the "+
					"operator-facing cost statement named one file",
					got, survivors, files[tc.damaged])
			}
		})
	}
}

// TestWALQuarantine_EveryFaultAcrossEveryFileIsEnumerated pins the improvement
// ref-rabbitmq.md §1 names as the one available over Ra: the refusal must be
// ONE complete report, not a bisect-by-restart. Two damaged files must both be
// named in a single pass, and the four undamaged ones must still be recovered.
func TestWALQuarantine_EveryFaultAcrossEveryFileIsEnumerated(t *testing.T) {
	dataDir := t.TempDir()
	files := seedMultiFileWAL(t, dataDir, 6, 3)

	// File 2 gets an interior CRC failure, file 5 a foreign header. File 2
	// sorts FIRST, so the old "return on the first fatal" could never reach
	// file 5.
	corruptRecordPayload(t, files[1], 0)
	plantForeignHeaderOver(t, files[4])

	survivors := 0
	for i, f := range files {
		if i == 1 || i == 4 {
			continue
		}
		survivors += recordsIn(t, f)
	}

	// WITHOUT the flag the refusal must name BOTH files in ONE message. The
	// refusal text asserts that its artifact list is complete; a construction
	// -time refusal that enumerated only the first damaged file would make that
	// sentence false and force an operator to bisect by restart.
	if st, cerr := NewDisruptorStorageWithEngineConfig(dataDir, interfaces.EngineConfig{WALFileSize: 2048}); cerr == nil {
		if st != nil {
			_ = st.Close()
		}
		t.Fatalf("construction succeeded on a directory with two damaged WAL files and no --unsafe-recovery")
	} else {
		for _, want := range []string{files[1], files[4]} {
			if !strings.Contains(cerr.Error(), want) {
				t.Fatalf("the construction refusal does not name %s, so its \"the list above is "+
					"COMPLETE\" claim is false:\n%s", want, cerr.Error())
			}
		}
	}

	got, rerr := recoverAll(t, dataDir)
	if rerr == nil {
		t.Fatalf("two damaged files produced no fault at all")
	}
	for _, want := range []string{files[1], files[4]} {
		if !containsArtifact(rerr, want) {
			t.Fatalf("fault enumeration is incomplete: %s is missing, so an operator would have to "+
				"bisect by restart; artifacts=%v", want, artifacts(rerr))
		}
	}
	if got != survivors {
		t.Fatalf("recovered %d of %d records held by the four undamaged files", got, survivors)
	}
}

// --- BLOCKING-2 -------------------------------------------------------------

// TestWALTail_ZeroFilledTailBoundaryShapes drives the four shapes review-3
// named. The middle two are where a `recStart < fileSize` implementation is
// most likely to be wrong, and they demand OPPOSITE answers from the same
// zero region depending only on where it starts — which is the tell.
func TestWALTail_ZeroFilledTailBoundaryShapes(t *testing.T) {
	t.Run("zero_fill_starting_mid_record_is_a_torn_tail", func(t *testing.T) {
		dataDir := t.TempDir()
		path := seedWAL(t, dataDir, 12)
		spans := walkWALRecords(t, path)
		if len(spans) != 12 {
			t.Fatalf("fixture premise broken: walked %d records", len(spans))
		}
		st, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		// review-3's exact shape: zero a multi-record tail region that STARTS
		// INSIDE a record. That record then fails CRC with a run of ZEROS after
		// it, which `recStart < fileSize` reads as "not the last record".
		const damaged = 8
		d := spans[damaged]
		zeroFrom := d.dataAt + int64(d.dataLen)/2
		if zeroFrom <= d.headerAt {
			t.Fatalf("fixture premise broken: zero region would start on a record boundary")
		}
		zeroTail(t, path, st.Size()-zeroFrom)
		// Premise, checked: bytes really do follow the damaged record, so the
		// offset comparison genuinely takes the interior branch.
		if st.Size() <= d.dataAt+int64(d.dataLen) {
			t.Fatalf("fixture is out of range of its own defect: nothing follows record %d, so the "+
				"offset comparison would already call it the last record", damaged)
		}

		intact := damaged
		got, rerr := recoverAll(t, dataDir)
		if rerr != nil && interfaces.OutcomeOf(rerr).GatesBoot() {
			t.Fatalf("a zero-filled tail must not gate the boot (no broker in the reference study "+
				"asks for operator action on a torn tail); got %v: %v", interfaces.OutcomeOf(rerr), rerr)
		}
		if got != intact {
			t.Fatalf("recovered %d of the %d records that precede the torn tail", got, intact)
		}
	})

	t.Run("zero_fill_starting_on_a_record_boundary_is_reported_not_silently_accepted", func(t *testing.T) {
		dataDir := t.TempDir()
		path := seedWAL(t, dataDir, 12)
		spans := walkWALRecords(t, path)
		const from = 9
		if from >= len(spans) {
			t.Fatalf("fixture premise broken: walked %d records", len(spans))
		}
		zeroFromRecordBoundary(t, path, from)

		// The scanner must SEE this as a torn tail. Today it parses the zero
		// region as a run of zero-length zero-CRC records and reports nothing,
		// so the same damage is silently tolerated here and fatal one byte over.
		st, err := NewDisruptorStorageWithEngineConfig(dataDir, interfaces.EngineConfig{})
		if err != nil {
			t.Fatalf("a torn tail must not refuse construction: %v", err)
		}
		defer st.Close()
		msgs, rerr := st.GetRecoverableMessages()
		if rerr != nil && interfaces.OutcomeOf(rerr).GatesBoot() {
			t.Fatalf("a zero-filled tail must not gate the boot; got %v: %v",
				interfaces.OutcomeOf(rerr), rerr)
		}
		if got := len(msgs["q"]); got != from {
			t.Fatalf("recovered %d of the %d records that precede the zero region", got, from)
		}
		// And it must be COUNTED, not silently swallowed: the scan has to
		// report the truncation as a benign fault naming the file.
		faults := scanFaultsFor(t, dataDir, path)
		if len(faults) == 0 {
			t.Fatalf("a zero-filled tail starting on a record boundary was silently accepted as data: "+
				"no fault of any class was reported for %s", path)
		}
	})

	t.Run("genuinely_truncated_file_is_a_torn_tail", func(t *testing.T) {
		dataDir := t.TempDir()
		path := seedWAL(t, dataDir, 12)
		spans := walkWALRecords(t, path)
		last := spans[len(spans)-1]
		if err := os.Truncate(path, last.dataAt+int64(last.dataLen)/2); err != nil {
			t.Fatalf("truncate: %v", err)
		}
		intact := len(spans) - 1
		got, rerr := recoverAll(t, dataDir)
		if rerr != nil && interfaces.OutcomeOf(rerr).GatesBoot() {
			t.Fatalf("a truncated tail must not gate the boot; got %v: %v",
				interfaces.OutcomeOf(rerr), rerr)
		}
		if got != intact {
			t.Fatalf("recovered %d of the %d intact records", got, intact)
		}
	})

	t.Run("real_data_after_a_bad_crc_record_stays_fatal", func(t *testing.T) {
		// R-TAIL-HOLE SUPERSEDES the historical rule that real data after a
		// bad CRC record is ALWAYS fatal. Under the new rule it is fatal only
		// when the non-zero suffix is OUT OF BOUND, or the file is not one of
		// the two newest. seedWAL produces a single file (which IS the newest),
		// so the unconfirmed bound is shrunk to zero here to keep asserting the
		// FATAL arm deterministically.
		t.Cleanup(setWALTailBoundForTest(0, 0))

		dataDir := t.TempDir()
		path := seedWAL(t, dataDir, 12)
		spans := walkWALRecords(t, path)
		const interior = 4
		if interior >= len(spans)-1 {
			t.Fatalf("fixture is out of range of its own defect: record %d is the last one", interior)
		}
		corruptRecordPayload(t, path, interior)
		// Premise, checked: what follows the damaged record is REAL DATA, not
		// zeros — otherwise this would be testing the tail rule.
		if allZeroFrom(t, path, spans[interior+1].headerAt) {
			t.Fatalf("fixture premise broken: the bytes after the damaged record are all zero, " +
				"so the tail rule would apply and the interior rule would never be reached")
		}
		_, rerr := recoverAll(t, dataDir)
		if interfaces.OutcomeOf(rerr) != interfaces.RecoveryFatal {
			t.Fatalf("an interior CRC failure with real data after it classified %v, want fatal: %v",
				interfaces.OutcomeOf(rerr), rerr)
		}
	})
}

// allZeroFrom reports whether every byte from `from` to EOF is zero.
func allZeroFrom(t *testing.T, path string, from int64) bool {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if from >= int64(len(b)) {
		return true
	}
	for _, c := range b[from:] {
		if c != 0 {
			return false
		}
	}
	return true
}

// scanFaultsFor returns every fault scanWALFile reports for one file, including
// BENIGN ones — which joinFaults deliberately filters out of the package's
// error returns, so they are unobservable through GetRecoverableMessages.
func scanFaultsFor(t *testing.T, dataDir, path string) []*interfaces.RecoveryFault {
	t.Helper()
	qw := &QueueWAL{name: "shared", dataDir: filepath.Dir(path), cfg: DefaultWALConfig()}
	_, err := qw.scanWALFile(path)
	if err == nil {
		return nil
	}
	faults := interfaces.FaultsOf(err)
	if len(faults) == 0 {
		t.Fatalf("scanWALFile returned an UNCLASSIFIED error for %s: %v", path, err)
	}
	for _, f := range faults {
		if !strings.Contains(f.Artifact, filepath.Base(path)) {
			t.Fatalf("fault names %q, not the file under test %q", f.Artifact, path)
		}
	}
	return faults
}

// TestWALTail_TheWritePathNeverProducesAZeroLengthRecord asserts, at runtime,
// the premise walBlankRecordHeader rests on (canon rule 11 — assert it, do not
// document it).
//
// If ANY record type this build writes could carry a zero-length payload, the
// zero-region rule would silently discard real data from that point on. Every
// record's payload begins with a one-byte type tag, so the premise holds; this
// test is what makes it stay true rather than being a comment that was true
// once. It drives the real write path for all three record shapes the WAL
// emits: an ordinary message, a transaction boundary pair, and a shared
// BodyBlock plus its reference records.
func TestWALTail_TheWritePathNeverProducesAZeroLengthRecord(t *testing.T) {
	dataDir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dataDir, DefaultWALConfig())
	if err != nil {
		t.Fatalf("wal manager: %v", err)
	}

	// 1. Ordinary message records, with the smallest payload a caller can
	//    produce: an EMPTY body and no optional properties at all.
	for i := 1; i <= 4; i++ {
		tag := uint64(1)<<44 | uint64(i)
		if err := wm.Write("q", &protocol.Message{
			DeliveryTag:  tag,
			Body:         []byte{},
			DeliveryMode: 2,
		}, tag); err != nil {
			t.Fatalf("write message: %v", err)
		}
	}

	// 2. Transaction boundary records — the smallest record TYPE the WAL emits.
	if err := wm.WriteTxAtomic([]*RecoveryMessage{{
		QueueName: "q",
		Offset:    uint64(1)<<44 | 99,
		Message:   &protocol.Message{DeliveryTag: uint64(1)<<44 | 99, DeliveryMode: 2},
	}}); err != nil {
		t.Fatalf("tx write: %v", err)
	}

	// 3. A shared BodyBlock plus its reference records (ITER5 fan-out).
	body := []byte("shared-body")
	subs := make([]sharedSub, 2)
	dones := make([]chan error, 2)
	for i := range subs {
		dones[i] = make(chan error, 1)
		tag := uint64(1)<<44 | uint64(200+i)
		subs[i] = sharedSub{
			queueName: fmt.Sprintf("q%d", i),
			offset:    tag,
			message:   &protocol.Message{DeliveryTag: tag, DeliveryMode: 2, Body: body},
			done:      dones[i],
		}
	}
	if err := wm.WriteSharedAsync(subs, body, 2); err != nil {
		t.Fatalf("shared write: %v", err)
	}
	for i := range dones {
		if derr := <-dones[i]; derr != nil {
			t.Fatalf("shared sub %d: %v", i, derr)
		}
	}
	_ = wm.Close()

	sharedDir := filepath.Join(dataDir, "wal", "shared")
	files := walFilesIn(t, sharedDir)
	if len(files) == 0 {
		t.Fatalf("fixture premise broken: no WAL file was produced, so nothing is inspected")
	}
	// Premise, checked: all three record TYPES really are present, so a zero
	// header cannot be legal for any of them by omission.
	counts, cerr := WALRecordCountsForTest(dataDir)
	if cerr != nil {
		t.Fatalf("record counts: %v", cerr)
	}
	if counts.Messages == 0 || counts.TxBoundary == 0 || counts.BodyBlocks == 0 {
		t.Fatalf("fixture premise broken: messages=%d txBoundary=%d bodyBlocks=%d — a record type "+
			"this build writes is absent from the sample", counts.Messages, counts.TxBoundary, counts.BodyBlocks)
	}

	inspected := 0
	for _, f := range files {
		for _, sp := range walkWALRecords(t, f) {
			inspected++
			if sp.dataLen == 0 {
				t.Fatalf("the write path produced a ZERO-LENGTH record at byte offset %d of %s. "+
					"walBlankRecordHeader treats a zero header as the end of the file's data, so "+
					"this record and everything after it would be silently discarded at recovery",
					sp.headerAt, f)
			}
		}
	}
	if inspected == 0 {
		t.Fatalf("fixture premise broken: 0 records inspected across %d files", len(files))
	}
}

// TestListQueues_FaultNamesAnAbsolutePath pins review-3 H-3.
//
// broker.initOrdinalAllocator used to build this fault itself and could only
// name the artifact `metadata/queues` — a RELATIVE path in a refusal message an
// operator running several brokers has to act on. RecoveryFault.Artifact's own
// doc requires an absolute path when a file is involved, and every other
// artifact in the refusal is absolute.
func TestListQueues_FaultNamesAnAbsolutePath(t *testing.T) {
	dataDir := t.TempDir()
	st, err := NewDisruptorStorageWithEngineConfig(dataDir, interfaces.EngineConfig{})
	if err != nil {
		t.Fatalf("storage: %v", err)
	}
	defer st.Close()

	queuesDir := filepath.Join(dataDir, MetadataDir, QueuesDir)
	if _, serr := os.Stat(queuesDir); serr != nil {
		t.Fatalf("fixture premise broken: %s does not exist: %v", queuesDir, serr)
	}
	if cerr := os.Chmod(queuesDir, 0o000); cerr != nil {
		t.Fatalf("chmod: %v", cerr)
	}
	defer os.Chmod(queuesDir, 0o755)

	_, lerr := st.ListQueues()
	if lerr == nil {
		// STRENGTHENED, not weakened. This branch used to skip unconditionally on
		// the theory that a nil error can only mean "chmod 000 did not take
		// (running as root?)". It can also mean the CODE STOPPED READING THE
		// DIRECTORY — and it did: a revision of ListQueues' enumeration helper
		// swallowed the ReadDir failure and returned an EMPTY queue list with
		// err == nil, which makes every queue's write-once Ordinal invisible at
		// boot. This test was the only thing that noticed, and it reported a SKIP,
		// which reads exactly like a pass. So the two causes are now
		// distinguished by an INDEPENDENT probe: if the test process itself cannot
		// read the directory, the mode bits took effect and a nil error is a
		// defect in ListQueues, not an out-of-range fixture.
		if _, probeErr := os.ReadDir(queuesDir); probeErr != nil {
			t.Fatalf("ListQueues returned nil while %s is UNREADABLE to this process (%v): the "+
				"enumeration is swallowing its own error, so an unreadable queues directory reads "+
				"as \"no queues exist\" and every ordinal is re-minted over live records",
				queuesDir, probeErr)
		}
		t.Skipf("fixture is out of range of its own defect: %s is still readable after chmod 000 "+
			"(running as root?), so no fault is produced", queuesDir)
	}
	faults := interfaces.FaultsOf(lerr)
	if len(faults) != 1 {
		t.Fatalf("ListQueues returned an UNCLASSIFIED error: %v", lerr)
	}
	if !filepath.IsAbs(faults[0].Artifact) {
		t.Fatalf("artifact %q is not an absolute path; an operator with several brokers cannot "+
			"act on it", faults[0].Artifact)
	}
	if faults[0].Artifact != queuesDir {
		t.Fatalf("artifact %q, want %q", faults[0].Artifact, queuesDir)
	}
	if faults[0].Outcome != interfaces.RecoveryFatal {
		t.Fatalf("an unreadable queue-metadata directory classified %v, want fatal", faults[0].Outcome)
	}
}

// TestWALEnumeration_TheTwoDirectoryWalksAgree pins review-3 H-4.
//
// rebuildBootState accepted only <digits>.wal while RecoverFromWAL accepted any
// *.wal, AND THE LOOSER ONE WAS THE FATAL ONE. So a .wal file with a
// non-numeric stem was invisible to the boot-state rebuild — no fileNum
// protection, no oldFiles entry, no offset inventory — but was still scanned by
// recovery, where an unparseable header refused the boot. The refusal advises
// moving the listed files aside; an operator who moved one aside as
// `damaged.wal` INSIDE the same directory re-armed the refusal through a path
// the rebuild could not even see.
func TestWALEnumeration_TheTwoDirectoryWalksAgree(t *testing.T) {
	dataDir := t.TempDir()
	files := seedMultiFileWAL(t, dataDir, 3, 3)
	total := 0
	for _, f := range files {
		total += recordsIn(t, f)
	}

	// The operator's remedy, done the way the old message invited: rename the
	// file aside without leaving the directory.
	sharedDir := filepath.Dir(files[0])
	aside := filepath.Join(sharedDir, "damaged.wal")
	if err := os.WriteFile(aside, append([]byte(WALMagic), byte(1)), 0o644); err != nil {
		t.Fatalf("write aside file: %v", err)
	}
	// Premise, checked: this name really is outside the segment predicate, so
	// the test is exercising the disagreement rather than an ordinary segment.
	if _, ok := parseWALFileNum(filepath.Base(aside)); ok {
		t.Fatalf("fixture premise broken: %s parses as a WAL segment stem", aside)
	}

	st, err := NewDisruptorStorageWithEngineConfig(dataDir, interfaces.EngineConfig{WALFileSize: 2048})
	if err != nil {
		t.Fatalf("a non-segment .wal file must not refuse the boot (the boot-state rebuild cannot "+
			"even see it, so recovery must not be fatal over it either): %v", err)
	}
	defer st.Close()
	msgs, rerr := st.GetRecoverableMessages()
	if rerr != nil {
		t.Fatalf("a non-segment .wal file produced a recovery fault: %v", rerr)
	}
	if got := len(msgs["q"]); got != total {
		t.Fatalf("recovered %d of %d records; the aside file changed what recovery returned", got, total)
	}
}
