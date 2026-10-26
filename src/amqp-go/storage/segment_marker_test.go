package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// review-5 CRITICAL-1 — a degenerate .queue-name marker must never be read as
// an authoritative owner name.
//
// The defect: readSegmentDirMarker validated nothing. Zero-length, truncated
// and whitespace-decorated content all returned (content, true, nil) and became
// the directory's owner. The measured consequence was a CLEAN BOOT — err == nil
// — in which a queue's checkpointed durable messages were attributed to queue
// "", the queue minted a fresh empty directory, recovered nothing, and
// performCheckpoint had already unlinked the source WAL.
//
// Every predicate below is RED against d5c3f85: see .notes/loop-2/fix-5.md for
// the mutation runs.
// ---------------------------------------------------------------------------

// TestSegmentMarker_DegenerateContentIsUnusableNotAnOwner is the unit-level
// gate. "Present but unusable" is a THIRD state and must be reported as an
// error, distinct from both "missing" (which attributes by directory name) and
// "valid" (which attributes by content).
func TestSegmentMarker_DegenerateContentIsUnusableNotAnOwner(t *testing.T) {
	valid := string(encodeSegmentDirMarker("orders"))

	cases := []struct {
		name string
		raw  string
	}{
		{"zero length", ""},
		{"bare queue name, the pre-fix format", "orders"},
		{"bare queue name with a trailing newline", "orders\n"},
		{"header only, body lost", valid[:strings.IndexByte(valid, '\n')+1]},
		{"truncated mid-body", valid[:len(valid)-2]},
		{"one byte of trailing decoration", valid + "\n"},
		{"whitespace decoration inside the body", strings.Replace(valid, "orders", "order ", 1)},
		{"wrong magic", strings.Replace(valid, segmentDirMarkerMagic, "strangeq-segment-ownes", 1)},
		{"wrong version", strings.Replace(valid, "\tv1\t", "\tv2\t", 1)},
		{"length field is not a number", strings.Replace(valid, "\t6\t", "\tsix\t", 1)},
		{"length field disagrees with the body", strings.Replace(valid, "\t6\t", "\t5\t", 1)},
		{"checksum is not hex", strings.Replace(valid, valid[strings.LastIndexByte(valid[:strings.IndexByte(valid, '\n')], '\t')+1:strings.IndexByte(valid, '\n')], "zzzzzzzz", 1)},
		{"checksum does not match the body", strings.Replace(valid, "orders", "ordera", 1)},
		{"declares the empty queue name", string(encodeSegmentDirMarker(""))},
		{"no header line at all", "orders-with-no-newline-anywhere"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(dir, segmentDirMarkerName), []byte(tc.raw), 0644))

			name, present, err := readSegmentDirMarker(dir)

			require.Error(t, err,
				"a %s marker was accepted; readSegmentDirMarker must report present-but-unusable, "+
					"because whatever it returns here becomes the directory's OWNER and is handed that "+
					"queue's checkpointed durable messages (review-5 CRITICAL-1)", tc.name)
			require.True(t, present,
				"the marker file exists, so present must stay true; reporting absent would attribute "+
					"the directory to its own name, i.e. to the wrong queue (review-4 B-1)")
			require.Empty(t, name, "an unusable marker must not yield a name at all")
		})
	}
}

// TestSegmentMarker_ValidMarkerRoundTripsEveryQueueName guards the other
// direction: framing must not reject names that are legal on the wire. A
// validator that rejected real names would strand real queues, which is the
// same failure with the sign flipped.
func TestSegmentMarker_ValidMarkerRoundTripsEveryQueueName(t *testing.T) {
	names := []string{
		"orders", "a/b", "%2f", "café", "héllo-ünicode", ".queue-name",
		"queue with spaces", "trailing space ", "\ttabbed", "new\nline",
		"a\x00b", "50%off", "amq.gen-JzTY6a2Cs0M1F0J2vTvKLg", ".", "..",
		strings.Repeat("a", segmentDirMarkerMaxNameLen),
		string([]byte{0xff, 0xfe, 0x00, 0x41}),
	}

	for _, name := range names {
		dir := t.TempDir()
		require.NoError(t, writeSegmentDirMarker(dir, name), "name %q", name)

		got, present, err := readSegmentDirMarker(dir)
		require.NoError(t, err, "name %q", name)
		require.True(t, present, "name %q", name)
		require.Equal(t, name, got,
			"the marker must record the queue name byte for byte; framing validates the RECORD, "+
				"never the name (name %q)", name)
	}
}

// TestSegmentMarker_WriteIsFramedAndLeavesNoTempFile pins the two properties
// that make damage detectable at all.
//
// Framing is what distinguishes a truncated marker from a short queue name: a
// marker truncated from "payments-eu-west" to "payme" is a perfectly good queue
// name, and the pre-fix raw format recovered another queue's messages under key
// "payme" with err == nil. The temp-file-and-rename shape is what stops a
// failed or interrupted write from leaving a short file wearing the real name.
func TestSegmentMarker_WriteIsFramedAndLeavesNoTempFile(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, writeSegmentDirMarker(dir, "payments-eu-west"))

	raw, err := os.ReadFile(filepath.Join(dir, segmentDirMarkerName))
	require.NoError(t, err)

	require.NotEqual(t, "payments-eu-west", string(raw),
		"the marker is the bare queue name, so nothing on disk distinguishes a complete marker "+
			"from a truncated one (review-5 CRITICAL-1)")
	require.True(t, strings.HasPrefix(string(raw), segmentDirMarkerMagic),
		"a marker must announce itself so a foreign or damaged file is not parsed as a name")
	require.Contains(t, string(raw), "payments-eu-west",
		"the name is still written verbatim so an operator can read it out of the file")

	// Truncating the written record must now be detectable, which is the whole
	// point of the frame.
	for cut := 1; cut < len(raw); cut++ {
		require.NoError(t, os.WriteFile(filepath.Join(dir, segmentDirMarkerName), raw[:cut], 0644))
		_, _, rerr := readSegmentDirMarker(dir)
		require.Error(t, rerr, "a marker truncated to %d of %d bytes was accepted", cut, len(raw))
	}

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		require.False(t, strings.HasSuffix(e.Name(), TempFileExtension),
			"a temp file survived a successful write: %s", e.Name())
	}
}

// TestSegmentMarker_ZeroLengthMarkerCannotStrandACheckpointedQueue is the
// end-to-end reproduction of the review's probe A, promoted from the throwaway
// harness into the suite.
//
// The pre-fix outcome, measured: RecoverFromSegments returned err == nil having
// attributed queue "orders"'s three checkpointed messages to queue "", while
// segmentDirFor("orders") walked away to a fresh empty directory. A clean boot
// that silently loses a queue's durable messages is the worst shape a recovery
// bug can take, because nothing anywhere reports it.
func TestSegmentMarker_ZeroLengthMarkerCannotStrandACheckpointedQueue(t *testing.T) {
	root := t.TempDir()
	sm, err := NewSegmentManager(root)
	require.NoError(t, err)

	const queueName = "orders"
	for i := uint64(1); i <= 3; i++ {
		msg := &protocol.Message{Exchange: "ex", RoutingKey: "rk", Body: []byte("payload")}
		require.NoError(t, writeLegacyForTest(sm, queueName, msg, i))
	}
	require.NoError(t, sm.Close())

	// The marker becomes zero-length. Reachable before this fix by a failed
	// os.WriteFile, which O_CREATEd before writing, or by a crash after the
	// directory entry was fsynced but before the contents were.
	markerPath := filepath.Join(root, "segments", queueName, segmentDirMarkerName)
	require.FileExists(t, markerPath)
	require.NoError(t, os.WriteFile(markerPath, nil, 0644))

	sm2, err := NewSegmentManager(root)
	require.NoError(t, err)
	defer func() { _ = sm2.Close() }()

	recovered, rerr := sm2.RecoverFromSegments()

	require.Error(t, rerr,
		"recovery reported a CLEAN BOOT over a directory it could not attribute; the operator is "+
			"never told, and the source WAL has already been unlinked (review-5 CRITICAL-1)")
	require.NotContains(t, recovered, "",
		"the records were attributed to the empty queue name, which no queue can have, so they are "+
			"handed to a queue that does not exist")
	require.NotContains(t, recovered, queueName,
		"an unusable marker must strand the directory loudly, never guess its owner")
}

// TestSegmentMarker_UnusableMarkerIsNeverAdoptedByTheDirectoryName gates the
// write path's half of the same defect: a directory whose marker cannot be
// trusted must not be adopted just because its NAME spells the queue. The
// marker may name a different queue and simply be damaged.
func TestSegmentMarker_UnusableMarkerIsNeverAdoptedByTheDirectoryName(t *testing.T) {
	root := t.TempDir()
	sm, err := NewSegmentManager(root)
	require.NoError(t, err)
	defer func() { _ = sm.Close() }()

	// The marker holds the BARE queue name — the pre-fix on-disk format, and the
	// one content an unvalidated reader would hand back as this very queue's
	// own name. That is what makes this an adoption test rather than a repeat of
	// the probe-onward cases: an unframed file is exactly as likely to be a
	// foreign file, a truncation, or a fragment of something else, so it must
	// not be honoured just because its bytes happen to spell us.
	segRoot := filepath.Join(root, "segments")
	require.NoError(t, os.MkdirAll(filepath.Join(segRoot, "orders"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(segRoot, "orders", segmentDirMarkerName), []byte("orders"), 0644))

	dir, derr := sm.segmentDirFor("orders")
	require.NoError(t, derr, "the queue must still get a usable directory")
	require.NotEqual(t, "orders", dir,
		"a directory with an unusable marker was adopted on the strength of its content; an "+
			"unframed file carries no evidence that it is a complete marker naming this queue")

	// The refused directory is left exactly as found — stranded, not rewritten.
	raw, rerr := os.ReadFile(filepath.Join(segRoot, "orders", segmentDirMarkerName))
	require.NoError(t, rerr)
	require.Equal(t, "orders", string(raw),
		"the refused directory's marker was modified; it must be left untouched")
}

// TestSegmentMarker_FailedMarkerWriteFailsTheOpen gates CRITICAL-1's third
// part: a failed marker write used to be a WARNING when the directory name
// spelled the queue name, and the open proceeded.
//
// The injection is `.queue-name.tmp` pre-created as a directory, which makes
// atomicWriteFile's O_CREATE|O_WRONLY temp open fail with EISDIR while leaving
// segment creation in the same directory working. That is the narrow shape in
// which the two outcomes differ at all — see fix-5.md for why every realistic
// filesystem failure that blocks a 60-byte marker also blocks the segment file
// that follows it, which is what makes refusing here cost no availability.
func TestSegmentMarker_FailedMarkerWriteFailsTheOpen(t *testing.T) {
	root := t.TempDir()
	sm, err := NewSegmentManager(root)
	require.NoError(t, err)
	defer func() { _ = sm.Close() }()

	segRoot := filepath.Join(root, "segments")
	queueDir := filepath.Join(segRoot, "orders")
	require.NoError(t, os.MkdirAll(queueDir, 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(queueDir, segmentDirMarkerName+TempFileExtension), 0755))

	// Premise: the injection really does defeat the marker write. Asserted at
	// runtime rather than documented (canon rule 11) — if a future
	// atomicWriteFile stopped using this temp path, the test below would pass
	// vacuously.
	require.Error(t, writeSegmentDirMarker(queueDir, "orders"),
		"the injection no longer blocks the marker write, so this fixture gates nothing")

	qs, gerr := sm.getOrCreateQueueSegmentsAt("orders", "orders")
	require.Error(t, gerr,
		"the open proceeded after the ownership marker could not be written; a warning is not a "+
			"handler when the directory is left permanently unattributable (review-5 CRITICAL-1)")
	require.Nil(t, qs)
}

// TestSegmentMarker_EmptyQueueNameIsRefused closes the review's probe J:
// getOrCreateQueueSegmentsAt accepted the empty queue name and built a
// QueueSegments around it, which is how a degenerate marker's "" became a
// working owner rather than an error.
func TestSegmentMarker_EmptyQueueNameIsRefused(t *testing.T) {
	root := t.TempDir()
	sm, err := NewSegmentManager(root)
	require.NoError(t, err)
	defer func() { _ = sm.Close() }()

	qs, gerr := sm.getOrCreateQueueSegmentsAt("", "orders")
	require.Error(t, gerr,
		"the empty queue name was accepted; no queue can be named \"\" because an empty "+
			"queue.declare is given a generated amq.gen.* name, so \"\" here was invented by a "+
			"damaged marker rather than by a client")
	require.Nil(t, qs)
}

// ---------------------------------------------------------------------------
// review-5 CRITICAL-2 — adopting a markerless legacy directory must never
// dispossess the queue that wrote it.
//
// os.Stat(".../queue") succeeds against an on-disk "Queue" on a folding volume,
// and segmentDirFor passed the name it ASKED for to segmentDirOwner rather than
// the name that EXISTS. So whichever case-variant declared first adopted the
// other's legacy directory, stamped its own name into the marker, and evicted
// the queue that actually wrote the data to a fresh empty directory.
//
// Pre-marker the two queues shared one directory and both read every record:
// wrong, but symmetric and non-destructive. The commit turned that into
// permanent reattribution, which is the regression these tests gate. The
// underlying non-injectivity stays registered and is NOT fixed here.
// ---------------------------------------------------------------------------

// volumeFoldsCase reports whether this volume resolves a differently-cased
// spelling to the same entry. The premise is measured, not assumed, so the
// tests below skip honestly on a case-sensitive volume instead of passing
// vacuously (canon rule 11).
func volumeFoldsCase(t *testing.T, dir string) bool {
	t.Helper()
	probe := filepath.Join(dir, "FoldPremiseProbe")
	require.NoError(t, os.MkdirAll(probe, 0755))
	defer func() { _ = os.RemoveAll(probe) }()

	_, err := os.Stat(filepath.Join(dir, "foldpremiseprobe"))
	return err == nil
}

// TestSegmentDir_LegacyDirectoryIsNeverStolenByACaseVariant is the promoted
// form of the review's probe H.
func TestSegmentDir_LegacyDirectoryIsNeverStolenByACaseVariant(t *testing.T) {
	root := t.TempDir()
	segRoot := filepath.Join(root, "segments")
	require.NoError(t, os.MkdirAll(segRoot, 0755))

	if !volumeFoldsCase(t, segRoot) {
		t.Skip("this volume is case-sensitive, so the folding defect is not reachable here")
	}

	// A legacy, markerless directory written by a build that predates markers.
	// Its NAME is its owner: queue "Queue" wrote it.
	legacy := filepath.Join(segRoot, "Queue")
	require.NoError(t, os.MkdirAll(legacy, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(legacy, "00000000000000000001.seg"), []byte{}, 0644))

	sm, err := NewSegmentManager(root)
	require.NoError(t, err)
	defer func() { _ = sm.Close() }()

	// The case variant declares FIRST. This goes through getOrCreateQueueSegments,
	// not segmentDirFor alone, because the destructive step is the marker STAMP
	// that follows resolution — and because comparing the returned name to
	// "Queue" would prove nothing: the buggy path returns the string "queue",
	// which the filesystem then resolves to the "Queue" directory anyway. What
	// the directory IS matters, not what it is called.
	variant, verr := sm.getOrCreateQueueSegments("queue")
	require.NoError(t, verr,
		"queue \"queue\" must still get a usable directory; refusing to serve it would pin the "+
			"shared WAL for every queue in the same file, and would recover nothing extra")

	legacyInfo, lerr := os.Stat(legacy)
	require.NoError(t, lerr)
	variantInfo, verr2 := os.Stat(variant.dataDir)
	require.NoError(t, verr2)
	require.False(t, os.SameFile(legacyInfo, variantInfo),
		"queue \"queue\" was given queue \"Queue\"'s legacy directory because os.Stat folded the "+
			"spelling; the name asked for is not evidence about the name that exists")

	// The load-bearing assertion: the legacy directory is untouched. Stamping a
	// marker into it is what dispossesses its owner.
	_, present, merr := readSegmentDirMarker(legacy)
	require.NoError(t, merr)
	require.False(t, present,
		"a marker was stamped into a legacy directory this queue did not write; the queue that "+
			"DID write it is now refused by its own directory and evicted to an empty one")

	// And the rightful owner still gets it afterwards, whatever the declaration
	// order was — the records it checkpointed are still reachable from it.
	owner, oerr := sm.getOrCreateQueueSegments("Queue")
	require.NoError(t, oerr)
	ownerInfo, oerr2 := os.Stat(owner.dataDir)
	require.NoError(t, oerr2)
	require.True(t, os.SameFile(legacyInfo, ownerInfo),
		"the queue whose name the legacy directory spells was evicted from it; its checkpointed "+
			"durable messages are now attributed to the case variant that declared first")
}

// TestSegmentDir_LegacyDirectoryIsStillAdoptedWhenTheSpellingMatches is the
// constraint-(c) guard. The fix refuses to adopt on a FOLDED match; it must not
// refuse an exact one, or every legacy directory on every platform is stranded
// and the cure is worse than the disease.
func TestSegmentDir_LegacyDirectoryIsStillAdoptedWhenTheSpellingMatches(t *testing.T) {
	root := t.TempDir()
	segRoot := filepath.Join(root, "segments")
	require.NoError(t, os.MkdirAll(segRoot, 0755))

	for _, queueName := range []string{"orders", "Queue", "50%off", "%2f"} {
		legacy := filepath.Join(segRoot, queueName)
		require.NoError(t, os.MkdirAll(legacy, 0755))

		sm, err := NewSegmentManager(root)
		require.NoError(t, err)

		dir, derr := sm.segmentDirFor(queueName)
		require.NoError(t, derr, "queue %q", queueName)
		require.Equal(t, queueName, dir,
			"the legacy directory named exactly %q was not adopted by the queue that wrote it; "+
				"adoption must turn on the real entry's spelling, and this one matches byte for byte",
			queueName)
		require.NoError(t, sm.Close())
	}
}

// TestSegmentDir_CaseVariantsGetDistinctDirectoriesOnANewVolume guards the
// negative result the review measured and certified: on a folding volume the
// NEW-write path is already correct and is an improvement on the pre-marker
// build. It depends on a markered mismatch continuing to probe onward rather
// than faulting, so it is the test that fails if a future fix makes the
// markered branch fail closed.
func TestSegmentDir_CaseVariantsGetDistinctDirectoriesOnANewVolume(t *testing.T) {
	root := t.TempDir()
	segRoot := filepath.Join(root, "segments")
	require.NoError(t, os.MkdirAll(segRoot, 0755))

	if !volumeFoldsCase(t, segRoot) {
		t.Skip("this volume is case-sensitive, so the folding defect is not reachable here")
	}

	sm, err := NewSegmentManager(root)
	require.NoError(t, err)
	defer func() { _ = sm.Close() }()

	upper, uerr := sm.getOrCreateQueueSegments("Queue")
	require.NoError(t, uerr)
	require.NotNil(t, upper)

	lower, lerr := sm.getOrCreateQueueSegments("queue")
	require.NoError(t, lerr,
		"a case variant could not be served at all; the markered-mismatch branch must probe onward, "+
			"not fail closed")
	require.NotNil(t, lower)

	require.NotEqual(t, upper.dataDir, lower.dataDir,
		"two distinct AMQP queues share one segment directory on a new volume")

	for _, qs := range []*QueueSegments{upper, lower} {
		name, present, merr := readSegmentDirMarker(qs.dataDir)
		require.NoError(t, merr)
		require.True(t, present, "a directory this build created has no marker: %s", qs.dataDir)
		require.Equal(t, qs.queueName, name, "directory %s names the wrong owner", qs.dataDir)
	}
}

// ---------------------------------------------------------------------------
// review-7 MINOR-2/3/4/6 — regressions on the fix itself.
//
// Each predicate below was mutation-proven RED against the state review-7
// measured; the arms, the assertion that fired and its message are recorded in
// .notes/loop-2/fix-5.md.
// ---------------------------------------------------------------------------

// segmentDirEntries lists the directory entries under a segments root.
func segmentDirEntries(t *testing.T, segRoot string) []string {
	t.Helper()
	entries, err := os.ReadDir(segRoot)
	require.NoError(t, err)
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}

// TestSegmentMarker_EmptyNameCreatesNoDirectoryAndLeaksNone gates review-7
// MINOR-2 and MINOR-3 together, because they are one defect seen twice: the
// empty-name guard sat downstream of segmentDirFor, which had already mkdir'd a
// candidate by the time the guard ran.
//
// MINOR-2 was the fault text — it claimed "nothing on disk is modified" while a
// directory had just been created. MINOR-3 was the accumulation: performCheckpoint
// retries the whole file, and each retry probed one candidate further, so a
// repeated empty-name checkpoint left %, %-1, %-2 ... up to segmentDirProbeLimit.
// Neither is fixable by rewording; both disappear when the guard runs first.
func TestSegmentMarker_EmptyNameCreatesNoDirectoryAndLeaksNone(t *testing.T) {
	root := t.TempDir()
	sm, err := NewSegmentManager(root)
	require.NoError(t, err)
	defer func() { _ = sm.Close() }()

	segRoot := filepath.Join(root, "segments")
	require.Empty(t, segmentDirEntries(t, segRoot),
		"PREMISE: the segments root must start empty or this fixture proves nothing")

	// One attempt: the fault must be true when it says nothing was modified.
	cerr := writeLegacyBatchForTest(sm, "", segTestMessages("", []uint64{1000}))
	require.Error(t, cerr)
	require.Empty(t, segmentDirEntries(t, segRoot),
		"the empty-name refusal created a directory on disk; the guard is downstream of "+
			"segmentDirFor's mkdir, so every fault it raises misstates what it did (review-7 MINOR-2)")

	// Many attempts: the retry shape performCheckpoint actually produces.
	for i := 0; i < 70; i++ {
		require.Error(t, writeLegacyBatchForTest(sm, "", segTestMessages("", []uint64{uint64(2000 + i)})))
	}
	require.Empty(t, segmentDirEntries(t, segRoot),
		"repeated empty-name checkpoints leaked directories; each retry probed one candidate "+
			"further and mkdir'd it, up to segmentDirProbeLimit (review-7 MINOR-3)")
}

// TestSegmentMarker_MarkerlessGeneratedDirIsNotAMetricLabel gates review-7
// MINOR-4. QueueNameForSegmentDir closed the unusable-marker route but still
// answered (hexName, true) for a markerless directory carrying a generated name
// — verbatim the hex label its own doc comment says the bool exists to prevent.
//
// The state is the residue of segmentDirFor's mkdir-before-marker window, which
// is reachable by a crash there or by any marker-write failure.
func TestSegmentMarker_MarkerlessGeneratedDirIsNotAMetricLabel(t *testing.T) {
	root := t.TempDir()
	sm, err := NewSegmentManager(root)
	require.NoError(t, err)
	defer func() { _ = sm.Close() }()

	segRoot := filepath.Join(root, "segments")

	// Exactly what segmentDirFor leaves behind before the marker is written.
	dirName, derr := sm.segmentDirFor("a/b")
	require.NoError(t, derr)
	require.Equal(t, segmentDirEncoded("a/b"), dirName,
		"PREMISE: the name must be generated, not literal, or this fixture tests nothing")
	_, present, merr := readSegmentDirMarker(filepath.Join(segRoot, dirName))
	require.NoError(t, merr)
	require.False(t, present,
		"PREMISE: segmentDirFor must return before the marker is written, or the window this "+
			"fixture describes does not exist")

	name, attributed := QueueNameForSegmentDir(segRoot, dirName)
	require.False(t, attributed,
		"a markerless directory holding no segments was attributed; it is the residue of the "+
			"mkdir-before-marker window, not a queue, and labelling it puts a hex string on a "+
			"metric series (review-7 MINOR-4)")
	require.Empty(t, name)

	// The other half must keep working: a markerless directory that HOLDS
	// segments is a genuine legacy directory and its name IS its owner.
	legacy := filepath.Join(segRoot, "orders")
	require.NoError(t, os.MkdirAll(legacy, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(legacy, "00000000000000000001.seg"), []byte{}, 0644))
	legacyName, legacyOK := QueueNameForSegmentDir(segRoot, "orders")
	require.True(t, legacyOK,
		"a legacy markerless directory holding segments must still be attributed, or every "+
			"pre-marker deployment loses its segment metrics")
	require.Equal(t, "orders", legacyName)
}

// TestSegmentDir_UnlistableSegmentsRootIsReported gates review-7 MINOR-6. When
// the segments root cannot be listed, every literal candidate is skipped and the
// queue quietly gets a generated directory instead of its own legacy one — the
// same stranding shape as MAJOR-1, and it was the one branch in this loop that
// logged nothing at all.
//
// The injection is mode 0311 — write and traverse, no read. Traverse lets
// os.Stat reach the legacy candidate, write lets the generated candidate be
// created, and the missing read bit is what defeats Readdirnames. Mode 0111
// would be wrong: without the write bit the generated candidate cannot be
// mkdir'd either, so segmentDirFor exhausts every candidate and faults, and the
// fixture would be measuring a refusal rather than the silent strand.
func TestSegmentDir_UnlistableSegmentsRootIsReported(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: permission bits do not restrict access, so the injection cannot work")
	}

	root := t.TempDir()
	segRoot := filepath.Join(root, "segments")
	require.NoError(t, os.MkdirAll(filepath.Join(segRoot, "orders"), 0755))

	sm, err := NewSegmentManager(root)
	require.NoError(t, err)
	defer func() { _ = sm.Close() }()

	logger := &recordingLogger{}
	sm.SetLogger(logger)

	require.NoError(t, os.Chmod(segRoot, 0311))
	defer func() { _ = os.Chmod(segRoot, 0755) }()

	// Premise: the injection must defeat the listing while leaving the candidate
	// reachable, or the branch under test is never entered (canon rule 11).
	_, lerr := loadDirEntryNames(segRoot)
	require.Error(t, lerr,
		"the injection no longer blocks the directory listing, so this fixture gates nothing")
	_, serr := os.Stat(filepath.Join(segRoot, "orders"))
	require.NoError(t, serr,
		"the injection also blocked traversal, so segmentDirFor never reaches the listing branch")

	dir, derr := sm.segmentDirFor("orders")
	require.NoError(t, derr, "the queue must still get a usable directory")
	require.NotEqual(t, "orders", dir,
		"PREMISE: an unprovable candidate must be skipped; if it were adopted this fixture "+
			"would be asserting the wrong branch")

	var found bool
	for _, m := range logger.messages() {
		if strings.Contains(m, "could not be listed") {
			found = true
		}
	}
	require.True(t, found,
		"the queue was stranded on a generated directory with no operator-visible line; the two "+
			"sibling refusal branches both log and this one did not (review-7 MINOR-6). Logged: %v",
		logger.messages())
}
