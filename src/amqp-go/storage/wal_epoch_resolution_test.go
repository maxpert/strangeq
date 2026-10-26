package storage

// R3 — candidate C's gate: can an ack in boot E2 name the epoch of a record
// written in boot E1, once that record has been checkpointed into a segment?
//
// Candidate C (Loop 3 Step 0, TASK C) proposes giving every record a durable
// uid (bootEpoch, tag), with the epoch stored ONCE IN THE WAL FILE HEADER
// rather than per record — which is what buys it zero per-publish cost. The
// consequence its own author named as the strongest argument against it:
//
//	the ack path knows the TAG, not the epoch, and must name the record's
//	ORIGINAL epoch. A record written in E1 and acked in E2 must be acked as
//	(E1, T). WAL-resident records resolve via
//	    offsetIndex -> offsetLocation.fileNum -> that file's header epoch
//	which is "machinery that already exists".
//
// and the failure mode if that resolution is wrong:
//
//	C names the ACKING epoch instead of the record's. Then (E2,T) never
//	matches the record's (E1,T), EVERY ACK SILENTLY BECOMES A NO-OP, the
//	feature is inert, and every test that only checks "nothing was destroyed"
//	stays GREEN.
//
// So the gate is: prove runnably that the resolution above still answers, in
// E2, for a record that E1 checkpointed into a segment. These tests execute
// that resolution rather than reasoning about it.
//
// Candidate C is NOT implemented and no production code is added here. What is
// under test is the RESOLUTION MECHANISM C would have to use, which does exist
// today: the offsetIndex map, its population in rebuildBootState/commitBatch,
// and its teardown in performCheckpoint.

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const epochResolutionQueue = "epoch-resolution-queue"

// epochResolution is the answer candidate C's ack path would get for one tag.
//
// It performs EXACTLY the lookup chain C prescribes, in order, and reports
// where the chain breaks. It deliberately does not fall back to anything: the
// whole question is whether the chain answers at all, and a probe that
// substituted a default would be answering a different question (and would be
// the very bug the gate exists to catch).
type epochResolution struct {
	indexed     bool   // offsetIndex held an entry for the tag
	fileNum     uint64 // offsetLocation.fileNum, if indexed
	fileOnDisk  bool   // the file that fileNum names still exists
	recoverable bool   // the record is still live in SOME durable tier
}

// resolveEpochLikeCandidateC runs C's tag -> file -> epoch chain for one tag.
//
// C stores the epoch in the WAL FILE HEADER, so the chain can only answer if
// BOTH links hold: offsetIndex must map the tag to a fileNum, AND that file
// must still exist to be read for its header. Either link missing means the
// ack path cannot name the record's original epoch.
func resolveEpochLikeCandidateC(t *testing.T, qw *QueueWAL, tag uint64) epochResolution {
	t.Helper()

	var r epochResolution

	qw.offsetIndexMutex.RLock()
	loc, ok := qw.offsetIndex[recordKey{queueName: epochResolutionQueue, deliveryTag: tag}]
	if ok {
		r.indexed = true
		r.fileNum = loc.pos.segmentID
	}
	qw.offsetIndexMutex.RUnlock()

	if r.indexed {
		path := filepath.Join(qw.dataDir, walFileName(r.fileNum))
		if _, err := os.Stat(path); err == nil {
			r.fileOnDisk = true
		}
	}
	return r
}

// walFileName rebuilds a stem the way openNextFile does. Kept next to the probe
// so the probe cannot drift from the production format silently; the round trip
// is asserted in the test below rather than trusted.
func walFileName(fileNum uint64) string {
	return fmt.Sprintf("%020d%s", fileNum, WALFileExtension)
}

// writeUnackedUntilRoll writes DURABLE, UNACKNOWLEDGED records until the WAL
// rolls, and returns their tags. Unacknowledged is the point: performCheckpoint
// moves exactly the unacked records into segments and drops the acked ones, so
// an acked fixture would checkpoint nothing and the test would be vacuous.
func (in *walIncarnation) writeUnackedUntilRoll(queue string, firstOffset uint64) []uint64 {
	in.t.Helper()

	body := string(make([]byte, 128))
	var written []uint64
	const maxRecords = 5000
	for i := 0; i < maxRecords; i++ {
		off := firstOffset + uint64(i)
		in.write(queue, off, body)
		written = append(written, off)

		in.wal.sharedWAL.oldFilesMutex.RLock()
		rolled := len(in.wal.sharedWAL.oldFiles)
		in.wal.sharedWAL.oldFilesMutex.RUnlock()
		if rolled > 0 {
			return written
		}
	}
	in.t.Fatalf("PREMISE BROKEN: the WAL did not roll after %d unacknowledged records at "+
		"FileSize=%d; performCheckpoint only ever operates on ROLLED files, so nothing "+
		"downstream of a checkpoint is under test", maxRecords, incarnationWALFileSize)
	return nil
}

// ---------------------------------------------------------------------------
// the gate
// ---------------------------------------------------------------------------

// TestCandidateC_EpochResolutionSurvivesCheckpointIntoSegment is candidate C's
// gate, stated as the property C needs to be TRUE.
//
// Contract C requires: for a record written in boot E1, checkpointed into a
// segment by E1, and still live, an ack arriving in boot E2 can resolve the
// record's ORIGINAL epoch through offsetIndex -> fileNum -> file header.
//
// If this test fails, candidate C cannot attribute acks to the segment tier
// through the file-header epoch, its §4.4 advantage over candidate A does not
// exist, and it needs the epoch propagated INTO the segment record — the same
// second attribution scheme whose cost was the argument against A.
func TestCandidateC_EpochResolutionSurvivesCheckpointIntoSegment(t *testing.T) {
	t.Skip("GATE VERDICT — THIS IS NOT A DEFECT IN THIS TEST, NOT A FLAKE, AND NOT STALE. " +
		"This test asserts the property candidate C REQUIRES in order to work at all, and it " +
		"was measured FAILING on purpose. Its failure IS the finding: candidate C's gate DOES " +
		"NOT CLEAR. Do not 'fix' it, do not weaken the assertion, and do not delete it. " +
		"\n\n" +
		"MECHANISM — STRUCTURAL, NOT A RACE. Candidate C stores a bootEpoch in the WAL FILE " +
		"HEADER, so its ack path must resolve tag -> offsetIndex -> offsetLocation.fileNum -> " +
		"that file's header, in order to name the epoch a record was ORIGINALLY written under. " +
		"offsetIndex is IN-MEMORY, and rebuildBootState repopulates it ONLY by scanning WAL " +
		"files that still exist in the directory. performCheckpoint moves a record into a " +
		"segment and then os.Remove()s its WAL file. Therefore NO BOOT AFTER THE CHECKPOINT " +
		"CAN EVER HOLD AN INDEX ENTRY for that record — for the ENTIRE segment tier, in every " +
		"subsequent boot. This is unreachable by construction, not a timing window, so it " +
		"cannot be closed by ordering, locking, or retry. " +
		"\n\n" +
		"DEFEATING performCheckpoint'S DELETE CHANGES NOTHING. The sibling test " +
		"TestCandidateC_OffsetIndexIsRebuiltOnlyFromSurvivingWALFiles hand-restores the " +
		"offsetIndex entry after the checkpoint, removing that cause entirely, and the entry " +
		"STILL does not survive the next boot (measured: indexed=false, wal_epoch_resolution_" +
		"test.go:279). So 'stop performCheckpoint deleting the entry' is not a fix. " +
		"\n\n" +
		"CONSEQUENCE: C's §4.4 claim — 'satisfied without a second attribution scheme; this is " +
		"where C beats A' — is FALSE. C needs the epoch propagated INTO the segment record, " +
		"which is the same second attribution scheme whose cost was the argument against " +
		"candidate A. C's failure mode is also DESTRUCTIVE, not merely inert: if resolution " +
		"falls back to the acking epoch, (E2,tag) is recorded, and boot E2 re-mints that same " +
		"tag (see TestDurableAckGap_Member4_TransientQueueRemintsAnAcknowledgedTagBand, 200 " +
		"measured collisions), so the ack destroys E2's own new record. " +
		"\n\n" +
		"IF YOU ARE REVISITING CANDIDATE C: delete this t.Skip (and nothing else). The test " +
		"MUST THEN PASS. If it still fails, candidate C is still dead and nothing has changed. " +
		"Passing requires giving a record an identity that survives the WAL->segment move — " +
		"the control TestCandidateC_WALResidentRecordResolvesAcrossABootControl stays green " +
		"throughout and proves the chain works for WAL-resident records, so a red here is " +
		"always about the checkpoint. Evidence: .notes/loop-3/step0.md TASK C and Loop 3 " +
		"Step 1's R3.")

	dir := t.TempDir()

	// ---- boot E1: write durable records, roll, checkpoint into segments -----
	e1 := bootIncarnation(t, dir)

	tags := e1.writeUnackedUntilRoll(epochResolutionQueue, 5_000_000)
	require.NotEmpty(t, tags)
	probe := tags[0]

	// The stem format the probe reconstructs must match the one production
	// writes, or every "file not on disk" answer below would be an artifact of
	// this test rather than a fact about the broker.
	filesNow := walFilesOnDisk(t, e1.sharedDir)
	require.NotEmpty(t, filesNow)
	require.Contains(t, filesNow, walFileName(1),
		"FIXTURE BROKEN: this test rebuilds WAL stems as %q but the directory holds %v; the "+
			"probe's 'file is gone' answers would be meaningless", walFileName(1), filesNow)

	// PREMISE: before the checkpoint, C's resolution DOES answer. Without this
	// the test could not tell "C's chain is broken by the checkpoint" from
	// "C's chain never worked in this fixture".
	beforeCP := resolveEpochLikeCandidateC(t, e1.wal.sharedWAL, probe)
	require.True(t, beforeCP.indexed,
		"PREMISE BROKEN: tag %d is not in offsetIndex even BEFORE any checkpoint, so this "+
			"fixture cannot demonstrate that the checkpoint is what breaks the resolution", probe)
	require.True(t, beforeCP.fileOnDisk,
		"PREMISE BROKEN: tag %d resolves to file %d which is not on disk, before any "+
			"checkpoint ran", probe, beforeCP.fileNum)
	t.Logf("PREMISE (E1, pre-checkpoint): tag %d -> fileNum %d, file on disk=%v — C's chain answers",
		probe, beforeCP.fileNum, beforeCP.fileOnDisk)

	filesBeforeCP := walFilesOnDisk(t, e1.sharedDir)
	e1.wal.sharedWAL.performCheckpoint()
	filesAfterCP := walFilesOnDisk(t, e1.sharedDir)

	require.NotEqual(t, filesBeforeCP, filesAfterCP,
		"PREMISE BROKEN: performCheckpoint removed no WAL file (before %v, after %v), so no "+
			"record was moved into a segment and the operation under test did not happen",
		filesBeforeCP, filesAfterCP)
	t.Logf("PREMISE: performCheckpoint ran; WAL files %v -> %v", filesBeforeCP, filesAfterCP)

	// The record must still be LIVE. If the checkpoint simply destroyed it there
	// would be nothing left to acknowledge and the gate would be moot.
	inSegments, err := e1.seg.RecoverFromSegments()
	require.NoError(t, err)
	segTags := make(map[uint64]bool)
	for _, rm := range inSegments[epochResolutionQueue] {
		segTags[rm.Offset] = true
	}
	require.True(t, segTags[probe],
		"PREMISE BROKEN: tag %d is not in the segment tier after the checkpoint, so it was "+
			"not 'checkpointed into a segment' and this test is not exercising C's §4.4 claim",
		probe)
	t.Logf("PREMISE: tag %d is live in the SEGMENT tier after checkpoint (%d segment records)",
		probe, len(inSegments[epochResolutionQueue]))

	afterCP := resolveEpochLikeCandidateC(t, e1.wal.sharedWAL, probe)
	t.Logf("RESULT (E1, post-checkpoint): tag %d indexed=%v fileNum=%d fileOnDisk=%v",
		probe, afterCP.indexed, afterCP.fileNum, afterCP.fileOnDisk)

	e1.close()

	// ---- boot E2: the ack arrives here -------------------------------------
	e2 := bootIncarnation(t, dir)
	defer e2.close()

	// The record is still live from E2's point of view — this is what makes the
	// ack a real ack rather than an ack of nothing.
	stillLive := recoverableOffsets(t, dir, epochResolutionQueue)
	require.True(t, stillLive[probe],
		"PREMISE BROKEN: tag %d is not recoverable in E2 from either tier, so there is no "+
			"record for an E2 ack to cancel and the gate is moot", probe)

	got := resolveEpochLikeCandidateC(t, e2.wal.sharedWAL, probe)
	t.Logf("RESULT (E2, the acking boot): tag %d indexed=%v fileNum=%d fileOnDisk=%v recoverable=%v",
		probe, got.indexed, got.fileNum, got.fileOnDisk, stillLive[probe])

	require.True(t, got.indexed,
		"CANDIDATE C'S GATE FAILS: in boot E2, tag %d has NO offsetIndex entry, so the ack "+
			"path cannot resolve tag -> fileNum -> file-header epoch and cannot name the "+
			"record's ORIGINAL epoch E1. The record is still live (recoverable=%v) and would "+
			"be acked under E2 instead, which never matches its (E1, tag) uid: every such ack "+
			"is a silent no-op and the feature is inert for the whole segment tier. "+
			"offsetIndex is in-memory and rebuildBootState repopulates it ONLY from WAL files "+
			"that still exist; a checkpointed record's WAL file has been os.Remove()d, so this "+
			"is unreachable by construction, not a race.",
		probe, stillLive[probe])

	require.True(t, got.fileOnDisk,
		"CANDIDATE C'S GATE FAILS: in boot E2, tag %d resolves to fileNum %d but that file is "+
			"not on disk, so its header — where candidate C stores the epoch — cannot be read",
		probe, got.fileNum)
}

// TestCandidateC_WALResidentRecordResolvesAcrossABootControl is the A/B control
// that makes the gate result attributable to the CHECKPOINT.
//
// Identical shape to the gate test — write in E1, close, resolve in E2 — with
// exactly one variable removed: the record is never checkpointed, so its WAL
// file survives. If C's chain answers here, then "indexed=false" in the gate
// test cannot be explained by "the probe never works across a boot" or by
// "rebuildBootState does not populate offsetIndex at all", and the only
// remaining difference is the checkpoint.
//
// This control MUST pass. If it ever fails, the gate test proves nothing and
// its verdict must be withdrawn.
func TestCandidateC_WALResidentRecordResolvesAcrossABootControl(t *testing.T) {
	dir := t.TempDir()

	e1 := bootIncarnation(t, dir)

	// A handful of records, deliberately NOT enough to roll the file: nothing
	// enters oldFiles, so performCheckpoint has nothing to operate on even at
	// Close, and the record stays WAL-resident.
	const probe = uint64(7_000_000)
	e1.write(epochResolutionQueue, probe, "wal-resident")

	e1.wal.sharedWAL.oldFilesMutex.RLock()
	rolled := len(e1.wal.sharedWAL.oldFiles)
	e1.wal.sharedWAL.oldFilesMutex.RUnlock()
	require.Zero(t, rolled,
		"PREMISE BROKEN: the file rolled, so this control is no longer the no-checkpoint arm")

	e1.close()

	e2 := bootIncarnation(t, dir)
	defer e2.close()

	got := resolveEpochLikeCandidateC(t, e2.wal.sharedWAL, probe)
	t.Logf("CONTROL (no checkpoint): tag %d indexed=%v fileNum=%d fileOnDisk=%v",
		probe, got.indexed, got.fileNum, got.fileOnDisk)

	require.True(t, got.indexed,
		"CONTROL FAILED: C's tag -> fileNum chain does not answer in E2 even for a record "+
			"whose WAL file was never checkpointed and is still on disk. The checkpoint is "+
			"then NOT the variable under test and the gate verdict proves nothing")
	require.True(t, got.fileOnDisk,
		"CONTROL FAILED: tag %d resolved to fileNum %d but that file is not on disk, though "+
			"nothing was ever checkpointed", probe, got.fileNum)
}

// TestCandidateC_OffsetIndexIsRebuiltOnlyFromSurvivingWALFiles isolates the
// MECHANISM behind the gate result, one level below the end-to-end test.
//
// It matters because there are two independent reasons the resolution can fail
// in E2, and they have different fixes:
//
//	(a) performCheckpoint deletes the offsetIndex entries when it reclaims the
//	    file. In-process only — could in principle be "fixed" by not deleting.
//	(b) offsetIndex is IN-MEMORY and rebuildBootState repopulates it only from
//	    WAL files that still exist. A checkpointed record's file is gone, so no
//	    later boot can ever index it, whatever (a) does.
//
// (b) is the fatal one and (a) cannot compensate for it. Pinning it separately
// stops a future reader concluding that keeping the entries would rescue C.
func TestCandidateC_OffsetIndexIsRebuiltOnlyFromSurvivingWALFiles(t *testing.T) {
	t.Skip("GATE VERDICT — THIS IS NOT A DEFECT IN THIS TEST, NOT A FLAKE, AND NOT STALE. " +
		"This is the MECHANISM half of candidate C's gate (the end-to-end half is " +
		"TestCandidateC_EpochResolutionSurvivesCheckpointIntoSegment, skipped with the same " +
		"verdict). It asserts the property C requires and was measured FAILING on purpose. " +
		"Its failure IS the finding. Do not 'fix' it and do not weaken the assertion. " +
		"\n\n" +
		"MECHANISM — STRUCTURAL, NOT A RACE. There are two independent reasons C's " +
		"tag -> file -> epoch chain cannot answer in a later boot, and they have DIFFERENT " +
		"fixes, which is why this test exists separately: (a) performCheckpoint deletes the " +
		"offsetIndex entries when it reclaims the file — in-process only, and in principle " +
		"'fixable' by not deleting; (b) offsetIndex is IN-MEMORY and rebuildBootState " +
		"repopulates it ONLY from WAL files that still exist, and a checkpointed record's file " +
		"has been os.Remove()d. (b) is the fatal one and (a) cannot compensate for it. " +
		"\n\n" +
		"THIS TEST PROVES (b) BY DEFEATING (a) COMPLETELY. It hand-restores the offsetIndex " +
		"entry after the checkpoint, so whatever performCheckpoint removed is back — and the " +
		"entry still does not survive the next boot (measured: indexed=false at " +
		"wal_epoch_resolution_test.go:279). It exists so that a future reader cannot conclude " +
		"that keeping the entries would rescue candidate C. IT WOULD NOT. " +
		"\n\n" +
		"IF YOU ARE REVISITING CANDIDATE C: delete this t.Skip (and nothing else). The test " +
		"MUST THEN PASS. If it still fails, candidate C is still dead and nothing has changed. " +
		"Evidence: .notes/loop-3/step0.md TASK C and Loop 3 Step 1's R3.")

	dir := t.TempDir()

	e1 := bootIncarnation(t, dir)
	tags := e1.writeUnackedUntilRoll(epochResolutionQueue, 6_000_000)
	probe := tags[0]

	e1.wal.sharedWAL.performCheckpoint()

	// Re-add the entry by hand, defeating (a) completely: whatever
	// performCheckpoint removed is now back.
	e1.wal.sharedWAL.offsetIndexMutex.Lock()
	e1.wal.sharedWAL.offsetIndex[recordKey{queueName: epochResolutionQueue, deliveryTag: probe}] = &segmentRecord{pos: recordPos{segmentID: 1, position: 0}}
	e1.wal.sharedWAL.offsetIndexMutex.Unlock()

	restored := resolveEpochLikeCandidateC(t, e1.wal.sharedWAL, probe)
	require.True(t, restored.indexed,
		"FIXTURE BROKEN: the hand-restored offsetIndex entry for tag %d is not readable", probe)
	require.False(t, restored.fileOnDisk,
		"PREMISE BROKEN: file %d is still on disk after the checkpoint, so this test is not "+
			"exercising the 'the file the index points at is gone' case", restored.fileNum)
	t.Logf("PREMISE (E1): offsetIndex entry hand-restored for tag %d -> fileNum %d, but the "+
		"file itself is gone (fileOnDisk=%v)", probe, restored.fileNum, restored.fileOnDisk)

	e1.close()

	e2 := bootIncarnation(t, dir)
	defer e2.close()

	got := resolveEpochLikeCandidateC(t, e2.wal.sharedWAL, probe)
	t.Logf("RESULT (E2): after a fresh boot, tag %d indexed=%v — the hand-restored entry did "+
		"not survive, because offsetIndex is in-memory and is rebuilt only from surviving "+
		"WAL files", probe, got.indexed)

	require.True(t, got.indexed,
		"MECHANISM CONFIRMED (b): offsetIndex is in-memory and rebuildBootState repopulates it "+
			"ONLY by scanning WAL files still present in the directory. The checkpointed "+
			"record's file was removed by performCheckpoint, so no boot after the checkpoint "+
			"can ever hold an index entry for tag %d — not preventing performCheckpoint's "+
			"delete would change nothing. Candidate C's tag -> file -> epoch chain is "+
			"structurally unavailable for the entire segment tier.", probe)
}
