package storage

// WAL file-incarnation contract.
//
// QueueWAL.fileNum is an atomic.Uint64 whose only mutation site in the whole
// repository is `qw.fileNum.Add(1)` in openNextFile, and it starts at zero on
// every construction. So every boot opens 00000000000000000001.wal, with
// O_APPEND, on top of whatever a previous incarnation left there. Nothing at
// boot reconstructs oldFiles, currentFileOffsets, offsetIndex or ackBitmap.
//
// The consequence these tests pin: when the reopened file eventually rolls,
// rollFile registers it in oldFiles carrying ONLY this incarnation's offset
// bitmap, so tryReclaimOldFiles evaluates "every message in this file is acked"
// against a set that does not contain the previous incarnation's records — and
// os.Remove()s a file full of confirmed, unacknowledged durable messages.
//
// These are correctness tests. The cleanup path is driven directly instead of
// being waited for, so nothing here depends on which of two same-period tickers
// the runtime services first (canon rule 9: assert what happens, not how fast).

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/maxpert/amqp-go/protocol"
)

// ---------------------------------------------------------------------------
// fixture
// ---------------------------------------------------------------------------

const (
	// incarnationWALFileSize is small enough that a few hundred small records
	// roll the file inside a test, and large enough that the first
	// incarnation's handful of records do NOT roll it. Both halves matter and
	// both are asserted at runtime below rather than assumed here.
	incarnationWALFileSize = 32 * 1024

	incarnationCanaryQueue = "canary-queue"
	incarnationLaterQueue  = "later-queue"
)

// incarnationWALConfig disables both background reclamation loops (cleanup and
// checkpoint) so that this test decides when reclamation runs. In production
// these two tick at the same 5-minute period under config.DefaultConfig() and
// race each other for the file; a test that waited on that race would be a coin
// flip, and a green coin flip is not evidence.
func incarnationWALConfig() WALConfig {
	cfg := DefaultWALConfig()
	cfg.FileSize = incarnationWALFileSize
	cfg.CleanupInterval = time.Hour
	cfg.CheckpointInterval = time.Hour
	return cfg
}

func incarnationSegmentConfig() SegmentConfig {
	cfg := DefaultSegmentConfig()
	cfg.CompactionInterval = time.Hour
	cfg.CheckpointInterval = time.Hour
	return cfg
}

// walIncarnation is one broker lifetime over a fixed data directory: a WAL
// manager plus the segment manager production wires into it
// (NewDisruptorStorageWithEngineConfig does exactly this pairing).
type walIncarnation struct {
	t   *testing.T
	wal *WALManager
	seg *SegmentManager

	// sharedDir is the directory the shared WAL actually writes into, read off
	// the manager itself rather than reconstructed from path literals here. The
	// layout is dataDir/wal/shared, and a test that hardcoded that would keep
	// reporting "no WAL files" — indistinguishable from a pass — if it ever
	// moved. Captured at boot because Close() nils out sharedWAL.
	sharedDir string
}

func bootIncarnation(t *testing.T, dir string) *walIncarnation {
	t.Helper()
	wal, err := NewWALManagerWithConfig(dir, incarnationWALConfig())
	require.NoError(t, err)
	seg, err := NewSegmentManagerWithConfig(dir, incarnationSegmentConfig())
	require.NoError(t, err)
	wal.SetSegmentManager(seg)
	return &walIncarnation{t: t, wal: wal, seg: seg, sharedDir: wal.sharedWAL.dataDir}
}

func (in *walIncarnation) close() {
	in.t.Helper()
	require.NoError(in.t, in.wal.Close())
	require.NoError(in.t, in.seg.Close())
}

// write performs one durable write. WALManager.Write returns only after the
// record has been fsynced, so a nil return is the broker's durability promise
// — the same promise a publisher confirm carries.
func (in *walIncarnation) write(queue string, offset uint64, body string) {
	in.t.Helper()
	require.NoError(in.t, in.wal.Write(queue, &protocol.Message{
		Exchange:     "",
		RoutingKey:   queue,
		Body:         []byte(body),
		DeliveryMode: 2,
	}, offset), "durable write of offset %d", offset)
}

// walFilesOnDisk lists the WAL files present in the shared WAL directory.
// sharedDir must come from walIncarnation.sharedDir (i.e. from the manager),
// never from a path literal.
func walFilesOnDisk(t *testing.T, sharedDir string) []string {
	t.Helper()
	entries, err := os.ReadDir(sharedDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		require.NoError(t, err)
	}
	var out []string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == WALFileExtension {
			out = append(out, e.Name())
		}
	}
	return out
}

// recoverableOffsets returns every offset a fresh boot over dir could still
// hand back to the broker, from EITHER durable tier.
//
// Both tiers are consulted on purpose: this is the contract ("a confirmed
// durable message is still recoverable"), not a claim about where it lives. A
// fix that migrates the previous incarnation's records into segments before
// reclaiming the file satisfies it just as well as a fix that stops reusing the
// file number, and the test must not pick the implementation for the
// implementer.
func recoverableOffsets(t *testing.T, dir, queue string) map[uint64]bool {
	t.Helper()

	in := bootIncarnation(t, dir)
	defer in.close()

	found := make(map[uint64]bool)

	fromWAL, err := in.wal.RecoverFromWAL()
	require.NoError(t, err)
	for _, rm := range fromWAL {
		if rm.QueueName == queue {
			found[rm.Offset] = true
		}
	}

	fromSegments, err := in.seg.RecoverFromSegments()
	require.NoError(t, err)
	for _, rm := range fromSegments[queue] {
		found[rm.Offset] = true
	}
	return found
}

// waitForAcksApplied retains the old test hook's name, but acknowledgement is
// now synchronous. Verify the physical records rather than a cleanup goroutine.
func waitForAcksApplied(t *testing.T, wal *WALManager, offsets []uint64) {
	t.Helper()
	wal.sharedWAL.offsetIndexMutex.RLock()
	defer wal.sharedWAL.offsetIndexMutex.RUnlock()
	for _, off := range offsets {
		found := false
		for key, copies := range wal.sharedWAL.recordCopies {
			if key.deliveryTag != off {
				continue
			}
			for _, r := range copies {
				found = true
				require.True(t, r.acked.Load(), "physical copy for tag %d is not acknowledged", off)
			}
		}
		require.True(t, found, "no physical copy found for tag %d", off)
	}
}

// fillUntilRoll writes acknowledged records for `queue` starting at
// `firstOffset` until the WAL rolls at least once, and returns the offsets it
// wrote. It fails rather than looping forever if no roll happens, because "the
// file rolled" is the premise of everything downstream.
func (in *walIncarnation) fillUntilRoll(queue string, firstOffset uint64) []uint64 {
	in.t.Helper()

	body := string(make([]byte, 128))
	var written []uint64
	const maxRecords = 5000
	for i := 0; i < maxRecords; i++ {
		off := firstOffset + uint64(i)
		in.write(queue, off, body)
		in.wal.Acknowledge(queue, off)
		written = append(written, off)

		in.wal.sharedWAL.oldFilesMutex.RLock()
		rolled := len(in.wal.sharedWAL.oldFiles)
		in.wal.sharedWAL.oldFilesMutex.RUnlock()
		if rolled > 0 {
			return written
		}
	}
	in.t.Fatalf("PREMISE BROKEN: the WAL did not roll after %d records at FileSize=%d; "+
		"nothing downstream of a roll can be under test", maxRecords, incarnationWALFileSize)
	return nil
}

// ---------------------------------------------------------------------------
// C-2 primary: a previous incarnation's confirmed records must survive
// ---------------------------------------------------------------------------

// TestWALIncarnation_PriorIncarnationRecordsSurviveReclamation is the primary
// C-2 assertion.
//
// Contract: a durable record whose Write() returned nil — the broker's
// durability promise, the thing a publisher confirm is built on — and which has
// NOT been acknowledged must still be recoverable after a later incarnation of
// the broker runs its WAL reclamation over the same data directory.
//
// Note what the first incarnation does here: it shuts down GRACEFULLY, and its
// file never rolls. That is enough. performCheckpoint's own contract is "only
// operates on old/rolled files — never touches the current active file", so a
// clean Close() leaves an unrolled file, with its unacknowledged records,
// exactly where the next boot will reopen it with O_APPEND. The claim that this
// defect requires an unclean stop holds only for the case where the first
// incarnation had already rolled.
func TestWALIncarnation_PriorIncarnationRecordsSurviveReclamation(t *testing.T) {
	dir := t.TempDir()

	// --- incarnation A: confirmed durable records, never acknowledged --------
	canaries := []uint64{101, 102, 103, 104, 105}
	a := bootIncarnation(t, dir)
	for _, off := range canaries {
		a.write(incarnationCanaryQueue, off, "canary")
	}
	filesAfterA := walFilesOnDisk(t, a.sharedDir)
	require.NotEmpty(t, filesAfterA, "incarnation A must have left at least one WAL file")
	a.wal.sharedWAL.oldFilesMutex.RLock()
	rolledInA := len(a.wal.sharedWAL.oldFiles)
	a.wal.sharedWAL.oldFilesMutex.RUnlock()
	require.Zero(t, rolledInA,
		"PREMISE BROKEN: incarnation A rolled its file, so its records were eligible for "+
			"checkpoint at Close and this test is no longer exercising the reopen-and-append case")
	a.close()

	require.Equal(t, filesAfterA, walFilesOnDisk(t, a.sharedDir),
		"PREMISE BROKEN: a graceful Close of an incarnation that never rolled must leave its "+
			"active WAL file on disk; if it does not, the starting state of this test is wrong")

	// The promise, restated as a fact: at this instant the canaries are
	// recoverable. Anything that is not recoverable AFTER the next incarnation
	// runs was therefore destroyed by it, not missing all along.
	beforeReuse := recoverableOffsets(t, dir, incarnationCanaryQueue)
	for _, off := range canaries {
		require.True(t, beforeReuse[off],
			"PREMISE BROKEN: canary offset %d was not recoverable before the second "+
				"incarnation ran, so this test could not detect its destruction", off)
	}
	t.Logf("PREMISE: %d confirmed durable canaries recoverable after incarnation A; WAL files on disk: %v",
		len(canaries), filesAfterA)

	// --- incarnation B: writes and acknowledges its own records, then rolls --
	b := bootIncarnation(t, dir)
	bOffsets := b.fillUntilRoll(incarnationLaterQueue, 1_000_000)
	waitForAcksApplied(t, b.wal, bOffsets)

	// Drive reclamation deterministically instead of waiting for the ticker.
	b.wal.sharedWAL.tryReclaimOldFiles()
	filesAfterCleanup := walFilesOnDisk(t, b.sharedDir)
	b.close()

	// --- incarnation C: what is still recoverable? --------------------------
	after := recoverableOffsets(t, dir, incarnationCanaryQueue)

	var lost []uint64
	for _, off := range canaries {
		if !after[off] {
			lost = append(lost, off)
		}
	}
	t.Logf("RESULT: WAL files after B's reclamation: %v; canary offsets still recoverable: %d of %d (lost: %v)",
		filesAfterCleanup, len(canaries)-len(lost), len(canaries), lost)

	require.Empty(t, lost,
		"DATA DESTRUCTION: %d of %d confirmed, unacknowledged durable records written by a "+
			"previous incarnation are no longer recoverable from either the WAL or the segments "+
			"after the next incarnation ran WAL reclamation. Lost offsets: %v",
		len(lost), len(canaries), lost)
}

// TestWALIncarnation_ReclaimedFileHeldOnlyAckedRecords pins the safety
// precondition of the reclamation decision itself, one level below the
// end-to-end test above.
//
// Contract: tryReclaimOldFiles may physically remove a WAL file only when EVERY
// record actually present in that file is acknowledged. The production
// predicate tests `info.offsets ⊆ ackBitmap`, where info.offsets is whatever
// currentFileOffsets happened to accumulate during THIS incarnation — so the
// predicate is sound only if that bitmap is a complete inventory of the file.
// It is not, for any file this process did not create from empty.
//
// Stating it as "the file it decided to delete really did contain only acked
// records" keeps the test independent of how the defect is fixed: restoring
// fileNum, registering pre-existing files as un-reclaimable, or rebuilding the
// offset bitmap from a scan all satisfy it.
func TestWALIncarnation_ReclaimedFileHeldOnlyAckedRecords(t *testing.T) {
	dir := t.TempDir()

	canaries := []uint64{201, 202, 203}
	a := bootIncarnation(t, dir)
	for _, off := range canaries {
		a.write(incarnationCanaryQueue, off, "canary")
	}
	a.close()

	b := bootIncarnation(t, dir)
	defer b.close()
	bOffsets := b.fillUntilRoll(incarnationLaterQueue, 2_000_000)
	waitForAcksApplied(t, b.wal, bOffsets)

	// Evaluate the production predicate over every rolled file, and for each
	// file it would delete, compare the decision against what is physically in
	// the file.
	type verdict struct {
		path      string
		unacked   []uint64
		inventory int
	}
	var wouldDeleteWithUnacked []verdict
	inspected := 0

	qw := b.wal.sharedWAL
	qw.oldFilesMutex.Lock()
	for _, info := range qw.oldFiles {
		inspected++
		allAcked := info.inv != nil && info.inv.valid && len(info.inv.records) > 0 && info.inv.unacked.Load() == 0
		if !allAcked {
			continue
		}
		records, err := qw.scanWALFile(info.path)
		if err != nil {
			continue
		}
		v := verdict{path: filepath.Base(info.path), inventory: len(records)}
		for _, rec := range records {
			r := info.inv.records[rec.pos.position]
			if r == nil || !r.acked.Load() {
				v.unacked = append(v.unacked, rec.Offset)
			}
		}
		if len(v.unacked) > 0 {
			wouldDeleteWithUnacked = append(wouldDeleteWithUnacked, v)
		}
	}
	qw.oldFilesMutex.Unlock()

	require.NotZero(t, inspected,
		"PREMISE BROKEN: no rolled WAL file was available to evaluate, so this test "+
			"inspected nothing and its result means nothing")
	t.Logf("RESULT: inspected %d rolled WAL file(s); files judged fully-acked but physically "+
		"holding unacked records: %d (%+v)", inspected, len(wouldDeleteWithUnacked), wouldDeleteWithUnacked)

	require.Empty(t, wouldDeleteWithUnacked,
		"UNSOUND RECLAMATION: tryReclaimOldFiles would physically delete %d WAL file(s) it "+
			"judged fully acknowledged, each of which still contains unacknowledged durable "+
			"records: %+v", len(wouldDeleteWithUnacked), wouldDeleteWithUnacked)
}

// ---------------------------------------------------------------------------
// controls — these are green on the unfixed tree and must stay green
// ---------------------------------------------------------------------------

// TestWALIncarnation_SingleIncarnationReclamationControl is the A/B control
// that makes the two tests above attributable to the reuse of a previous
// incarnation's file: the identical write/ack/roll/reclaim sequence, run by a
// single incarnation that created every file it reclaims, must lose nothing.
//
// If this ever fails, the fixture is broken and no conclusion may be drawn from
// its siblings.
func TestWALIncarnation_SingleIncarnationReclamationControl(t *testing.T) {
	dir := t.TempDir()

	in := bootIncarnation(t, dir)
	canaries := []uint64{301, 302, 303}
	for _, off := range canaries {
		in.write(incarnationCanaryQueue, off, "canary")
	}
	offsets := in.fillUntilRoll(incarnationLaterQueue, 3_000_000)
	waitForAcksApplied(t, in.wal, offsets)
	in.wal.sharedWAL.tryReclaimOldFiles()
	in.close()

	after := recoverableOffsets(t, dir, incarnationCanaryQueue)
	var lost []uint64
	for _, off := range canaries {
		if !after[off] {
			lost = append(lost, off)
		}
	}
	t.Logf("CONTROL (single incarnation): canary offsets still recoverable: %d of %d (lost: %v)",
		len(canaries)-len(lost), len(canaries), lost)

	require.Empty(t, lost,
		"CONTROL FAILED: a single incarnation destroyed its own unacknowledged records (%v). "+
			"The previous incarnation is then not the variable under test and the sibling "+
			"tests prove nothing", lost)
}

// TestWALIncarnation_UnackedRolledFileIsNotReclaimedControl bounds the defect:
// reclamation is triggered by the all-acked predicate, not by the mere passage
// of a cleanup tick. A rolled file holding unacknowledged records of THIS
// incarnation survives reclamation.
//
// This is the control that keeps the primary tests honest about their
// mechanism: without it, "the file disappeared" would be compatible with
// "cleanup deletes rolled files unconditionally", which is a different defect
// with a different fix.
func TestWALIncarnation_UnackedRolledFileIsNotReclaimedControl(t *testing.T) {
	dir := t.TempDir()

	in := bootIncarnation(t, dir)
	defer in.close()

	// Write without acking until the file rolls.
	body := string(make([]byte, 128))
	var written []uint64
	rolled := false
	for i := 0; i < 5000 && !rolled; i++ {
		off := uint64(4_000_000 + i)
		in.write(incarnationLaterQueue, off, body)
		written = append(written, off)
		in.wal.sharedWAL.oldFilesMutex.RLock()
		rolled = len(in.wal.sharedWAL.oldFiles) > 0
		in.wal.sharedWAL.oldFilesMutex.RUnlock()
	}
	require.True(t, rolled,
		"PREMISE BROKEN: the WAL did not roll, so no reclamation decision was reachable")

	before := walFilesOnDisk(t, in.sharedDir)
	require.NotEmpty(t, before,
		"FIXTURE DEAD: no WAL files were found on disk, so comparing the directory before and "+
			"after reclamation compares nothing. This check exists because the first draft of "+
			"this test read the wrong directory and reported a green PASS on [] == [].")
	in.wal.sharedWAL.tryReclaimOldFiles()
	after := walFilesOnDisk(t, in.sharedDir)

	t.Logf("CONTROL (nothing acked): WAL files before reclamation %v, after %v", before, after)
	require.Equal(t, before, after,
		"CONTROL FAILED: reclamation removed a rolled WAL file none of whose %d records were "+
			"acknowledged; the trigger is then not the all-acked predicate the primary tests "+
			"attribute the destruction to", len(written))
}
