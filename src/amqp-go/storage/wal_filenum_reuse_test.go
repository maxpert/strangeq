package storage

// R2 — is a WAL fileNum REUSABLE across incarnations?
//
// Loop 3 Step 0 recorded "finding A-extra" as DERIVED, not executed:
//
//	rebuildBootState stores maxNum = the highest stem CURRENTLY EXISTING, so
//	openNextFile's Add(1) yields maxNum+1. If the highest-numbered file is
//	reclaimed while a lower-numbered one survives, fileNum regresses and the
//	next incarnation recreates that number. Therefore (fileNum, filePosition)
//	— already materialised as offsetLocation — is not unique across
//	incarnations.
//
// That claim prices candidate A (physical-locator attribution): if a locator
// can be re-minted, A needs a generation field and an ack naming a locator can
// silently cancel a DIFFERENT record than the one it was issued for.
//
// These tests exist to decide it by execution rather than by reading. They
// drive the exact state the finding names — a lower-numbered file surviving
// while higher-numbered files are reclaimed — and then ask the two questions
// the finding actually turns on:
//
//	Q1  can reclamation ever remove the highest-numbered file on disk?
//	Q2  does the next incarnation ever open a file number a previous
//	    incarnation already used?
//
// Q2 is the contract. It is stated as "no incarnation reopens a used number"
// rather than "fileNum is monotone" so that a fix which persisted a counter,
// or which tombstoned reclaimed stems, would satisfy it just as well as the
// current derive-from-disk scheme.
//
// Both tests reuse the incarnation harness in wal_incarnation_reuse_test.go
// (bootIncarnation / incarnationWALConfig / walFilesOnDisk / waitForAcksApplied),
// which already pins FileSize small enough that a few hundred records roll.

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	filenumSurvivorQueue = "filenum-survivor-queue"
	filenumChurnQueue    = "filenum-churn-queue"
)

// walFileNumsOnDisk returns the parsed stem numbers of every WAL file in
// sharedDir. It parses with the production parser (parseWALFileNum) rather than
// with a local sscanf, so a change to the filename format cannot leave this
// test quietly reporting an empty set — which would look exactly like a pass.
func walFileNumsOnDisk(t *testing.T, sharedDir string) map[uint64]bool {
	t.Helper()
	out := make(map[uint64]bool)
	for _, name := range walFilesOnDisk(t, sharedDir) {
		num, ok := parseWALFileNum(name)
		require.True(t, ok,
			"FIXTURE BROKEN: %q is in the shared WAL directory with a .wal extension but "+
				"parseWALFileNum refuses it; this test cannot enumerate the file set it is "+
				"reasoning about", name)
		out[num] = true
	}
	return out
}

func maxOf(nums map[uint64]bool) uint64 {
	var max uint64
	for n := range nums {
		if n > max {
			max = n
		}
	}
	return max
}

// rollAcked writes and acknowledges records for `queue` starting at
// `firstOffset` until oldFiles holds at least `wantRolls` entries, and returns
// every offset it wrote. Unlike fillUntilRoll it does not stop at the first
// roll, because this fixture needs a rolled file whose records are ALL acked
// sitting above a rolled file that still holds an unacked record.
func (in *walIncarnation) rollAcked(queue string, firstOffset uint64, wantRolls int) []uint64 {
	in.t.Helper()

	body := string(make([]byte, 128))
	var written []uint64
	const maxRecords = 20000
	for i := 0; i < maxRecords; i++ {
		off := firstOffset + uint64(i)
		in.write(queue, off, body)
		in.wal.Acknowledge(queue, off)
		written = append(written, off)

		in.wal.sharedWAL.oldFilesMutex.RLock()
		rolled := len(in.wal.sharedWAL.oldFiles)
		in.wal.sharedWAL.oldFilesMutex.RUnlock()
		if rolled >= wantRolls {
			return written
		}
	}
	in.t.Fatalf("PREMISE BROKEN: the WAL did not reach %d rolled file(s) after %d records at "+
		"FileSize=%d; the state this test is built on was never reached",
		wantRolls, maxRecords, incarnationWALFileSize)
	return nil
}

// ---------------------------------------------------------------------------
// Q1 — can reclamation remove the highest-numbered file on disk?
// ---------------------------------------------------------------------------

// TestWALFileNum_ReclamationNeverRemovesTheHighestNumberedFile drives the state
// finding A-extra names — a surviving low-numbered file, a reclaimed
// higher-numbered one — and records whether the HIGHEST number on disk can be
// the one reclaimed.
//
// This is the load-bearing half of the finding. A fileNum can only regress if
// the maximum stem on disk goes DOWN across a shutdown, and rebuildBootState
// takes its maximum over every *.wal file present, so the question reduces to:
// is the highest-numbered file ever removable?
func TestWALFileNum_ReclamationNeverRemovesTheHighestNumberedFile(t *testing.T) {
	dir := t.TempDir()

	in := bootIncarnation(t, dir)
	defer in.close()

	// A survivor: one unacknowledged record pins the file it lands in, so that
	// file can never satisfy tryReclaimOldFiles' all-acked predicate.
	in.write(filenumSurvivorQueue, 101, "survivor")

	// Roll twice. Roll 1 retires the file holding the survivor; roll 2 retires a
	// file holding only acknowledged records.
	acked := in.rollAcked(filenumChurnQueue, 1_000_000, 2)
	waitForAcksApplied(t, in.wal, acked)

	before := walFileNumsOnDisk(t, in.sharedDir)
	activeBefore := in.wal.sharedWAL.fileNum.Load()

	require.GreaterOrEqual(t, len(before), 3,
		"PREMISE BROKEN: expected at least three WAL files on disk (a survivor, a fully-acked "+
			"rolled file, and the active file); got %v. The state finding A-extra describes was "+
			"never reached and nothing may be concluded", before)
	require.True(t, before[activeBefore],
		"PREMISE BROKEN: the active file number %d is not present on disk (%v)", activeBefore, before)
	require.Equal(t, activeBefore, maxOf(before),
		"PREMISE BROKEN: the active file %d is not the highest-numbered file on disk (%v); this "+
			"test's whole question is whether the MAXIMUM can be reclaimed",
		activeBefore, before)

	in.wal.sharedWAL.tryReclaimOldFiles()

	after := walFileNumsOnDisk(t, in.sharedDir)
	var reclaimed []uint64
	for n := range before {
		if !after[n] {
			reclaimed = append(reclaimed, n)
		}
	}

	t.Logf("RESULT: files before reclamation %v (active=%d, max=%d); after %v; reclaimed %v; "+
		"max after=%d", before, activeBefore, maxOf(before), after, reclaimed, maxOf(after))

	// The finding requires reclamation to be capable of removing the top of the
	// range. Assert it did happen to a lower one, so the fixture is proven live
	// rather than passing because nothing was reclaimed at all.
	require.NotEmpty(t, reclaimed,
		"FIXTURE DEAD: reclamation removed nothing, so 'it did not remove the highest' is "+
			"vacuous and this test distinguishes nothing")
	require.NotEmpty(t, after,
		"FIXTURE DEAD: no WAL files remain at all, so there is no surviving lower-numbered "+
			"file for the reclaimed one to be compared against")

	require.NotContains(t, reclaimed, maxOf(before),
		"FILENUM CAN REGRESS: reclamation removed the highest-numbered WAL file (%d) while "+
			"lower-numbered files survived (%v). rebuildBootState takes maxNum over the files "+
			"that still exist, so the next incarnation will resume BELOW %d and recreate a stem "+
			"a previous incarnation already used — which is exactly finding A-extra, and it "+
			"means offsetLocation{fileNum, filePosition} is not unique across incarnations",
		maxOf(before), after, maxOf(before))

	require.Equal(t, maxOf(before), maxOf(after),
		"FILENUM CAN REGRESS: the highest WAL stem on disk fell from %d to %d across a "+
			"reclamation pass; rebuildBootState would resume from the lower value",
		maxOf(before), maxOf(after))
}

// ---------------------------------------------------------------------------
// Q2 — does any incarnation reopen a stem a previous incarnation used?
// ---------------------------------------------------------------------------

// TestWALFileNum_NoIncarnationReopensAUsedStem is the contract test, and it is
// what actually decides whether candidate A needs a generation field.
//
// Contract: across a sequence of incarnations over one data directory, with
// reclamation running and with a low-numbered file pinned by an unacknowledged
// record (so holes really do open up below the maximum), no incarnation may
// ever open a WAL stem that an earlier incarnation already wrote to. If one
// does, offsetLocation{fileNum, filePosition} names two different physical
// records across a restart and cannot serve as a record identity.
//
// Three incarnations, not two: the state that would produce a regression only
// exists once a boot has ALREADY inherited someone else's files, reclaimed some
// of them, and shut down again.
func TestWALFileNum_NoIncarnationReopensAUsedStem(t *testing.T) {
	dir := t.TempDir()

	// used tracks every stem any incarnation has opened for writing.
	used := make(map[uint64]int) // stem -> incarnation index that opened it

	var sharedDir string
	nextOffset := uint64(1_000_000)

	for boot := 1; boot <= 3; boot++ {
		in := bootIncarnation(t, dir)
		if sharedDir == "" {
			sharedDir = in.sharedDir
		}

		// The stem this incarnation opened at boot, straight off the manager.
		opened := in.wal.sharedWAL.fileNum.Load()
		if prev, seen := used[opened]; seen {
			in.close()
			t.Fatalf("STEM REUSED: incarnation %d opened WAL stem %d, which incarnation %d had "+
				"already written to. offsetLocation{fileNum, filePosition} is therefore NOT "+
				"unique across incarnations — finding A-extra reproduces, and candidate A needs "+
				"a generation field", boot, opened, prev)
		}
		used[opened] = boot

		if boot == 1 {
			// Pin a low-numbered file forever with an unacknowledged record.
			in.write(filenumSurvivorQueue, 101, "survivor")
		}

		// Churn: roll at least twice so reclamation has fully-acked rolled files
		// to remove, opening holes below the maximum.
		acked := in.rollAcked(filenumChurnQueue, nextOffset, 2)
		nextOffset += uint64(len(acked)) + 1000
		waitForAcksApplied(t, in.wal, acked)

		// Every stem this incarnation opened via rollFile counts as used too.
		for n := range walFileNumsOnDisk(t, in.sharedDir) {
			if _, seen := used[n]; !seen {
				used[n] = boot
			}
		}

		in.wal.sharedWAL.tryReclaimOldFiles()
		onDisk := walFileNumsOnDisk(t, in.sharedDir)
		t.Logf("BOOT %d: opened stem %d; after churn+reclamation files on disk = %v (max=%d); "+
			"stems used so far = %d", boot, opened, onDisk, maxOf(onDisk), len(used))

		// Assert the fixture really is producing holes below the maximum —
		// otherwise "no stem was reused" is true for an uninteresting reason.
		if boot >= 2 {
			var holes []uint64
			for n := uint64(1); n < maxOf(onDisk); n++ {
				if used[n] != 0 && !onDisk[n] {
					holes = append(holes, n)
				}
			}
			require.NotEmpty(t, holes,
				"FIXTURE DEAD at boot %d: no reclaimed stem sits BELOW the highest surviving "+
					"stem (%v on disk, max %d). Without a hole under the maximum, this test is "+
					"not exercising the state finding A-extra describes and its green means "+
					"nothing", boot, onDisk, maxOf(onDisk))
			t.Logf("BOOT %d: reclaimed stems below the maximum (holes): %v", boot, holes)
		}

		in.close()
	}
}
