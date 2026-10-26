package storage

// Two follow-up determinations for Loop 3 Step 1, both requested by the
// observer and both kept SEPARATE from R1's and R3's verdicts.
//
// D1 — DOES THE TAG COLLISION ALONE HARM ANYTHING TODAY?
//
// R1 proved that after a restart and redeclare of a non-durable queue the
// broker re-mints delivery tags already carried by physically-present records
// of the previous incarnation. The shared WAL is GLOBALLY TAG-KEYED with no
// queue discriminator (Step 0, DO NOT RE-DERIVE #5), so two records sharing one
// key is not obviously benign, and the question is separate from any ack set:
// what do the offset index, the read path, and recovery each do when the key is
// ambiguous?
//
// D2 — IS performCheckpoint's CROSS-FILE INDEX EVICTION REACHABLE, AND WHAT
//      DOES IT COST?
//
// performCheckpoint's offsetIndex cleanup deletes BY OFFSET VALUE ONLY:
//
//	it := info.offsets.Iterator()
//	for it.HasNext() { delete(qw.offsetIndex, it.Next()) }
//
// with no check that offsetIndex[offset].fileNum == the file being reclaimed.
// So checkpointing file X can evict an index entry that belongs to file Y. That
// is the same shape as the binding-filename defect Loop 2 closed: cleanup keyed
// on something insufficiently discriminating, evicting a live record of a
// different identity. It is independent of candidate C and must not leave with
// it if C is abandoned.
//
// The read path is what decides the COST, and it is why this is not obviously
// only a performance question — readMessageSequential iterates oldFiles in GO
// MAP ORDER and returns the first file whose bitmap contains the offset:
//
//	for _, fileInfo := range qw.oldFiles {
//	    if fileInfo.offsets != nil && fileInfo.offsets.Contains(offset) {
//	        return qw.readMessageFromFile(queueName, fileInfo.path, offset)
//	    }
//	}
//
// If two files both contain the tag and the index entry is gone, that loop can
// return EITHER record. Returning the older one is not a slow read, it is the
// WRONG PAYLOAD.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/maxpert/amqp-go/protocol"
)

const ambiguousQueue = "ambiguous-tag-queue"

// writeBody writes one durable record carrying an identifiable body.
func (in *walIncarnation) writeBody(queue string, offset uint64, body string) {
	in.t.Helper()
	require.NoError(in.t, in.wal.Write(queue, &protocol.Message{
		Exchange:     "",
		RoutingKey:   queue,
		Body:         []byte(body),
		DeliveryMode: 2,
		MessageID:    body,
	}, offset), "durable write of offset %d body %q", offset, body)
}

// indexedFileNum reports which file the offset index currently attributes a tag
// to, if any.
func indexedFileNum(qw *QueueWAL, tag uint64) (uint64, bool) {
	qw.offsetIndexMutex.RLock()
	defer qw.offsetIndexMutex.RUnlock()
	for key, loc := range qw.offsetIndex {
		if key.deliveryTag == tag {
			return loc.pos.segmentID, true
		}
	}
	return 0, false
}

// ---------------------------------------------------------------------------
// D1 — what happens when one tag names two physically-present records
// ---------------------------------------------------------------------------

// TestAmbiguousTag_ReadPathReturnsTheNewestRecord pins what the three consumers
// of a delivery tag do when two records carry it.
//
// Contract: whatever the storage layer does with an ambiguous key, it must not
// hand back a record the caller did not ask for. The tag the caller holds was
// minted by the CURRENT incarnation, so the current incarnation's record is the
// only correct answer.
func TestAmbiguousTag_ReadPathReturnsTheNewestRecord(t *testing.T) {
	dir := t.TempDir()

	in := bootIncarnation(t, dir)
	defer in.close()

	const tag = uint64(9_000_000)

	// OLD record, then roll it into oldFiles so both records are in DIFFERENT
	// files and both are physically present at once.
	in.writeBody(ambiguousQueue, tag, "OLD")
	filler := in.fillUntilRoll(ambiguousQueue+"-filler", 9_500_000)
	require.NotEmpty(t, filler)

	oldFileNum, indexed := indexedFileNum(in.wal.sharedWAL, tag)
	require.True(t, indexed, "PREMISE BROKEN: the OLD record was never indexed")

	// NEW record, same tag, different body, into the current file.
	in.writeBody(ambiguousQueue, tag, "NEW")

	newFileNum, indexed := indexedFileNum(in.wal.sharedWAL, tag)
	require.True(t, indexed, "PREMISE BROKEN: the NEW record is not indexed")
	require.NotEqual(t, oldFileNum, newFileNum,
		"PREMISE BROKEN: both records landed in file %d, so this fixture is not exercising "+
			"an ambiguous key across two files", oldFileNum)
	t.Logf("PREMISE: tag %d is carried by a record in file %d (body OLD) and a record in "+
		"file %d (body NEW); the offset index attributes it to file %d",
		tag, oldFileNum, newFileNum, newFileNum)

	// (a) THE READ PATH, index intact.
	msg, err := in.wal.sharedWAL.readMessage(ambiguousQueue, tag)
	require.NoError(t, err)
	t.Logf("RESULT (a) readMessage with index intact: body=%q", string(msg.Body))
	require.Equal(t, "NEW", string(msg.Body),
		"AMBIGUOUS KEY RESOLVED WRONG: readMessage returned the PREVIOUS record's payload "+
			"for a tag minted by the current incarnation")

	// (b) RECOVERY. Does it report one record or two for this tag?
	recovered, err := in.wal.RecoverFromWAL()
	require.NoError(t, err)
	var atTag []string
	for _, rm := range recovered {
		if rm.Offset == tag {
			atTag = append(atTag, string(rm.Message.Body))
		}
	}
	t.Logf("RESULT (b) RecoverFromWAL reports %d record(s) at tag %d: %v", len(atTag), tag, atTag)

	// (c) THE ACK BITMAP. One Acknowledge covers both records, because the
	// bitmap holds tag VALUES.
	in.wal.Acknowledge(ambiguousQueue, tag)
	waitForAcksApplied(t, in.wal, []uint64{tag})
	in.wal.sharedWAL.bitmapMutex.RLock()
	acked := in.wal.sharedWAL.ackBitmaps[ambiguousQueue].Contains(tag)
	in.wal.sharedWAL.bitmapMutex.RUnlock()
	t.Logf("RESULT (c) a single Acknowledge(tag=%d) marks the tag acked=%v — it cannot "+
		"distinguish which of the two records it cancels", tag, acked)
	require.True(t, acked)
}

// ---------------------------------------------------------------------------
// D2 — cross-file index eviction
// ---------------------------------------------------------------------------

// TestCrossFileIndexEviction_CheckpointingOneFileEvictsAnotherFilesEntry pins
// performCheckpoint's index cleanup against the identity it is supposed to
// respect.
//
// Contract: reclaiming file X may only remove index entries that POINT AT
// file X. An entry attributed to a different, still-live file must survive.
//
// This is the "does it hit the right target" arm (canon: presence is not
// correctness). Deleting the cleanup entirely would prove only that cleanup
// happens; the decoy here — a live entry for a different file carrying the same
// offset value — is what makes the arm meaningful.
func TestCrossFileIndexEviction_CheckpointingOneFileEvictsAnotherFilesEntry(t *testing.T) {
	dir := t.TempDir()

	in := bootIncarnation(t, dir)
	defer in.close()

	const tag = uint64(9_100_000)

	in.writeBody(ambiguousQueue, tag, "OLD")
	filler := in.fillUntilRoll(ambiguousQueue+"-filler2", 9_600_000)
	require.NotEmpty(t, filler)

	oldFileNum, ok := indexedFileNum(in.wal.sharedWAL, tag)
	require.True(t, ok)

	in.writeBody(ambiguousQueue, tag, "NEW")
	newFileNum, ok := indexedFileNum(in.wal.sharedWAL, tag)
	require.True(t, ok)
	require.NotEqual(t, oldFileNum, newFileNum,
		"PREMISE BROKEN: no decoy — both records are in file %d", oldFileNum)

	// The index entry under test belongs to the CURRENT file, which is live and
	// is not being reclaimed.
	t.Logf("PREMISE: offsetIndex[%d] -> file %d (live, NOT being reclaimed); file %d is about "+
		"to be checkpointed and its inventory also contains %d",
		tag, newFileNum, oldFileNum, tag)

	in.wal.sharedWAL.performCheckpoint()

	after, stillIndexed := indexedFileNum(in.wal.sharedWAL, tag)
	t.Logf("RESULT: after checkpointing file %d, offsetIndex[%d] indexed=%v (fileNum=%d)",
		oldFileNum, tag, stillIndexed, after)

	require.True(t, stillIndexed,
		"CROSS-FILE INDEX EVICTION: checkpointing file %d deleted the offsetIndex entry for "+
			"tag %d, which pointed at the LIVE file %d, not at the reclaimed one. "+
			"performCheckpoint's cleanup deletes by offset VALUE with no check that "+
			"offsetIndex[offset].fileNum == the file being reclaimed.",
		oldFileNum, tag, newFileNum)
	require.Equal(t, newFileNum, after,
		"CROSS-FILE INDEX EVICTION: offsetIndex[%d] was re-attributed from the live file %d "+
			"to %d by a checkpoint of file %d", tag, newFileNum, after, oldFileNum)
}

// waitForIndexEntryGone waits until the ack path's own cleanup goroutine has
// removed a tag from the offset index.
//
// cleanupLoop adds to ackBitmap and THEN deletes the index entry, so waiting on
// the bitmap (waitForAcksApplied) does not establish the delete. This waits on
// the thing actually under test.
func waitForIndexEntryGone(t *testing.T, qw *QueueWAL, tag uint64) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		if _, present := indexedFileNum(qw, tag); !present {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("PREMISE BROKEN: the ack path did not remove offsetIndex[%d] within 30s, "+
				"so the state this test measures was never reached", tag)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestAmbiguousTag_RealAckPathProducesTheStaleReadState is the REACHABILITY
// test, and it is the one that decides severity.
//
// D2b (below) established that the read path returns a previous incarnation's
// record when the index entry is missing — but it reached that state by
// deleting the entry directly, which proves the read is unsafe GIVEN the state
// without proving anything produces it. This test removes that gap: the index
// entry is deleted by the PRODUCTION ACK PATH (WALManager.Acknowledge ->
// cleanupLoop's `delete(qw.offsetIndex, ack.offset)`), and nothing is touched
// by hand.
//
// WHY A READ OF AN ACKED TAG IS AN ORDINARY OPERATION, not a contrived one:
// broker/queue_reaper.go reapTTLSweep walks `for tag := tail; tag < end; tag++`
// and calls storage.GetMessage on EVERY tag in the window. Its own comment
// documents the expectation this test attacks:
//
//	msg, err := b.storage.GetMessage(name, tag)
//	if err != nil || msg == nil {
//	    continue // gap (acked/reaped/never-present tag)
//	}
//
// The reaper therefore probes acked tags BY DESIGN and relies on getting
// nothing back. If a stale record answers instead, the reaper treats a previous
// incarnation's message as live: it evaluates that message's TTL, may
// dead-letter it (deadLetter(..., DeadLetterExpired)), and — in the tail-advance
// loop at the end of the same function — STOPS advancing the tail on
// `msg != nil`, so the queue's tail never moves past the tag.
//
// Contract: a tag that has been acknowledged must not read back as a live
// message.
func TestAmbiguousTag_RealAckPathProducesTheStaleReadState(t *testing.T) {
	dir := t.TempDir()

	in := bootIncarnation(t, dir)
	defer in.close()

	const tag = uint64(9_300_000)

	// OLD record, rolled into a file that STAYS in oldFiles (nothing is acked
	// in it yet, so it cannot be reclaimed).
	in.writeBody(ambiguousQueue, tag, "OLD")
	filler := in.fillUntilRoll(ambiguousQueue+"-filler4", 9_800_000)
	require.NotEmpty(t, filler)

	// NEW record, same tag, into the current file. This is the record a client
	// published in the current incarnation and had confirmed.
	in.writeBody(ambiguousQueue, tag, "NEW")

	qw := in.wal.sharedWAL

	// PREMISE: with the index intact the read is correct, so any wrong answer
	// below is caused by the ack, not by the fixture.
	pre, err := qw.readMessage(ambiguousQueue, tag)
	require.NoError(t, err)
	require.Equal(t, "NEW", string(pre.Body),
		"PREMISE BROKEN: the read was already wrong before the acknowledgement")

	// PREMISE: a file in oldFiles really does carry this tag, so the sequential
	// fallback has somewhere wrong to go.
	qw.oldFilesMutex.RLock()
	decoys := 0
	for _, info := range qw.oldFiles {
		if info.offsets != nil && info.offsets.Contains(tag) {
			decoys++
		}
	}
	qw.oldFilesMutex.RUnlock()
	require.NotZero(t, decoys,
		"PREMISE BROKEN: no oldFiles entry carries tag %d", tag)
	t.Logf("PREMISE: read is correct (%q) before the ack; %d oldFiles entry/entries carry tag %d",
		"NEW", decoys, tag)

	// THE ONLY ACTION: a real acknowledgement through the production API.
	in.wal.Acknowledge(ambiguousQueue, tag)
	t.Logf("PREMISE: the production ack path synchronously installed the read gate")

	// The reaper's probe, at the layer where the answer is decided.
	msg, err := qw.readMessage(ambiguousQueue, tag)
	if err != nil {
		t.Logf("RESULT: after a real ack, reading tag %d returned an error (%v) — the reaper's "+
			"documented expectation holds and the stale record is NOT reachable this way", tag, err)
		return
	}
	t.Logf("RESULT: after a real ack, reading tag %d returned a LIVE MESSAGE with body=%q",
		tag, string(msg.Body))

	require.NotEqual(t, "OLD", string(msg.Body),
		"REACHABLE STALE READ: tag %d was acknowledged through the production ack path, which "+
			"deleted its offset-index entry, and the read then fell through to a previous "+
			"incarnation's record (body %q) in a file still held in oldFiles. "+
			"broker/queue_reaper.go reapTTLSweep probes exactly this state on every sweep and "+
			"documents that an acked tag yields err!=nil || msg==nil; that expectation is FALSE "+
			"here. The reaper will treat a dead incarnation's message as live: evaluate its "+
			"TTL, potentially dead-letter it as DeadLetterExpired, and stop advancing the "+
			"queue tail. No ack-durability feature is involved — this is the shipping tree",
		tag, string(msg.Body))
}

// TestCrossFileIndexEviction_CostIsAWrongPayloadNotASlowRead measures what the
// eviction COSTS, which is the question that decides its severity.
//
// If the fallback after an index miss returns the correct record, the eviction
// is a performance defect (an O(1) lookup degraded to a scan). If it can return
// the OTHER record that carries the same tag, it is a correctness defect:
// cross-incarnation payload corruption, a confirmed message answered with a
// different message's bytes.
func TestCrossFileIndexEviction_CostIsAWrongPayloadNotASlowRead(t *testing.T) {
	dir := t.TempDir()

	in := bootIncarnation(t, dir)
	defer in.close()

	const tag = uint64(9_200_000)

	// Put the OLD record in a rolled file that STAYS in oldFiles.
	in.writeBody(ambiguousQueue, tag, "OLD")
	filler := in.fillUntilRoll(ambiguousQueue+"-filler3", 9_700_000)
	require.NotEmpty(t, filler)

	// Put the NEW record in the current file.
	in.writeBody(ambiguousQueue, tag, "NEW")

	qw := in.wal.sharedWAL

	// Establish the decoy really is present in a file that is still in oldFiles.
	qw.oldFilesMutex.RLock()
	decoyFiles := 0
	for _, info := range qw.oldFiles {
		if info.offsets != nil && info.offsets.Contains(tag) {
			decoyFiles++
		}
	}
	qw.oldFilesMutex.RUnlock()
	require.NotZero(t, decoyFiles,
		"PREMISE BROKEN: no file in oldFiles carries tag %d, so the sequential fallback has "+
			"no decoy to return and this test cannot distinguish a slow read from a wrong one",
		tag)

	// Evict the index entry, which is exactly what an ack or a cross-file
	// checkpoint does. Done directly so the test measures the READ, not the
	// route that caused the miss.
	qw.offsetIndexMutex.Lock()
	for key := range qw.offsetIndex {
		if key.deliveryTag == tag {
			delete(qw.offsetIndex, key)
			delete(qw.recordCopies, key)
		}
	}
	qw.offsetIndexMutex.Unlock()

	msg, err := qw.readMessage(ambiguousQueue, tag)
	require.Error(t, err, "an index miss is an error; the ambiguous sequential fallback was removed")
	require.Nil(t, msg)
	t.Logf("RESULT: with the index entry evicted and %d oldFiles entry/entries carrying tag %d, "+
		"readMessage refused the ambiguous lookup instead of returning an older payload", decoyFiles, tag)
}
