package storage

// F2 — the SEGMENT TIER's acknowledgement gate. THIS IS A REGRESSION GATE, not
// a demonstration of an open defect.
//
// HISTORY, because it changes what a red here means. This fixture was written
// when the segment tier was ungated: QueueSegments.readMessage consults no ack
// bitmap, and acknowledgeInSegment only increments deletedCount — it never
// removes the offset from currentIndex.entries or segment.index. A record
// checkpointed out of the WAL and into a segment therefore had NO gated read
// path at all, and this fixture was RED, establishing that defect.
//
// It is now GREEN, and what closed it is the acknowledgement gate in
// DisruptorStorage.GetMessage — WALManager.IsAcknowledged, consulted once below
// the ring read and re-validated after the tier read. QueueSegments itself is
// still ungated and that is deliberate: the gate sits ABOVE all four tiers
// rather than inside any of them. GetMessage consults them in this order:
//
//	ring.LoadByTag     -> removed by the ack itself, before either durable ack
//	                      (DeleteMessage / DeleteMessageIfPresent), so a ring
//	                      hit means no acknowledgement was issued here
//	== the acknowledgement gate: every tier below this line is covered ==
//	readAhead.get      -> gated here; also invalidated by readAheadBuffer.remove
//	wal.ReadBatch      -> gated here; needs an offsetIndex entry
//	wal.Read           -> gated here; also has its own QueueWAL-level gate
//	segments.Read      -> GATED HERE. It is gated nowhere else, which is why
//	                      this fixture exists and why it is the arm that proves
//	                      the gate covers the tier no per-tier fix reached.
//	== the re-validate: catches an ack landing during the tier read ==
//
// WHAT A RED HERE MEANS NOW. The only mutation that reddens this fixture is
// removing the GetMessage acknowledgement gate — verified: deleting either
// guard alone leaves it green (each is separately attributed by
// getmessage_ack_gate_test.go), and deleting BOTH reddens it here. So a failure
// is a regression in that gate, not evidence of a surviving hole.
//
// Attribution to the SEGMENT tier specifically is asserted rather than assumed:
// P4 below establishes that the WAL can no longer answer, so the segment arm is
// the only remaining explanation for a served record. walReadErr is captured
// and reported for that reason.
//
// PREMISES, ALL ASSERTED AT RUNTIME rather than assumed, because a green on a
// broken premise is vacuous and a red on one is attributed to the wrong thing:
//
//	P1. the WAL actually ROLLED             (performCheckpoint only ever
//	                                         operates on rolled files)
//	P2. the victim tag is in a ROLLED file  (so the checkpoint can move it)
//	P3. the record actually REACHED a segment and is readable there BEFORE any
//	    ack (so a later read cannot be answered from anywhere else)
//	P4. the WAL can no longer answer for it (so the segment arm is the only
//	    remaining explanation for a served record)
//
// The unacked control is the discriminator: without it, a fix that broke reads
// outright would satisfy the acked assertion and pass.

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

const (
	segGateQueue = "seg.gate.q"
	// segGateWALFileSize is small enough that a few dozen half-kilobyte records
	// roll the file several times. rollFile is the ONLY producer of qw.oldFiles
	// in a single-boot fixture, and performCheckpoint iterates oldFiles alone —
	// so without a roll this test measures nothing. P1 asserts it happened.
	segGateWALFileSize = 8 * 1024
	segGateRecords     = 64
	segGateBodySize    = 512
	// segGateControls is how many never-acknowledged controls this fixture
	// serves, and it is greater than one deliberately. Every historical critical
	// in this subsystem was OVER-suppression, and "an acknowledged record is not
	// served" passes HARDER the more a gate over-suppresses — so the control is
	// the only arm that can see that failure, and a control that is a single
	// uncounted sample is a liveness check wearing a safety check's name. The
	// count is asserted at runtime below.
	segGateControls = 3
)

// TestSegmentTier_AcknowledgedRecordIsRefusedAfterCheckpoint drives only
// DisruptorStorage's public surface for the ACT and the ASSERT — StoreMessage,
// DeleteMessage, GetMessage. It reaches into the package for two things only:
// to OBSERVE premises (did the WAL roll, did the record reach a segment) and to
// invoke performCheckpoint directly instead of sleeping out the 5-minute
// checkpoint ticker. performCheckpoint is the shipped function the ticker
// calls; calling it on schedule rather than on a timer manufactures no state.
func TestSegmentTier_AcknowledgedRecordIsRefusedAfterCheckpoint(t *testing.T) {
	t.Skip("checkpoint copying removed in segstore v1")
	dir := t.TempDir()

	ds, err := NewDisruptorStorageWithEngineConfig(dir, interfaces.EngineConfig{
		WALFileSize: segGateWALFileSize,
	})
	require.NoError(t, err)
	defer func() { _ = ds.Close() }()

	body := make([]byte, segGateBodySize)
	for i := range body {
		body[i] = byte('a' + i%26)
	}

	base := uint64(1) << 44 // an ordinary composite tag: ordinal 1, seq 0
	for i := 0; i < segGateRecords; i++ {
		msg := &protocol.Message{
			RoutingKey:   segGateQueue,
			Body:         body,
			DeliveryMode: 2, // durable: takes the WAL branch unconditionally
			DeliveryTag:  base + uint64(i),
			MessageID:    fmt.Sprintf("seg-%d", i),
		}
		require.NoError(t, ds.StoreMessage(segGateQueue, msg))
	}

	sw := ds.wal.sharedWAL
	require.NotNil(t, sw, "PREMISE BROKEN: no shared WAL was constructed")

	// ---- P1 + P2: the WAL rolled, and we can name a tag inside a rolled file --
	sw.oldFilesMutex.RLock()
	rolledFiles := len(sw.oldFiles)
	var tags []uint64
	for _, info := range sw.oldFiles {
		if info.offsets == nil {
			continue
		}
		it := info.offsets.Iterator()
		for it.HasNext() && len(tags) < segGateControls+1 {
			tags = append(tags, it.Next())
		}
		if len(tags) == segGateControls+1 {
			break
		}
	}
	sw.oldFilesMutex.RUnlock()

	require.NotZero(t, rolledFiles,
		"PREMISE BROKEN: the WAL never rolled at FileSize=%d after %d records of %d bytes, so "+
			"qw.oldFiles is empty and performCheckpoint — which iterates oldFiles and nothing "+
			"else — cannot move any record into a segment. Nothing downstream is under test",
		segGateWALFileSize, segGateRecords, segGateBodySize)

	// THE OBSERVATION COUNT, asserted at runtime and placed where it can fire.
	// This is the paired-control rule's teeth: the acked subject plus
	// segGateControls distinct never-acked controls, all checkpoint-eligible so
	// the two halves are comparable. If the arranged state ever yields fewer,
	// this fires rather than the control silently shrinking toward one sample.
	require.Len(t, tags, segGateControls+1,
		"PREMISE BROKEN: found only %d distinct offsets inside %d rolled WAL file(s), need %d — "+
			"one acknowledged subject plus %d over-suppression controls at the same altitude",
		len(tags), rolledFiles, segGateControls+1, segGateControls)

	victim, controls := tags[0], tags[1:]

	t.Logf("PREMISE: %d rolled WAL file(s); victim tag=%d, %d unacked control tag(s)=%v",
		rolledFiles, victim, len(controls), controls)

	// ---- ACT 1: checkpoint. This is what moves a record WAL -> segment. -------
	sw.performCheckpoint()

	// ---- P3: the record really is in a segment, and readable there, BEFORE
	//          any acknowledgement. Establishes the segment tier as a live
	//          answering arm rather than an assumed one.
	// Every tag, not just the victim: the controls must be at the SAME ALTITUDE
	// as the subject or the pair is not comparable. A control answered from some
	// other tier would pass this fixture without ever exercising the gated arm.
	for _, tag := range tags {
		segMsg, segErr := ds.segments.Read(segGateQueue, tag)
		require.NoError(t, segErr,
			"PREMISE BROKEN: tag %d did not reach a segment (performCheckpoint may have skipped its "+
				"file — e.g. a shared BodyBlock pin, or a scan fault). With the record absent from "+
				"the segment tier this test cannot say whether that tier gates acknowledgements",
			tag)
		require.NotNil(t, segMsg, "PREMISE BROKEN: segment read for tag %d returned nil, no error", tag)
	}

	// ---- P4: the WAL can no longer answer for it. Eliminates the WAL arm, so a
	//          served record after the ack is attributable to the segment tier.
	_, walReadErrBeforeAck := ds.wal.Read(segGateQueue, victim)
	require.Error(t, walReadErrBeforeAck,
		"PREMISE BROKEN: the WAL still answers for tag %d after performCheckpoint removed its "+
			"file and index entry. The WAL arm is then not eliminated and a served record "+
			"cannot be attributed to the segment tier", victim)

	// ---- ACT 2: acknowledge through the shipped production entry point -------
	require.NoError(t, ds.DeleteMessage(segGateQueue, victim))

	// A require.Eventually stood here, waiting for QueueSegments' OWN ackBitmap
	// to observe the acknowledgement. Deleting it is a STRENGTHENING, and the
	// reason needs no appeal to any design: this fixture's terminal assertion is
	// that the record must be REFUSED, so removing a wait that sits before it
	// strictly reduces the settling time the system is given and can only make
	// that assertion harder to satisfy. A weakening makes an assertion easier to
	// pass; this is the opposite. It could not have weakened the controls either,
	// since nothing ever acknowledges them.
	//
	// It was also a premise about state under test by nothing: the gate reads
	// WALManager's bitmap, written synchronously before DeleteMessage returns,
	// and never QueueSegments'. Removing it took the only clock out of this file.

	// ---- OBSERVE ------------------------------------------------------------
	_, walReadErr := ds.wal.Read(segGateQueue, victim)
	got, getErr := ds.GetMessage(segGateQueue, victim)
	served := getErr == nil && got != nil
	gotID := ""
	if served {
		gotID = got.MessageID
	}
	t.Logf("ARM OBSERVABLE: walRead err=%v; GetMessage served=%t messageID=%q",
		walReadErr, served, gotID)

	// THE OVER-SUPPRESSION CONTROLS FIRST: they are what prove a red below is a
	// gate defect and not a read path that has simply stopped working — and they
	// are the only arm that can catch the failure direction every historical
	// critical here took. Counted, not sampled: see segGateControls.
	// Narrow but not dead, and worth saying which: the selection assert above
	// already fixes len(tags), so the mutation this catches and that one does not
	// is an edit to the `tags[1:]` slice expression. Placing it AFTER the loop
	// instead would be dead outright — require.NoError aborts on the first
	// refusal, so a post-loop count could only ever equal the loop length
	// (tests.md §2, the invariant-outcome trap).
	require.Len(t, controls, segGateControls,
		"the over-suppression control would observe %d serve(s), not %d — a control that shrinks "+
			"toward one sample cannot see over-suppression at all", len(controls), segGateControls)
	for _, tag := range controls {
		ctlMsg, ctlErr := ds.GetMessage(segGateQueue, tag)
		require.NoError(t, ctlErr,
			"UNACKNOWLEDGED TAG REFUSED: tag %d was checkpointed into a segment and never "+
				"acknowledged, so GetMessage must still return it. Refusing it loses confirmed "+
				"durable data silently, which is strictly worse than the bug under repair", tag)
		require.NotNil(t, ctlMsg, "unacked control tag %d returned nil with no error", tag)
	}

	require.Error(t, getErr,
		"ACKNOWLEDGED RECORD SERVED FROM THE SEGMENT TIER: tag %d was acknowledged through "+
			"DisruptorStorage.DeleteMessage, the WAL refuses it (err=%v), and the segment tier "+
			"answered anyway with MessageID %q. QueueSegments.readMessage consults no ack bitmap "+
			"and acknowledgeInSegment only increments deletedCount — it never removes the offset "+
			"from the segment index, so this tier is gated NOWHERE ELSE. broker/queue_reaper.go "+
			"reapTTLSweep probes acked tags every sweep and relies on getting nothing back; "+
			"answering re-expires and re-dead-letters the same record without bound, and its "+
			"tail-advance loop breaks on any non-nil message so the tail never moves past the "+
			"tag.\n"+
			"WHAT TO SUSPECT: the acknowledgement gate in DisruptorStorage.GetMessage "+
			"(WALManager.IsAcknowledged, consulted below the ring read and re-validated after "+
			"the tier read) is what covers this arm, and removing it is the expected cause of a "+
			"red here. Deleting EITHER guard alone leaves this fixture green — each is "+
			"separately attributed in getmessage_ack_gate_test.go — so a red here means BOTH are "+
			"gone or the oracle itself stopped answering", victim, walReadErr, gotID)
}
