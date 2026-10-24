package storage

// Loop 3 Step 2, item 3 — WHICH READ ARM answers a probe of an ACKNOWLEDGED tag?
//
// The end-to-end fixture (reaper_stale_read_chain_test.go, S1) measured that a
// consumer-less TTL queue's reaper re-delivers a message it has already expired,
// dead-lettered, removed from the ring and acknowledged — without bound. That is
// the REACHABILITY evidence and it stands on its own.
//
// What it could not say is WHICH of DisruptorStorage.GetMessage's fall-through
// arms produced the message, and the fix depends entirely on that answer:
//
//	ring.LoadByTag        -> eliminated: the reaper's own deleteIfPresent removed it
//	readAhead.get         -> a cache, never invalidated on ack
//	wal.ReadBatch         -> requires an offsetIndex entry (it errors without one)
//	wal.Read -> readMessage
//	     fast path        -> requires an offsetIndex entry
//	     readMessageSequential
//	         oldFiles arm -> requires a ROLLED or INHERITED file
//	         current arm  -> gated ONLY by currentFileOffsets.Contains(offset)
//	segments.Read         -> requires a checkpoint, which only moves oldFiles
//
// This test drives the WALManager's PUBLIC API only — Write, ReadBatch,
// Acknowledge, Read — and eliminates the arms by EXECUTION rather than by
// reading the code:
//
//	P1. one physical WAL file exists  => oldFiles is empty       => oldFiles arm
//	    cannot match, and no checkpoint can have run => segments empty
//	P2. ReadBatch SUCCEEDS before the ack                        => the offsetIndex
//	    entry exists, so the fixture is exercising a real indexed record
//	P3. ReadBatch FAILS after the ack                            => the offsetIndex
//	    entry is GONE, so the readMessage fast path is eliminated AND no future
//	    ReadBatch can ever repopulate the read-ahead buffer
//
// With all of those executed, exactly ONE arm is left. If Read still returns the
// message, the current-file arm answered it, and the defective line is named:
// WALManager.Acknowledge removes the offset from offsetIndex (cleanupLoop) and
// NEVER from currentFileOffsets, which is only ever added to (commitBatch) and
// reset only by rollFile.
//
// NOTE ON WHAT THIS IS AND IS NOT. This is an INSTRUMENT for S1, not a
// replacement for it. It manufactures nothing: the ack goes through the shipped
// public entry point, and no index entry is hand-edited. But a WAL-layer test
// cannot establish reachability — S1 does that — so this file must never be
// cited as evidence that a deployment reaches this state.

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/maxpert/amqp-go/protocol"
)

// TestWALReadArm_AcknowledgedRecordIsStillServedFromTheCurrentFile pins the
// property the read path must have and names the arm that breaks it.
//
// CONTRACT: once a tag has been acknowledged through WALManager.Acknowledge, a
// read of that tag must not return the acknowledged record as a live message.
// The reaper's own comment in broker/queue_reaper.go states this expectation
// directly — `continue // gap (acked/reaped/never-present tag)`.
func TestWALReadArm_AcknowledgedRecordIsStillServedFromTheCurrentFile(t *testing.T) {
	const (
		queue = "arm.q"
		tag   = uint64(1) << 44 // an ordinary composite tag: ordinal 1, seq 0
	)

	dir := t.TempDir()

	cfg := DefaultWALConfig()
	// Long intervals so neither reclamation nor checkpointing can run during the
	// measurement and remove the record by a route other than the one under test.
	cfg.CleanupInterval = time.Hour
	cfg.CheckpointInterval = time.Hour

	wal, err := NewWALManagerWithConfig(dir, cfg)
	require.NoError(t, err)
	defer func() { _ = wal.Close() }()

	msg := &protocol.Message{
		RoutingKey:   queue,
		Body:         []byte("ACKED-THEN-READ"),
		DeliveryMode: 2,
		DeliveryTag:  tag,
		MessageID:    "arm-1",
	}
	require.NoError(t, wal.Write(queue, msg, tag), "durable write blocks until fsync")

	// ---- P1: exactly one physical WAL file => oldFiles empty, segments empty --
	walFiles, err := filepath.Glob(filepath.Join(dir, "wal", "shared", "*.wal"))
	require.NoError(t, err)
	require.Len(t, walFiles, 1,
		"PREMISE BROKEN: expected exactly one WAL file for a single small write; found %d. "+
			"With a rolled or inherited file present, readMessageSequential's oldFiles arm could "+
			"answer and this test can no longer attribute the read by elimination", len(walFiles))

	// ---- P2: the record is genuinely INDEXED before the ack -----------------
	pre, err := wal.Read(queue, tag)
	require.NoError(t, err, "PREMISE BROKEN: the record must be readable before the ack, or this "+
		"test is measuring a write that never landed")
	require.Equal(t, []byte("ACKED-THEN-READ"), pre.Body)

	_, err = wal.ReadBatch(queue, tag)
	require.NoError(t, err,
		"PREMISE BROKEN: ReadBatch must succeed before the ack. ReadBatch fails exactly when "+
			"offsetIndex has no entry for the offset, so its success here is what establishes "+
			"that an index entry existed to be deleted")

	// ---- ACT: acknowledge through the shipped public entry point ------------
	wal.Acknowledge(queue, tag)

	// ---- P3: the offsetIndex entry is GONE ----------------------------------
	// Acknowledge hands the ack to cleanupLoop over a channel, so this is
	// observed rather than assumed. ReadBatch's failure is the observation: it
	// returns "offset %d not in WAL index" precisely when the entry is absent.
	// Once absent it can never succeed again, so the read-ahead buffer can never
	// be repopulated for this tag either.
	require.Eventually(t, func() bool {
		_, berr := wal.ReadBatch(queue, tag)
		return berr != nil
	}, 10*time.Second, 5*time.Millisecond,
		"PREMISE BROKEN: the offsetIndex entry for an acknowledged tag was never removed, so "+
			"the readMessage fast path is not eliminated and this test cannot attribute the arm")

	// ---- OBSERVE: every other arm is now eliminated -------------------------
	got, err := wal.Read(queue, tag)
	answered := err == nil && got != nil
	body := ""
	if answered {
		body = string(got.Body)
	}
	t.Logf("ARM OBSERVABLE: servedAfterAck=%t body=%q walFiles=%d",
		answered, body, len(walFiles))

	require.Error(t, err,
		"STALE READ CONFIRMED AT THE CURRENT-FILE ARM: tag %d was acknowledged through "+
			"WALManager.Acknowledge, its offsetIndex entry is gone (ReadBatch fails), there is "+
			"exactly ONE WAL file so qw.oldFiles is empty and readMessageSequential's oldFiles "+
			"loop matches nothing, and no checkpoint can have run so the segment tier is empty. "+
			"Every arm but one is eliminated by execution, and Read still returned the "+
			"acknowledged record (body=%q). The remaining arm is readMessageSequential's "+
			"current-file branch, gated only by currentFileOffsets.Contains(offset) — and "+
			"currentFileOffsets is added to by commitBatch, reset only by rollFile, and NEVER "+
			"touched by Acknowledge, which deletes from offsetIndex alone", tag, body)
}
