package storage

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// F5 — THE BOOT INDEX AND THE RECOVERY SCAN MUST READ A WAL FILE THE SAME
// WAY.
//
// The two readers used to be two grammars. The boot-time physical index
// (indexWALFile -> parseWalPayload) accepted a payload the recovery scan
// (scanWALFile -> parseMessagePayload) refused — a payload with the reserved
// continuation bit set, or a body kind the scan did not know. Such a record
// was INDEXED (its key and its position were in the offset index) but never
// SCANNED, and RecoverFromWAL dropped it with a bare `continue` on the
// index/scan position mismatch: a CONFIRMED DURABLE RECORD, SILENTLY GONE.
//
// These tests pin the three properties that fix must hold:
//
//	(a) a record the two readers disagree about is served exactly once OR
//	    reported as a classified fault — never silently dropped;
//	(b) a shared-body record whose BodyBlock is missing is reported, not
//	    skipped in silence;
//	(c) a file whose records are all well formed recovers EVERY record and
//	    produces NO fault.

// grammarWALDir returns the shared-WAL directory of a data directory.
func grammarWALDir(dataDir string) string {
	return filepath.Join(dataDir, "wal", "shared")
}

// grammarMessagePayload builds a v4 message payload: the presence bitmap, the
// queue name, the delivery tag, the exchange, the routing key, the delivery
// mode and an INLINE body.
func grammarMessagePayload(queue string, offset uint64, body []byte, flags uint16) []byte {
	p := binary.BigEndian.AppendUint16(nil, flags)
	p = append(p, byte(len(queue)))
	p = append(p, queue...)
	p = binary.BigEndian.AppendUint64(p, offset)
	p = append(p, 0x00) // exchange (empty shortstr)
	p = append(p, 0x00) // routing key (empty shortstr)
	p = append(p, 0x02) // delivery mode (persistent)
	p = append(p, bodyKindInline)
	p = binary.BigEndian.AppendUint32(p, uint32(len(body)))
	p = append(p, body...)
	return p
}

// grammarReferencePayload builds a v4 message payload whose body is a
// REFERENCE to a BodyBlock at the given file-relative offset.
func grammarReferencePayload(queue string, offset uint64, blockOffset uint64) []byte {
	p := grammarMessagePayload(queue, offset, nil, 0)
	// Replace the inline arm (bodyKind byte + bodyLen word) with the
	// reference arm.
	p = p[:len(p)-5]
	p = append(p, bodyKindReference)
	p = binary.BigEndian.AppendUint16(p, 8)
	p = binary.BigEndian.AppendUint64(p, blockOffset)
	return p
}

// grammarBodyBlockPayload builds a BodyBlock record's payload:
// [fanoutHint u32][bodyLen u32][body].
func grammarBodyBlockPayload(body []byte, fanout uint32) []byte {
	p := binary.BigEndian.AppendUint32(nil, fanout)
	p = binary.BigEndian.AppendUint32(p, uint32(len(body)))
	p = append(p, body...)
	return p
}

// grammarWALRecord frames one record: [CRC u32][len u32][recordType u8][payload].
func grammarWALRecord(recType uint8, payload []byte) []byte {
	data := append([]byte{recType}, payload...)
	rec := make([]byte, 8, 8+len(data))
	binary.BigEndian.PutUint32(rec[4:8], uint32(len(data)))
	rec = append(rec, data...)
	binary.BigEndian.PutUint32(rec[0:4], crc32.ChecksumIEEE(rec[4:]))
	return rec
}

// writeGrammarWALFile writes a WAL file by hand: the v4 file header, then
// one framed record per payload. The records are laid out in the order they
// are given.
func writeGrammarWALFile(t *testing.T, dataDir, name string, records [][]byte) string {
	t.Helper()
	shared := grammarWALDir(dataDir)
	require.NoError(t, os.MkdirAll(shared, 0o755))

	buf := append([]byte(WALMagic), WALFormatVersion)
	for _, rec := range records {
		buf = append(buf, rec...)
	}
	path := filepath.Join(shared, name)
	require.NoError(t, os.WriteFile(path, buf, 0o644))
	return path
}

// grammarCountRecovered counts the recovered records of one (queue, tag).
func grammarCountRecovered(recovered []*RecoveryMessage, queue string, offset uint64) int {
	n := 0
	for _, rm := range recovered {
		if rm.QueueName == queue && rm.Offset == offset {
			n++
		}
	}
	return n
}

// grammarFaultsFor returns the classified faults of an error.
func grammarFaultsFor(t *testing.T, err error) []*interfaces.RecoveryFault {
	t.Helper()
	require.Error(t, err, "recovery must report a fault")
	faults := interfaces.FaultsOf(err)
	require.NotEmpty(t, faults, "recovery must report a CLASSIFIED fault, not a bare error")
	return faults
}

// TestWALRecovery_ContinuationFlaggedRecordIsNeverSilentlyDropped is (a).
//
// A message payload with the reserved continuation bit set is durable data
// this build cannot interpret (a future format appended a second flags
// word, so nothing after the flags word can be trusted). It must NOT be
// silently dropped: it is either served exactly once, or reported as a
// classified fault that names the file.
//
// BEFORE THE FIX the record was INDEXED but never SCANNED, and the
// index/scan mismatch was a silent `continue`: the record vanished and
// recovery reported success.
func TestWALRecovery_ContinuationFlaggedRecordIsNeverSilentlyDropped(t *testing.T) {
	dir := t.TempDir()

	// One ordinary record and one continuation-flagged record, in the SAME
	// file: the ordinary one must still be recovered (a fault must not cost
	// the records that CAN be read).
	writeGrammarWALFile(t, dir, "00000000000000000001.wal", [][]byte{
		grammarWALRecord(WALRecordTypeMessage, grammarMessagePayload("q-ok", 1, []byte("fine"), 0)),
		grammarWALRecord(WALRecordTypeMessage, grammarMessagePayload("q-cont", 2, []byte("future"), msgFlagContinuation)),
	})

	wm, err := NewWALManagerWithConfig(dir, DefaultWALConfig())
	require.NoError(t, err)
	defer wm.Close()

	recovered, rerr := wm.RecoverFromWAL()

	// The ordinary record is recovered EXACTLY ONCE.
	assert.Equal(t, 1, grammarCountRecovered(recovered, "q-ok", 1),
		"a well-formed record must be recovered exactly once")

	// The continuation-flagged record is served at most once.
	served := grammarCountRecovered(recovered, "q-cont", 2)
	assert.LessOrEqual(t, served, 1, "a record must never be recovered twice")

	if served == 0 {
		// It was not served, so it must be REPORTED. This is the assertion
		// the old silent `continue` failed.
		faults := grammarFaultsFor(t, rerr)
		degraded := false
		for _, f := range faults {
			assert.Contains(t, f.Artifact, "00000000000000000001.wal",
				"the fault must name the file that holds the unreadable record")
			if f.Outcome == interfaces.RecoveryDegraded {
				degraded = true
			}
		}
		assert.True(t, degraded,
			"an unreadable durable record must be reported as a DEGRADED fault, got %v", faults)
	} else {
		// It was served: then it must ALSO be reported, because this build
		// cannot vouch for the record's full contents.
		assert.Error(t, rerr, "a record this build cannot fully interpret must still be reported")
	}
}

// TestWALRecovery_SharedBodyWithoutItsBlockIsReported is (b).
//
// A shared-body record whose BodyBlock is missing (the unit was torn, or the
// block was lost) is a CONFIRMED durable record whose body this boot cannot
// find. The old code skipped it with a bare `continue`: the message was lost
// AND the file stayed un-reclaimable forever, with nothing said.
func TestWALRecovery_SharedBodyWithoutItsBlockIsReported(t *testing.T) {
	dir := t.TempDir()
	shared := grammarWALDir(dir)
	require.NoError(t, os.MkdirAll(shared, 0o755))

	body := []byte("the shared body")
	blockRec := grammarWALRecord(WALRecordTypeBodyBlock, grammarBodyBlockPayload(body, 1))
	// The reference points at the block that IS in the file...
	refRec := grammarWALRecord(WALRecordTypeMessage, grammarReferencePayload("q-ref", 11, uint64(WALHeaderSize)))
	// ...and is then patched to point at a block that is NOT there, which is
	// what a torn unit looks like to the reader.
	layout := parseWalPayload(refRec[9:])
	require.True(t, layout.ok)
	require.Equal(t, bodyKindReference, layout.bodyKind)
	locatorOff := 9 + layout.locatorOff
	require.Equal(t, uint64(WALHeaderSize), binary.BigEndian.Uint64(refRec[locatorOff:locatorOff+8]))
	binary.BigEndian.PutUint64(refRec[locatorOff:locatorOff+8], 0x00FF_FFFF_FFFF_FFFF)
	binary.BigEndian.PutUint32(refRec[0:4], crc32.ChecksumIEEE(refRec[4:]))

	writeGrammarWALFile(t, dir, "00000000000000000001.wal", [][]byte{blockRec, refRec})

	wm, err := NewWALManagerWithConfig(dir, DefaultWALConfig())
	require.NoError(t, err)
	defer wm.Close()

	recovered, rerr := wm.RecoverFromWAL()

	// The record cannot be served without its body, so it is NOT served.
	assert.Equal(t, 0, grammarCountRecovered(recovered, "q-ref", 11),
		"a record whose body block is missing must not be served with an empty body")

	// And it must be REPORTED.
	faults := grammarFaultsFor(t, rerr)
	degraded := false
	for _, f := range faults {
		assert.Contains(t, f.Artifact, "00000000000000000001.wal",
			"the fault must name the file that holds the dangling reference")
		if f.Outcome == interfaces.RecoveryDegraded {
			degraded = true
		}
	}
	assert.True(t, degraded,
		"a durable record whose body block is missing must be reported as a DEGRADED fault, got %v", faults)
}

// TestWALRecovery_WellFormedFileRecoversEveryRecordWithoutFaults is (c): the
// guard against the new fault firing on a healthy directory.
func TestWALRecovery_WellFormedFileRecoversEveryRecordWithoutFaults(t *testing.T) {
	dir := t.TempDir()

	const N = 25
	records := make([][]byte, 0, N)
	for i := 1; i <= N; i++ {
		records = append(records, grammarWALRecord(WALRecordTypeMessage,
			grammarMessagePayload("q-ok", uint64(i), []byte("payload"), 0)))
	}
	writeGrammarWALFile(t, dir, "00000000000000000001.wal", records)

	wm, err := NewWALManagerWithConfig(dir, DefaultWALConfig())
	require.NoError(t, err)
	defer wm.Close()

	recovered, rerr := wm.RecoverFromWAL()
	require.NoError(t, rerr, "a healthy WAL file must produce NO fault")
	require.Len(t, recovered, N, "every record must be recovered")
	for i := 1; i <= N; i++ {
		assert.Equal(t, 1, grammarCountRecovered(recovered, "q-ok", uint64(i)),
			"record %d must be recovered exactly once", i)
	}
}

// TestWALRecovery_WellFormedFileWrittenByTheRealPathRecoversWithoutFaults is
// (c) for the REAL write path (not a hand-built file): the ordinary durable
// message, written and recovered through the production code, must produce no
// fault either.
func TestWALRecovery_WellFormedFileWrittenByTheRealPathRecoversWithoutFaults(t *testing.T) {
	dir := t.TempDir()

	wm, err := NewWALManager(dir)
	require.NoError(t, err)

	const N = 10
	for i := 1; i <= N; i++ {
		require.NoError(t, wm.Write("q", &protocol.Message{
			DeliveryMode: 2,
			DeliveryTag:  uint64(i),
			Body:         []byte("hello"),
		}, uint64(i)))
	}
	require.NoError(t, waitDurable(wm, "q", N))
	require.NoError(t, wm.Close())

	wm2, err := NewWALManager(dir)
	require.NoError(t, err)
	defer wm2.Close()

	recovered, rerr := wm2.RecoverFromWAL()
	require.NoError(t, rerr, "a healthy WAL directory must produce NO fault")
	require.Len(t, recovered, N)
	for i := 1; i <= N; i++ {
		assert.Equal(t, 1, grammarCountRecovered(recovered, "q", uint64(i)))
	}
}

// TestWALGrammar_IndexAndScanShareOneWalk pins the ROOT CAUSE fix directly:
// the two readers of a v4 message payload must agree about the record's
// identity — its queue name, its delivery tag, its body kind and its body
// locator — and must agree about WHETHER they can read it at all.
//
// Before the fix they were two grammars: the index accepted a payload with
// the reserved continuation bit set and any body kind, while the scan
// refused both, so a record could be indexed and never scanned.
func TestWALGrammar_IndexAndScanShareOneWalk(t *testing.T) {
	unknownKind := grammarMessagePayload("q", 9, []byte("body"), 0)
	unknownLayout := parseWalPayload(unknownKind)
	require.True(t, unknownLayout.ok)
	unknownKind[unknownLayout.bodyOff-1] = 0x7F // an arm this build does not know

	// A payload whose presence bitmap claims a field the payload does not
	// carry: the STRUCTURAL PREFIX is intact (the index reads the record's
	// identity) but the optional fields cannot be read (the scan cannot build
	// the message). This is the one legitimate difference between the two
	// readers, and the scan REPORTS it.
	truncatedOptional := grammarMessagePayload("q", 11, []byte("body"), msgFlagContentType)

	withContentType := grammarMessagePayload("q", 6, []byte("body"), msgFlagContentType)
	withContentType = append(withContentType, byte(len("text/plain")))
	withContentType = append(withContentType, []byte("text/plain")...)

	cases := []struct {
		name    string
		payload []byte
	}{
		{"inline", grammarMessagePayload("q", 5, []byte("body"), 0)},
		{"inline-with-flags", withContentType},
		{"reference", grammarReferencePayload("q", 7, 4096)},
		{"continuation-flag", grammarMessagePayload("q", 8, []byte("body"), msgFlagContinuation)},
		{"unknown-body-kind", unknownKind},
		{"truncated", grammarMessagePayload("q", 10, []byte("body"), 0)[:6]},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			layout := parseWalPayload(tc.payload)
			queue, offset, msg, ok := parseMessagePayload(tc.payload)

			// The two walks must agree about WHETHER the record is readable.
			assert.Equal(t, layout.ok, ok,
				"the index and the scan must agree about a payload's readability")

			if !layout.ok {
				assert.Empty(t, queue)
				assert.Nil(t, msg)
				return
			}

			// ... and about the record's IDENTITY.
			assert.Equal(t, layout.queueName, queue, "the queue name must come from the same walk")
			assert.Equal(t, layout.offset, offset, "the delivery tag must come from the same walk")
			require.NotNil(t, msg)
			assert.Equal(t, layout.exchange, msg.Exchange)
			assert.Equal(t, layout.routingKey, msg.RoutingKey)
			assert.Equal(t, layout.deliveryMode, msg.DeliveryMode)
			switch layout.bodyKind {
			case bodyKindInline:
				assert.Nil(t, msg.BodyRef)
				assert.Equal(t, tc.payload[layout.bodyOff:layout.bodyOff+layout.bodyLen], msg.Body)
			case bodyKindReference:
				assert.Len(t, msg.BodyRef, layout.refLen)
				assert.Equal(t, layout.bodyRef, int64(binary.BigEndian.Uint64(msg.BodyRef)))
			}
		})
	}

	// THE ONE LEGITIMATE DIFFERENCE: the structural prefix is intact but the
	// optional fields are not. The index still knows WHICH record this is
	// (it must, or its file could be reclaimed); the scan cannot build the
	// message. The scan must REPORT that, not skip the record in silence.
	t.Run("optional-fields-truncated", func(t *testing.T) {
		layout := parseWalPayload(truncatedOptional)
		require.True(t, layout.ok, "the structural prefix is intact")
		queue, offset, msg, ok := parseMessagePayload(truncatedOptional)
		assert.False(t, ok, "the scan cannot build a message whose optional fields are truncated")
		assert.Nil(t, msg)
		assert.Empty(t, queue)
		assert.Zero(t, offset)
	})
}

// TestWALRecovery_SameIncarnationRecoveryDoesNotDisagreeWithTheIndex guards
// the OTHER direction of the index/scan comparison: a record written by THIS
// incarnation is in the offset index (the write path publishes it) and in the
// file the scan reads. The two must agree about WHERE it is, or the new
// disagreement fault would fire on a perfectly healthy WAL.
func TestWALRecovery_SameIncarnationRecoveryDoesNotDisagreeWithTheIndex(t *testing.T) {
	dir := t.TempDir()

	wm, err := NewWALManager(dir)
	require.NoError(t, err)
	defer wm.Close()

	const N = 5
	for i := 1; i <= N; i++ {
		require.NoError(t, wm.Write("q", &protocol.Message{
			DeliveryMode: 2,
			DeliveryTag:  uint64(i),
			Body:         []byte("hello"),
		}, uint64(i)))
	}
	require.NoError(t, waitDurable(wm, "q", N))

	recovered, rerr := wm.RecoverFromWAL()
	require.NoError(t, rerr, "the write path and the scan must agree about a record's position")
	require.Len(t, recovered, N)
	for i := 1; i <= N; i++ {
		assert.Equal(t, 1, grammarCountRecovered(recovered, "q", uint64(i)))
	}
}
