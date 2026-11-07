package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// R-TAIL-HOLE recovery tests.
//
// Under crash_model: barrier a crashed active WAL file can hold
// [confirmed records][hole or torn record][more unconfirmed bytes] followed by a
// long zero tail. BEFORE this change that shape was a BOOT-BLOCKING FATAL fault
// that abandoned even the confirmed prefix.
//
// The new rule: replay stops at the FIRST defect, everything from there on is
// unconfirmed, and a bounded non-zero suffix on one of the two newest files is
// a benign unconfirmed tail that boot durably truncates. A suffix out of bound,
// or a defect on an older file, stays FATAL.
//
// These fixtures are HAND-BUILT byte streams (not the real write path), so they
// compile and run against the pre-change tree too: TestWALTail_BoundedHole* is
// the RED test that fails with the boot-refusal output before the fix.

func walHeaderBytes() []byte {
	buf := make([]byte, 0, WALHeaderSize)
	buf = append(buf, WALMagic...)
	buf = append(buf, WALFormatVersion)
	return buf
}

// appendHoleRecord appends one valid v4 message record for offset off.
func appendHoleRecord(t *testing.T, buf []byte, off uint64) []byte {
	t.Helper()
	b, err := appendMessageRecord(buf, "q", &protocol.Message{
		Exchange:     "amq.direct",
		RoutingKey:   "q",
		DeliveryMode: 2,
		DeliveryTag:  off,
		Body:         []byte(fmt.Sprintf("hole-record-%02d", off)),
	}, off, false)
	require.NoError(t, err)
	return b
}

// writeSharedWALFile writes a hand-built WAL file <num>.wal into the shared
// directory of dataDir and returns its path.
func writeSharedWALFile(t *testing.T, dataDir string, num uint64, buf []byte) string {
	t.Helper()
	shared := filepath.Join(dataDir, "wal", "shared")
	require.NoError(t, os.MkdirAll(shared, 0o755))
	p := filepath.Join(shared, fmt.Sprintf("%020d%s", num, WALFileExtension))
	require.NoError(t, os.WriteFile(p, buf, 0o644))
	return p
}

// TestWALTail_BoundedHoleOnNewestFileStopsReplayAndTruncates is THE red test
// for R-TAIL-HOLE.
//
// Fixture: [records 1..5][64 KiB never-written hole][records 6..9][EOF] on the
// single (and therefore newest) WAL file. A hole is a zero region that starts
// exactly on a record boundary and is FOLLOWED BY DATA — today that is a fatal
// fault that refuses the boot and abandons records 1..5.
//
// Green: the boot succeeds, replay stops at the hole, exactly records 1..5 come
// back, records 6..9 are NEVER delivered (they are past the first defect and
// therefore unconfirmed), and the file is durably truncated at the hole.
func TestWALTail_BoundedHoleOnNewestFileStopsReplayAndTruncates(t *testing.T) {
	dir := t.TempDir()
	buf := walHeaderBytes()
	for off := uint64(1); off <= 5; off++ {
		buf = appendHoleRecord(t, buf, off)
	}
	holeStart := int64(len(buf))
	buf = append(buf, make([]byte, 64*1024)...) // the never-written hole
	for off := uint64(6); off <= 9; off++ {
		buf = appendHoleRecord(t, buf, off)
	}
	path := writeSharedWALFile(t, dir, 1, buf)

	st, err := NewDisruptorStorageWithEngineConfig(dir, interfaces.EngineConfig{})
	require.NoError(t, err,
		"a bounded hole on the newest WAL file must NOT refuse the boot (R-TAIL-HOLE); "+
			"the pre-change code returned a FatalFault here and abandoned the confirmed prefix")
	defer st.Close()

	msgs, rerr := st.GetRecoverableMessages()
	require.NoError(t, rerr)
	require.Len(t, msgs["q"], 5,
		"replay must stop at the first defect: only the contiguous confirmed prefix (records 1..5) is recovered")

	for _, m := range msgs["q"] {
		require.LessOrEqual(t, m.DeliveryTag, uint64(5),
			"a record past the hole must NEVER be delivered (bounded claim: records after the first defect are unconfirmed)")
	}

	got, serr := os.Stat(path)
	require.NoError(t, serr)
	require.Equal(t, holeStart, got.Size(),
		"boot must durably truncate the newest file at the first defect, so a later boot never sees the shape")
}

// TestWALTail_TornChunkInNewestFileStopsReplay is the torn-64-KiB-record
// variant: an 8-byte frame that claims a 64 KiB body whose CRC cannot match,
// followed by more valid records. A CRC failure with real data after it was
// fatal before; on a newest file within the bound it is now a benign
// unconfirmed tail.
func TestWALTail_TornChunkInNewestFileStopsReplay(t *testing.T) {
	dir := t.TempDir()
	buf := walHeaderBytes()
	for off := uint64(1); off <= 5; off++ {
		buf = appendHoleRecord(t, buf, off)
	}
	// A torn frame: non-zero CRC and a 64 KiB declared length, then 64 KiB of
	// pattern bytes whose CRC will not match.
	torn := make([]byte, 8+64*1024)
	binary.BigEndian.PutUint32(torn[0:4], 0xdeadbeef)
	binary.BigEndian.PutUint32(torn[4:8], 64*1024)
	for i := 8; i < len(torn); i++ {
		torn[i] = byte(i)
	}
	buf = append(buf, torn...)
	for off := uint64(6); off <= 9; off++ {
		buf = appendHoleRecord(t, buf, off)
	}
	writeSharedWALFile(t, dir, 1, buf)

	st, err := NewDisruptorStorageWithEngineConfig(dir, interfaces.EngineConfig{})
	require.NoError(t, err, "a bounded torn chunk on the newest file must not refuse the boot")
	defer st.Close()

	msgs, rerr := st.GetRecoverableMessages()
	require.NoError(t, rerr)
	require.Len(t, msgs["q"], 5, "replay stops at the torn chunk")
	for _, m := range msgs["q"] {
		require.LessOrEqual(t, m.DeliveryTag, uint64(5))
	}
}

// TestWALTail_HoleOnThirdNewestFileStaysFatal pins the OTHER side of the bound:
// a defect on a file that is NOT one of the two newest cannot be an unconfirmed
// tail, so it stays FATAL. Three files; the hole is in file 1.
func TestWALTail_HoleOnThirdNewestFileStaysFatal(t *testing.T) {
	dir := t.TempDir()
	buf := walHeaderBytes()
	for off := uint64(1); off <= 3; off++ {
		buf = appendHoleRecord(t, buf, off)
	}
	buf = append(buf, make([]byte, 64*1024)...) // hole
	for off := uint64(4); off <= 5; off++ {
		buf = appendHoleRecord(t, buf, off)
	}
	writeSharedWALFile(t, dir, 1, buf)

	clean2 := walHeaderBytes()
	for off := uint64(10); off <= 12; off++ {
		clean2 = appendHoleRecord(t, clean2, off)
	}
	writeSharedWALFile(t, dir, 2, clean2)

	clean3 := walHeaderBytes()
	for off := uint64(20); off <= 22; off++ {
		clean3 = appendHoleRecord(t, clean3, off)
	}
	writeSharedWALFile(t, dir, 3, clean3)

	_, err := NewDisruptorStorageWithEngineConfig(dir, interfaces.EngineConfig{})
	require.Error(t, err,
		"a hole in the third-newest WAL file is real interior corruption and must stay fatal (R-TAIL-HOLE)")
	require.Contains(t, err.Error(), "00000000000000000001.wal",
		"the refusal must name the damaged file")
}

// TestWALTail_OutOfBoundSuffixStaysFatal pins the BOUND: a hole followed by
// data more than walMaxUnconfirmedBytes+walMaxFrameBytes away is real interior
// corruption, not a crash tail, and stays fatal. The gap is sparse, so it costs
// no disk.
func TestWALTail_OutOfBoundSuffixStaysFatal(t *testing.T) {
	dir := t.TempDir()
	shared := filepath.Join(dir, "wal", "shared")
	require.NoError(t, os.MkdirAll(shared, 0o755))
	path := filepath.Join(shared, fmt.Sprintf("%020d%s", uint64(1), WALFileExtension))

	prefix := walHeaderBytes()
	for off := uint64(1); off <= 3; off++ {
		prefix = appendHoleRecord(t, prefix, off)
	}
	holeStart := int64(len(prefix))
	const gap = 400 << 20 // > 256 MiB bound + 64 MiB frame
	suffix := walHeaderBytes()
	for off := uint64(4); off <= 6; off++ {
		suffix = appendHoleRecord(t, suffix, off)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	_, err = f.Write(prefix)
	require.NoError(t, err)
	_, err = f.WriteAt(suffix, holeStart+gap)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, cerr := NewDisruptorStorageWithEngineConfig(dir, interfaces.EngineConfig{})
	require.Error(t, cerr,
		"a hole whose suffix is beyond walMaxUnconfirmedBytes+walMaxFrameBytes must stay fatal")
	require.Contains(t, cerr.Error(), "00000000000000000001.wal")
}

// TestWALTail_AllZeroHeaderPreallocCrashIsEmptyBenign pins the header case: a
// crash can persist a zero-extended i_size without the file header, so an
// all-zero header block on an otherwise all-zero file is EMPTY, not a version
// refusal.
func TestWALTail_AllZeroHeaderPreallocCrashIsEmptyBenign(t *testing.T) {
	dir := t.TempDir()
	writeSharedWALFile(t, dir, 1, make([]byte, 64*1024)) // all zeros

	st, err := NewDisruptorStorageWithEngineConfig(dir, interfaces.EngineConfig{})
	require.NoError(t, err,
		"an all-zero header block on an all-zero (prealloc-crash) WAL file must be treated as empty, not refused")
	defer st.Close()

	msgs, rerr := st.GetRecoverableMessages()
	require.NoError(t, rerr)
	require.Empty(t, msgs["q"], "an all-zero WAL file holds no records")
}
