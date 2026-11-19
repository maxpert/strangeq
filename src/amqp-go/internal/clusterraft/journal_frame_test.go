package clusterraft

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/require"
)

func journalFrameFixture(t *testing.T, sequence uint64, records ...journalRecord) []byte {
	t.Helper()
	frame, _, err := encodeJournalFrame(nil, sequence, records)
	require.NoError(t, err)
	return frame
}

func TestJournalFrameCarriesSequenceGroupAndBodyLocation(t *testing.T) {
	records := []journalRecord{{group: 17, body: []byte("entry")}, {group: ^uint64(0), body: []byte("catalog")}}
	frame, locations, err := encodeJournalFrame(nil, 56, records)
	require.NoError(t, err)
	require.Len(t, locations, len(records))
	require.Equal(t, []byte("SQJ1"), frame[:4])
	require.Equal(t, uint64(56), binary.BigEndian.Uint64(frame[4:12]))
	require.Equal(t, uint32(2), binary.BigEndian.Uint32(frame[16:20]))
	require.Equal(t, []byte("SQJE"), frame[len(frame)-journalFrameTrailerSize+4:len(frame)-8])
	for i, record := range records {
		location := locations[i]
		require.Equal(t, uint64(56), location.sequence)
		require.Equal(t, len(record.body), location.size)
		require.Equal(t, record.body, frame[location.offset:location.offset+int64(location.size)])
		records[i].location = location
	}

	calls := 0
	got, err := scanJournal(bytes.NewReader(frame), int64(len(frame)), int64(len(frame)), 56, func(batch []journalRecord) error {
		calls++
		require.Equal(t, records, batch)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	require.Equal(t, journalScan{end: int64(len(frame)), nextSequence: 57}, got)
}

func TestJournalScanAllowsOnlyIncompletePhysicalTailInActiveSegment(t *testing.T) {
	first := journalFrameFixture(t, 1, journalRecord{group: 7, body: []byte("retained")})
	second := journalFrameFixture(t, 2, journalRecord{group: 8, body: []byte("pending")})
	for cut := 1; cut < len(second); cut++ {
		data := append(bytes.Clone(first), second[:cut]...)
		calls := 0
		got, err := scanJournal(bytes.NewReader(data), int64(len(data)), -1, 1, func(batch []journalRecord) error {
			calls++
			require.Equal(t, []byte("retained"), batch[0].body)
			return nil
		})
		require.NoError(t, err, "active cut %d", cut)
		require.Equal(t, 1, calls, "active cut %d", cut)
		require.Equal(t, journalScan{end: int64(len(first)), nextSequence: 2, partialTail: true}, got)

		_, err = scanJournal(bytes.NewReader(data), int64(len(data)), int64(len(data)), 1, nil)
		require.ErrorIs(t, err, errJournalCorrupt, "sealed cut %d", cut)
	}
	// The durable sealed extent also distinguishes truncation to an otherwise
	// valid frame boundary from a legitimate end of an active file.
	_, err := scanJournal(bytes.NewReader(first), int64(len(first)), int64(len(first)+len(second)), 1, nil)
	require.ErrorIs(t, err, errJournalCorrupt)
}

func TestJournalScanRejectsCompleteCorruptFramesWithoutVisitingThem(t *testing.T) {
	first := journalFrameFixture(t, 41, journalRecord{group: 7, body: []byte("retained")})
	second := journalFrameFixture(t, 42, journalRecord{group: 8, body: []byte("pending")})
	third := journalFrameFixture(t, 43, journalRecord{group: 9, body: []byte("later")})
	for _, mutation := range []struct {
		name  string
		apply func([]byte)
	}{
		{"bad-header-length", func(frame []byte) {
			binary.BigEndian.PutUint32(frame[12:16], uint32(len(frame)+1024))
		}},
		{"invalid-size-with-valid-header-checksum", func(frame []byte) {
			binary.BigEndian.PutUint32(frame[12:16], maxJournalFrameBytes+1)
			binary.BigEndian.PutUint32(frame[20:24], crc32.Checksum(frame[:20], crc32.MakeTable(crc32.Castagnoli)))
		}},
		{"bad-body", func(frame []byte) { frame[journalFrameHeaderSize+journalRecordHeaderSize] ^= 1 }},
		{"bad-trailer", func(frame []byte) { frame[len(frame)-1] ^= 1 }},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			damaged := bytes.Clone(second)
			mutation.apply(damaged)
			for _, after := range [][]byte{nil, third} {
				data := append(append(bytes.Clone(first), damaged...), after...)
				unchanged := bytes.Clone(data)
				var seen []uint64
				_, err := scanJournal(bytes.NewReader(data), int64(len(data)), -1, 41, func(batch []journalRecord) error {
					seen = append(seen, batch[0].group)
					return nil
				})
				require.ErrorIs(t, err, errJournalCorrupt)
				require.Equal(t, []uint64{7}, seen)
				require.Equal(t, unchanged, data)
			}
		})
	}
}

func TestJournalScanRejectsSequenceGapsAndMalformedRecordBatches(t *testing.T) {
	frame := journalFrameFixture(t, 1, journalRecord{group: 1, body: []byte("first")},
		journalRecord{group: 2, body: []byte("second")})
	_, err := scanJournal(bytes.NewReader(frame), int64(len(frame)), -1, 2, nil)
	require.ErrorIs(t, err, errJournalCorrupt)

	second := journalFrameHeaderSize + journalRecordHeaderSize + len("first")
	binary.BigEndian.PutUint64(frame[second:second+8], 0) // Group zero is invalid.
	trailer := len(frame) - journalFrameTrailerSize
	checksum := crc32.Checksum(frame[journalFrameHeaderSize:trailer], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(frame[trailer:trailer+4], checksum)
	calls := 0
	_, err = scanJournal(bytes.NewReader(frame), int64(len(frame)), -1, 1, func([]journalRecord) error {
		calls++
		return nil
	})
	require.ErrorIs(t, err, errJournalCorrupt)
	require.Zero(t, calls, "a corrupt compound frame should not expose its first operation")
}
