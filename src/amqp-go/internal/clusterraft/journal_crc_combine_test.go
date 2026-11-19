package clusterraft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestJournalCombineCRC32CMatchesConcatenation(t *testing.T) {
	rng := rand.NewChaCha8([32]byte{1, 2, 3})
	body := make([]byte, 1<<20)
	_, _ = rng.Read(body)
	boundaries := []int{0, 1, 2, 3, 4, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 255, 256, 1023, 4095, 4096, 65535, 65536, len(body)}
	for range 100 {
		boundaries = append(boundaries, int(rng.Uint64()%uint64(len(body))))
	}
	want := crc32.Checksum(body, journalChecksum)
	for _, at := range boundaries {
		a := crc32.Checksum(body[:at], journalChecksum)
		b := crc32.Checksum(body[at:], journalChecksum)
		require.Equal(t, want, journalCombineCRC32C(a, b, uint32(len(body)-at)), "split %d", at)
	}
	// Incremental, uneven chunks exercise chaining and the zero-length identity.
	var got uint32
	for at := 0; at < len(body); {
		n := min(1+int(rng.Uint64()%70001), len(body)-at)
		got = journalCombineCRC32C(got, crc32.Checksum(body[at:at+n], journalChecksum), uint32(n))
		at += n
	}
	require.Equal(t, want, got)
	require.Equal(t, want, journalCombineCRC32C(want, 0, 0))
}

func composedCRCRecord(t testing.TB, group uint64, sizes ...int) journalRecord {
	t.Helper()
	op := journalGroupOp{kind: journalGroupReady, identity: []byte("queue-identity"), hard: &pb.HardState{Term: new(uint64(4)), Vote: new(uint64(2)), Commit: new(uint64(7))}}
	rng := rand.NewChaCha8([32]byte{byte(group), 5, 13})
	for i, n := range sizes {
		data := make([]byte, n)
		_, _ = rng.Read(data)
		op.entries = append(op.entries, &pb.Entry{Index: new(uint64(i + 1)), Term: new(uint64(4)), Data: data})
	}
	n, err := op.size()
	require.NoError(t, err)
	body := make([]byte, n)
	delta, err := op.encode(body)
	require.NoError(t, err)
	return journalRecord{group: group, body: body, entryChecksums: delta.entries}
}

func assertComposedFrameIdentical(t *testing.T, sequence uint64, records []journalRecord) []byte {
	t.Helper()
	actual, locations, err := encodeJournalFrame(nil, sequence, records)
	require.NoError(t, err)
	raw, wantLocations, err := encodeJournalFrameRawCRCReference(nil, sequence, records)
	require.NoError(t, err)
	require.Equal(t, raw, actual, "entire encoded frame must match the original raw encoder byte-for-byte")
	require.Equal(t, wantLocations, locations)
	_, err = scanJournal(bytes.NewReader(actual), int64(len(actual)), -1, sequence, nil)
	require.NoError(t, err)
	return actual
}

func TestJournalFrameComposedCRCMatchesRawFrames(t *testing.T) {
	mixed := []journalRecord{
		{group: 1, body: []byte("untagged metadata")},
		composedCRCRecord(t, 2, 0, 4095, 4096, 65536, 1, 131072),
		{group: 3, body: bytes.Repeat([]byte{81}, 70000)},
		composedCRCRecord(t, 4, 65537, 31, 65536),
		composedCRCRecord(t, 5, 0, 1, 17),
		composedCRCRecord(t, 6),
	}
	for _, sequence := range []uint64{1, 57, math.MaxUint64 - 1} {
		assertComposedFrameIdentical(t, sequence, mixed)
	}
	rng := rand.New(rand.NewPCG(51, 79))
	sizes := []int{0, 1, 17, 127, 1023, 4095, 4096, 4097, 16384, 65536, 65593}
	for i := range 80 {
		var records []journalRecord
		for j := range 1 + rng.IntN(4) {
			lengths := make([]int, 1+rng.IntN(5))
			for k := range lengths {
				lengths[k] = sizes[rng.IntN(len(sizes))]
			}
			r := composedCRCRecord(t, uint64(j+1), lengths...)
			if rng.IntN(3) == 0 {
				r.entryChecksums = nil
			}
			records = append(records, r)
		}
		assertComposedFrameIdentical(t, uint64(i+1), records)
	}
}

func TestJournalFrameComposedCRCRejectsInvalidRanges(t *testing.T) {
	mutations := map[string]func(*journalRecord){
		"negative-offset": func(r *journalRecord) { r.entryChecksums[0].offset = -1 },
		"huge-offset":     func(r *journalRecord) { r.entryChecksums[0].offset = math.MaxInt64 },
		"promoted-offset": func(r *journalRecord) { r.entryChecksums[0].offset += 1000000 },
		"zero-size":       func(r *journalRecord) { r.entryChecksums[0].size = 0 },
		"oversize":        func(r *journalRecord) { r.entryChecksums[0].size = math.MaxUint32 },
		"wrong-checksum":  func(r *journalRecord) { r.entryChecksums[0].checksum ^= 1 },
		"missing-entry":   func(r *journalRecord) { r.entryChecksums = r.entryChecksums[:1] },
		"reordered":       func(r *journalRecord) { slices.Reverse(r.entryChecksums) },
		"duplicate":       func(r *journalRecord) { r.entryChecksums[1] = r.entryChecksums[0] },
		"wrong-version":   func(r *journalRecord) { r.body[0]++ },
		"huge-identity":   func(r *journalRecord) { binary.BigEndian.PutUint32(r.body[2:6], math.MaxUint32) },
		"all-huge-lengths": func(r *journalRecord) {
			for _, at := range []int{2, 6, 10, 14} {
				binary.BigEndian.PutUint32(r.body[at:at+4], math.MaxUint32)
			}
		},
		"shifted-fixed-end": func(r *journalRecord) { binary.BigEndian.PutUint32(r.body[6:10], 0) },
		"wrong-count":       func(r *journalRecord) { binary.BigEndian.PutUint32(r.body[26:30], math.MaxUint32) },
		"extra-tail":        func(r *journalRecord) { r.body = append(r.body, 1) },
		"short-body":        func(r *journalRecord) { r.body = r.body[:len(r.body)-1] },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			r := composedCRCRecord(t, 1, 65536, 9, 65537)
			mutate(&r)
			require.False(t, validJournalEntryChecksums(r))
			assertComposedFrameIdentical(t, 1, []journalRecord{r})
		})
	}
	small := composedCRCRecord(t, 1, 16)
	require.False(t, validJournalEntryChecksums(small))
	assertComposedFrameIdentical(t, 1, []journalRecord{small})
}

func TestJournalFrameComposedCRCRetainsCorruptionProtection(t *testing.T) {
	r := composedCRCRecord(t, 1, 65536, 10, 65537)
	require.True(t, validJournalEntryChecksums(r))
	frame := assertComposedFrameIdentical(t, 1, []journalRecord{r})
	for _, at := range []int{0, 20, journalFrameHeaderSize, journalFrameHeaderSize + journalRecordHeaderSize + 5, journalFrameHeaderSize + journalRecordHeaderSize + int(r.entryChecksums[0].offset) + 71, len(frame) - 16, len(frame) - 1} {
		broken := bytes.Clone(frame)
		broken[at] ^= 1
		_, err := scanJournal(bytes.NewReader(broken), int64(len(broken)), -1, 1, nil)
		require.ErrorIs(t, err, errJournalCorrupt, "corruption byte %d", at)
	}
	// Even an illegal staging mutation after the original per-entry CRC cannot
	// hide behind the composition: recovery still hashes all actual frame bytes.
	r.body[int(r.entryChecksums[0].offset)+71] ^= 1
	broken, _, err := encodeJournalFrame(nil, 1, []journalRecord{r})
	require.NoError(t, err)
	_, err = scanJournal(bytes.NewReader(broken), int64(len(broken)), -1, 1, nil)
	require.ErrorIs(t, err, errJournalCorrupt)
}

func TestJournalWriterComposedCRCUsesRelativeOffsetsAndSyncFence(t *testing.T) {
	r := composedCRCRecord(t, 11, 65536, 7, 65537)
	relative := slices.Clone(r.entryChecksums)
	delta := journalGroupDelta{kind: journalGroupReady, entries: r.entryChecksums}
	limits := journalLimits{pendingBytes: 1 << 20, pendingRequests: 4, batchBytes: 1 << 20, batchRequests: 4}
	writer, file := openGatedJournal(t, limits, false)
	pending, err := writer.enqueueEncoded(r.group, len(r.body), true, &delta, func(dst []byte) error { copy(dst, r.body); return nil })
	require.NoError(t, err)
	awaitJournalSync(t, file, pending)
	require.Equal(t, relative, delta.entries, "offsets cannot be promoted before the durability fence")
	want, _, err := encodeJournalFrameRawCRCReference(nil, 1, []journalRecord{r})
	require.NoError(t, err)
	require.Equal(t, want, file.bytes())
	file.syncRelease <- nil
	completion := awaitJournalCompletion(t, pending)
	require.NoError(t, completion.err)
	for i, entry := range delta.entries {
		require.Equal(t, relative[i].offset+completion.location.offset, entry.offset)
	}
}

func BenchmarkJournalFrameCRCComposition(b *testing.B) {
	for _, size := range []int{1024, 4096, 65536} {
		for _, tagged := range []bool{false, true} {
			name := fmtSize(size) + "/raw"
			if tagged {
				name = fmtSize(size) + "/composed"
			}
			b.Run(name, func(b *testing.B) {
				records := make([]journalRecord, 16)
				for i := range records {
					records[i] = composedCRCRecord(b, uint64(i+1), size)
					if !tagged {
						records[i].entryChecksums = nil
					}
				}
				var frame []byte
				b.SetBytes(int64(16 * size))
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					var err error
					frame, _, err = encodeJournalFrame(frame, 1, records)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
func fmtSize(size int) string {
	switch size {
	case 1024:
		return "1KiB"
	case 4096:
		return "4KiB"
	default:
		return "64KiB"
	}
}

// Original 042d545 encoder, retained as an independent full-frame oracle.
func encodeJournalFrameRawCRCReference(frame []byte, sequence uint64, records []journalRecord) ([]byte, []journalLocation, error) {
	if sequence == 0 || sequence == math.MaxUint64 || len(records) == 0 {
		return nil, nil, errors.New("invalid raft journal batch")
	}
	if len(records) > maxJournalFrameRecords {
		return nil, nil, errJournalTooLarge
	}
	var size int
	for _, record := range records {
		if record.group == 0 || len(record.body) == 0 {
			return nil, nil, errors.New("invalid raft journal record")
		}
		if len(record.body) > maxJournalFrameBytes-size-journalRecordHeaderSize {
			return nil, nil, errJournalTooLarge
		}
		size += journalRecordHeaderSize + len(record.body)
	}
	frameSize := journalFrameHeaderSize + size + journalFrameTrailerSize
	if frameSize > cap(frame) {
		frame = make([]byte, frameSize)
	} else {
		frame = frame[:frameSize]
	}
	copy(frame[:4], "SQJ1")
	binary.BigEndian.PutUint64(frame[4:12], sequence)
	binary.BigEndian.PutUint32(frame[12:16], uint32(size))
	binary.BigEndian.PutUint32(frame[16:20], uint32(len(records)))
	binary.BigEndian.PutUint32(frame[20:24], crc32.Checksum(frame[:20], journalChecksum))

	locations := make([]journalLocation, 0, len(records))
	offset := journalFrameHeaderSize
	for _, record := range records {
		binary.BigEndian.PutUint64(frame[offset:offset+8], record.group)
		binary.BigEndian.PutUint32(frame[offset+8:offset+journalRecordHeaderSize], uint32(len(record.body)))
		offset += journalRecordHeaderSize
		locations = append(locations, journalLocation{offset: int64(offset), size: len(record.body), sequence: sequence})
		offset += copy(frame[offset:], record.body)
	}
	binary.BigEndian.PutUint32(frame[offset:offset+4], crc32.Checksum(frame[journalFrameHeaderSize:offset], journalChecksum))
	copy(frame[offset+4:offset+8], "SQJE")
	binary.BigEndian.PutUint64(frame[offset+8:], sequence)
	return frame, locations, nil
}
