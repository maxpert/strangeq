package clusterraft

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
)

func preparedRecordOp(sizes ...int) journalGroupOp {
	op := journalGroupOp{kind: journalGroupReady, hard: &pb.HardState{Term: new(uint64(4)), Vote: new(uint64(1)), Commit: new(uint64(7))}}
	rng := rand.NewChaCha8([32]byte{47, 71, 33})
	for i, n := range sizes {
		data := make([]byte, n)
		_, _ = rng.Read(data)
		op.entries = append(op.entries, &pb.Entry{Term: new(uint64(4)), Index: new(uint64(i + 1)), Type: pb.EntryNormal.Enum(), Data: data})
	}
	return op
}

func checkPreparedOpExact(t *testing.T, op journalGroupOp, sequence uint64) journalRecord {
	t.Helper()
	require.True(t, op.canPrepareJournalRecord())
	size, err := op.size()
	require.NoError(t, err)
	raw := make([]byte, size)
	rawDelta, err := op.encode(raw)
	require.NoError(t, err)
	prepared, delta, err := prepareJournalRecord(op, size)
	require.NoError(t, err)
	require.Equal(t, rawDelta, delta)
	flat := make([]byte, size)
	require.Equal(t, size, prepared.copyInto(flat))
	require.Equal(t, raw, flat)
	require.Equal(t, crc32.Checksum(raw, journalChecksum), prepared.checksum)
	for i, entry := range op.entries {
		if len(entry.Data) > 0 {
			require.Same(t, &entry.Data[0], &prepared.entries[i].data[0], "prepare must borrow immutable payload, not copy")
		}
	}
	record := journalRecord{group: 11, prepared: prepared}
	actual, locations, err := encodeJournalFrame(nil, sequence, []journalRecord{record})
	require.NoError(t, err)
	oracle, wantLocations, err := encodeJournalFrameRawCRCReference(nil, sequence, []journalRecord{{group: 11, body: raw}})
	require.NoError(t, err)
	require.Equal(t, oracle, actual)
	require.Equal(t, wantLocations, locations)
	_, err = scanJournal(bytes.NewReader(actual), int64(len(actual)), -1, sequence, func(records []journalRecord) error { _, err := decodeJournalGroupOp(records[0]); return err })
	require.NoError(t, err)
	return record
}

func TestJournalPreparedRecordMatchesCanonicalRawWire(t *testing.T) {
	sizes := []int{0, 1, 15, 16, 127, 128, 4095, 4096, 16383, 16384, 65535, 65536, 65537}
	rng := rand.New(rand.NewPCG(1, 19))
	for i := range 100 {
		lengths := []int{65536}
		for range rng.IntN(7) {
			lengths = append(lengths, sizes[rng.IntN(len(sizes))])
		}
		op := preparedRecordOp(lengths...)
		for j, e := range op.entries {
			if j%2 == 0 {
				e.Type = nil
			}
			if j%3 == 0 {
				e.Term = new(uint64(math.MaxUint64))
				e.Index = new(uint64(math.MaxUint64))
			}
			if len(e.Data) == 0 && j%2 == 0 {
				e.Data = nil
			}
		}
		if i%3 == 0 {
			op.hard = nil
		} else if i%3 == 1 {
			op.hard = &pb.HardState{Term: new(uint64(0)), Vote: new(uint64(0)), Commit: new(uint64(0))}
		}
		checkPreparedOpExact(t, op, uint64(i+1))
	}
	// Mixed raw/prepared records retain every interleaved frame header byte.
	first := checkPreparedOpExact(t, preparedRecordOp(65536, 7), 1)
	second := checkPreparedOpExact(t, preparedRecordOp(8192, 65537), 1)
	rawFirst := make([]byte, first.prepared.size)
	first.prepared.copyInto(rawFirst)
	rawSecond := make([]byte, second.prepared.size)
	second.prepared.copyInto(rawSecond)
	actual, loc, err := encodeJournalFrame(nil, 91, []journalRecord{first, {group: 12, body: []byte("raw metadata")}, second})
	require.NoError(t, err)
	want, wantLoc, err := encodeJournalFrameRawCRCReference(nil, 91, []journalRecord{{group: 11, body: rawFirst}, {group: 12, body: []byte("raw metadata")}, {group: 11, body: rawSecond}})
	require.NoError(t, err)
	require.Equal(t, want, actual)
	require.Equal(t, wantLoc, loc)
}

func TestJournalPreparedRecordConservativeFallback(t *testing.T) {
	changes := map[string]func(*journalGroupOp){
		"small":         func(op *journalGroupOp) { op.entries[0].Data = []byte("small") },
		"identity":      func(op *journalGroupOp) { op.identity = []byte{} },
		"kind":          func(op *journalGroupOp) { op.kind = journalGroupCreate },
		"snapshot":      func(op *journalGroupOp) { op.snapshot.metadata = &pb.SnapshotMetadata{} },
		"snapshot-name": func(op *journalGroupOp) { op.snapshot.name = "unusual" },
		"entry-unknown": func(op *journalGroupOp) {
			op.entries[0].ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1))
		},
		"hard-unknown": func(op *journalGroupOp) {
			op.hard.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1))
		},
		"configuration": func(op *journalGroupOp) { op.entries[0].Type = pb.EntryConfChange.Enum() },
		"missing-term":  func(op *journalGroupOp) { op.entries[0].Term = nil },
		"missing-index": func(op *journalGroupOp) { op.entries[0].Index = nil },
		"zero-term":     func(op *journalGroupOp) { op.entries[0].Term = new(uint64(0)) },
		"nil-entry":     func(op *journalGroupOp) { op.entries[0] = nil },
		"too-many":      func(op *journalGroupOp) { op.entries = slices.Repeat(op.entries, journalPreparedMaxEntries+1) },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			op := preparedRecordOp(65536)
			change(&op)
			require.False(t, op.canPrepareJournalRecord())
		})
	}
	writer := journalWriter{durability: &journalDurability{}, file: &gatedJournalFile{}}
	require.False(t, writer.preparedProcessAllowed(), "ordinary output must keep raw staging")
}

func TestJournalPreparedRecordOwnsMetadataAndDetectsPayloadCorruption(t *testing.T) {
	op := preparedRecordOp(65536)
	size, err := op.size()
	require.NoError(t, err)
	prepared, _, err := prepareJournalRecord(op, size)
	require.NoError(t, err)
	before := make([]byte, size)
	prepared.copyInto(before)
	*op.entries[0].Term = 11
	*op.entries[0].Index = 99
	*op.entries[0].Type = pb.EntryConfChange
	after := make([]byte, size)
	prepared.copyInto(after)
	require.Equal(t, before, after, "prefix must own scalar metadata")
	op.entries[0].Data[13] ^= 1 // Illegal ownership violation must still fail recovery CRC.
	frame, _, err := encodeJournalFrame(nil, 1, []journalRecord{{group: 11, prepared: prepared}})
	require.NoError(t, err)
	_, err = scanJournal(bytes.NewReader(frame), int64(len(frame)), -1, 1, nil)
	require.ErrorIs(t, err, errJournalCorrupt)
}

func openPreparedWriterFixture(t *testing.T, writeAt func(*os.File, []byte, int64) (int, error)) (*journalWriter, *journalSegmentReader) {
	t.Helper()
	out, reader, state, err := openJournalSegmentsWithQuota(t.TempDir(), true, 2<<20, 4<<20, journalSegmentHooks{processRoots: true, processBootID: "prepared-kernel", writeAt: writeAt}, nil)
	require.NoError(t, err)
	limits := journalLimits{pendingBytes: 1 << 20, pendingRequests: 4, batchBytes: 1 << 20, batchRequests: 4}
	writer, err := newJournalWriter(out, state.end, state.nextSequence, limits)
	require.NoError(t, err)
	writer.durability = &journalDurability{}
	t.Cleanup(func() { _ = writer.close(); _ = reader.Close() })
	return writer, reader
}

func TestJournalPreparedRecordWriteBoundaryChargingAndFailure(t *testing.T) {
	for _, short := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "short"}[short], func(t *testing.T) {
			started := make(chan struct{})
			release := make(chan struct{})
			var frame []byte
			writer, _ := openPreparedWriterFixture(t, func(f *os.File, p []byte, at int64) (int, error) {
				frame = bytes.Clone(p)
				close(started)
				<-release
				if short {
					return len(p) - 1, nil
				}
				return f.WriteAt(p, at)
			})
			op := preparedRecordOp(65536, 7)
			size, err := op.size()
			require.NoError(t, err)
			var delta journalGroupDelta
			pending, err := writer.enqueuePreparedMode(11, size, &delta, op)
			require.NoError(t, err)
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("writer never reached OS write gate")
			}
			writer.mu.Lock()
			require.Equal(t, size+journalRecordHeaderSize, writer.pendingBytes)
			require.Equal(t, 1, writer.pendingRequests)
			writer.mu.Unlock()
			select {
			case <-pending:
				t.Fatal("completion preceded kernel write return")
			default:
			}
			raw := make([]byte, size)
			_, err = op.encode(raw)
			require.NoError(t, err)
			want, _, err := encodeJournalFrameRawCRCReference(nil, 1, []journalRecord{{group: 11, body: raw}})
			require.NoError(t, err)
			require.Equal(t, want, frame)
			// All scratch slots remain lazy: preparation never acquires a full body.
			for range cap(writer.staging.free) {
				b := <-writer.staging.free
				require.Nil(t, b)
				writer.staging.free <- b
			}
			close(release)
			completion := awaitJournalCompletion(t, pending)
			if short {
				require.ErrorIs(t, completion.err, io.ErrShortWrite)
				require.Zero(t, completion.location)
			} else {
				require.NoError(t, completion.err)
				require.Equal(t, int64(journalFrameHeaderSize+journalRecordHeaderSize), completion.location.offset)
			}
			writer.mu.Lock()
			require.Zero(t, writer.pendingBytes)
			require.Zero(t, writer.pendingRequests)
			writer.mu.Unlock()
		})
	}
}

func BenchmarkJournalPreparedRecordPipeline(b *testing.B) {
	for _, count := range []int{1, 16} {
		for _, prepared := range []bool{false, true} {
			name := fmt.Sprintf("%dx64KiB/raw", count)
			if prepared {
				name = fmt.Sprintf("%dx64KiB/prepared", count)
			}
			b.Run(name, func(b *testing.B) {
				sizes := make([]int, count)
				for i := range sizes {
					sizes[i] = 65536
				}
				op := preparedRecordOp(sizes...)
				size, err := op.size()
				require.NoError(b, err)
				staging := make([]byte, size)
				var frame []byte
				b.SetBytes(int64(count * 65536))
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					size, err := op.size()
					if err != nil {
						b.Fatal(err)
					}
					var record journalRecord
					if prepared {
						if !op.canPrepareJournalRecord() {
							b.Fatal("not eligible")
						}
						p, _, err := prepareJournalRecord(op, size)
						if err != nil {
							b.Fatal(err)
						}
						record = journalRecord{group: 11, prepared: p}
					} else {
						d, err := op.encode(staging)
						if err != nil {
							b.Fatal(err)
						}
						record = journalRecord{group: 11, body: staging, entryChecksums: d.entries}
					}
					frame, _, err = encodeJournalFrame(frame, 1, []journalRecord{record})
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
