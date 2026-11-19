package clusterraft

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type journalCheckpointFixtureRecord struct {
	group uint64
	op    journalGroupOp
}

func journalCheckpointFixtureFrame(t *testing.T, sequence uint64, base int64, ops ...journalCheckpointFixtureRecord) ([]byte, [][]journalEntryRef) {
	t.Helper()
	records := make([]journalRecord, len(ops))
	refs := make([][]journalEntryRef, len(ops))
	for i, input := range ops {
		size, err := input.op.size()
		require.NoError(t, err)
		body := make([]byte, size)
		delta, err := input.op.encode(body)
		require.NoError(t, err)
		records[i] = journalRecord{group: input.group, body: body}
		refs[i] = delta.entries
	}
	frame, locations, err := encodeJournalFrame(nil, sequence, records)
	require.NoError(t, err)
	for i, entries := range refs {
		for j := range entries {
			entries[j].offset += base + locations[i].offset
		}
	}
	return frame, refs
}

func journalCheckpointFixtureIdentity(group uint64, peers ...uint64) []byte {
	identity := binary.BigEndian.AppendUint64(nil, group)
	identity = binary.BigEndian.AppendUint64(identity, 1)
	for _, peer := range peers {
		identity = binary.BigEndian.AppendUint64(identity, peer)
	}
	return identity
}

func journalCheckpointFixtureHard(term, vote, commit uint64) *pb.HardState {
	return &pb.HardState{Term: new(term), Vote: new(vote), Commit: new(commit)}
}

func journalCheckpointFixtureEntry(index, term uint64, body []byte) *pb.Entry {
	return &pb.Entry{Index: new(index), Term: new(term), Data: body}
}

func journalCheckpointFixtureSnapshot(name string, index, term uint64, data []byte) journalSnapshotRef {
	return journalSnapshotRef{
		metadata: &pb.SnapshotMetadata{
			Index: new(index), Term: new(term), ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}},
		},
		name: journalSnapshotPrefix + name, size: uint32(len(data)), checksum: crc32.Checksum(data, journalChecksum),
	}
}

func installJournalSegmentCheckpointFixture(output *journalSegmentOutput, reader *journalSegmentReader, sequence uint64, end int64) (string, error) {
	staged, err := reader.stageCheckpoint(sequence, end)
	if err != nil {
		return "", err
	}
	return filepath.Join(staged.store.dir, staged.ref.name), output.installCheckpoint(staged)
}

func openJournalSegmentCheckpointFixture(dir string, extentBytes int64,
	restore func(uint64, int64, map[uint64]*journalGroupState) error,
	visit func([]journalRecord) error,
) (*journalSegmentOutput, *journalSegmentReader, journalScan, error) {
	return openJournalSegments(dir, false, extentBytes, journalSegmentHooks{restoreCheckpoint: restore}, visit)
}

func requireJournalCheckpointFixtureEntry(t *testing.T, reader *journalSegmentReader, ref journalEntryRef, want []byte) {
	t.Helper()
	raw := make([]byte, ref.size)
	require.NoError(t, readJournalAt(reader, raw, ref.offset))
	require.Equal(t, ref.checksum, crc32.Checksum(raw, journalChecksum))
	entry := &pb.Entry{}
	require.NoError(t, proto.Unmarshal(raw, entry))
	require.Equal(t, ref.index, entry.GetIndex())
	require.Equal(t, ref.term, entry.GetTerm())
	require.Equal(t, want, entry.GetData())
}

func TestJournalSegmentCheckpointRestoresSealedStateBeforeTail(t *testing.T) {
	const hot, cold = uint64(11), uint64(22)
	dir := t.TempDir()
	hotID, coldID := journalCheckpointFixtureIdentity(hot, 1, 2, 3), journalCheckpointFixtureIdentity(cold)
	snapshotData := encodeSnapshot(1, []byte("current snapshot"))
	genesis := journalCheckpointFixtureSnapshot("genesis", 1, 1, encodeSnapshot(1, []byte("old snapshot")))
	checkpointSnap := journalCheckpointFixtureSnapshot("current", 2, 2, snapshotData)
	// The superseded genesis blob has already been reclaimed. Its metadata
	// remains in the create record and the build must not load its body.
	blob, err := os.OpenFile(filepath.Join(dir, checkpointSnap.name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	require.NoError(t, err)
	_, err = blob.Write(snapshotData)
	require.NoError(t, err)
	require.NoError(t, syncJournalFile(blob))
	require.NoError(t, blob.Close())
	require.NoError(t, syncDirectory(dir))

	one, _ := journalCheckpointFixtureFrame(t, 1, 0,
		journalCheckpointFixtureRecord{hot, journalGroupOp{
			kind: journalGroupCreate, identity: hotID, hard: journalCheckpointFixtureHard(1, 0, 1), snapshot: genesis,
		}},
		journalCheckpointFixtureRecord{cold, journalGroupOp{kind: journalGroupCreate, identity: coldID}},
	)
	hotHard, coldHard := journalCheckpointFixtureHard(2, 2, 2), journalCheckpointFixtureHard(3, 1, 1)
	coldBody := []byte("cold payload from the covered extent")
	two, prefixRefs := journalCheckpointFixtureFrame(t, 2, int64(len(one)),
		journalCheckpointFixtureRecord{hot, journalGroupOp{
			kind: journalGroupReady, hard: hotHard, entries: []*pb.Entry{
				journalCheckpointFixtureEntry(2, 2, []byte("covered by snapshot")),
				journalCheckpointFixtureEntry(3, 2, []byte("uncommitted old suffix")),
			},
		}},
		journalCheckpointFixtureRecord{cold, journalGroupOp{
			kind: journalGroupReady, hard: coldHard, entries: []*pb.Entry{journalCheckpointFixtureEntry(1, 3, coldBody)},
		}},
	)
	three, _ := journalCheckpointFixtureFrame(t, 3, int64(len(one)+len(two)),
		journalCheckpointFixtureRecord{hot, journalGroupOp{kind: journalGroupSnap, snapshot: checkpointSnap}},
	)
	cutoff := int64(len(one) + len(two) + len(three))
	replacement := bytes.Repeat([]byte("r"), int(cutoff)/2+1)
	lastBody := bytes.Repeat([]byte("t"), len(replacement))
	tailHard := journalCheckpointFixtureHard(3, 3, 3)
	four, tailRefs := journalCheckpointFixtureFrame(t, 4, cutoff,
		journalCheckpointFixtureRecord{hot, journalGroupOp{
			kind: journalGroupReady, hard: tailHard, entries: []*pb.Entry{
				journalCheckpointFixtureEntry(3, 3, replacement),
				journalCheckpointFixtureEntry(4, 3, []byte("first retained tail")),
			},
		}},
	)
	five, lastRefs := journalCheckpointFixtureFrame(t, 5, cutoff+int64(len(four)),
		journalCheckpointFixtureRecord{hot, journalGroupOp{
			kind: journalGroupReady, entries: []*pb.Entry{journalCheckpointFixtureEntry(5, 3, lastBody)},
		}},
	)
	require.LessOrEqual(t, int64(len(four)), cutoff)
	require.LessOrEqual(t, int64(len(five)), cutoff)
	require.Greater(t, int64(len(four)+len(five)), cutoff)

	output, reader, _, err := openJournalSegments(dir, true, cutoff, journalSegmentHooks{}, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = output.Close()
		_ = reader.Close()
	})
	var offset int64
	for _, frame := range [][]byte{one, two, three, four, five} {
		n, err := output.WriteAt(frame, offset)
		require.NoError(t, err)
		require.Equal(t, len(frame), n)
		require.NoError(t, output.Sync())
		offset += int64(n)
	}
	before, err := loadJournalSegmentManifest(dir)
	require.NoError(t, err)
	require.Len(t, before.extents, 3)
	require.Equal(t, cutoff, before.extents[0].sealedBytes)
	require.Equal(t, uint64(4), before.extents[1].firstSequence)
	indexPath, err := installJournalSegmentCheckpointFixture(output, reader, 3, cutoff)
	require.NoError(t, err)
	require.NoError(t, output.Close())
	require.NoError(t, reader.Close())

	recovered := &journalGroupNode{dir: dir, groups: make(map[uint64]*journalGroupState)}
	var restored bool
	var replayed []uint64
	reopened, reopenedReader, tip, err := openJournalSegmentCheckpointFixture(dir, cutoff,
		func(sequence uint64, end int64, groups map[uint64]*journalGroupState) error {
			require.Equal(t, uint64(3), sequence)
			require.Equal(t, cutoff, end)
			require.Len(t, groups, 2)
			require.Equal(t, hotID, groups[hot].identity)
			require.True(t, proto.Equal(hotHard, groups[hot].hard))
			snapshot := groups[hot].snapshot
			require.Equal(t, checkpointSnap.name, snapshot.name)
			require.Equal(t, checkpointSnap.size, snapshot.size)
			require.Equal(t, checkpointSnap.checksum, snapshot.checksum)
			require.True(t, proto.Equal(checkpointSnap.metadata, snapshot.metadata))
			require.Equal(t, prefixRefs[0][1:], groups[hot].entries)
			require.Equal(t, coldID, groups[cold].identity)
			require.True(t, proto.Equal(coldHard, groups[cold].hard))
			require.Equal(t, prefixRefs[1], groups[cold].entries)
			recovered.groups = groups
			restored = true
			return nil
		},
		func(records []journalRecord) error {
			replayed = append(replayed, records[0].location.sequence)
			return recovered.replayBatch(records)
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = reopened.Close()
		_ = reopenedReader.Close()
	})
	require.Equal(t, []uint64{4, 5}, replayed, "only sequences after the checkpoint cutoff may replay")
	require.True(t, restored)
	require.Equal(t, uint64(6), tip.nextSequence)
	require.Equal(t, offset, tip.end)
	require.True(t, proto.Equal(tailHard, recovered.groups[hot].hard))
	require.Equal(t, append(tailRefs[0], lastRefs[0]...), recovered.groups[hot].entries)
	require.Equal(t, prefixRefs[1], recovered.groups[cold].entries)
	requireJournalCheckpointFixtureEntry(t, reopenedReader, recovered.groups[hot].entries[0], replacement)
	requireJournalCheckpointFixtureEntry(t, reopenedReader, recovered.groups[cold].entries[0], coldBody)
	snapshot, err := recovered.readSnapshot(recovered.groups[hot].snapshot)
	require.NoError(t, err)
	require.Equal(t, snapshotData, snapshot.GetData())
	require.NoError(t, reopened.Close())
	require.NoError(t, reopenedReader.Close())

	// The old extents still exist. Corruption of the manifest-selected
	// index must stop recovery instead of silently replaying those files.
	require.NotEmpty(t, indexPath)
	index, err := os.ReadFile(indexPath)
	require.NoError(t, err)
	require.NotEmpty(t, index)
	index[len(index)/2] ^= 0x40
	require.NoError(t, os.WriteFile(indexPath, index, 0600))
	afterCorruption, badReader, _, err := openJournalSegmentCheckpointFixture(dir, cutoff,
		func(uint64, int64, map[uint64]*journalGroupState) error {
			t.Error("corrupt checkpoint supplied group state")
			return nil
		},
		func([]journalRecord) error {
			t.Error("recovery replayed old extents despite corrupt checkpoint")
			return nil
		},
	)
	if afterCorruption != nil {
		_ = afterCorruption.Close()
	}
	if badReader != nil {
		_ = badReader.Close()
	}
	require.ErrorIs(t, err, errJournalCorrupt)
}
