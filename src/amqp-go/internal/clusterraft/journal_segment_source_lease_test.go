package clusterraft

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
)

// Retain is the smallest cross-package contract needed by a cold message or
// captured encoder. The returned reader must itself support Retain, and each
// release must be independent and idempotent.
func retainJournalSourceFixture(source io.ReaderAt) (io.ReaderAt, func(), error) {
	if retained, ok := source.(interface {
		Retain() (io.ReaderAt, func(), error)
	}); ok {
		return retained.Retain()
	}
	return nil, nil, errors.New("journal command source has no retained ownership")
}

func TestJournalSegmentRetainedSourceOutlivesMessageUntilCaptureRelease(t *testing.T) {
	const group = uint64(11)
	dir := t.TempDir()
	command := bytes.Repeat([]byte{0xa1}, 32<<10)
	unrelatedCommand := bytes.Repeat([]byte{0xb2}, len(command))
	entry := journalCheckpointFixtureEntry(1, 1, append(make([]byte, len(key{})), command...))
	unrelatedEntry := journalCheckpointFixtureEntry(2, 1, append(make([]byte, len(key{})), unrelatedCommand...))
	genesis, _ := journalCheckpointFixtureFrame(t, 1, 0, journalCheckpointFixtureRecord{
		group, journalGroupOp{kind: journalGroupCreate, identity: journalCheckpointFixtureIdentity(group)},
	})
	publish, publishRefs := journalCheckpointFixtureFrame(t, 2, int64(len(genesis)), journalCheckpointFixtureRecord{
		group, journalGroupOp{kind: journalGroupReady, hard: journalCheckpointFixtureHard(1, 0, 1), entries: []*pb.Entry{entry}},
	})
	unrelated, unrelatedRefs := journalCheckpointFixtureFrame(t, 3, int64(len(genesis)+len(publish)), journalCheckpointFixtureRecord{
		group, journalGroupOp{kind: journalGroupReady, hard: journalCheckpointFixtureHard(1, 0, 2), entries: []*pb.Entry{unrelatedEntry}},
	})
	extentBytes := int64(len(publish))
	require.Equal(t, len(publish), len(unrelated))
	output, reader, _ := openTestRetainedJournalSegments(t, dir, true, extentBytes, 128<<10, journalSegmentHooks{}, nil)
	var end int64
	for _, frame := range [][]byte{genesis, publish, unrelated} {
		writeRetainedJournalFixtureFrame(t, output, frame, end)
		end += int64(len(frame))
	}
	node := &journalGroupNode{
		dir: dir, reader: reader, groups: make(map[uint64]*journalGroupState),
		syncFile: syncJournalFile, syncDir: syncDirectory,
	}
	_, err := scanJournal(reader, end, end, 1, node.replayBatch)
	require.NoError(t, err)
	store := &journalGroupStore{node: node, group: group, state: node.groups[group]}
	require.Equal(t, append(append([]journalEntryRef(nil), publishRefs[0]...), unrelatedRefs[0]...), store.state.entries)
	source, err := store.commandSource(entry)
	require.NoError(t, err)
	unrelatedSource, err := store.commandSource(unrelatedEntry)
	require.NoError(t, err)

	// Two captures in the same group can finish in either order. A capture
	// from another group cannot retain this group's journal history.
	_, releaseMessage, err := retainJournalSourceFixture(source)
	require.NoError(t, err)
	defer releaseMessage()
	firstCapture, err := reader.store.pinCapture(group)
	require.NoError(t, err)
	defer firstCapture()
	secondCapture, err := reader.store.pinCapture(group)
	require.NoError(t, err)
	defer secondCapture()
	otherCapture, err := reader.store.pinCapture(12)
	require.NoError(t, err)
	defer otherCapture()
	_, releaseUnrelated, err := retainJournalSourceFixture(unrelatedSource)
	require.NoError(t, err)
	defer releaseUnrelated()

	// Materialize both bodies into a real durable snapshot, then select a
	// checkpoint after it. The original command ranges remain immutable, but
	// neither the local Raft state nor recovery roots need their old entries.
	snapshot, err := node.stageSnapshot(&pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{Index: new(uint64(2)), Term: new(uint64(1)), ConfState: &pb.ConfState{Voters: []uint64{1}}},
		Data:     encodeSnapshot(1, bytes.Join([][]byte{command, unrelatedCommand}, nil)),
	})
	require.NoError(t, err)
	checkpoint, _ := journalCheckpointFixtureFrame(t, 4, end, journalCheckpointFixtureRecord{
		group, journalGroupOp{kind: journalGroupSnap, snapshot: snapshot},
	})
	writeRetainedJournalFixtureFrame(t, output, checkpoint, end)
	end += int64(len(checkpoint))
	require.NoError(t, store.state.publishCheckpoint(snapshot))
	require.Empty(t, store.state.entries)
	require.NoError(t, output.rotate(end))
	staged, err := reader.stageCheckpoint(4, end)
	require.NoError(t, err)
	require.NoError(t, output.installCheckpoint(staged))
	manifest, err := loadJournalSegmentManifest(dir)
	require.NoError(t, err)
	require.Len(t, manifest.extents, 5)
	require.Equal(t, staged.ref, manifest.checkpoint)
	require.Less(t, publishRefs[0][0].offset, manifest.extents[2].base)
	require.GreaterOrEqual(t, publishRefs[0][0].offset, manifest.extents[1].base)
	require.GreaterOrEqual(t, unrelatedRefs[0][0].offset, manifest.extents[2].base)
	original := reader.store.path(manifest.extents[1].id)
	later := reader.store.path(manifest.extents[2].id)
	require.Empty(t, reader.store.checkpointOwners, "only live source leases should keep these files")
	require.NoError(t, output.reclaimSealedFiles())
	require.NoFileExists(t, reader.store.path(manifest.extents[0].id))
	require.FileExists(t, original)
	require.FileExists(t, later)
	releaseUnrelated()
	releaseMessage()
	releaseMessage()
	secondCapture()
	require.NoError(t, output.reclaimSealedFiles())
	require.FileExists(t, later, "a completed capture cannot shorten an earlier capture's lifetime")
	require.FileExists(t, original, "an outstanding capture owns the body after ACK")
	body := make([]byte, len(command))
	require.NoError(t, readJournalAt(source, body, 0))
	require.Equal(t, command, body)
	firstCapture()
	require.NoError(t, output.reclaimSealedFiles())
	require.NoFileExists(t, later)
	require.NoFileExists(t, original)
}
