package clusterraft

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestJournalSegmentCheckpointPreservesCommandSpan(t *testing.T) {
	const group = uint64(11)
	dir := t.TempDir()
	command := []byte("retained command bytes")
	entry := journalCheckpointFixtureEntry(1, 3, append(make([]byte, len(key{})), command...))
	wire, err := proto.Marshal(entry)
	require.NoError(t, err)
	meta, err := parseEntryWire(wire)
	require.NoError(t, err)
	ref := journalEntryRef{
		index: 1, term: 3, size: uint32(len(wire)), checksum: crc32.Checksum(wire, journalChecksum),
		dataOffset: meta.dataOffset, dataSize: meta.dataSize,
	}
	state := &journalGroupState{
		identity: journalCheckpointFixtureIdentity(group), hard: journalCheckpointFixtureHard(3, 1, 1),
		entries: []journalEntryRef{ref},
	}
	covered := []journalSegmentExtent{{id: 1, firstSequence: 1, sealedBytes: int64(len(wire))}}
	storage := &journalSegmentStorage{
		dir: dir, hooks: journalSegmentHooks{syncFile: syncJournalFile, syncDir: syncDirectory},
		quota: &journalSegmentQuota{limit: journalGroupMaxFileBytes},
	}
	checkpoint, err := storage.writeCheckpoint(1, int64(len(wire)), map[uint64]*journalGroupState{group: state}, covered)
	require.NoError(t, err)
	path := filepath.Join(dir, checkpoint.name)
	groups, err := readJournalCheckpoint(path, checkpoint, covered)
	require.NoError(t, err)
	require.Equal(t, state.entries, groups[group].entries)

	entryPath := filepath.Join(dir, "entry")
	require.NoError(t, os.WriteFile(entryPath, wire, 0600))
	file, err := os.Open(entryPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })
	store := &journalGroupStore{node: &journalGroupNode{reader: file}, state: groups[group]}
	source, err := store.commandSource(entry)
	require.NoError(t, err)
	restored, err := io.ReadAll(source)
	require.NoError(t, err)
	require.Equal(t, command, restored, "the restored source must exclude the proposal key and preserve the command")

	original, err := os.ReadFile(path)
	require.NoError(t, err)
	metadataSize := int(binary.BigEndian.Uint32(original[journalCheckpointHeader+8 : journalCheckpointHeader+12]))
	entryAt := journalCheckpointHeader + journalCheckpointGroupHeader + metadataSize
	for _, test := range []struct {
		name   string
		mutate func([]byte)
	}{
		{"legacy_codec", func(data []byte) { copy(data[:8], "SQJIDX01") }},
		{"offset_outside_entry", func(data []byte) {
			binary.BigEndian.PutUint32(data[entryAt+32:entryAt+36], ref.size+1)
		}},
		{"span_past_entry", func(data []byte) {
			binary.BigEndian.PutUint32(data[entryAt+36:entryAt+40], ref.size-ref.dataOffset+1)
		}},
		{"span_overflow", func(data []byte) {
			binary.BigEndian.PutUint32(data[entryAt+36:entryAt+40], ^uint32(0))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := bytes.Clone(original)
			test.mutate(data)
			selected := checkpoint
			selected.checksum = crc32.Checksum(data[:len(data)-journalCheckpointTrailer], journalChecksum)
			binary.BigEndian.PutUint32(data[len(data)-journalCheckpointTrailer:], selected.checksum)
			require.NoError(t, os.WriteFile(path, data, 0600))
			_, err := readJournalCheckpoint(path, selected, covered)
			require.ErrorIs(t, err, errJournalCorrupt, "valid checksums must not bypass codec or span validation")
		})
	}
}

func journalCheckpointBoundsRecord(t *testing.T, group uint64, op journalGroupOp) journalRecord {
	t.Helper()
	size, err := op.size()
	require.NoError(t, err)
	body := make([]byte, size)
	_, err = op.encode(body)
	require.NoError(t, err)
	return journalRecord{group: group, body: body}
}

func TestJournalSegmentCheckpointRejectsMetadataBeforeReplay(t *testing.T) {
	t.Run("incoming_metadata", func(t *testing.T) {
		body := make([]byte, maxJournalCheckpointMetadata+1)
		body[0], body[1] = journalGroupVersion, journalGroupCreate
		binary.BigEndian.PutUint32(body[2:6], 16)
		binary.BigEndian.PutUint32(body[6:10], uint32(len(body)-journalOpHeader-16))
		copy(body[journalOpHeader:], journalCheckpointFixtureIdentity(11))
		// Invalid protobuf bytes make the error class distinguish the length
		// guard from a later decode failure.
		copy(body[journalOpHeader+16:], bytes.Repeat([]byte{0x80}, len(body)-journalOpHeader-16))
		shadow := &journalGroupNode{groups: make(map[uint64]*journalGroupState)}
		retained := int64(journalCheckpointHeader + journalCheckpointTrailer)
		err := replayJournalCheckpointRecord(shadow, &retained, journalRecord{group: 11, body: body})
		require.ErrorIs(t, err, errJournalTooLarge)
		require.Empty(t, shadow.groups)
	})
	t.Run("retained_metadata", func(t *testing.T) {
		hard := journalCheckpointFixtureHard(1, 0, 0)
		hard.ProtoReflect().SetUnknown(bytes.Repeat([]byte{0x78, 1}, 20<<10))
		prior := &journalGroupState{identity: journalCheckpointFixtureIdentity(11), hard: hard}
		shadow := &journalGroupNode{groups: map[uint64]*journalGroupState{11: prior}}
		snapshot := journalCheckpointFixtureSnapshot("bounded", 1, 1, []byte("snapshot"))
		snapshot.metadata.ProtoReflect().SetUnknown(bytes.Repeat([]byte{0x78, 1}, 20<<10))
		record := journalCheckpointBoundsRecord(t, 11, journalGroupOp{kind: journalGroupReady, snapshot: snapshot})
		retained := int64(journalCheckpointHeader+journalCheckpointTrailer) + journalCheckpointGroupBytes(prior)
		before := retained
		err := replayJournalCheckpointRecord(shadow, &retained, record)
		require.ErrorIs(t, err, errJournalTooLarge)
		require.Equal(t, before, retained)
		require.Nil(t, prior.snapshot.metadata, "oversized combined metadata must not reach replay")
	})
}

func TestJournalSegmentCheckpointChargesMetadataAcrossGroups(t *testing.T) {
	// Share immutable protobuf bytes in this fixture so the production-sized
	// aggregate bound can be exercised without allocating 64 MiB of test data.
	hard := &pb.HardState{}
	hard.ProtoReflect().SetUnknown(bytes.Repeat([]byte{0x78, 1}, (maxJournalCheckpointMetadata-journalOpHeader-16)/2))
	shadow := &journalGroupNode{groups: make(map[uint64]*journalGroupState)}
	retained := int64(journalCheckpointHeader + journalCheckpointTrailer)
	for group := uint64(1); group < 1024; group++ {
		state := &journalGroupState{identity: journalCheckpointFixtureIdentity(group), hard: hard}
		shadow.groups[group] = state
		retained += journalCheckpointGroupBytes(state)
	}
	require.Less(t, retained, int64(maxJournalCheckpointBytes))
	record := journalCheckpointBoundsRecord(t, 1024, journalGroupOp{
		kind: journalGroupCreate, identity: journalCheckpointFixtureIdentity(1024), hard: hard,
	})
	before := retained
	err := replayJournalCheckpointRecord(shadow, &retained, record)
	require.ErrorIs(t, err, errJournalTooLarge)
	require.Equal(t, before, retained)
	require.NotContains(t, shadow.groups, uint64(1024), "aggregate metadata admission must precede replay")
}
