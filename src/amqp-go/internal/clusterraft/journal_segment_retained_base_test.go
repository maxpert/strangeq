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
)

// Build fixture bytes independently of manifest validation.
func retainedBaseManifestFixture(magic string, m journalSegmentManifest) []byte {
	data := append([]byte(nil), magic...)
	data = binary.BigEndian.AppendUint64(data, uint64(m.extentBytes))
	data = binary.BigEndian.AppendUint32(data, uint32(len(m.extents)))
	for _, extent := range m.extents {
		data = binary.BigEndian.AppendUint64(data, extent.id)
		data = binary.BigEndian.AppendUint64(data, uint64(extent.base))
		data = binary.BigEndian.AppendUint64(data, uint64(extent.sealedBytes))
		data = binary.BigEndian.AppendUint64(data, extent.firstSequence)
	}
	anchor := m.checkpoint
	data = binary.BigEndian.AppendUint64(data, anchor.sequence)
	data = binary.BigEndian.AppendUint64(data, uint64(anchor.end))
	data = binary.BigEndian.AppendUint64(data, uint64(anchor.size))
	data = binary.BigEndian.AppendUint32(data, anchor.checksum)
	data = append(data, anchor.name...)
	return binary.BigEndian.AppendUint32(data, crc32.Checksum(data, journalChecksum))
}

func TestJournalSegmentRetainedBaseV3ReopensBeyondVirtualCapacity(t *testing.T) {
	const (
		group         = uint64(11)
		firstSequence = uint64(41)
		extentID      = uint64(73)
		extentBytes   = int64(4096)
		base          = int64(journalGroupMaxFileBytes + 4096)
	)
	dir := t.TempDir()
	identity := journalCheckpointFixtureIdentity(group)
	seed := &journalSegmentStorage{
		dir: dir, quota: &journalSegmentQuota{limit: extentBytes},
		hooks: journalSegmentHooks{syncFile: syncJournalFile, syncDir: syncDirectory},
	}
	// The checkpoint owns no old payload references. All preceding extents
	// may therefore be absent, and its boundary is retained extent index zero.
	anchor, err := seed.writeCheckpoint(firstSequence-1, base,
		map[uint64]*journalGroupState{group: {identity: identity}}, nil)
	require.NoError(t, err)
	require.True(t, anchor.valid())
	decoded, err := readJournalCheckpoint(filepath.Join(dir, anchor.name), anchor, nil)
	require.NoError(t, err, "seed checkpoint must pass the production checksum and metadata decoder")
	require.Len(t, decoded, 1)
	require.Equal(t, identity, decoded[group].identity)
	require.Empty(t, decoded[group].entries)

	manifest := journalSegmentManifest{
		extentBytes: extentBytes, checkpoint: anchor,
		extents: []journalSegmentExtent{{id: extentID, base: base, firstSequence: firstSequence}},
	}
	firstBody, nextBody := []byte("retained tail"), []byte("appended above eight GiB")
	first, firstRefs := journalCheckpointFixtureFrame(t, firstSequence, base,
		journalCheckpointFixtureRecord{group, journalGroupOp{kind: journalGroupReady,
			hard:    journalCheckpointFixtureHard(1, 1, 1),
			entries: []*pb.Entry{journalCheckpointFixtureEntry(1, 1, firstBody)},
		}})
	nextOffset := base + int64(len(first))
	next, nextRefs := journalCheckpointFixtureFrame(t, firstSequence+1, nextOffset,
		journalCheckpointFixtureRecord{group, journalGroupOp{kind: journalGroupReady,
			hard:    journalCheckpointFixtureHard(1, 1, 2),
			entries: []*pb.Entry{journalCheckpointFixtureEntry(2, 1, nextBody)},
		}})
	end := nextOffset + int64(len(next))
	third, _ := journalCheckpointFixtureFrame(t, firstSequence+2, end,
		journalCheckpointFixtureRecord{group, journalGroupOp{kind: journalGroupReady,
			entries: []*pb.Entry{journalCheckpointFixtureEntry(3, 1, []byte("capacity rejection"))},
		}})
	orphan := journalFrameFixture(t, firstSequence-1,
		journalRecord{group: 99, body: bytes.Repeat([]byte("x"), 256)})
	require.Greater(t, len(orphan), len(third))
	require.Less(t, int64(len(first)+len(next)+len(third)), extentBytes, "no rotation is needed in this fixture")
	limit := anchor.size + int64(len(first)+len(next))
	require.Less(t, limit, int64(4096), "the physical fixture must remain tiny")
	require.Greater(t, base, int64(journalGroupMaxFileBytes))
	require.NoError(t, os.WriteFile(seed.path(extentID), first, 0600))
	require.NoError(t, os.WriteFile(seed.path(extentID-1), orphan, 0600))
	encoded := retainedBaseManifestFixture(journalSegmentManifestMagic, manifest)
	require.Len(t, encoded, journalSegmentManifestHeader+journalSegmentManifestExtent+
		journalSegmentManifestAnchor+len(anchor.name)+journalSegmentManifestTrailer)
	require.Equal(t, journalSegmentManifestMagic, string(encoded[:8]))
	require.Equal(t, uint64(base), binary.BigEndian.Uint64(encoded[28:36]))
	require.Equal(t, firstSequence, binary.BigEndian.Uint64(encoded[44:52]))
	require.Equal(t, firstSequence-1, binary.BigEndian.Uint64(encoded[52:60]))
	require.Equal(t, uint64(base), binary.BigEndian.Uint64(encoded[60:68]))
	require.NoError(t, os.WriteFile(filepath.Join(dir, journalSegmentManifestName), encoded, 0600))

	var recovered *journalGroupNode
	var replayed []uint64
	restores := 0
	open := func() (*journalSegmentOutput, *journalSegmentReader, journalScan, error) {
		recovered = &journalGroupNode{dir: dir, groups: make(map[uint64]*journalGroupState)}
		replayed = nil
		wantRestores := restores + 1
		return openJournalSegmentsWithQuota(dir, false, extentBytes, limit, journalSegmentHooks{
			restoreCheckpoint: func(sequence uint64, at int64, groups map[uint64]*journalGroupState) error {
				require.Equal(t, firstSequence-1, sequence)
				require.Equal(t, base, at)
				require.Len(t, groups, 1)
				require.Equal(t, identity, groups[group].identity)
				recovered.groups = groups
				restores++
				return nil
			},
		}, func(records []journalRecord) error {
			require.Equal(t, wantRestores, restores, "checkpoint restoration must precede retained tail replay on every reopen")
			for _, record := range records {
				replayed = append(replayed, record.location.sequence)
			}
			return recovered.replayBatch(records)
		})
	}
	output, reader, tip, err := open()
	t.Cleanup(func() {
		if output != nil {
			_ = output.Close()
		}
		if reader != nil {
			_ = reader.Close()
		}
	})
	require.NoError(t, err, "RETAINED_BASE_V3_REOPEN: accept a retained origin above 8 GiB and checkpoint boundary at retained index zero")
	require.Equal(t, 1, restores)
	require.Equal(t, []uint64{firstSequence}, replayed, "the stale orphan must never replay")
	require.NoFileExists(t, seed.path(extentID-1))
	require.Equal(t, nextOffset, tip.end)
	require.Equal(t, firstSequence+1, tip.nextSequence)
	require.Equal(t, firstRefs[0], recovered.groups[group].entries)
	requireJournalCheckpointFixtureEntry(t, reader, firstRefs[0][0], firstBody)

	writeRetainedJournalFixtureFrame(t, output, next, tip.end)
	requireJournalCheckpointFixtureEntry(t, reader, nextRefs[0][0], nextBody)
	before := retainedJournalFixtureFiles(t, dir)
	n, err := output.WriteAt(third, end)
	require.ErrorIs(t, err, errJournalTooLarge, "selected files fill the physical capacity")
	require.Zero(t, n)
	require.Equal(t, before, retainedJournalFixtureFiles(t, dir), "capacity rejection must not alter retained artifacts")
	require.NoError(t, output.Close())
	require.NoError(t, reader.Close())

	reopened, reopenedReader, tip, err := open()
	t.Cleanup(func() {
		if reopened != nil {
			_ = reopened.Close()
		}
		if reopenedReader != nil {
			_ = reopenedReader.Close()
		}
	})
	require.NoError(t, err)
	require.Equal(t, 2, restores)
	require.Equal(t, []uint64{firstSequence, firstSequence + 1}, replayed)
	require.Equal(t, end, tip.end)
	require.Equal(t, firstSequence+2, tip.nextSequence)
	require.Equal(t, append(firstRefs[0], nextRefs[0]...), recovered.groups[group].entries)
	require.Equal(t, uint64(2), recovered.groups[group].hard.GetCommit())
	requireJournalCheckpointFixtureEntry(t, reopenedReader, firstRefs[0][0], firstBody)
	requireJournalCheckpointFixtureEntry(t, reopenedReader, nextRefs[0][0], nextBody)
	require.Equal(t, before, retainedJournalFixtureFiles(t, dir), "recovery must preserve all selected files")
}
