package clusterraft

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The budget covers extent and checkpoint-index files only; these
// fixtures make no whole-directory claim about snapshots or manifest space.
func openRetainedJournalSegmentFixture(dir string, create bool, extentBytes, retainedBytes int64,
	hooks journalSegmentHooks, visit func([]journalRecord) error,
) (*journalSegmentOutput, *journalSegmentReader, journalScan, error) {
	return openJournalSegmentsWithQuota(dir, create, extentBytes, retainedBytes, hooks, visit)
}

func openTestRetainedJournalSegments(t *testing.T, dir string, create bool, extentBytes, retainedBytes int64,
	hooks journalSegmentHooks, visit func([]journalRecord) error,
) (*journalSegmentOutput, *journalSegmentReader, journalScan) {
	t.Helper()
	output, reader, state, err := openRetainedJournalSegmentFixture(dir, create, extentBytes, retainedBytes, hooks, visit)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = output.Close()
		_ = reader.Close()
	})
	return output, reader, state
}

func writeRetainedJournalFixtureFrame(t *testing.T, output *journalSegmentOutput, frame []byte, offset int64) {
	t.Helper()
	n, err := output.WriteAt(frame, offset)
	require.NoError(t, err)
	require.Equal(t, len(frame), n)
	require.NoError(t, output.Sync())
}

func retainedJournalFixtureFiles(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	retained := make(map[string][]byte)
	for _, file := range files {
		name := file.Name()
		if !strings.HasPrefix(name, "extent-") && !strings.HasPrefix(name, journalCheckpointPrefix) {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, name))
		require.NoError(t, err)
		retained[name] = data
	}
	return retained
}

func TestJournalSegmentRecoveryCollectsUnselectedExtentBeforeRotation(t *testing.T) {
	dir := t.TempDir()
	first := journalFrameFixture(t, 1, journalRecord{group: 11, body: []byte("selected")})
	second := journalFrameFixture(t, 2, journalRecord{group: 22, body: []byte("nextdata")})
	orphan := journalFrameFixture(t, 2, journalRecord{group: 99, body: []byte("orphaned")})
	require.Equal(t, len(first), len(second))
	require.Equal(t, len(first), len(orphan))
	extentBytes := int64(len(first))
	limit := int64(len(first) + len(second))
	seed, seedReader, _ := openTestRetainedJournalSegments(t, dir, true, extentBytes, limit, journalSegmentHooks{}, nil)
	writeRetainedJournalFixtureFrame(t, seed, first, 0)
	orphanPath := seed.store.path(2)
	require.NoError(t, seed.Close())
	require.NoError(t, seedReader.Close())
	// A complete known-format extent exists outside the selected manifest.
	// It consumes disk space but must never contribute replayed state.
	require.NoError(t, os.WriteFile(orphanPath, orphan, 0600))
	rotations := 0
	var replayed []journalRecord
	output, reader, recovered := openTestRetainedJournalSegments(t, dir, false, extentBytes, limit, journalSegmentHooks{
		publishManifest: func(install func() error) error {
			rotations++
			return install()
		},
	}, func(records []journalRecord) error {
		for _, record := range records {
			replayed = append(replayed, journalRecord{group: record.group, location: record.location})
		}
		return nil
	})
	require.Len(t, replayed, 1)
	require.Equal(t, uint64(11), replayed[0].group)
	require.Equal(t, uint64(2), recovered.nextSequence)
	require.Equal(t, extentBytes, recovered.end)
	require.NoFileExists(t, orphanPath)
	require.Equal(t, extentBytes, reader.store.quota.used)
	requireJournalVirtualBody(t, reader, replayed[0].location, []byte("selected"))
	writeRetainedJournalFixtureFrame(t, output, second, recovered.end)
	require.Equal(t, 1, rotations)
	require.Equal(t, limit, reader.store.quota.used)
}

// Two tiny create records produce a sealed prefix and a full active extent.
// The resulting one-group checkpoint has a deterministic encoded size.
func seedRetainedCheckpointFixture(t *testing.T) (dir string, first, third []byte, indexBytes int64) {
	t.Helper()
	dir = t.TempDir()
	op := journalGroupOp{kind: journalGroupCreate, identity: journalCheckpointFixtureIdentity(11)}
	first, _ = journalCheckpointFixtureFrame(t, 1, 0, journalCheckpointFixtureRecord{11, op})
	second, _ := journalCheckpointFixtureFrame(t, 2, int64(len(first)), journalCheckpointFixtureRecord{
		22, journalGroupOp{kind: journalGroupCreate, identity: journalCheckpointFixtureIdentity(22)},
	})
	third, _ = journalCheckpointFixtureFrame(t, 3, int64(len(first)+len(second)), journalCheckpointFixtureRecord{
		33, journalGroupOp{kind: journalGroupCreate, identity: journalCheckpointFixtureIdentity(33)},
	})
	require.Equal(t, len(first), len(second))
	require.Equal(t, len(first), len(third))
	metadataBytes, err := op.size()
	require.NoError(t, err)
	indexBytes = int64(journalCheckpointHeader + journalCheckpointGroupHeader + metadataBytes + journalCheckpointTrailer)
	seed, reader, _ := openTestRetainedJournalSegments(t, dir, true, int64(len(first)), 4096, journalSegmentHooks{}, nil)
	writeRetainedJournalFixtureFrame(t, seed, first, 0)
	writeRetainedJournalFixtureFrame(t, seed, second, int64(len(first)))
	require.NoError(t, seed.Close())
	require.NoError(t, reader.Close())
	manifest, err := loadJournalSegmentManifest(dir)
	require.NoError(t, err)
	require.Len(t, manifest.extents, 2)
	require.Equal(t, int64(len(first)), manifest.extents[0].sealedBytes)
	return dir, first, third, indexBytes
}

func TestJournalSegmentRetainedCapacityRejectsUnreservedCheckpoint(t *testing.T) {
	dir, first, _, indexBytes := seedRetainedCheckpointFixture(t)
	extentBytes := int64(len(first))
	limit := 2*extentBytes + indexBytes - 1
	_, reader, recovered := openTestRetainedJournalSegments(t, dir, false, extentBytes, limit, journalSegmentHooks{}, nil)
	require.Equal(t, 2*extentBytes, recovered.end)
	before := retainedJournalFixtureFiles(t, dir)
	manifestBefore, err := os.ReadFile(filepath.Join(dir, journalSegmentManifestName))
	require.NoError(t, err)
	staged, err := reader.stageCheckpoint(1, extentBytes)
	require.ErrorIs(t, err, errJournalTooLarge, "checkpoint staging must reserve its retained bytes before creating the index")
	require.Empty(t, staged.ref.name)
	require.Equal(t, before, retainedJournalFixtureFiles(t, dir))
	manifestAfter, err := os.ReadFile(filepath.Join(dir, journalSegmentManifestName))
	require.NoError(t, err)
	require.Equal(t, manifestBefore, manifestAfter)
	read := make([]byte, len(first))
	require.NoError(t, readJournalAt(reader, read, 0))
	require.Equal(t, first, read)
}

func TestJournalSegmentRetainedCapacityCountsStagedCheckpointBeforeRotation(t *testing.T) {
	dir, first, third, indexBytes := seedRetainedCheckpointFixture(t)
	extentBytes := int64(len(first))
	limit := 2*extentBytes + indexBytes + int64(len(third)) - 1
	rotations := 0
	output, reader, recovered := openTestRetainedJournalSegments(t, dir, false, extentBytes, limit, journalSegmentHooks{
		publishManifest: func(install func() error) error {
			rotations++
			return install()
		},
	}, nil)
	staged, err := reader.stageCheckpoint(1, extentBytes)
	require.NoError(t, err)
	require.Equal(t, indexBytes, staged.ref.size)
	require.FileExists(t, filepath.Join(dir, staged.ref.name))
	manifest, err := loadJournalSegmentManifest(dir)
	require.NoError(t, err)
	require.Empty(t, manifest.checkpoint.name, "the staged index is retained even before authority publication")
	before := retainedJournalFixtureFiles(t, dir)
	manifestBefore, err := os.ReadFile(filepath.Join(dir, journalSegmentManifestName))
	require.NoError(t, err)
	require.LessOrEqual(t, recovered.end+int64(len(third)), limit, "extent bytes alone fit the budget")

	n, err := output.WriteAt(third, recovered.end)
	require.ErrorIs(t, err, errJournalTooLarge, "a durable staged index must consume capacity before its manifest is installed")
	require.Zero(t, n)
	require.Zero(t, rotations)
	require.Equal(t, before, retainedJournalFixtureFiles(t, dir), "keep the staged index and original extents after rejecting the append")
	manifestAfter, err := os.ReadFile(filepath.Join(dir, journalSegmentManifestName))
	require.NoError(t, err)
	require.Equal(t, manifestBefore, manifestAfter)
	read := make([]byte, len(first))
	require.NoError(t, readJournalAt(reader, read, 0))
	require.Equal(t, first, read)
}
