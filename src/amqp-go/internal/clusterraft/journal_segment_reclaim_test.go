package clusterraft

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJournalSegmentReclaimsOnlyAfterManifestIsDurable(t *testing.T) {
	for _, failPublication := range []bool{false, true} {
		name := "stable manifest"
		if failPublication {
			name = "manifest directory sync fails"
		}
		t.Run(name, func(t *testing.T) {
			dir, first, next, _ := seedRetainedCheckpointFixture(t)
			extentBytes := int64(len(first))
			failure := errors.New("retirement directory sync failed")
			armed := false
			output, reader, recovered := openTestRetainedJournalSegments(t, dir, false, extentBytes, 4096, journalSegmentHooks{
				syncDir: func(path string) error {
					if armed && failPublication {
						return failure
					}
					return syncDirectory(path)
				},
			}, nil)
			_, err := installJournalSegmentCheckpointFixture(output, reader, 1, extentBytes)
			require.NoError(t, err)
			manifest := reader.store.manifest
			old := reader.store.path(manifest.extents[0].id)
			require.FileExists(t, old)
			before := reader.store.quota.used

			armed = true
			err = output.reclaimSealedFiles()
			armed = false
			if failPublication {
				require.ErrorIs(t, err, failure)
				require.FileExists(t, old, "an uncertain manifest publication must preserve the old bytes")
				require.Equal(t, before, reader.store.quota.used)
			} else {
				require.NoError(t, err)
				require.NoFileExists(t, old)
				require.Equal(t, before-extentBytes, reader.store.quota.used)
				writeRetainedJournalFixtureFrame(t, output, next, recovered.end)
			}
			require.NoError(t, output.Close())
			require.NoError(t, reader.Close())

			restored := &journalGroupNode{}
			reopened, reopenedReader, tip, err := openJournalSegmentsWithQuota(dir, false, extentBytes, 4096, journalSegmentHooks{
				restoreCheckpoint: func(_ uint64, _ int64, groups map[uint64]*journalGroupState) error {
					restored.groups = groups
					return nil
				},
			}, restored.replayBatch)
			require.NoError(t, err)
			defer reopenedReader.Close()
			defer reopened.Close()
			require.NoFileExists(t, old, "reopen may collect the retired file once the selected manifest is fenced")
			require.Contains(t, restored.groups, uint64(11))
			require.Contains(t, restored.groups, uint64(22))
			if !failPublication {
				require.Contains(t, restored.groups, uint64(33))
				require.Equal(t, recovered.end+extentBytes, tip.end)
			}
		})
	}
}
