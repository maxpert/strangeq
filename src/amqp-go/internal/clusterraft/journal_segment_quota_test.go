package clusterraft

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJournalSegmentQuotaReservesCheckpointDuringConcurrentAppend(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	dir, first, third, indexBytes := seedRetainedCheckpointFixture(t)
	extentBytes := int64(len(first))
	limit := 2*extentBytes + indexBytes + int64(len(third)) - 1
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	output, reader, recovered := openTestRetainedJournalSegments(t, dir, false, extentBytes, limit, journalSegmentHooks{
		syncFile: func(file *os.File) error {
			if strings.HasPrefix(filepath.Base(file.Name()), journalCheckpointPrefix) {
				close(started)
				<-release
			}
			return syncJournalFile(file)
		},
	}, nil)
	type stageResult struct {
		staged stagedJournalSegmentCheckpoint
		err    error
	}
	done := make(chan stageResult, 1)
	go func() {
		staged, err := reader.stageCheckpoint(1, extentBytes)
		done <- stageResult{staged, err}
	}()
	select {
	case <-started:
	case result := <-done:
		t.Fatalf("checkpoint finished before its sync gate: %v", result.err)
	case <-ctx.Done():
		t.Fatal("checkpoint did not reach its sync gate")
	}
	n, err := output.WriteAt(third, recovered.end)
	require.ErrorIs(t, err, errJournalTooLarge, "an in-progress checkpoint must reserve bytes before its durability fence")
	require.Zero(t, n)
	read := make([]byte, len(first))
	require.NoError(t, readJournalAt(reader, read, 0))
	require.Equal(t, first, read)
	unblock()
	select {
	case result := <-done:
		require.NoError(t, result.err)
		require.Equal(t, indexBytes, result.staged.ref.size)
	case <-ctx.Done():
		t.Fatal("checkpoint did not finish after its sync gate released")
	}
}

func TestJournalSegmentQuotaReleasesFailedCheckpointCreation(t *testing.T) {
	dir, first, _, indexBytes := seedRetainedCheckpointFixture(t)
	extentBytes := int64(len(first))
	_, reader, _ := openTestRetainedJournalSegments(t, dir, false, extentBytes, 2*extentBytes+indexBytes, journalSegmentHooks{}, nil)
	before := retainedJournalFixtureFiles(t, dir)
	manifest, err := loadJournalSegmentManifest(dir)
	require.NoError(t, err)
	groups := map[uint64]*journalGroupState{11: {identity: journalCheckpointFixtureIdentity(11)}}
	// No concurrent operation uses the store while only the creation path is
	// redirected. Replay and all source files remain at their original paths.
	reader.store.dir = filepath.Join(dir, "missing-parent")
	failed, err := reader.store.writeCheckpoint(1, extentBytes, groups, manifest.extents[:1])
	reader.store.dir = dir
	require.Error(t, err)
	require.Empty(t, failed.name)
	require.Equal(t, before, retainedJournalFixtureFiles(t, dir))
	staged, err := reader.stageCheckpoint(1, extentBytes)
	require.NoError(t, err, "a creation failure with no artifact must return its exact reservation")
	require.Equal(t, indexBytes, staged.ref.size)
}

func TestJournalSegmentQuotaKeepsFailedIndexUntilExclusiveRecovery(t *testing.T) {
	dir, first, third, indexBytes := seedRetainedCheckpointFixture(t)
	extentBytes := int64(len(first))
	limit := 2*extentBytes + max(int64(len(third)), indexBytes)
	failure := errors.New("injected checkpoint sync failure")
	output, reader, _ := openTestRetainedJournalSegments(t, dir, false, extentBytes, limit, journalSegmentHooks{
		syncFile: func(file *os.File) error {
			if strings.HasPrefix(filepath.Base(file.Name()), journalCheckpointPrefix) {
				return failure
			}
			return syncJournalFile(file)
		},
	}, nil)
	before := retainedJournalFixtureFiles(t, dir)
	manifestBefore, err := os.ReadFile(filepath.Join(dir, journalSegmentManifestName))
	require.NoError(t, err)
	staged, err := reader.stageCheckpoint(1, extentBytes)
	require.ErrorIs(t, err, failure)
	require.Empty(t, staged.ref.name)
	after := retainedJournalFixtureFiles(t, dir)
	require.Len(t, after, len(before)+1, "a failed created index must remain available for recovery inspection")
	for name, original := range before {
		require.Equal(t, original, after[name])
	}
	require.ErrorIs(t, reader.store.quota.reserve(limit-2*extentBytes-indexBytes+1), errJournalTooLarge, "a created index retains its full charge after failure")
	require.NoError(t, output.Close())
	require.NoError(t, reader.Close())

	output, reader, recovered := openTestRetainedJournalSegments(t, dir, false, extentBytes, limit, journalSegmentHooks{}, nil)
	require.Equal(t, before, retainedJournalFixtureFiles(t, dir), "exclusive recovery collects the unpublished index")
	manifestAfter, err := os.ReadFile(filepath.Join(dir, journalSegmentManifestName))
	require.NoError(t, err)
	require.Equal(t, manifestBefore, manifestAfter)
	writeRetainedJournalFixtureFrame(t, output, third, recovered.end)
	read := make([]byte, len(first))
	require.NoError(t, readJournalAt(reader, read, 0))
	require.Equal(t, first, read)
}

func TestJournalSegmentQuotaKeepsPartialAppendChargeUntilRecovery(t *testing.T) {
	dir := t.TempDir()
	first := journalFrameFixture(t, 1, journalRecord{group: 11, body: []byte("selected")})
	second := journalFrameFixture(t, 2, journalRecord{group: 22, body: []byte("nextdata")})
	limit := int64(len(first) + len(second))
	seed, seedReader, _ := openTestRetainedJournalSegments(t, dir, true, limit, limit, journalSegmentHooks{}, nil)
	writeRetainedJournalFixtureFrame(t, seed, first, 0)
	require.NoError(t, seed.Close())
	require.NoError(t, seedReader.Close())
	output, reader, recovered := openTestRetainedJournalSegments(t, dir, false, limit, limit, journalSegmentHooks{
		writeAt: func(file *os.File, data []byte, offset int64) (int, error) {
			return file.WriteAt(data[:3], offset)
		},
	}, nil)
	n, err := output.WriteAt(second, recovered.end)
	require.ErrorIs(t, err, io.ErrShortWrite)
	require.Equal(t, 3, n)
	info, err := output.file.Stat()
	require.NoError(t, err)
	require.Equal(t, int64(len(first)+3), info.Size(), "the failed append must leave a real partial tail")
	require.ErrorIs(t, reader.store.quota.reserve(1), errJournalTooLarge, "a partial append must keep its whole reservation")
	read := make([]byte, len(first))
	require.NoError(t, readJournalAt(reader, read, 0))
	require.Equal(t, first, read)
	require.NoError(t, output.Close())
	require.NoError(t, reader.Close())

	output, reader, recovered = openTestRetainedJournalSegments(t, dir, false, limit, limit, journalSegmentHooks{}, nil)
	require.Equal(t, int64(len(first)), recovered.end)
	require.Equal(t, uint64(2), recovered.nextSequence)
	writeRetainedJournalFixtureFrame(t, output, second, recovered.end)
	require.NoError(t, readJournalAt(reader, read, 0))
	require.Equal(t, first, read, "synced active-tail repair must preserve the prior committed prefix")
}

func TestJournalSegmentQuotaRejectsNonRegularOwnedArtifactBeforeInitialization(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "unrelated")
	require.NoError(t, os.WriteFile(target, []byte("keep"), 0600))
	require.NoError(t, os.Symlink(target, filepath.Join(dir, "extent-0000000000000002.raft-journal")))
	output, reader, _, err := openJournalSegmentsWithQuota(dir, true, 256, 1024, journalSegmentHooks{}, nil)
	if output != nil {
		_ = output.Close()
	}
	if reader != nil {
		_ = reader.Close()
	}
	require.ErrorIs(t, err, errJournalCorrupt)
	require.NoFileExists(t, filepath.Join(dir, journalSegmentManifestName), "inventory must precede creating authority")
	kept, err := os.ReadFile(target)
	require.NoError(t, err)
	require.Equal(t, []byte("keep"), kept)
}
