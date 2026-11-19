package clusterraft

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessRootCertificationFencesUnprovenExtents(t *testing.T) {
	dir := t.TempDir()
	body := []byte("one journal frame")
	frame := journalFrameFixture(t, 1, journalRecord{group: 11, body: body})
	extentName := func(id uint64) string { return fmt.Sprintf("extent-%016x.raft-journal", id) }
	rootPath := filepath.Join(dir, processPhysicalRoot)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	var hold atomic.Bool
	var mu sync.Mutex
	counts := make(map[string]int)
	var publication []string
	takeCounts := func() map[string]int {
		mu.Lock()
		defer mu.Unlock()
		previous := counts
		counts = make(map[string]int)
		return previous
	}
	takePublication := func() []string {
		mu.Lock()
		defer mu.Unlock()
		previous := publication
		publication = nil
		return previous
	}
	handle := openTestJournalSegments(t, dir, true, int64(2*len(frame)), journalSegmentHooks{
		processRoots: true, processBootID: "incremental-kernel",
		syncFile: func(file *os.File) error {
			name := filepath.Base(file.Name())
			if strings.HasPrefix(name, "extent-") {
				mu.Lock()
				counts[name]++
				mu.Unlock()
			}
			if strings.HasPrefix(name, processRootTempPrefix) {
				mu.Lock()
				publication = append(publication, "physical root file")
				mu.Unlock()
			}
			if name == extentName(2) && hold.CompareAndSwap(true, false) {
				close(entered)
				<-release
			}
			return syncJournalFile(file)
		},
		syncDir: func(path string) error {
			if path == dir {
				mu.Lock()
				publication = append(publication, "directory")
				mu.Unlock()
			}
			return syncDirectory(path)
		},
	}, nil)
	initial, err := readProcessRecoveryRoot(rootPath)
	require.NoError(t, err)
	require.Zero(t, initial.End)
	require.Equal(t, map[string]int{extentName(1): 1}, takeCounts())
	require.Equal(t, []string{"directory", "physical root file", "directory"}, takePublication())
	for range 5 {
		require.NoError(t, awaitJournalCompletion(t, enqueueJournal(t, handle.writer, 11, body, false)).err)
	}
	live, err := readProcessRecoveryRoot(filepath.Join(dir, processLiveRoot))
	require.NoError(t, err)
	manifest, err := decodeJournalSegmentManifest(live.Manifest)
	require.NoError(t, err)
	require.Len(t, manifest.extents, 3)
	require.Empty(t, takeCounts(), "process writes and rotation did not physically fence the new sealed extents")
	require.Empty(t, takePublication())

	hold.Store(true)
	promoted := make(chan error, 1)
	go func() { promoted <- handle.writer.requireFullDurability() }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("promotion did not attempt to fence a process-sealed extent")
	}
	blocked, err := readProcessRecoveryRoot(rootPath)
	require.NoError(t, err)
	require.Equal(t, initial, blocked)
	select {
	case err := <-promoted:
		t.Fatalf("promotion completed before the sealed extent fence returned: %v", err)
	default:
	}
	unblock()
	select {
	case err := <-promoted:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("promotion did not finish after releasing the fence")
	}
	require.Equal(t, map[string]int{extentName(1): 1, extentName(2): 1, extentName(3): 1}, takeCounts())
	require.Equal(t, []string{"directory", "physical root file", "directory"}, takePublication(), "new dependencies must be fenced before the physical root")
	physical, err := readProcessRecoveryRoot(rootPath)
	require.NoError(t, err)
	require.EqualValues(t, 5*len(frame), physical.End)
	livePath := filepath.Join(dir, processLiveRoot)
	liveBefore, err := os.Stat(livePath)
	require.NoError(t, err)

	require.NoError(t, awaitJournalCompletion(t, enqueueJournal(t, handle.writer, 11, body, true)).err)
	require.Equal(t, map[string]int{extentName(3): 1}, takeCounts(), "steady strict append only needs the changed active extent")
	assert.Equal(t, []string{"physical root file", "directory"}, takePublication(), "a strict append with the same dependency names only needs the publication directory fence")
	liveAfter, err := os.Stat(livePath)
	require.NoError(t, err)
	assert.True(t, os.SameFile(liveBefore, liveAfter), "an unchanged live manifest needs no live root replacement")
	physical, err = readProcessRecoveryRoot(rootPath)
	require.NoError(t, err)
	require.EqualValues(t, 6*len(frame), physical.End)

	require.NoError(t, awaitJournalCompletion(t, enqueueJournal(t, handle.writer, 11, body, true)).err)
	require.Equal(t, map[string]int{extentName(3): 1, extentName(4): 2}, takeCounts(), "rotation fences the newly sealed extent and the newly created active extent")
	physical, err = readProcessRecoveryRoot(rootPath)
	require.NoError(t, err)
	require.EqualValues(t, 7*len(frame), physical.End)
	live, err = readProcessRecoveryRoot(livePath)
	require.NoError(t, err)
	manifest, err = decodeJournalSegmentManifest(live.Manifest)
	require.NoError(t, err)
	require.EqualValues(t, 4, manifest.extents[len(manifest.extents)-1].id, "the live root must select the rotated extent")
}

func TestProcessRootCertificationRejectsUnfencedCheckpoint(t *testing.T) {
	dir := t.TempDir()
	frame := func(sequence, group uint64) []byte {
		data, _ := journalCheckpointFixtureFrame(t, sequence, 0, journalCheckpointFixtureRecord{
			group: group, op: journalGroupOp{kind: journalGroupCreate, identity: journalCheckpointFixtureIdentity(group)},
		})
		return data
	}
	first, second := frame(1, 11), frame(2, 22)
	extentBytes := int64(len(first))
	var attempted []string
	fenceFailure := errors.New("injected checkpoint fence failure")
	output, reader, _, err := openJournalSegments(dir, true, extentBytes, journalSegmentHooks{
		processRoots: true, processBootID: "checkpoint-kernel",
		syncFile: func(file *os.File) error {
			name := filepath.Base(file.Name())
			if strings.HasPrefix(name, journalCheckpointPrefix) {
				attempted = append(attempted, name)
				return fenceFailure
			}
			return syncJournalFile(file)
		},
	}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })
	t.Cleanup(func() { _ = output.Close() })
	n, err := output.WriteAt(first, 0)
	require.NoError(t, err)
	require.Equal(t, len(first), n)
	require.NoError(t, output.store.processRoots.certify(output))
	rootPath := filepath.Join(dir, processPhysicalRoot)
	physical, err := readProcessRecoveryRoot(rootPath)
	require.NoError(t, err)
	require.Equal(t, extentBytes, physical.End)
	n, err = output.WriteAt(second, extentBytes)
	require.NoError(t, err)
	require.Equal(t, len(second), n)
	checkpoint, err := installJournalSegmentCheckpointFixture(output, reader, 1, extentBytes)
	require.NoError(t, err)
	require.FileExists(t, checkpoint)
	live, err := readProcessRecoveryRoot(filepath.Join(dir, processLiveRoot))
	require.NoError(t, err)
	manifest, err := decodeJournalSegmentManifest(live.Manifest)
	require.NoError(t, err)
	require.Equal(t, filepath.Base(checkpoint), manifest.checkpoint.name)
	require.Empty(t, attempted, "the selected process checkpoint has never been physically fenced")

	writer, err := newJournalWriter(output, output.store.end, output.nextSequence, testJournalLimits())
	require.NoError(t, err)
	t.Cleanup(func() { _ = writer.close() })
	require.ErrorIs(t, writer.requireFullDurability(), fenceFailure)
	require.Equal(t, []string{filepath.Base(checkpoint)}, attempted)
	unchanged, err := readProcessRecoveryRoot(rootPath)
	require.NoError(t, err)
	require.Equal(t, physical, unchanged)
	require.ErrorIs(t, writer.close(), fenceFailure)

	image := copyPrototypeImage(t, dir)
	var replayed []uint64
	coldOutput, coldReader, recovered, err := openJournalSegments(image, false, extentBytes, journalSegmentHooks{
		processRoots: true, processBootID: "next-checkpoint-kernel",
	}, func(records []journalRecord) error {
		for _, record := range records {
			replayed = append(replayed, record.group)
		}
		return nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = coldReader.Close() })
	t.Cleanup(func() { _ = coldOutput.Close() })
	require.Equal(t, physical.End, recovered.end)
	require.Equal(t, physical.Next, recovered.nextSequence)
	require.Equal(t, []uint64{11}, replayed)
}
