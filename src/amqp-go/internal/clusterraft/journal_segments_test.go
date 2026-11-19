package clusterraft

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func openJournalSegments(dir string, create bool, extentBytes int64, hooks journalSegmentHooks,
	visit func([]journalRecord) error,
) (*journalSegmentOutput, *journalSegmentReader, journalScan, error) {
	return openJournalSegmentsWithQuota(dir, create, extentBytes, journalGroupMaxFileBytes, hooks, visit)
}

type journalSegmentFixtureHooks = journalSegmentHooks

type journalSegmentFixtureReader interface {
	io.ReaderAt
	io.Closer
}

type journalSegmentFixtureHandle struct {
	writer *journalWriter
	reader journalSegmentFixtureReader
}

func openJournalSegmentFixture(dir string, create bool, extentBytes int64, hooks journalSegmentFixtureHooks,
	visit func([]journalRecord) error,
) (journalFile, journalSegmentFixtureReader, journalScan, error) {
	return openJournalSegments(dir, create, extentBytes, hooks, visit)
}

func openTestJournalSegments(t *testing.T, dir string, create bool, extentBytes int64,
	hooks journalSegmentFixtureHooks, visit func([]journalRecord) error,
) journalSegmentFixtureHandle {
	t.Helper()
	output, reader, state, err := openJournalSegmentFixture(dir, create, extentBytes, hooks, visit)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })
	limits := testJournalLimits()
	limits.batchRequests = 1
	writer, err := newJournalWriter(output, state.end, state.nextSequence, limits)
	if err != nil {
		_ = output.Close()
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = writer.close() })
	return journalSegmentFixtureHandle{writer: writer, reader: reader}
}

func requireJournalVirtualBody(t *testing.T, reader io.ReaderAt, location journalLocation, body []byte) {
	t.Helper()
	actual := make([]byte, location.size)
	require.NoError(t, readJournalAt(reader, actual, location.offset))
	require.Equal(t, body, actual)
}

func TestJournalStartupValidationHoldsOwnerBeforeReadingRecoveryRoots(t *testing.T) {
	dir := t.TempDir()
	// Reaching root preparation before validation would fail on this root.
	require.NoError(t, os.WriteFile(filepath.Join(dir, processPhysicalRoot), []byte("invalid root"), 0600))
	refusal := errors.New("startup validation refused")
	var probeErr error
	out, reader, _, err := openJournalSegments(dir, true, journalServingExtentBytes, journalSegmentHooks{
		validateBeforeRecovery: func() error {
			probeOut, probeReader, _, err := openJournalSegments(dir, true, journalServingExtentBytes, journalSegmentHooks{}, nil)
			probeErr = err
			if err == nil {
				_ = probeOut.Close()
				_ = probeReader.Close()
			}
			return refusal
		},
	}, nil)
	require.ErrorIs(t, err, refusal)
	require.Nil(t, out)
	require.Nil(t, reader)
	require.ErrorContains(t, probeErr, "lock node raft journal", "validation must hold the lifetime journal lock")

	require.NoError(t, os.Remove(filepath.Join(dir, processPhysicalRoot)))
	node, err := OpenNodeJournal(dir, true)
	require.NoError(t, err, "failed validation must release journal ownership")
	require.NoError(t, node.Close())
}

func TestJournalSegmentRotationFencesPublicationAndKeepsVirtualOffsets(t *testing.T) {
	type extentSync struct {
		path string
		size int64
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	dir := t.TempDir()
	entered := make(chan struct{}, 1)
	release := make(chan struct{}, 1)
	defer close(release)
	syncs := make(chan extentSync, 16)
	var armed atomic.Bool
	hooks := journalSegmentFixtureHooks{
		syncFile: func(file *os.File) error {
			if err := syncJournalFile(file); err != nil {
				return err
			}
			if armed.Load() {
				info, err := file.Stat()
				if err != nil {
					return err
				}
				syncs <- extentSync{path: file.Name(), size: info.Size()}
			}
			return nil
		},
		publishManifest: func(install func() error) error {
			if armed.Load() {
				entered <- struct{}{}
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return install()
		},
	}
	first, second := []byte("first extent"), []byte("later extent")
	frameBytes := int64(len(journalFrameFixture(t, 1, journalRecord{group: 11, body: first})))
	handle := openTestJournalSegments(t, dir, true, frameBytes, hooks, nil)
	one := awaitJournalCompletion(t, enqueueJournal(t, handle.writer, 11, first, false))
	require.NoError(t, one.err)
	require.Equal(t, uint64(1), one.location.sequence)
	// The first frame did not request a sync. Rollover must itself seal it.
	armed.Store(true)
	pending := enqueueJournal(t, handle.writer, 22, second, true)
	select {
	case <-entered:
	case early := <-pending:
		t.Fatalf("second frame completed before entering manifest publication: %+v", early)
	case <-ctx.Done():
		t.Fatal("rollover did not reach manifest publication")
	}

	var oldPath, newPath string
	for len(syncs) != 0 {
		sync := <-syncs
		switch sync.size {
		case frameBytes:
			oldPath = sync.path
		case 0:
			newPath = sync.path
		}
	}
	require.NotEmpty(t, oldPath, "the sealed extent must be fully synced before installing the manifest")
	require.NotEmpty(t, newPath, "the empty new extent must be fully synced before installing the manifest")
	require.NotEqual(t, oldPath, newPath)
	reads := make(chan error, 1)
	go func() {
		body := make([]byte, one.location.size)
		err := readJournalAt(handle.reader, body, one.location.offset)
		if err == nil && string(body) != string(first) {
			err = errors.New("previous virtual offset returned different bytes")
		}
		reads <- err
	}()
	select {
	case err := <-reads:
		require.NoError(t, err, "old offsets remain readable during manifest publication")
	case <-ctx.Done():
		t.Fatal("old offset blocked on manifest publication")
	}
	info, err := os.Stat(newPath)
	require.NoError(t, err)
	require.Zero(t, info.Size(), "the writer must not append to the new extent before publication")
	select {
	case early := <-pending:
		t.Fatalf("second frame completed while manifest publication was held: %+v", early)
	default:
	}
	release <- struct{}{}
	two := awaitJournalCompletion(t, pending)
	require.NoError(t, two.err)
	require.Equal(t, uint64(2), two.location.sequence)
	require.Equal(t, frameBytes+one.location.offset, two.location.offset)
	requireJournalVirtualBody(t, handle.reader, one.location, first)
	requireJournalVirtualBody(t, handle.reader, two.location, second)
	require.NoError(t, handle.writer.close())
	requireJournalVirtualBody(t, handle.reader, one.location, first)
	require.NoError(t, handle.reader.Close())

	var replayed []journalLocation
	reopened := openTestJournalSegments(t, dir, false, frameBytes, journalSegmentFixtureHooks{}, func(records []journalRecord) error {
		for _, record := range records {
			replayed = append(replayed, record.location)
		}
		return nil
	})
	require.Equal(t, []journalLocation{one.location, two.location}, replayed)
	requireJournalVirtualBody(t, reopened.reader, one.location, first)
	requireJournalVirtualBody(t, reopened.reader, two.location, second)
}

func TestJournalSegmentFailedPublicationRecoversOnlyManifestExtents(t *testing.T) {
	dir := t.TempDir()
	failure := errors.New("injected segment manifest publication failure")
	var armed atomic.Bool
	staged := make(chan string, 4)
	hooks := journalSegmentFixtureHooks{
		syncFile: func(file *os.File) error {
			if err := syncJournalFile(file); err != nil {
				return err
			}
			info, err := file.Stat()
			if err == nil && armed.Load() && info.Size() == 0 {
				staged <- file.Name()
			}
			return err
		},
		publishManifest: func(install func() error) error {
			if armed.Load() {
				return failure
			}
			return install()
		},
	}
	first, second := []byte("first extent"), []byte("later extent")
	frameBytes := int64(len(journalFrameFixture(t, 1, journalRecord{group: 11, body: first})))
	handle := openTestJournalSegments(t, dir, true, frameBytes, hooks, nil)
	one := awaitJournalCompletion(t, enqueueJournal(t, handle.writer, 11, first, true))
	require.NoError(t, one.err)
	armed.Store(true)
	failed := awaitJournalCompletion(t, enqueueJournal(t, handle.writer, 22, second, true))
	require.ErrorIs(t, failed.err, failure, "rotation failure must reach the would-be append")
	require.Zero(t, failed.location, "no unpublished data offset can escape")
	require.ErrorIs(t, handle.writer.close(), failure)
	requireJournalVirtualBody(t, handle.reader, one.location, first)
	require.NoError(t, handle.reader.Close())

	var orphanPath string
	select {
	case orphanPath = <-staged:
	default:
		t.Fatal("rollover failed before staging its new extent")
	}
	// Leave a valid frame in precisely the unreferenced staged extent path.
	// Recovery must select by the durable manifest instead of filename order.
	orphan := journalFrameFixture(t, 2, journalRecord{group: 99, body: []byte("unpublished")})
	require.NoError(t, os.WriteFile(orphanPath, orphan, 0600))
	var replayed []uint64
	reopened := openTestJournalSegments(t, dir, false, frameBytes, journalSegmentFixtureHooks{}, func(records []journalRecord) error {
		for _, record := range records {
			replayed = append(replayed, record.group)
		}
		return nil
	})
	require.Equal(t, []uint64{11}, replayed, "the orphan extent has no recovery authority")
	requireJournalVirtualBody(t, reopened.reader, one.location, first)
	two := awaitJournalCompletion(t, enqueueJournal(t, reopened.writer, 22, second, true))
	require.NoError(t, two.err)
	require.Equal(t, uint64(2), two.location.sequence)
	require.Equal(t, frameBytes+one.location.offset, two.location.offset)
	requireJournalVirtualBody(t, reopened.reader, two.location, second)
}
