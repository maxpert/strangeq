package clusterraft

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJournalSegmentReadDoesNotDelayActiveAppendFence(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	dir := t.TempDir()
	entered := make(chan struct{}, 1)
	release := make(chan struct{}, 1)
	defer close(release)
	var armed atomic.Bool
	synced := make(chan struct{}, 1)
	hooks := journalSegmentFixtureHooks{
		syncFile: func(file *os.File) error {
			if err := syncJournalFile(file); err != nil {
				return err
			}
			if armed.Load() {
				synced <- struct{}{}
			}
			return nil
		},
	}
	first, second := []byte("read this"), []byte("and append")
	extentBytes := int64(3 * len(journalFrameFixture(t, 1, journalRecord{group: 11, body: first})))
	handle := openTestJournalSegments(t, dir, true, extentBytes, hooks, nil)
	one := awaitJournalCompletion(t, enqueueJournal(t, handle.writer, 11, first, true))
	require.NoError(t, one.err)
	reader := handle.reader.(*journalSegmentReader)
	readBody := make([]byte, len(first))
	readDone := make(chan error, 1)
	go func() {
		_, err := reader.readAt(readBody, one.location.offset,
			func(file *os.File, dst []byte, offset int64) (int, error) {
				entered <- struct{}{}
				<-release
				return file.ReadAt(dst, offset)
			})
		readDone <- err
	}()
	select {
	case <-entered:
	case err := <-readDone:
		t.Fatalf("read did not reach the disk gate: %v", err)
	case <-ctx.Done():
		t.Fatal("read did not reach the disk gate")
	}

	armed.Store(true)
	pending := enqueueJournal(t, handle.writer, 22, second, true)
	var two journalCompletion
	select {
	case two = <-pending:
		require.NoError(t, two.err)
	case <-ctx.Done():
		t.Fatal("an ordinary append could not finish its durability fence while a read was held")
	}
	select {
	case <-synced:
	default:
		t.Fatal("append completed without syncing the active extent")
	}
	require.Equal(t, uint64(2), two.location.sequence)
	select {
	case err := <-readDone:
		t.Fatalf("read left its disk gate early: %v", err)
	default:
	}
	release <- struct{}{}
	select {
	case err := <-readDone:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("read did not finish after releasing its disk gate")
	}
	require.Equal(t, first, readBody)
	requireJournalVirtualBody(t, handle.reader, two.location, second)
}

func TestJournalSegmentReopensAfterRenameWithUncertainDirectorySync(t *testing.T) {
	dir := t.TempDir()
	failure := errors.New("injected directory sync failure after journal manifest rename")
	var armed atomic.Bool
	var publishing bool // Only the writer executes these hooks after open.
	hooks := journalSegmentFixtureHooks{
		publishManifest: func(install func() error) error {
			publishing = armed.Load()
			defer func() { publishing = false }()
			return install()
		},
		syncDir: func(path string) error {
			if publishing {
				return failure
			}
			return syncDirectory(path)
		},
	}
	first, second := []byte("prior extent"), []byte("next  extent")
	extentBytes := int64(len(journalFrameFixture(t, 1, journalRecord{group: 11, body: first})))
	handle := openTestJournalSegments(t, dir, true, extentBytes, hooks, nil)
	one := awaitJournalCompletion(t, enqueueJournal(t, handle.writer, 11, first, true))
	require.NoError(t, one.err)
	armed.Store(true)
	failed := awaitJournalCompletion(t, enqueueJournal(t, handle.writer, 22, second, true))
	require.ErrorIs(t, failed.err, failure)
	require.Zero(t, failed.location, "an ambiguous manifest update cannot acknowledge a new append")
	require.ErrorIs(t, handle.writer.close(), failure)

	renamed, err := loadJournalSegmentManifest(dir)
	require.NoError(t, err)
	require.Len(t, renamed.extents, 2, "the selected manifest must have been renamed before the injected failure")
	require.Equal(t, extentBytes, renamed.extents[0].sealedBytes)
	activePath := handle.reader.(*journalSegmentReader).store.path(renamed.extents[1].id)
	info, err := os.Stat(activePath)
	require.NoError(t, err)
	require.Zero(t, info.Size(), "the new selected extent must be intact and empty")
	requireJournalVirtualBody(t, handle.reader, one.location, first)
	require.NoError(t, handle.reader.Close())

	var replayed []journalLocation
	reopened := openTestJournalSegments(t, dir, false, extentBytes, journalSegmentFixtureHooks{}, func(records []journalRecord) error {
		for _, record := range records {
			replayed = append(replayed, record.location)
		}
		return nil
	})
	require.Equal(t, []journalLocation{one.location}, replayed)
	two := awaitJournalCompletion(t, enqueueJournal(t, reopened.writer, 22, second, true))
	require.NoError(t, two.err)
	require.Equal(t, uint64(2), two.location.sequence)
	require.Equal(t, extentBytes+one.location.offset, two.location.offset)
	adopted, err := loadJournalSegmentManifest(dir)
	require.NoError(t, err)
	require.Equal(t, renamed, adopted, "reopen must keep the renamed manifest instead of allocating another extent")
	requireJournalVirtualBody(t, reopened.reader, one.location, first)
	requireJournalVirtualBody(t, reopened.reader, two.location, second)
}
