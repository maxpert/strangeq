package clusterraft

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type gatedJournalFile struct {
	mu          sync.Mutex
	contents    []byte
	syncStarted chan struct{}
	syncRelease chan error
	shortWrite  bool
}

func (f *gatedJournalFile) WriteAt(data []byte, offset int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.shortWrite {
		data = data[:len(data)-1]
	}
	end := int(offset) + len(data)
	if end > len(f.contents) {
		f.contents = append(f.contents, make([]byte, end-len(f.contents))...)
	}
	return copy(f.contents[offset:], data), nil
}

func (f *gatedJournalFile) Sync() error {
	f.syncStarted <- struct{}{}
	return <-f.syncRelease
}

func (f *gatedJournalFile) Close() error { return nil }

func (f *gatedJournalFile) bytes() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return bytes.Clone(f.contents)
}

func testJournalLimits() journalLimits {
	return journalLimits{pendingBytes: 4096, pendingRequests: 8, batchBytes: 1024, batchRequests: 8}
}

func openGatedJournal(t *testing.T, limits journalLimits, shortWrite bool) (*journalWriter, *gatedJournalFile) {
	t.Helper()
	file := &gatedJournalFile{syncStarted: make(chan struct{}, 8), syncRelease: make(chan error, 8), shortWrite: shortWrite}
	writer, err := newJournalWriter(file, 0, 1, limits)
	require.NoError(t, err)
	t.Cleanup(func() {
		close(file.syncRelease)
		_ = writer.close()
	})
	return writer, file
}

func enqueueJournal(t *testing.T, writer *journalWriter, group uint64, body []byte, syncRequired bool) <-chan journalCompletion {
	t.Helper()
	result, err := writer.enqueue(group, body, syncRequired)
	require.NoError(t, err)
	require.NotNil(t, result)
	return result
}

func awaitJournalSync(t *testing.T, file *gatedJournalFile, pending <-chan journalCompletion) {
	t.Helper()
	select {
	case early := <-pending:
		t.Fatalf("completion arrived before the durability fence started: %+v", early)
	case <-file.syncStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("journal writer never reached the requested fence")
	}
	select {
	case early := <-pending:
		t.Fatalf("completion arrived before the durability fence returned: %+v", early)
	default:
	}
}

func awaitJournalCompletion(t *testing.T, pending <-chan journalCompletion) journalCompletion {
	t.Helper()
	select {
	case result := <-pending:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("journal request remained incomplete")
		return journalCompletion{}
	}
}

func TestJournalWriterSyncsBatchBeforeCompletion(t *testing.T) {
	writer, file := openGatedJournal(t, testJournalLimits(), false)
	first := enqueueJournal(t, writer, 1, []byte("old"), true)
	awaitJournalSync(t, file, first)

	body := []byte("new")
	second := enqueueJournal(t, writer, 1, body, true)
	third := enqueueJournal(t, writer, 2, []byte("peer"), true)
	copy(body, "bad") // The second record is still queued behind the first sync.
	file.syncRelease <- nil
	one := awaitJournalCompletion(t, first)
	require.NoError(t, one.err)
	awaitJournalSync(t, file, second)
	select {
	case early := <-third:
		t.Fatalf("second group completed before the shared durability fence: %+v", early)
	default:
	}
	file.syncRelease <- nil
	two, three := awaitJournalCompletion(t, second), awaitJournalCompletion(t, third)
	require.NoError(t, two.err)
	require.NoError(t, three.err)
	require.Equal(t, uint64(1), one.location.sequence)
	require.Equal(t, uint64(2), two.location.sequence)
	require.Equal(t, two.location.sequence, three.location.sequence)
	require.Less(t, two.location.offset, three.location.offset)

	data := file.bytes()
	var actual []journalRecord
	end, err := scanJournal(bytes.NewReader(data), int64(len(data)), -1, 1, func(batch []journalRecord) error {
		for _, record := range batch {
			record.body = bytes.Clone(record.body)
			actual = append(actual, record)
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, int64(len(data)), end.end)
	require.Equal(t, []journalRecord{
		{group: 1, body: []byte("old"), location: one.location},
		{group: 1, body: []byte("new"), location: two.location},
		{group: 2, body: []byte("peer"), location: three.location},
	}, actual)
}

func TestJournalWriterAdmissionCountsAnActiveBatch(t *testing.T) {
	for _, limit := range []string{"bytes", "requests"} {
		t.Run(limit, func(t *testing.T) {
			limits := testJournalLimits()
			body := []byte("1234")
			if limit == "bytes" {
				limits.pendingBytes = 2 * (len(body) + journalRecordHeaderSize)
				limits.batchBytes = limits.pendingBytes
			} else {
				limits.pendingRequests = 2
				limits.batchRequests = 2
			}
			writer, file := openGatedJournal(t, limits, false)
			first := enqueueJournal(t, writer, 1, body, true)
			awaitJournalSync(t, file, first)
			second := enqueueJournal(t, writer, 2, body, true)
			pending, err := writer.enqueue(3, body, true)
			require.ErrorIs(t, err, errJournalBusy)
			require.Nil(t, pending)
			file.syncRelease <- nil
			require.NoError(t, awaitJournalCompletion(t, first).err)
			awaitJournalSync(t, file, second)
			file.syncRelease <- nil
			require.NoError(t, awaitJournalCompletion(t, second).err)
		})
	}
}

func TestJournalWriterFenceOnlyRequestSyncsPreviousWrite(t *testing.T) {
	writer, file := openGatedJournal(t, testJournalLimits(), false)
	write := enqueueJournal(t, writer, 1, []byte("written"), false)
	require.NoError(t, awaitJournalCompletion(t, write).err)
	select {
	case <-file.syncStarted:
		t.Fatal("a response-free append performed an immediate durability fence")
	default:
	}
	before := file.bytes()
	barrier := enqueueJournal(t, writer, 1, nil, true)
	awaitJournalSync(t, file, barrier)
	require.Equal(t, before, file.bytes(), "a fence-only request should append no empty WAL record")
	file.syncRelease <- nil
	require.NoError(t, awaitJournalCompletion(t, barrier).err)
}

func TestJournalWriterSyncErrorIsTerminalAndCompletesQueuedRequests(t *testing.T) {
	writer, file := openGatedJournal(t, testJournalLimits(), false)
	failure := errors.New("injected journal sync failure")
	first := enqueueJournal(t, writer, 1, []byte("one"), true)
	awaitJournalSync(t, file, first)
	other := enqueueJournal(t, writer, 2, []byte("two"), true)
	file.syncRelease <- failure
	for _, pending := range []<-chan journalCompletion{first, other} {
		result := awaitJournalCompletion(t, pending)
		require.ErrorIs(t, result.err, failure)
		require.Zero(t, result.location)
	}
	pending, err := writer.enqueue(3, []byte("later"), true)
	require.ErrorIs(t, err, failure)
	require.Nil(t, pending)
	require.ErrorIs(t, writer.close(), failure)
}

func TestJournalWriterShortWriteFailsWithoutSync(t *testing.T) {
	writer, file := openGatedJournal(t, testJournalLimits(), true)
	pending := enqueueJournal(t, writer, 1, []byte("message"), true)
	result := awaitJournalCompletion(t, pending)
	require.ErrorIs(t, result.err, io.ErrShortWrite)
	require.Zero(t, result.location)
	select {
	case <-file.syncStarted:
		t.Fatal("journal tried to sync a short write")
	default:
	}
	require.ErrorIs(t, writer.close(), io.ErrShortWrite)
}
