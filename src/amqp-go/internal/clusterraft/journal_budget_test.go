package clusterraft

import (
	"errors"
	"io"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func openSharedBudgetWriters(t *testing.T, limits journalLimits) (*journalBudget, [2]*journalWriter, [2]*gatedJournalFile) {
	t.Helper()
	budget := newJournalBudget(limits)
	var writers [2]*journalWriter
	var files [2]*gatedJournalFile
	for index := range writers {
		file := &gatedJournalFile{syncStarted: make(chan struct{}, 8), syncRelease: make(chan error, 8)}
		writer, err := newJournalWriterWithBudget(file, 0, 1, limits, budget)
		require.NoError(t, err)
		writers[index], files[index] = writer, file
		t.Cleanup(func() {
			close(file.syncRelease)
			_ = writer.close()
		})
	}
	return budget, writers, files
}

func requireBudgetPending(t *testing.T, budget *journalBudget, bytes, requests int) {
	t.Helper()
	budget.mu.Lock()
	defer budget.mu.Unlock()
	require.Equal(t, bytes, budget.pendingBytes)
	require.Equal(t, requests, budget.pendingRequests)
}

func TestJournalSharedBudgetRetainsSingleMaximumAdmission(t *testing.T) {
	limits := testJournalLimits()
	limits.pendingBytes, limits.pendingRequests = 64, 4
	budget, writers, files := openSharedBudgetWriters(t, limits)
	size := limits.pendingBytes - journalRecordHeaderSize
	for index, writer := range writers {
		pending, err := writer.enqueueEncoded(uint64(index+1), size, true, func(body []byte) error {
			clear(body)
			return nil
		})
		require.NoError(t, err, "one stream must retain the full admissible node-sized request")
		awaitJournalSync(t, files[index], pending)
		requireBudgetPending(t, budget, limits.pendingBytes, 1)
		other, err := writers[1-index].enqueue(3, nil, true)
		require.ErrorIs(t, err, errJournalBusy, "active bytes in either stream consume the node budget")
		require.Nil(t, other)
		files[index].syncRelease <- nil
		require.NoError(t, awaitJournalCompletion(t, pending).err)
		requireBudgetPending(t, budget, 0, 0)
	}
}

func TestJournalSharedBudgetFailureWakesPeerWithoutReleasingPeerReservations(t *testing.T) {
	limits := testJournalLimits()
	limits.pendingBytes, limits.pendingRequests = 60, 4
	budget, writers, files := openSharedBudgetWriters(t, limits)
	first := enqueueJournal(t, writers[0], 1, []byte("12345678"), true) // charge 20
	awaitJournalSync(t, files[0], first)
	second := enqueueJournal(t, writers[1], 2, []byte("12345678"), true)
	awaitJournalSync(t, files[1], second)
	requireBudgetPending(t, budget, 40, 2)
	type admitted struct {
		result <-chan journalCompletion
		err    error
	}
	queued := make(chan admitted, 1)
	go func() {
		result, err := writers[1].enqueueEncoded(3, 9, true, func(body []byte) error {
			copy(body, "123456789")
			return nil
		})
		queued <- admitted{result: result, err: err}
	}()
	require.Eventually(t, func() bool {
		budget.mu.Lock()
		defer budget.mu.Unlock()
		return writers[1].admissionWaiters == 1
	}, time.Second, time.Millisecond, "peer must be asleep in shared admission before the failure")
	failure := errors.New("stream 0 sync failed")
	files[0].syncRelease <- failure
	require.ErrorIs(t, awaitJournalCompletion(t, first).err, failure)
	var next admitted
	select {
	case next = <-queued:
	case <-time.After(time.Second):
		t.Fatal("stream failure did not wake shared admission")
	}
	require.NoError(t, next.err)
	requireBudgetPending(t, budget, 41, 2) // stream 1 active + newly queued only
	_, err := writers[0].enqueue(4, nil, true)
	require.ErrorIs(t, err, failure)
	files[1].syncRelease <- nil
	require.NoError(t, awaitJournalCompletion(t, second).err)
	awaitJournalSync(t, files[1], next.result)
	files[1].syncRelease <- nil
	require.NoError(t, awaitJournalCompletion(t, next.result).err)
	requireBudgetPending(t, budget, 0, 0)
}

func TestJournalSharedBudgetFailedWriterKeepsInProgressEncodingCharged(t *testing.T) {
	limits := testJournalLimits()
	limits.pendingBytes, limits.pendingRequests = 96, 8
	budget, writers, files := openSharedBudgetWriters(t, limits)
	first := enqueueJournal(t, writers[0], 1, []byte("12345678"), true)
	awaitJournalSync(t, files[0], first)
	second := enqueueJournal(t, writers[1], 2, []byte("12345678"), true)
	awaitJournalSync(t, files[1], second)
	encoding, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	encoded := make(chan error, 1)
	go func() {
		_, err := writers[0].enqueueEncoded(3, 13, true, func(body []byte) error {
			close(encoding)
			<-release
			clear(body)
			return nil
		})
		encoded <- err
	}()
	select {
	case <-encoding:
	case <-time.After(time.Second):
		t.Fatal("encoder did not reserve its pending bytes")
	}
	requireBudgetPending(t, budget, 65, 3)
	failure := errors.New("failed while another encoding owns memory")
	files[0].syncRelease <- failure
	require.ErrorIs(t, awaitJournalCompletion(t, first).err, failure)
	requireBudgetPending(t, budget, 45, 2)
	_, err := writers[1].enqueue(4, make([]byte, 40), true) // charge 52; only 51 remain
	require.ErrorIs(t, err, errJournalBusy, "unfinished encoding cannot be refunded on writer failure")
	release <- struct{}{}
	select {
	case err := <-encoded:
		require.ErrorIs(t, err, failure)
	case <-time.After(time.Second):
		t.Fatal("failed encoder did not release its own reservation")
	}
	requireBudgetPending(t, budget, 20, 1)
	files[1].syncRelease <- nil
	require.NoError(t, awaitJournalCompletion(t, second).err)
	requireBudgetPending(t, budget, 0, 0)
}

func TestJournalSharedFileBudgetReservesBeforeIOAndKeepsPartialWriteCharged(t *testing.T) {
	limits := testJournalLimits()
	// A one-byte record occupies a 53-byte frame. Both writers are allowed
	// 100 bytes individually; the node can admit only one whole frame.
	limits.maxFileBytes = 100
	budget, writers, files := openSharedBudgetWriters(t, limits)
	files[0].shortWrite = true
	first := enqueueJournal(t, writers[0], 1, []byte("x"), true)
	require.ErrorIs(t, awaitJournalCompletion(t, first).err, io.ErrShortWrite)
	budget.mu.Lock()
	reserved := budget.fileBytes
	budget.mu.Unlock()
	require.Equal(t, int64(journalFrameHeaderSize+journalRecordHeaderSize+1+journalFrameTrailerSize), reserved)
	require.Equal(t, int(reserved)-1, len(files[0].bytes()))
	second := enqueueJournal(t, writers[1], 2, []byte("y"), true)
	require.ErrorIs(t, awaitJournalCompletion(t, second).err, errJournalTooLarge)
	require.Empty(t, files[1].bytes(), "the aggregate cap must reject before the peer's file write")
	_, err := writers[0].enqueue(3, []byte("again"), true)
	require.ErrorIs(t, err, io.ErrShortWrite)
	requireBudgetPending(t, budget, 0, 0)
}

func TestJournalSharedFileBudgetReconstructsBothEndsWithoutOverflow(t *testing.T) {
	for _, limit := range []int64{journalGroupMaxFileBytes, math.MaxInt64} {
		t.Run(stringLimit(limit), func(t *testing.T) {
			limits := testJournalLimits()
			limits.maxFileBytes = limit
			budget := newJournalBudget(limits)
			file := &gatedJournalFile{}
			first, err := newJournalWriterWithBudget(file, limit/2+1, 1, limits, budget)
			require.NoError(t, err)
			t.Cleanup(func() { _ = first.close() })
			second, err := newJournalWriterWithBudget(&gatedJournalFile{}, limit/2+1, 1, limits, budget)
			if second != nil {
				_ = second.close()
			}
			require.ErrorIs(t, err, errJournalTooLarge, "individually valid recovered ends must fit their aggregate cap")
			budget.mu.Lock()
			actual := budget.fileBytes
			budget.mu.Unlock()
			require.Equal(t, limit/2+1, actual)
		})
	}
}

func stringLimit(limit int64) string {
	if limit == math.MaxInt64 {
		return "int64_boundary"
	}
	return "node_8_gib"
}
