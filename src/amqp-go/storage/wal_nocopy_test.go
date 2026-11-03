package storage

import (
	"bytes"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// THE BODY IS NEVER COPIED BY THE WAL, AND IT IS RELEASED AT THE WRITE.
//
// The WAL used to copy every message body into the group-commit batch's
// staging buffer, so the batch held a SECOND copy of every body it was about
// to write (129 MB at the 64 KiB / 10x10 shape). It then held the body a
// THIRD time in the write request until the batch's fdatasync completed.
//
// The design under test:
//
//   - the stager builds only the record's HEADER and its optional-property
//     TAIL; the body is ALIASED and handed to the kernel by reference,
//   - the request drops its message reference the moment the write returns,
//     so the body is resident from the frame read until the write and not one
//     millisecond longer.

// nocopyBody is the body of the message the tests publish.
func nocopyBody(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i & 0xff)
	}
	return b
}

// nocopyMsg builds the test message (a 64 KiB body, like the benchmark).
func nocopyMsg(off uint64, body []byte) *protocol.Message {
	return &protocol.Message{
		Exchange:     "amq.direct",
		RoutingKey:   "q",
		Body:         body,
		DeliveryMode: 2,
		DeliveryTag:  off,
	}
}

// sameBackingArray reports whether a and b are slices over the SAME backing
// array (pointer identity of their first elements).
func sameBackingArray(a, b []byte) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	return uintptr(unsafe.Pointer(&a[0])) == uintptr(unsafe.Pointer(&b[0]))
}

// TestWALWrite_BodyIsPassedByReferenceNotCopied (TDD): the bytes the WAL
// hands to the kernel ARE the message's own body buffer — the WAL does not
// copy it — and the staged batch's buffer holds only the record's headers.
func TestWALWrite_BodyIsPassedByReferenceNotCopied(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, DefaultWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	body := nocopyBody(64 * 1024)
	msg := nocopyMsg(1, body)

	var parts [][]byte
	var mu = make(chan struct{}, 1)
	mu <- struct{}{}
	restore := SetWALWriteVecForTest(func(f *os.File, p [][]byte) (int, error) {
		<-mu
		parts = append(parts, p...)
		mu <- struct{}{}
		total := 0
		for _, b := range p {
			total += len(b)
		}
		return writevFile(f, p)
	})
	t.Cleanup(restore)

	done := make(chan error, 1)
	wm.WriteAsync("q", msg, 1, func(e error) { done <- e })
	require.NoError(t, <-done)

	<-mu
	captured := parts
	mu <- struct{}{}
	require.NotEmpty(t, captured, "the WAL must hand the record to the kernel")

	// The message's own body buffer must be among the pieces: the WAL wrote
	// the body BY REFERENCE.
	found := false
	for _, p := range captured {
		if sameBackingArray(p, body) {
			found = true
			break
		}
	}
	assert.True(t, found,
		"the WAL must hand the message's own body buffer to the kernel (no copy); captured %d pieces", len(captured))

	// And the staged batch's buffer must hold only the record's headers:
	// its serialized length is now the headers, not the body.
	snaps := wm.sharedWAL.pipelineBufsForTest()
	require.Len(t, snaps, 2)
	for i, s := range snaps {
		assert.Less(t, s.HandoffLen, 1024,
			"staged buffer %d holds %d bytes: the body must NOT be staged in it", i, s.HandoffLen)
	}
}

// TestWALWrite_BodyReleasedAtWriteNotAtFsync (TDD): with the committer
// parked inside its fdatasync, the record's bytes are already in the file, so
// the write request must no longer reference the body — and the completion
// (which fires after the fsync) must still be outstanding.
func TestWALWrite_BodyReleasedAtWriteNotAtFsync(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, DefaultWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	fsyncEntered := make(chan struct{})
	releaseFsync := make(chan struct{})
	var once sync.Once
	restoreFsync := SetWALGroupCommitFsyncForTest(func(f *os.File) error {
		once.Do(func() { close(fsyncEntered) })
		<-releaseFsync
		return nil
	})
	t.Cleanup(restoreFsync)

	body := nocopyBody(64 * 1024)
	msg := nocopyMsg(1, body)

	var written atomic.Int32
	done := make(chan error, 1)
	wm.WriteAsyncObserved("q", msg, 1, func() { written.Add(1) }, func(e error) { done <- e })

	// Wait until the committer is parked inside the fsync.
	select {
	case <-fsyncEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("the committer never reached the fsync")
	}

	// The write happened (the onWritten hook fired) ...
	assert.EqualValues(t, 1, written.Load(),
		"the onWritten hook must fire at the write, before the fsync")

	// ... and the write request no longer references the body.
	assert.Nil(t, inflightMessageForTest(wm, msg),
		"the write request must not reference the message once the write returned")

	// The completion is still outstanding: the fsync has not returned.
	select {
	case e := <-done:
		t.Fatalf("the completion must not fire before the fsync returned (fired with %v)", e)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseFsync)
	require.NoError(t, <-done)
}

// inflightMessageForTest returns the message a staged/committed write request
// still references for the given delivery tag, or nil when no request holds
// it any more. TEST-ONLY observability.
func inflightMessageForTest(wm *WALManager, msg *protocol.Message) *protocol.Message {
	sw := wm.sharedWAL
	sw.pipelineMu.Lock()
	batches := append([]*stagedBatch(nil), sw.pipelineBatches...)
	sw.pipelineMu.Unlock()
	for _, sb := range batches {
		for _, req := range sb.reqs {
			if req == nil {
				continue
			}
			if req.offset == msg.DeliveryTag && req.message != nil {
				return req.message
			}
		}
	}
	return nil
}

// TestWALWrite_RecordBytesAreIdenticalToTheContiguousWriter (TDD): the
// copy-free writer must produce byte-for-byte the same file as the
// single-buffer writer.
func TestWALWrite_RecordBytesAreIdenticalToTheContiguousWriter(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, DefaultWALConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	body := nocopyBody(1024)
	msg := nocopyMsg(1, body)

	var captured [][]byte
	restore := SetWALWriteVecForTest(func(f *os.File, p [][]byte) (int, error) {
		captured = append(captured, p...)
		return writevFile(f, p)
	})
	t.Cleanup(restore)

	done := make(chan error, 1)
	wm.WriteAsync("q", msg, 1, func(e error) { done <- e })
	require.NoError(t, <-done)

	var got []byte
	for _, p := range captured {
		got = append(got, p...)
	}
	want, err := appendMessageRecord(nil, "q", msg, 1, false)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(got, want),
		"the copy-free writer must produce the same bytes as the contiguous writer")
}
