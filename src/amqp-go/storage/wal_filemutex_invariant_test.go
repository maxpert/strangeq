package storage

// THE COMMITTER'S FILE LOCK IS HELD ACROSS THE FSYNC.
//
// This file pins the invariant that makes that lock NON-RELEASABLE, so the
// question "can the committer release fileMutex across the fdatasync?" has an
// ANSWER instead of an opinion.
//
// WHY IT CANNOT BE RELEASED.
//
// The WAL file is APPEND-ONLY and the batch's offset is reserved by HOLDING
// the lock:
//
//	base = fileOffset.Load()      // the write position
//	walWrite(currentFile, buf)    // the bytes land at base
//	fsync                         // the durability barrier
//	fileOffset.Add(n)             // the offset advances PAST the fsync
//
// Releasing the lock between the write and the fsync would break THREE
// things at once:
//
//   - I3 (invariants.go): "whenever fileMutex is released and the WAL is not
//     poisoned, qw.fileOffset == Stat(currentFile).Size()". The write makes
//     the file's size base+n while fileOffset is still base, so the invariant
//     is violated at the release point by construction.
//   - THE OFFSET RESERVATION: base is the write position. A second writer
//     that enters while the first is in its fsync would read the SAME base
//     and write over (or after) the first batch's bytes, while the first
//     batch advances fileOffset by its own length. The two writers' length
//     accounting no longer matches the file.
//   - THE REPAIR'S SOUNDNESS: repairAfterFailedWriteLocked(pre) truncates the
//     file back to `pre` when the fsync fails. ftruncate can only SHORTEN a
//     file from its end, so a truncate to `pre` would destroy bytes a
//     concurrent writer had already appended past `pre` -- bytes that writer
//     may already have been told were DURABLE.
//
// The transaction path (writeTxAtomic) bypasses the writeChan pipeline and
// takes fileMutex itself, so it is exactly such a concurrent writer.
//
// THE TEST BELOW makes the invariant executable: while the committer is
// inside its fsync, NOBODY can take fileMutex, so a concurrent writer cannot
// enter the write path at all.

import (
	"os"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

func TestWALFileMutexIsHeldAcrossTheFsync(t *testing.T) {
	dir := t.TempDir()
	wm, err := NewWALManagerWithConfig(dir, walIndexBoundConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })
	qw := wm.sharedWAL

	entered := make(chan struct{})
	release := make(chan struct{})
	restore := SetWALGroupCommitFsyncForTest(func(f *os.File) error {
		close(entered)
		<-release
		return f.Sync()
	})
	t.Cleanup(restore)

	durable := make(chan struct{})
	wm.WriteAsync("lock-q", &protocol.Message{
		RoutingKey: "lock-q", Body: []byte("body"), DeliveryMode: 2, DeliveryTag: 1,
	}, 1, func(error) { close(durable) })

	// The committer is now inside its fsync, holding fileMutex.
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the committer never reached the fsync")
	}

	acquired := make(chan struct{})
	go func() {
		qw.fileMutex.Lock()
		qw.fileMutex.Unlock()
		close(acquired)
	}()

	select {
	case <-acquired:
		t.Fatal("fileMutex was NOT held across the fsync: the batch's offset reservation " +
			"is not exclusive, so a concurrent writer would reserve the same offset")
	case <-time.After(250 * time.Millisecond):
		// EXPECTED: the lock is held for the whole fsync.
	}

	close(release)
	select {
	case <-durable:
	case <-time.After(10 * time.Second):
		t.Fatal("the publish never became durable")
	}

	select {
	case <-acquired:
	case <-time.After(10 * time.Second):
		t.Fatal("fileMutex was never released after the fsync returned")
	}
}
