package storage

// N1: the ACTIVE WAL file's cold-read path must NOT take fileMutex.
//
// readMessage used to take fileMutex (wal_manager.go, the active-file arm)
// merely to load currentReadFile, and the committer holds that same mutex from
// commitBatch's entry to its exit — across the disk write, every hot-cache
// admission, the fdatasync and the indexing. So every cold delivery of a
// record in the active file queued behind whichever fsync was in flight.
//
// These fixtures pin the OBSERVABLE property the fix buys: a read of a record
// in the active file completes while fileMutex is held (i.e. does not depend on
// it), the published snapshot tracks a roll, and readers racing forced rolls
// plus close() never observe a use-after-close fault while the WAL is open.
//
// The RED assertion is TestWALActiveReadDoesNotQueueBehindFileMutex: on the
// pre-fix tree the reader blocks on fileMutex and trips the deadline.

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/maxpert/amqp-go/protocol"
)

const activeReadQueue = "active.read.q"

// newActiveReadWAL builds a single-WAL manager sized so the fixture controls
// whether records live in the ACTIVE file or in a rolled one. The background
// maintenance loops are parked for the same reason newRolledWAL parks them: a
// reclaim moving a file out from under an assertion is noise, not signal.
func newActiveReadWAL(t *testing.T, fileSize int64) (*WALManager, *QueueWAL) {
	t.Helper()
	cfg := DefaultWALConfig()
	cfg.FileSize = fileSize
	cfg.CleanupInterval = time.Hour
	cfg.CompactionInterval = time.Hour
	cfg.RetentionPeriod = 0

	wm, err := NewWALManagerWithConfig(t.TempDir(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wm.Close() })

	sw := wm.sharedWAL
	require.NotNil(t, sw, "PREMISE BROKEN: no shared WAL was constructed")
	return wm, sw
}

// activeIndexPos returns the physical position the sharded index publishes for
// one record, read under that shard's read lock exactly as readMessage reads it.
func activeIndexPos(t *testing.T, sw *QueueWAL, queue string, tag uint64) recordPos {
	t.Helper()
	key := recordKey{queueName: queue, deliveryTag: tag}
	sh := sw.shardForKey(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	r := sh.offset[key]
	require.NotNil(t, r, "PREMISE BROKEN: no index entry for %s/%d", queue, tag)
	return r.pos
}

func activeReadMessage(t *testing.T, queue string, tag uint64) *protocol.Message {
	t.Helper()
	return &protocol.Message{
		RoutingKey:   queue,
		Body:         []byte("active-read-body"),
		DeliveryMode: 2,
		DeliveryTag:  tag,
		MessageID:    "ar",
	}
}

// TestWALActiveReadDoesNotQueueBehindFileMutex is the RED test.
//
// It holds fileMutex exactly where the committer holds it — across the
// group-commit critical section, from wal_manager.go commitBatch's Lock to its
// Unlock — and asserts a read of a record in the ACTIVE file still completes.
//
// PRE-FIX: readMessage's active arm blocks on fileMutex.Lock(), the read never
// returns, and the deadline fires.
// POST-FIX: the read is served from the published snapshot and returns.
func TestWALActiveReadDoesNotQueueBehindFileMutex(t *testing.T) {
	wm, sw := newActiveReadWAL(t, DefaultWALFileSize)

	require.NoError(t, wm.Write(activeReadQueue, activeReadMessage(t, activeReadQueue, 1), 1))

	// PREMISES: the record is in the ACTIVE file (so the read takes the arm
	// under test) and its published index entry agrees.
	require.Equal(t, uint64(1), sw.fileNum.Load(), "premise: the first record is in the active file")
	require.Equal(t, sw.fileNum.Load(), activeIndexPos(t, sw, activeReadQueue, 1).segmentID,
		"premise: the record's index entry points at the active file")

	// Hold fileMutex the way commitBatch does for the whole durable batch.
	sw.fileMutex.Lock()
	var once sync.Once
	unlock := func() { once.Do(sw.fileMutex.Unlock) }
	defer unlock()

	type result struct {
		msg *protocol.Message
		err error
	}
	done := make(chan result, 1)
	go func() {
		msg, err := wm.Read(activeReadQueue, 1)
		done <- result{msg, err}
	}()

	select {
	case got := <-done:
		require.NoError(t, got.err, "the active-file read must complete while fileMutex is held")
		require.NotNil(t, got.msg)
		require.Equal(t, []byte("active-read-body"), got.msg.Body)
	case <-time.After(5 * time.Second):
		unlock()
		t.Fatal("readMessage is serialized behind fileMutex: the active-file read did not " +
			"complete while the committer's group-commit critical section held it")
	}
}

// TestWALActiveReadConcurrentRollsAndClose drives readers against a WAL whose
// tiny FileSize forces constant rolls, then closes the WAL while the readers are
// still running. While the WAL is open every read of a durable, unacknowledged
// record MUST succeed; after close() the readers may fail but must not fault on
// a recycled descriptor or hang.
func TestWALActiveReadConcurrentRollsAndClose(t *testing.T) {
	wm, sw := newActiveReadWAL(t, oldHandleWALFileSize)

	const (
		records   = 400
		readersN  = 6
		bodyBytes = 256
	)

	body := make([]byte, bodyBytes)
	for i := range body {
		body[i] = byte('a' + i%26)
	}
	base := uint64(1) << 44

	var written atomic.Uint64
	var closing atomic.Bool
	var wg sync.WaitGroup
	readerErr := make(chan error, readersN)

	for r := 0; r < readersN; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for iter := 0; iter < 200000; iter++ {
				n := written.Load()
				if n == 0 {
					if closing.Load() {
						return
					}
					runtime.Gosched()
					continue
				}
				// The newest written tag: durable and unacknowledged, so on an
				// OPEN WAL the read must succeed.
				tag := base + n - 1
				_, err := wm.Read(activeReadQueue, tag)
				if err != nil {
					if !closing.Load() && !errorsIsWALClosed(err) {
						select {
						case readerErr <- err:
						default:
						}
						return
					}
					continue
				}
				if closing.Load() {
					return
				}
			}
		}()
	}

	for i := 0; i < records; i++ {
		require.NoError(t, wm.Write(activeReadQueue, &protocol.Message{
			RoutingKey:   activeReadQueue,
			Body:         body,
			DeliveryMode: 2,
			DeliveryTag:  base + uint64(i),
			MessageID:    "race",
		}, base+uint64(i)))
		written.Store(uint64(i + 1))
	}

	// Premise: the tiny file has actually rolled, so the readers have run across
	// the snapshot republish (and the old-file arm) rather than one static file.
	require.Greater(t, sw.fileNum.Load(), uint64(2),
		"PREMISE BROKEN: FileSize=%d did not force repeated rolls", oldHandleWALFileSize)

	// Close while the readers are still reading.
	closing.Store(true)
	require.NoError(t, wm.Close())

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("readers did not exit after close(): a use-after-close read is wedged")
	}

	close(readerErr)
	for err := range readerErr {
		t.Fatalf("a read of a durable, unacknowledged record failed while the WAL was open: %v", err)
	}
}

// errorsIsWALClosed reports whether err is the post-close "shared WAL not
// initialized" error, which is a legitimate outcome for a read racing close().
func errorsIsWALClosed(err error) bool {
	return err != nil && err.Error() == "shared WAL not initialized"
}
