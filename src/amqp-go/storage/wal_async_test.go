package storage

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Iteration 2 — async WAL completion (durable + confirm throughput).
//
// These tests pin the two lowest-level invariants of the async design:
//   A1 — a WriteAsync completion fires ONLY after the record's bytes are durable
//        (fdatasync returned) AND the offset index is populated, in enqueue
//        order (test 1).
//   A2 — DisruptorStorage.StoreMessageAsync keeps ring.Store SYNCHRONOUS on the
//        caller's goroutine: a durable message is ring-resident (retrievable)
//        the instant StoreMessageAsync returns, BEFORE its fsync completes. A
//        regression that deferred ring.Store into the completion callback would
//        fail test 8 (the crux guard). It also proves the completion is NOT
//        fired before fsync (retrievable-before-durable is safe; the ring is
//        volatile and gone on crash).
// plus a throughput guard that the group commit actually coalesces under
// pipelining (test 2).
// ============================================================================

// countingWALMetrics counts WAL writes and fsyncs so a test can assert the
// group-commit batch coalesces many messages per fsync.
type countingWALMetrics struct {
	writes atomic.Int64
	fsyncs atomic.Int64
	errs   atomic.Int64
}

func (m *countingWALMetrics) UpdateWALSize(float64)  {}
func (m *countingWALMetrics) RecordWALWrite()        { m.writes.Add(1) }
func (m *countingWALMetrics) RecordWALFsync(float64) { m.fsyncs.Add(1) }
func (m *countingWALMetrics) RecordWALWriteError()   { m.errs.Add(1) }

// TestWriteAsync_CompletionAfterFsync (TDD test 1, guards A1).
//
// Enqueues N async durable writes from a single goroutine and asserts that when
// each onDone(nil) fires the record is already durable and readable via Read
// (offset index populated), and that completions arrive in enqueue (tag) order.
func TestWriteAsync_CompletionAfterFsync(t *testing.T) {
	wm, err := NewWALManagerWithConfig(t.TempDir(), DefaultWALConfig())
	require.NoError(t, err)
	defer wm.Close()

	const n = 64
	type result struct {
		tag       uint64
		completed error
		readOK    bool
		bodyMatch bool
	}
	results := make([]result, 0, n)
	var mu sync.Mutex
	done := make(chan struct{}, n)

	for i := 1; i <= n; i++ {
		off := uint64(i)
		body := []byte(fmt.Sprintf("msg-%d", i))
		msg := &protocol.Message{
			Body:         body,
			RoutingKey:   "q",
			DeliveryMode: 2,
			DeliveryTag:  off,
		}
		wm.WriteAsync("q", msg, off, func(cerr error) {
			// At completion time the record MUST be durable + indexed, so a
			// positional Read finds it. This runs on the batch-writer goroutine;
			// Read takes fileMutex, which flushBatch has already released before
			// firing completions (no deadlock).
			got, rerr := wm.Read("q", off)
			r := result{tag: off, completed: cerr, readOK: rerr == nil}
			if rerr == nil {
				r.bodyMatch = string(got.Body) == string(body)
			}
			mu.Lock()
			results = append(results, r)
			mu.Unlock()
			done <- struct{}{}
		})
	}

	deadline := time.After(10 * time.Second)
	for i := 0; i < n; i++ {
		select {
		case <-done:
		case <-deadline:
			t.Fatalf("only %d/%d async completions fired", i, n)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, results, n)
	for i, r := range results {
		assert.NoError(t, r.completed, "completion %d must be success", i)
		assert.True(t, r.readOK, "record for tag %d must be readable when completion fires", r.tag)
		assert.True(t, r.bodyMatch, "record for tag %d must have matching body at completion", r.tag)
		// enqueue order: a single serial enqueuer + FIFO writeChan + in-order
		// batch iteration => completions fire in non-decreasing tag order.
		assert.Equal(t, uint64(i+1), r.tag, "completion %d fired out of enqueue order", i)
	}
}

// TestWriteAsync_BatchGrowsUnderPipelining (TDD test 2).
//
// Fires M async writes without awaiting any of them; with a deliberately slowed
// fsync, the single batch writer coalesces hundreds of records per fdatasync,
// so the fsync count is far below M (average batch >> the ~#connections ceiling
// of the blocking path). Guards that the async path actually pipelines.
func TestWriteAsync_BatchGrowsUnderPipelining(t *testing.T) {
	// Slow the group-commit fsync so enqueue outpaces it and the batch grows.
	restore := SetWALGroupCommitFsyncForTest(func(f *os.File) error {
		time.Sleep(2 * time.Millisecond)
		return nil
	})
	defer restore()

	cfg := DefaultWALConfig()
	wm, err := NewWALManagerWithConfig(t.TempDir(), cfg)
	require.NoError(t, err)
	defer wm.Close()

	metrics := &countingWALMetrics{}
	wm.SetMetrics(metrics)

	const m = 5000
	done := make(chan struct{}, m)
	for i := 1; i <= m; i++ {
		off := uint64(i)
		msg := &protocol.Message{Body: []byte("payload"), RoutingKey: "q", DeliveryMode: 2, DeliveryTag: off}
		wm.WriteAsync("q", msg, off, func(error) { done <- struct{}{} })
	}

	deadline := time.After(30 * time.Second)
	for i := 0; i < m; i++ {
		select {
		case <-done:
		case <-deadline:
			t.Fatalf("only %d/%d async completions fired", i, m)
		}
	}

	fsyncs := metrics.fsyncs.Load()
	require.Positive(t, fsyncs, "expected at least one fsync")
	require.EqualValues(t, m, metrics.writes.Load(), "every record must be counted as written")
	avgBatch := float64(m) / float64(fsyncs)
	assert.Less(t, fsyncs, int64(m/50), "async pipelining must coalesce many records per fsync (got %d fsyncs for %d msgs)", fsyncs, m)
	assert.Greater(t, avgBatch, 50.0, "average group-commit batch must be >> the blocking-path ceiling (got %.1f)", avgBatch)
}

// TestStoreMessageAsync_NotRetrievableBeforeFsync (TDD test 8, INVERTED for the
// frontier redirect — guards A3, deferred ring.Store).
//
// ring.Store is DEFERRED into the WAL success completion, so a durable message
// is NOT retrievable until its OWN fsync completes: with the fsync gated open,
// GetMessage misses and the ring has no entry; only after the fsync is released
// is the message retrievable. This is the storage-level half of A3 (a persistent
// tag is not claimable/deliverable before durable); the broker/frontier tests
// cover the cross-connection out-of-order case.
func TestStoreMessageAsync_NotRetrievableBeforeFsync(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	restore := SetWALGroupCommitFsyncForTest(func(f *os.File) error {
		<-release
		return nil
	})
	defer restore()
	defer once.Do(func() { close(release) })

	ds, err := NewDisruptorStorageWithEngineConfig(t.TempDir(), DefaultCheckpointInterval, interfaces.EngineConfig{})
	require.NoError(t, err)
	defer ds.Close()

	const tag = uint64(1)
	msg := &protocol.Message{
		Body:         []byte("durable-body"),
		RoutingKey:   "guard-q",
		DeliveryMode: 2,
		DeliveryTag:  tag,
	}

	var durableFired atomic.Bool
	require.NoError(t, ds.StoreMessageAsync("guard-q", msg, func(error) {
		durableFired.Store(true)
	}))

	// fsync gated: NOT durable ⇒ NOT retrievable. ring has no entry, and
	// GetMessage misses (ring miss + WAL index not yet populated).
	ring := ds.getQueueRing("guard-q")
	require.NotNil(t, ring, "queue ring must exist after StoreMessageAsync")
	_, ok := ring.ring.LoadByTag(tag)
	assert.False(t, ok, "durable message must NOT be ring-resident before its fsync (deferred ring.Store, A3)")
	_, gerr := ds.GetMessage("guard-q", tag)
	assert.Error(t, gerr, "durable message must NOT be retrievable before its fsync (A3)")
	assert.False(t, durableFired.Load(), "onDurable must not fire before fsync completes (A1)")

	// Release: the completion fires and the message becomes retrievable.
	once.Do(func() { close(release) })
	require.Eventually(t, durableFired.Load, 5*time.Second, 5*time.Millisecond,
		"onDurable must fire once the fsync completes")
	got, gerr2 := ds.GetMessage("guard-q", tag)
	require.NoError(t, gerr2, "durable message must be retrievable after its fsync")
	assert.Equal(t, msg.Body, got.Body)
}

// TestStoreMessageAsync_FsyncErrorNoRingStore (TDD test 8 part 2): on an fsync
// error the durable message must NEVER become ring-resident, so a frontier that
// advances past the errored tag finds nothing to deliver (no stale-slot delivery
// of a nacked message).
func TestStoreMessageAsync_FsyncErrorNoRingStore(t *testing.T) {
	restore := SetWALGroupCommitFsyncForTest(func(f *os.File) error {
		return fmt.Errorf("injected fsync failure")
	})
	defer restore()

	ds, err := NewDisruptorStorageWithEngineConfig(t.TempDir(), DefaultCheckpointInterval, interfaces.EngineConfig{})
	require.NoError(t, err)
	defer ds.Close()

	const tag = uint64(1)
	msg := &protocol.Message{Body: []byte("doomed"), RoutingKey: "err-q", DeliveryMode: 2, DeliveryTag: tag}

	done := make(chan error, 1)
	require.NoError(t, ds.StoreMessageAsync("err-q", msg, func(e error) { done <- e }))

	select {
	case e := <-done:
		require.Error(t, e, "fsync error must be reported to onDurable")
	case <-time.After(5 * time.Second):
		t.Fatal("onDurable never fired")
	}

	ring := ds.getQueueRing("err-q")
	require.NotNil(t, ring)
	_, ok := ring.ring.LoadByTag(tag)
	assert.False(t, ok, "an fsync-errored durable message must NEVER be ring-resident (no stale slot)")
	_, gerr := ds.GetMessage("err-q", tag)
	assert.Error(t, gerr, "an fsync-errored durable message must not be retrievable")
}
