package storage

// WS-COLDREAD-PATH storage tests (spec §5.1–§5.15).
//
// The contract: parseMessageAt reads the whole record in ONE pread, verifies the
// CRC contiguously, and — for records in [32 KiB, 1 MiB] on the DELIVERY path —
// reads into a pooled buffer whose release token travels with the message. A
// pooled buffer is never admitted (copy-on-admit); a peek never leases; a
// refused admission keeps the lease and the consumer releases it.
//
// Every test uses a deterministic seam: the WAL ReadAt hook, the admission seam,
// the hot-cache lock hook, or the pool's own test observers. No timing
// assertions.

import (
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

// --- helpers ----------------------------------------------------------------

// publishColdLargeCandidate publishes one durable message with the given body
// and evicts it (best-effort: with a tiny cache budget the publish was never
// admitted). The next read is therefore a COLD WAL read.
func publishColdLargeCandidate(t *testing.T, ds *DisruptorStorage, queue string, tag uint64, body []byte) {
	t.Helper()
	require.NoError(t, ds.StoreQueue(&protocol.Queue{Name: queue, Durable: true}))
	require.NoError(t, ds.StoreMessage(queue, &protocol.Message{
		Exchange:     "amq.direct",
		RoutingKey:   queue,
		Body:         body,
		DeliveryMode: 2,
		DeliveryTag:  tag,
	}))
	_ = ds.hot.Delete(queue, tag)
	_, _, resident := ds.hot.Get(queue, tag)
	require.False(t, resident, "premise: the message must be COLD before the read")
}

// indexRecordBytes returns the indexed record byte count and file position for
// (queue, tag).
func indexRecordBytes(t *testing.T, ds *DisruptorStorage, queue string, tag uint64) (int64, int64) {
	t.Helper()
	require.NotNil(t, ds.wal)
	require.NotNil(t, ds.wal.sharedWAL)
	key := recordKey{queueName: queue, deliveryTag: tag}
	sh := ds.wal.sharedWAL.shardForKey(key)
	sh.mu.RLock()
	loc := sh.offset[key]
	sh.mu.RUnlock()
	require.NotNil(t, loc, "premise: the tag must be in the WAL index")
	return loc.bytes, loc.pos.position
}

// newRefusedAdmissionStorage builds a storage whose hot cache can admit nothing,
// so every delivery read is a REFUSED admission and keeps its lease.
func newRefusedAdmissionStorage(t *testing.T) *DisruptorStorage {
	t.Helper()
	ds, err := NewDisruptorStorageWithEngineConfig(t.TempDir(), interfaces.EngineConfig{
		HotCacheQueueBytes: 1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })
	return ds
}

// largeColdBody is the 64 KiB body the pool gate targets.
func largeColdBody() []byte {
	b := make([]byte, 64*1024)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

// --- 1. one pread per record -------------------------------------------------

func TestColdRead_OnePreadPerRecord(t *testing.T) {
	ResetColdReadPoolForTest()
	t.Cleanup(ResetColdReadPoolForTest)
	ds := newColdReadStorage(t, t.TempDir())

	const queue = "cold-onepread-q"
	const tag = uint64(7)
	body := largeColdBody()
	publishColdLargeCandidate(t, ds, queue, tag, body)

	recordBytes, position := indexRecordBytes(t, ds, queue, tag)

	type call struct {
		off int64
		n   int
	}
	var mu sync.Mutex
	var calls []call
	restore := SetWALReadAtForTest(func(f *os.File, b []byte, off int64) (int, error) {
		mu.Lock()
		calls = append(calls, call{off: off, n: len(b)})
		mu.Unlock()
		return f.ReadAt(b, off)
	})

	got, err := ds.GetMessageForDelivery(queue, tag)
	restore()
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, body, got.Body, "the body must survive the one-pread read")

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, calls, 1, "the cold read must take exactly ONE pread")
	assert.Equal(t, position, calls[0].off, "the pread must be at the indexed position")
	assert.Equal(t, int(recordBytes), calls[0].n, "the pread must cover the whole record")

	got.ReleaseBody()
	require.Equal(t, int64(0), ColdReadPoolLeaseCountForTest())
}

// --- 2. recordBytes from the index, and the fallback -------------------------

func TestColdRead_RecordBytesFromIndex(t *testing.T) {
	ResetColdReadPoolForTest()
	t.Cleanup(ResetColdReadPoolForTest)
	ds := newColdReadStorage(t, t.TempDir())
	spy := &coldReadMetricsSpy{}
	ds.SetMetrics(spy)
	t.Cleanup(func() { ds.SetMetrics(nil) })

	const queue = "cold-index-q"
	const tag = uint64(8)
	body := largeColdBody()
	publishColdLargeCandidate(t, ds, queue, tag, body)

	recordBytes, _ := indexRecordBytes(t, ds, queue, tag)
	require.GreaterOrEqual(t, recordBytes, int64(coldReadPoolMinBytes))

	// Force the index's byte count to 0: the fast path cannot trust it and must
	// fall back to the legacy two-pread body, still returning the right bytes.
	key := recordKey{queueName: queue, deliveryTag: tag}
	sh := ds.wal.sharedWAL.shardForKey(key)
	sh.mu.RLock()
	loc := sh.offset[key]
	sh.mu.RUnlock()
	require.NotNil(t, loc)
	loc.bytes = 0

	var lens []int
	restore := SetWALReadAtForTest(func(f *os.File, b []byte, off int64) (int, error) {
		lens = append(lens, len(b))
		return f.ReadAt(b, off)
	})
	got, err := ds.GetMessageForDelivery(queue, tag)
	restore()

	require.NoError(t, err)
	require.Equal(t, body, got.Body, "the fallback must still return the correct body")
	require.Equal(t, []int{8, int(recordBytes) - 8}, lens,
		"a forced-zero index byte count must take the two-pread fallback")
	require.Equal(t, 1, spy.fallback, "the fallback must be counted")
	require.Equal(t, 0, spy.onePread, "no one-pread read may be counted on a fallback")
}

// --- 3. copy-on-admit, no alias ---------------------------------------------

func TestColdRead_AdmitCopiesLeasedBody_NoAlias(t *testing.T) {
	ResetColdReadPoolForTest()
	t.Cleanup(ResetColdReadPoolForTest)
	ds := newColdReadStorage(t, t.TempDir())

	const queue = "cold-alias-q"
	const tag = uint64(9)
	body := largeColdBody()
	publishColdLargeCandidate(t, ds, queue, tag, body)

	got, err := ds.GetMessageForDelivery(queue, tag)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.False(t, got.HasBodyRelease(),
		"an admitted message must not carry a live lease (INV-A1)")

	// The admitted entry must not carry a lease either.
	entry, _, resident := ds.hot.Get(queue, tag)
	require.True(t, resident, "premise: the delivery admits")
	require.NotNil(t, entry)
	require.False(t, entry.HasBodyRelease(), "no hot-cache entry may carry a lease")

	// POISON the pool: the pooled read buffer is back in the free-list. Writing
	// 0xAA over every free buffer must not touch the admitted body.
	free := ColdReadPoolFreeSlicesForTest()
	require.NotEmpty(t, free, "premise: the released buffer is retained by the pool")
	for _, b := range free {
		for i := range b {
			b[i] = 0xAA
		}
	}
	require.Equal(t, body, got.Body,
		"the admitted body must be an exact copy, not an alias of pooled memory")
}

// --- 4. refusal keeps the lease; delivery releases ---------------------------

func TestColdRead_RefusalKeepsLease_DeliveryReleases(t *testing.T) {
	ResetColdReadPoolForTest()
	t.Cleanup(ResetColdReadPoolForTest)
	ds := newRefusedAdmissionStorage(t)

	const queue = "cold-refuse-q"
	const tag = uint64(10)
	body := largeColdBody()
	publishColdLargeCandidate(t, ds, queue, tag, body)

	got, err := ds.GetMessageForDelivery(queue, tag)
	require.NoError(t, err, "a refused admission is never a delivery error")
	require.NotNil(t, got)
	require.Equal(t, body, got.Body)
	require.True(t, got.HasBodyRelease(), "a refused admission keeps the lease live")

	_, _, resident := ds.hot.GetResident(queue, tag)
	require.False(t, resident, "premise: nothing was admitted")

	_, before := ColdReadPoolStatsForTest()
	require.Equal(t, int64(1), ColdReadPoolLeaseCountForTest(), "one live lease")

	got.ReleaseBody()

	_, after := ColdReadPoolStatsForTest()
	require.Equal(t, before+1, after, "the release must return the buffer to the free-list")
	require.Equal(t, int64(0), ColdReadPoolLeaseCountForTest(), "no live lease after release")
}

// --- 5. release-once is idempotent ------------------------------------------

func TestColdRead_ReleaseIsIdempotent(t *testing.T) {
	ResetColdReadPoolForTest()
	t.Cleanup(ResetColdReadPoolForTest)
	ds := newRefusedAdmissionStorage(t)

	const queue = "cold-idem-q"
	const tag = uint64(11)
	publishColdLargeCandidate(t, ds, queue, tag, largeColdBody())

	got, err := ds.GetMessageForDelivery(queue, tag)
	require.NoError(t, err)
	require.True(t, got.HasBodyRelease())

	// A VALUE COPY carries the same closure. Releasing the copy first wins the
	// CAS; releasing the original is then a no-op.
	cp := *got
	cp.ReleaseBody()
	cp.ReleaseBody() // duplicate on the copy: no-op
	got.ReleaseBody()
	got.ReleaseBody() // duplicate on the original: no-op

	_, free := ColdReadPoolStatsForTest()
	require.Equal(t, 1, free, "the buffer must be returned exactly once")
	require.Equal(t, int64(0), ColdReadPoolLeaseCountForTest())
}

// --- 6. acknowledgement while the lease is in flight -------------------------

func TestColdRead_AckWhileLeaseInFlight(t *testing.T) {
	ResetColdReadPoolForTest()
	t.Cleanup(ResetColdReadPoolForTest)
	ds := newColdReadStorage(t, t.TempDir())

	const queue = "cold-ack-seam-q"
	const tag = uint64(12)
	body := largeColdBody()
	publishColdLargeCandidate(t, ds, queue, tag, body)

	// The acknowledgement is issued from the ADMISSION SEAM (the window between
	// the reader's acknowledgement re-check and its admission). The seam runs
	// with the per-tag shard lock held; the raw WAL acknowledgement does not
	// take that lock, so it lands in the window deterministically. (A real
	// acknowledgement goes through DeleteMessage and BLOCKS on the shard lock,
	// which is the invariant the lock provides.)
	var acked bool
	restore := SetColdReadAdmissionForTest(func(q string, gotTag uint64) {
		if q == queue && gotTag == tag && !acked {
			acked = true
			ds.wal.Acknowledge(queue, tag)
		}
	})
	t.Cleanup(restore)

	got, err := ds.GetMessageForDelivery(queue, tag)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.True(t, acked, "premise: the acknowledgement landed in the seam")
	require.Equal(t, body, got.Body, "the cold message body must still be valid")

	// The lease is either detached (the admission won and copied the body) or
	// live (the read refused). Either way no pooled buffer is aliased by a live
	// lease the caller does not own, and the buffer is off the free-list while
	// the message is live.
	if got.HasBodyRelease() {
		require.Equal(t, int64(1), ColdReadPoolLeaseCountForTest(),
			"a live lease means the buffer is off the free-list")
		got.ReleaseBody()
	}
	require.Equal(t, int64(0), ColdReadPoolLeaseCountForTest(),
		"no lease may leak out of the seam")
}

// --- 7. purge while the lease is in flight -----------------------------------

func TestColdRead_PurgeWhileLeaseInFlight(t *testing.T) {
	ResetColdReadPoolForTest()
	t.Cleanup(ResetColdReadPoolForTest)
	ds := newColdReadStorage(t, t.TempDir())

	const queue = "cold-purge-seam-q"
	const tag = uint64(13)
	body := largeColdBody()
	publishColdLargeCandidate(t, ds, queue, tag, body)

	reader := parkDeliveryAtAdmission(t, ds, queue, tag)
	reader.waitArrived(t)

	require.Equal(t, 1, ds.purgeQueueRange(queue, tag, tag), "premise: the purge settles the tag")
	reader.release()

	require.NoError(t, <-reader.done, "an epoch mismatch must NOT be a delivery error")
	got := <-reader.message
	require.NotNil(t, got, "the reader still returns the message it read")
	require.Equal(t, body, got.Body)
	require.True(t, got.HasBodyRelease(), "the refused admission keeps the lease live")
	require.Equal(t, int64(1), ColdReadPoolLeaseCountForTest())

	got.ReleaseBody()
	require.Equal(t, int64(0), ColdReadPoolLeaseCountForTest())
	_, _, resident := ds.hot.GetResident(queue, tag)
	require.False(t, resident, "a purged tag must not be admitted")
}

// --- 8. a peek never leases --------------------------------------------------

func TestColdRead_PeekNeverLeases(t *testing.T) {
	ResetColdReadPoolForTest()
	t.Cleanup(ResetColdReadPoolForTest)
	ds := newColdReadStorage(t, t.TempDir())

	const queue = "cold-peek-q"
	const tag = uint64(14)
	body := largeColdBody()
	publishColdLargeCandidate(t, ds, queue, tag, body)

	_, before := ColdReadPoolStatsForTest()

	got, err := ds.GetMessage(queue, tag)
	require.NoError(t, err)
	require.Equal(t, body, got.Body)
	require.False(t, got.HasBodyRelease(), "a peek must never lease (INV-A2)")

	_, after := ColdReadPoolStatsForTest()
	require.Equal(t, before, after, "a peek must not touch the pool free-list")
	require.Equal(t, int64(0), ColdReadPoolLeaseCountForTest())
}

// --- 9. the pool is bounded and exact-sized ---------------------------------

func TestColdRead_PoolIsBoundedAndExactSized(t *testing.T) {
	ResetColdReadPoolForTest()
	t.Cleanup(ResetColdReadPoolForTest)
	ds := newColdReadStorage(t, t.TempDir())

	const queue = "cold-pool-storm-q"
	const n = 64
	body := largeColdBody()
	for i := 0; i < n; i++ {
		publishColdLargeCandidate(t, ds, queue, uint64(i), body)
	}
	recordBytes, _ := indexRecordBytes(t, ds, queue, 0)

	var mu sync.Mutex
	var caps []int
	restoreAcquire := SetColdReadPoolAcquireObserverForTest(func(buf []byte, hit bool) {
		mu.Lock()
		caps = append(caps, cap(buf))
		mu.Unlock()
	})
	t.Cleanup(restoreAcquire)

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := g; i < n; i += 16 {
				m, err := ds.GetMessageForDelivery(queue, uint64(i))
				if err != nil {
					continue
				}
				m.ReleaseBody()
			}
		}(g)
	}
	wg.Wait()

	retained, _ := ColdReadPoolStatsForTest()
	require.LessOrEqual(t, retained, coldReadPoolBudgetBytes,
		"the pool must never retain more than its budget (INV-A4)")

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, caps, "premise: pooled acquisitions happened")
	for _, c := range caps {
		require.Equal(t, int(recordBytes), c,
			"every acquired buffer must have cap == recordBytes (no 2x slack)")
	}
	require.Equal(t, int64(0), ColdReadPoolLeaseCountForTest(), "no lease may leak")
}

// --- 10. a later delivery re-acquires the released bytes ---------------------

func TestColdRead_SequenceAdmitsAfterRelease(t *testing.T) {
	ResetColdReadPoolForTest()
	t.Cleanup(ResetColdReadPoolForTest)
	ds := newRefusedAdmissionStorage(t)

	const queue = "cold-seq-q"
	const tag = uint64(15)
	body := largeColdBody()
	publishColdLargeCandidate(t, ds, queue, tag, body)

	m1, err := ds.GetMessageForDelivery(queue, tag)
	require.NoError(t, err)
	require.Equal(t, body, m1.Body)
	require.True(t, m1.HasBodyRelease())
	m1.ReleaseBody()

	m2, err := ds.GetMessageForDelivery(queue, tag)
	require.NoError(t, err, "the message must still be deliverable after the first release")
	require.Equal(t, body, m2.Body, "no stale alias may survive a release")
	require.True(t, m2.HasBodyRelease())
	m2.ReleaseBody()

	require.Equal(t, int64(0), ColdReadPoolLeaseCountForTest())
	_, free := ColdReadPoolStatsForTest()
	require.Equal(t, 1, free, "exactly one buffer is retained after the second release")
}

// --- 11. the shared-body arm does not lease ---------------------------------

func TestColdRead_SharedBodyRefDoesNotLease(t *testing.T) {
	ResetColdReadPoolForTest()
	t.Cleanup(ResetColdReadPoolForTest)
	storage := newReadAheadTestStorage(t, t.TempDir(), 64)
	t.Cleanup(func() { storage.Close() })

	const queue = "cold-shared-q"
	const tag = uint64(0)
	require.NoError(t, storage.StoreQueue(&protocol.Queue{Name: queue, Durable: true}))

	body := sharedTestBody(4096)
	done := make(chan error, 1)
	require.NoError(t, storage.StoreSharedAsync([]interfaces.SharedDurableSub{{
		QueueName: queue,
		Message: &protocol.Message{
			Body:         body,
			DeliveryMode: 2,
			DeliveryTag:  tag,
		},
		OnDurable: func(err error) { done <- err },
	}}))
	require.NoError(t, <-done)

	storage.hot.Delete(queue, tag)
	_, _, resident := storage.hot.Get(queue, tag)
	require.False(t, resident, "premise: the shared-body message is cold")

	got, err := storage.GetMessageForDelivery(queue, tag)
	require.NoError(t, err)
	require.Equal(t, body, got.Body, "the shared body must be resolved")
	require.Nil(t, got.BodyRef)
	require.False(t, got.HasBodyRelease(),
		"the record buffer must be released immediately and attach no lease")
	require.Equal(t, int64(0), ColdReadPoolLeaseCountForTest())
}

// --- 12. durable gate and epoch/incarnation behaviour preserved --------------

func TestColdRead_PreservesDurableGateAndEpoch(t *testing.T) {
	t.Run("acknowledged-refused", func(t *testing.T) {
		ds := newColdReadStorage(t, t.TempDir())
		const queue = "cold-gate-q"
		const tag = uint64(16)
		publishColdLargeCandidate(t, ds, queue, tag, largeColdBody())
		ds.wal.Acknowledge(queue, tag)
		got, err := ds.GetMessageForDelivery(queue, tag)
		require.ErrorIs(t, err, interfaces.ErrMessageNotFound)
		require.Nil(t, got)
	})

	t.Run("epoch-mismatch-returns-message", func(t *testing.T) {
		ds := newColdReadStorage(t, t.TempDir())
		const queue = "cold-epoch-q"
		const tag = uint64(17)
		publishColdLargeCandidate(t, ds, queue, tag, largeColdBody())
		reader := parkDeliveryAtAdmission(t, ds, queue, tag)
		reader.waitArrived(t)
		ds.purgeQueueRange(queue, tag, tag)
		reader.release()
		require.NoError(t, <-reader.done, "an epoch mismatch is not a delivery error")
		got := <-reader.message
		require.NotNil(t, got)
		got.ReleaseBody()
	})

	t.Run("incarnation-mismatch-queue-not-found", func(t *testing.T) {
		ds := newColdReadStorage(t, t.TempDir())
		const queue = "cold-incarnation-q"
		const tag = uint64(18)
		publishColdLargeCandidate(t, ds, queue, tag, largeColdBody())
		reader := parkDeliveryAtAdmission(t, ds, queue, tag)
		reader.waitArrived(t)
		require.NoError(t, ds.DeleteQueue(queue))
		require.NoError(t, ds.StoreQueue(&protocol.Queue{Name: queue, Durable: true}))
		reader.release()
		require.ErrorIs(t, <-reader.done, interfaces.ErrQueueNotFound)
		require.Nil(t, <-reader.message)
	})
}

// --- 13. hot/cold counters count each delivery once -------------------------

func TestHotColdCounters_CountEachDeliveryOnce(t *testing.T) {
	ResetColdReadPoolForTest()
	t.Cleanup(ResetColdReadPoolForTest)
	ds := newColdReadStorage(t, t.TempDir())
	spy := &coldReadMetricsSpy{}
	ds.SetMetrics(spy)
	t.Cleanup(func() { ds.SetMetrics(nil) })

	const queue = "cold-counter-q"
	body := largeColdBody()
	const m = 4
	for i := 0; i < m; i++ {
		publishColdLargeCandidate(t, ds, queue, uint64(i), body)
	}

	// M cold deliveries.
	for i := 0; i < m; i++ {
		got, err := ds.GetMessageForDelivery(queue, uint64(i))
		require.NoError(t, err)
		got.ReleaseBody()
	}
	// N=2 hot re-deliveries.
	const nHits = 2
	for i := 0; i < nHits; i++ {
		got, err := ds.GetMessageForDelivery(queue, uint64(i))
		require.NoError(t, err)
		got.ReleaseBody()
	}

	require.Equal(t, nHits, spy.hotHits, "hot hits must be counted once each")
	require.Equal(t, m, spy.coldReads, "cold reads must be counted once each")
	require.Equal(t, m, spy.onePread, "one_pread == cold reads on the target shape")
	require.Equal(t, 0, spy.fallback)
}

// --- 14. peek vs admit split unchanged ---------------------------------------

func TestColdRead_PeekVsAdmitSplitUnchanged(t *testing.T) {
	ResetColdReadPoolForTest()
	t.Cleanup(ResetColdReadPoolForTest)
	ds := newColdReadStorage(t, t.TempDir())

	const queue = "cold-split-q"
	const tag = uint64(19)
	body := largeColdBody()
	publishColdLargeCandidate(t, ds, queue, tag, body)

	// The peek: leaves the count/list/residency unchanged, never leases.
	peeked, err := ds.GetMessage(queue, tag)
	require.NoError(t, err)
	require.Equal(t, body, peeked.Body)
	require.False(t, peeked.HasBodyRelease())
	count, err := ds.GetQueueMessageCount(queue)
	require.NoError(t, err)
	require.Equal(t, 0, count, "a peek must leave the count at 0")

	// The delivery: admits, count becomes 1, and the admitted entry carries no
	// pooled alias.
	got, err := ds.GetMessageForDelivery(queue, tag)
	require.NoError(t, err)
	require.Equal(t, body, got.Body)
	count, err = ds.GetQueueMessageCount(queue)
	require.NoError(t, err)
	require.Equal(t, 1, count, "the delivery admits and is counted")

	entry, _, resident := ds.hot.Get(queue, tag)
	require.True(t, resident)
	require.False(t, entry.HasBodyRelease(), "the admitted entry must not alias pooled memory")
}

// --- 15. admission budget unchanged by pooling -------------------------------

func TestColdRead_AdmissionBudgetUnchangedByPooling(t *testing.T) {
	ResetColdReadPoolForTest()
	t.Cleanup(ResetColdReadPoolForTest)

	t.Run("admitted", func(t *testing.T) {
		ds := newColdReadStorage(t, t.TempDir())
		const queue = "cold-budget-admit-q"
		const tag = uint64(20)
		publishColdLargeCandidate(t, ds, queue, tag, largeColdBody())

		globalBefore := ds.hot.globalUsedBytes()
		got, err := ds.GetMessageForDelivery(queue, tag)
		require.NoError(t, err)
		defer got.ReleaseBody()

		q := ds.hot.queueState(queue)
		require.NotNil(t, q)
		q.mu.Lock()
		entry := q.entries[tag]
		q.mu.Unlock()
		require.NotNil(t, entry, "premise: the delivery admits")
		wantCharge := cacheCharge(queue, entry.message)
		require.Equal(t, wantCharge, entry.charge,
			"the admitted entry's charge must be the message charge, not the pool's")
		require.Equal(t, wantCharge, ds.hot.globalUsedBytes()-globalBefore,
			"the global budget must move by exactly the charge")
		require.Equal(t, wantCharge, q.used,
			"the queue budget must move by exactly the charge")
	})

	t.Run("refused", func(t *testing.T) {
		ds := newRefusedAdmissionStorage(t)
		const queue = "cold-budget-refuse-q"
		const tag = uint64(21)
		publishColdLargeCandidate(t, ds, queue, tag, largeColdBody())

		globalBefore := ds.hot.globalUsedBytes()
		got, err := ds.GetMessageForDelivery(queue, tag)
		require.NoError(t, err)
		defer got.ReleaseBody()

		require.Equal(t, globalBefore, ds.hot.globalUsedBytes(),
			"a refused admission must not change the global budget")
		q := ds.hot.queueState(queue)
		require.Equal(t, int64(0), q.used, "a refused admission must not change the queue budget")
	})
}

// --- the optional-metrics spy ------------------------------------------------

// coldReadMetricsSpy implements StorageMetrics and ColdReadMetrics, recording
// the cold-read series and no-oping everything else.
type coldReadMetricsSpy struct {
	mu        sync.Mutex
	hotHits   int
	coldReads int
	onePread  int
	fallback  int
}

func (s *coldReadMetricsSpy) UpdateDiskMetrics(freeBytes, usedBytes float64)          {}
func (s *coldReadMetricsSpy) UpdateRingBufferUtilization(queueName string, u float64) {}
func (s *coldReadMetricsSpy) UpdateWALSize(bytes float64)                             {}
func (s *coldReadMetricsSpy) RecordWALWrite()                                         {}
func (s *coldReadMetricsSpy) RecordWALFsync(duration float64)                         {}
func (s *coldReadMetricsSpy) RecordWALWriteError()                                    {}
func (s *coldReadMetricsSpy) UpdateSegmentMetrics(queue string, count, size float64)  {}
func (s *coldReadMetricsSpy) RecordSegmentCompaction()                                {}
func (s *coldReadMetricsSpy) RecordSegmentCompactionFailure()                         {}
func (s *coldReadMetricsSpy) RecordSegmentReadError()                                 {}

func (s *coldReadMetricsSpy) RecordDeliveryHotHit() {
	s.mu.Lock()
	s.hotHits++
	s.mu.Unlock()
}
func (s *coldReadMetricsSpy) RecordDeliveryColdRead() {
	s.mu.Lock()
	s.coldReads++
	s.mu.Unlock()
}
func (s *coldReadMetricsSpy) RecordColdReadOnePread() {
	s.mu.Lock()
	s.onePread++
	s.mu.Unlock()
}
func (s *coldReadMetricsSpy) RecordColdReadTwoPreadFallback() {
	s.mu.Lock()
	s.fallback++
	s.mu.Unlock()
}
func (s *coldReadMetricsSpy) RecordColdReadCurrentFile()       {}
func (s *coldReadMetricsSpy) RecordColdReadOldFile()           {}
func (s *coldReadMetricsSpy) RecordColdReadBufferPoolHit()     {}
func (s *coldReadMetricsSpy) RecordColdReadBufferPoolMiss()    {}
func (s *coldReadMetricsSpy) SetColdReadBufferPoolBytes(int64) {}
func (s *coldReadMetricsSpy) AddColdReadLease(int64)           {}
func (s *coldReadMetricsSpy) SetHotCacheBytes(int64)           {}
