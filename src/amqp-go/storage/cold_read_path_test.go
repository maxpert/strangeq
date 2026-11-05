package storage

// WS-COLDREAD-PATH SAFE-CORE storage tests.
//
// The safe core is the part of the cold-read path that survived the rejection
// of the pooled-buffer mechanism: parseMessageAt reads the WHOLE record in ONE
// pread of segmentRecord.bytes, verifies the CRC in one contiguous sweep, and
// falls back ONCE (counted) to the legacy two-pread body when the index's byte
// count is unavailable or inconsistent. There is no pool and no lease, so every
// test here is deterministic: the WAL ReadAt hook, the admission seam, and the
// metrics spy are the only seams. No timing assertions.

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

// --- helpers ----------------------------------------------------------------

// publishColdLargeCandidate publishes one durable message with the given body
// and evicts it from the hot cache, so the next read is a COLD WAL read.
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
// so every delivery read is a REFUSED admission (the message stays cold and no
// lease/pool state is involved).
func newRefusedAdmissionStorage(t *testing.T) *DisruptorStorage {
	t.Helper()
	ds, err := NewDisruptorStorageWithEngineConfig(t.TempDir(), interfaces.EngineConfig{
		HotCacheQueueBytes: 1,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })
	return ds
}

// largeColdBody is the 64 KiB body the old pool gate targeted; the safe core
// must handle it identically to every other size.
func largeColdBody() []byte {
	b := make([]byte, 64*1024)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

// --- 1. one pread per record -------------------------------------------------

func TestColdRead_OnePreadPerRecord(t *testing.T) {
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
}

// --- 2. recordBytes from the index, and the counted fallback -----------------

func TestColdRead_RecordBytesFromIndex(t *testing.T) {
	ds := newColdReadStorage(t, t.TempDir())
	spy := &coldReadMetricsSpy{}
	ds.SetMetrics(spy)
	t.Cleanup(func() { ds.SetMetrics(nil) })

	const queue = "cold-index-q"
	const tag = uint64(8)
	body := largeColdBody()
	publishColdLargeCandidate(t, ds, queue, tag, body)

	recordBytes, _ := indexRecordBytes(t, ds, queue, tag)
	require.Greater(t, recordBytes, int64(8))

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

// --- 3. bodies are byte-correct for every size -------------------------------

// TestColdRead_BodyByteCorrect pins that the one-pread body is byte-identical
// to the published body across sizes that straddle the old pool gate (32 KiB)
// and beyond it: the safe core must not size-special-case anything.
func TestColdRead_BodyByteCorrect(t *testing.T) {
	ds := newColdReadStorage(t, t.TempDir())

	sizes := []int{0, 1, 1024, 32*1024 - 1, 32 * 1024, 64 * 1024, 256*1024 + 7}
	for i, size := range sizes {
		queue := "cold-bytes-q"
		tag := uint64(i)
		body := make([]byte, size)
		for j := range body {
			body[j] = byte((i*31 + j) % 251)
		}
		publishColdLargeCandidate(t, ds, queue, tag, body)

		got, err := ds.GetMessageForDelivery(queue, tag)
		require.NoError(t, err, "size %d", size)
		require.NotNil(t, got, "size %d", size)
		require.Equal(t, body, got.Body, "body must be byte-identical at size %d", size)

		// The peek and the delivery must agree byte-for-byte too.
		peek, err := ds.GetMessage(queue, tag)
		require.NoError(t, err, "peek size %d", size)
		require.Equal(t, body, peek.Body, "peek body must be byte-identical at size %d", size)
	}
}

// --- 4. the CRC check is still mandatory on the one-pread path ---------------

// TestColdRead_CRCMismatchRejected proves the contiguous CRC sweep is not
// decorative: a single flipped byte inside the CRC-covered region
// [4:recordBytes] must be rejected. The check is done at the WAL read, because
// readColdMessage deliberately swallows a WAL read error and falls through to
// the segment tier.
func TestColdRead_CRCMismatchRejected(t *testing.T) {
	ds := newColdReadStorage(t, t.TempDir())

	const queue = "cold-crc-q"
	const tag = uint64(30)
	publishColdLargeCandidate(t, ds, queue, tag, largeColdBody())
	recordBytes, _ := indexRecordBytes(t, ds, queue, tag)

	restore := SetWALReadAtForTest(func(f *os.File, b []byte, off int64) (int, error) {
		n, err := f.ReadAt(b, off)
		if err == nil && int64(len(b)) == recordBytes {
			// Flip a byte inside [4:recordBytes] (the first data byte).
			b[8] ^= 0xFF
		}
		return n, err
	})
	got, err := ds.wal.Read(queue, tag)
	restore()

	require.Error(t, err, "a corrupted record must not be served")
	require.Contains(t, err.Error(), "CRC mismatch")
	require.Nil(t, got)
}

// --- 5. hot/cold counters count each delivery once ---------------------------

func TestHotColdCounters_CountEachDeliveryOnce(t *testing.T) {
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
		require.NotNil(t, got)
	}
	// N=2 hot re-deliveries.
	const nHits = 2
	for i := 0; i < nHits; i++ {
		got, err := ds.GetMessageForDelivery(queue, uint64(i))
		require.NoError(t, err)
		require.NotNil(t, got)
	}

	require.Equal(t, nHits, spy.hotHits, "hot hits must be counted once each")
	require.Equal(t, m, spy.coldReads, "cold reads must be counted once each")
	require.Equal(t, m, spy.onePread, "one_pread == cold reads on the target shape")
	require.Equal(t, 0, spy.fallback)
	require.Equal(t, m, spy.currentFile, "a fresh message is cold-read from the active file")
	require.Equal(t, 0, spy.oldFile)
}

// --- 6. durable gate and epoch/incarnation behaviour preserved ---------------

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
		require.Equal(t, largeColdBody(), got.Body)
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

// --- 7. peek vs admit split unchanged ----------------------------------------

func TestColdRead_PeekVsAdmitSplitUnchanged(t *testing.T) {
	ds := newColdReadStorage(t, t.TempDir())

	const queue = "cold-split-q"
	const tag = uint64(19)
	body := largeColdBody()
	publishColdLargeCandidate(t, ds, queue, tag, body)

	// The peek: leaves the count/list unchanged, never admits.
	peeked, err := ds.GetMessage(queue, tag)
	require.NoError(t, err)
	require.Equal(t, body, peeked.Body)
	count, err := ds.GetQueueMessageCount(queue)
	require.NoError(t, err)
	require.Equal(t, 0, count, "a peek must leave the count at 0")

	// The delivery: admits and is counted.
	got, err := ds.GetMessageForDelivery(queue, tag)
	require.NoError(t, err)
	require.Equal(t, body, got.Body)
	count, err = ds.GetQueueMessageCount(queue)
	require.NoError(t, err)
	require.Equal(t, 1, count, "the delivery admits and is counted")

	_, _, resident := ds.hot.Get(queue, tag)
	require.True(t, resident)
}

// --- 8. admission budget unchanged -------------------------------------------

func TestColdRead_AdmissionBudgetUnchanged(t *testing.T) {
	t.Run("admitted", func(t *testing.T) {
		ds := newColdReadStorage(t, t.TempDir())
		const queue = "cold-budget-admit-q"
		const tag = uint64(20)
		publishColdLargeCandidate(t, ds, queue, tag, largeColdBody())

		globalBefore := atomic.LoadInt64(&ds.hot.globalUsed)
		got, err := ds.GetMessageForDelivery(queue, tag)
		require.NoError(t, err)
		require.NotNil(t, got)

		q := ds.hot.queueState(queue)
		require.NotNil(t, q)
		q.mu.Lock()
		entry := q.entries[tag]
		q.mu.Unlock()
		require.NotNil(t, entry, "premise: the delivery admits")
		wantCharge := cacheCharge(queue, entry.message)
		require.Equal(t, wantCharge, entry.charge,
			"the admitted entry's charge must be the message charge")
		require.Equal(t, wantCharge, atomic.LoadInt64(&ds.hot.globalUsed)-globalBefore,
			"the global budget must move by exactly the charge")
	})

	t.Run("refused", func(t *testing.T) {
		ds := newRefusedAdmissionStorage(t)
		const queue = "cold-budget-refuse-q"
		const tag = uint64(21)
		publishColdLargeCandidate(t, ds, queue, tag, largeColdBody())

		globalBefore := atomic.LoadInt64(&ds.hot.globalUsed)
		got, err := ds.GetMessageForDelivery(queue, tag)
		require.NoError(t, err)
		require.NotNil(t, got)

		require.Equal(t, globalBefore, atomic.LoadInt64(&ds.hot.globalUsed),
			"a refused admission must not change the global budget")
		q := ds.hot.queueState(queue)
		require.Equal(t, int64(0), q.used, "a refused admission must not change the queue budget")
	})
}

// --- 9. the shared-body arm still resolves its body --------------------------

// TestColdRead_SharedBodyRefResolves proves the one-pread record read still
// resolves an ITER5 shared-body reference (a SECOND read of the co-located
// BodyBlock) and returns the block's bytes, with no pool/lease involvement.
func TestColdRead_SharedBodyRefResolves(t *testing.T) {
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
}

// --- the optional-metrics spy ------------------------------------------------

// coldReadMetricsSpy implements StorageMetrics and ColdReadMetrics, recording
// the safe-core cold-read series and no-oping everything else.
type coldReadMetricsSpy struct {
	mu          sync.Mutex
	hotHits     int
	coldReads   int
	onePread    int
	fallback    int
	currentFile int
	oldFile     int
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
func (s *coldReadMetricsSpy) RecordColdReadCurrentFile() {
	s.mu.Lock()
	s.currentFile++
	s.mu.Unlock()
}
func (s *coldReadMetricsSpy) RecordColdReadOldFile() {
	s.mu.Lock()
	s.oldFile++
	s.mu.Unlock()
}
