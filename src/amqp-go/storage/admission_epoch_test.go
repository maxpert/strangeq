package storage

// A PURGED OR DELETED MESSAGE MUST NEVER COME BACK TO THE HOT CACHE.
//
// The delivery reader admits what it reads (the write-through admission), and
// the admission is guarded against the ACKNOWLEDGEMENT paths by the per-tag
// shard lock (coldRemovalShards): the reader's (acknowledgement re-check +
// admission) is atomic with the acknowledgement's (removal + acknowledgement),
// so an acknowledged tag is never admitted.
//
// THE RANGE PURGE AND THE QUEUE DELETION WERE NOT COVERED BY THAT LOCK.
//
//   - purgeQueueRange settles a BAND of tags in the COLD tiers (the WAL and
//     the segment tier) and then removes the hot entries in that band;
//   - DeleteQueue settles the whole queue and sweeps the hot cache.
//
// Neither takes the per-tag shard lock, so a reader that was between its cold
// read and its admission could admit a tag the purge had just settled: the
// message would come back to the hot cache, be counted by the queue's
// reported message count, and be served from there again.
//
// THE FIX IS THE PER-QUEUE ADMISSION EPOCH. The reader captures the queue's
// epoch immediately before its cold read, and the range removal bumps the
// epoch under the queue's lock. The admission validates the captured epoch
// (and the queue's incarnation) under the SAME lock, so an admission that
// raced a removal is refused without any rollback: the validation happens
// before the global reservation, before the map/LRU mutation and before the
// accounting.
//
// The fixtures below force the interleaving deterministically: the seam
// between the reader's cold read and its admission is a test-only observer
// (SetColdReadAdmissionForTest), and the purge or the delete runs while the
// reader is parked in that window.

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

// epochWindowWait bounds how long a fixture waits for the reader to reach the
// admission seam.
//
// It is NOT a threshold on any verdict: the verdict comes from what the reader
// and the purge DID, observed after both have finished. The wait only decides
// whether the fixture has to keep waiting for a reader that cannot have
// arrived yet.
const epochWindowWait = 10 * time.Second

// parkedReader is a delivery reader parked at the admission seam.
type parkedReader struct {
	release func()
	arrived <-chan struct{}
	done    <-chan error
	message <-chan *protocol.Message
}

// parkDeliveryAtAdmission starts a delivery read of (queue, tag) and parks it
// at the seam between its cold read and its hot-cache admission.
//
// The returned reader is released with release(); the reader then completes
// and reports its message and its error.
func parkDeliveryAtAdmission(t *testing.T, ds *DisruptorStorage, queue string, tag uint64) *parkedReader {
	t.Helper()

	arrived := make(chan struct{})
	releaseCh := make(chan struct{})
	var arriveOnce sync.Once
	var releaseOnce sync.Once

	restore := SetColdReadAdmissionForTest(func(q string, gotTag uint64) {
		if q != queue || gotTag != tag {
			return
		}
		arriveOnce.Do(func() { close(arrived) })
		<-releaseCh
	})
	t.Cleanup(restore)

	release := func() { releaseOnce.Do(func() { close(releaseCh) }) }
	// A fixture that fails before it releases the reader must not leave it
	// parked forever.
	t.Cleanup(release)

	msgCh := make(chan *protocol.Message, 1)
	errCh := make(chan error, 1)
	go func() {
		msg, err := ds.GetMessageForDelivery(queue, tag)
		msgCh <- msg
		errCh <- err
	}()

	return &parkedReader{
		release: release,
		arrived: arrived,
		done:    errCh,
		message: msgCh,
	}
}

// waitArrived waits for the reader to reach the admission seam.
func (p *parkedReader) waitArrived(t *testing.T) {
	t.Helper()
	select {
	case <-p.arrived:
	case <-time.After(epochWindowWait):
		t.Fatal("the reader never reached the admission seam")
	}
}

// TestDeliveryRead_RangePurgeDoesNotReAdmitThePurgedTag: a range purge that
// runs while the reader is between its cold read and its admission must NOT
// leave the purged tag resident, counted or servable.
func TestDeliveryRead_RangePurgeDoesNotReAdmitThePurgedTag(t *testing.T) {
	ds := newColdReadStorage(t, t.TempDir())

	const queue = "purge-epoch-q"
	const tag = uint64(77)
	publishOneColdCandidate(t, ds, queue, tag)

	reader := parkDeliveryAtAdmission(t, ds, queue, tag)
	reader.waitArrived(t)

	// THE PURGE, while the reader is parked between its cold read and its
	// admission. It settles the tag in the cold tiers and removes the hot
	// entries in the band.
	purged := ds.purgeQueueRange(queue, tag, tag)
	require.Equal(t, 1, purged, "premise: the purge must settle the tag")

	reader.release()

	require.NoError(t, <-reader.done,
		"an epoch mismatch must NOT fail the delivery: the purge may have been aimed at a "+
			"different band, and failing the read would be a spurious delivery error")
	require.NotNil(t, <-reader.message,
		"the reader must still return the message it read")

	// (1) THE PURGED TAG MUST NOT BE RESIDENT.
	_, _, resident := ds.hot.GetResident(queue, tag)
	require.False(t, resident,
		"a tag settled by a range purge must never be admitted into the hot cache")

	// (2) THE REPORTED COUNT MUST NOT INCLUDE IT.
	count, err := ds.GetQueueMessageCount(queue)
	require.NoError(t, err)
	require.Equal(t, 0, count,
		"a tag settled by a range purge must never be counted by the hot cache")

	// (3) A SUBSEQUENT READ MUST NOT SERVE IT.
	again, err := ds.GetMessageForDelivery(queue, tag)
	require.ErrorIs(t, err, interfaces.ErrMessageNotFound,
		"a tag settled by a range purge must not be served by a later read")
	require.Nil(t, again)
	peeked, err := ds.GetMessage(queue, tag)
	require.ErrorIs(t, err, interfaces.ErrMessageNotFound,
		"a tag settled by a range purge must not be served by a later peek")
	require.Nil(t, peeked)
}

// TestDeliveryRead_EmptyRangePurgeStillInvalidatesTheAdmission: a purge whose
// band intersects NO resident entry must still invalidate an in-flight
// admission.
//
// The band is settled in the COLD tiers whether or not the hot tier held a
// copy of it, so an empty band is not a no-op: the in-flight reader may be
// about to admit a tag the just-settled band covers.
func TestDeliveryRead_EmptyRangePurgeStillInvalidatesTheAdmission(t *testing.T) {
	ds := newColdReadStorage(t, t.TempDir())

	const queue = "empty-purge-epoch-q"
	const tag = uint64(78)
	publishOneColdCandidate(t, ds, queue, tag)

	reader := parkDeliveryAtAdmission(t, ds, queue, tag)
	reader.waitArrived(t)

	// THE EMPTY PURGE: the band is far away from the reader's tag, and the
	// message is not resident, so the band intersects nothing.
	purged := ds.purgeQueueRange(queue, tag+1000, tag+2000)
	require.Equal(t, 0, purged, "premise: the band must intersect nothing")

	reader.release()

	require.NoError(t, <-reader.done)
	require.NotNil(t, <-reader.message)

	_, _, resident := ds.hot.GetResident(queue, tag)
	require.False(t, resident,
		"an empty range purge must still invalidate an in-flight admission")

	count, err := ds.GetQueueMessageCount(queue)
	require.NoError(t, err)
	require.Equal(t, 0, count,
		"an empty range purge must still invalidate an in-flight admission")
}

// TestDeliveryRead_DeleteQueueDoesNotAdmitIntoTheSuccessorQueue: a queue
// deletion that runs while the reader is between its cold read and its
// admission must NOT let the old tag into the SUCCESSOR queue's cache or
// count.
//
// The hot cache keys its per-queue state by NAME and never removes it, so the
// successor queue inherits the deleted queue's state: without an incarnation
// check the old tag would be admitted into the successor's cache and counted
// by the successor's reported message count.
func TestDeliveryRead_DeleteQueueDoesNotAdmitIntoTheSuccessorQueue(t *testing.T) {
	ds := newColdReadStorage(t, t.TempDir())

	const queue = "delete-epoch-q"
	const tag = uint64(79)
	publishOneColdCandidate(t, ds, queue, tag)

	reader := parkDeliveryAtAdmission(t, ds, queue, tag)
	reader.waitArrived(t)

	// THE DELETE, then the REDECLARE.
	require.NoError(t, ds.DeleteQueue(queue))
	require.NoError(t, ds.StoreQueue(&protocol.Queue{Name: queue, Durable: true}))

	reader.release()

	// The queue is GONE (or is a different incarnation), so the reader must
	// not hand the old message to a consumer of the successor queue.
	require.ErrorIs(t, <-reader.done, interfaces.ErrQueueNotFound,
		"a delivery of a deleted queue must fail with the queue-not-found error")
	require.Nil(t, <-reader.message)

	// (1) THE SUCCESSOR QUEUE MUST NOT HOLD THE OLD TAG.
	_, _, resident := ds.hot.GetResident(queue, tag)
	require.False(t, resident,
		"an old tag must never enter the successor queue's hot cache")

	// (2) THE SUCCESSOR QUEUE'S COUNT MUST NOT INCLUDE IT.
	count, err := ds.GetQueueMessageCount(queue)
	require.NoError(t, err)
	require.Equal(t, 0, count,
		"an old tag must never be counted by the successor queue")

	// (3) THE SUCCESSOR MUST NOT SERVE IT FROM RAM.
	//
	// NOTE: the storage's DeleteQueue does NOT settle the WAL records — the
	// BROKER's delete path purges the incarnation's band first
	// (broker/storage_broker.go, deleteQueueIncarnation) — so a cold read of
	// the old tag still finds the record on disk. What the fix guarantees is
	// that the old tag does not come back to the HOT CACHE: it is not served
	// from RAM and it is not counted.
	_, _, served := ds.hot.Get(queue, tag)
	require.False(t, served,
		"an old tag must never be served from the successor queue's hot cache")
}

// TestDeliveryRead_AdmitsWhenNothingWasPurged is the POSITIVE CONTROL: with
// no purge and no deletion, the delivery still admits what it read and still
// serves it.
//
// Without it, the fixtures above would pass on a reader that admits nothing
// at all.
func TestDeliveryRead_AdmitsWhenNothingWasPurged(t *testing.T) {
	ds := newColdReadStorage(t, t.TempDir())

	const queue = "no-purge-epoch-q"
	const tag = uint64(80)
	publishOneColdCandidate(t, ds, queue, tag)

	reader := parkDeliveryAtAdmission(t, ds, queue, tag)
	reader.waitArrived(t)

	// NOTHING HAPPENS. The reader is released and completes its admission.
	reader.release()

	require.NoError(t, <-reader.done)
	require.NotNil(t, <-reader.message)

	_, _, resident := ds.hot.Get(queue, tag)
	require.True(t, resident,
		"a delivery read with no purge must admit what it read")

	count, err := ds.GetQueueMessageCount(queue)
	require.NoError(t, err)
	require.Equal(t, 1, count,
		"the admitted message is hot-resident, so it is counted")

	served, err := ds.GetMessageForDelivery(queue, tag)
	require.NoError(t, err)
	require.NotNil(t, served, "the admitted message must still be served")
}

// TestHotCacheDeleteRange_AdvancesTheAdmissionEpoch pins the epoch contract
// at the hot cache itself: EVERY range removal advances the queue's
// admission epoch, INCLUDING a range that matched no entry.
//
// An empty range is not a no-op: the in-flight reader may be about to admit a
// tag the just-settled band covers, and the band is settled in the COLD
// tiers whether or not the hot tier held a copy of it.
func TestHotCacheDeleteRange_AdvancesTheAdmissionEpoch(t *testing.T) {
	c := newHotCache(1<<20, 1<<20)
	const queue = "epoch-q"
	c.Put(queue, benchHotMsg(1), true)

	epoch, _ := c.captureAdmission(queue)

	// A range that matches NOTHING (the queue holds one entry, outside the
	// band).
	require.Equal(t, 0, c.DeleteRange(queue, 100, 200),
		"premise: the band must match no entry")
	after, _ := c.captureAdmission(queue)
	require.NotEqual(t, epoch, after,
		"an EMPTY range removal must advance the admission epoch")

	// A range that matches the entry.
	require.Equal(t, 1, c.DeleteRange(queue, 1, 1),
		"premise: the band must match the entry")
	afterRemoval, _ := c.captureAdmission(queue)
	require.NotEqual(t, after, afterRemoval,
		"a range removal must advance the admission epoch")
}

// TestHotCacheDeleteRange_AdvancesTheEpochBeforeRemovingTheEntries pins the
// ORDER: the epoch is advanced BEFORE the entries are removed, so a reader
// parked between its capture and its admission cannot slip an admission in
// behind the removal.
//
// The fixture observes the order through the hot cache's per-queue lock
// hook: the hook runs with the queue's lock held, so a reader that is still
// blocked on that lock is, by definition, behind the removal.
func TestHotCacheDeleteRange_AdvancesTheEpochBeforeRemovingTheEntries(t *testing.T) {
	c := newHotCache(1<<20, 1<<20)
	const queue = "epoch-order-q"
	c.Put(queue, benchHotMsg(1), true)

	epoch, _ := c.captureAdmission(queue)

	stale := false
	restore := SetHotCacheQueueLockForTest(func(name string) {
		if name != queue {
			return
		}
		// The lock is HELD here, inside the removal: the epoch must already
		// have been advanced. The field is read directly because the hook
		// runs on the goroutine that holds the lock, so taking it again would
		// deadlock.
		q := c.queueStateIfPresent(queue)
		if q != nil && q.admissionEpoch == epoch {
			stale = true
		}
	})
	t.Cleanup(restore)

	c.DeleteRange(queue, 1, 1)
	require.False(t, stale,
		"the admission epoch must be advanced BEFORE the entries are removed")
}
