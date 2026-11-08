package storage

// AN ACKNOWLEDGED TAG MUST NEVER BE ADMITTED INTO THE HOT CACHE.
//
// The hot cache is a RAM HOLDER, and the delivery path admits what it reads
// (the write-through admission). That admission has ONE invariant to keep:
//
//	an acknowledged delivery tag is never present in the hot cache, is never
//	served from it, is never counted by it, and is never admitted to it
//	after the acknowledgement.
//
// WHY IT MATTERS. The hot cache's removal path REPORTS A WIN to the broker
// (DeleteMessageIfPresentDetailed), and the broker decrements the queue
// depth on that win. So an acknowledged tag that is resurrected into the hot
// cache is removed a SECOND time, and the queue depth is decremented a
// SECOND time for one message — the depth under-counts, and the queue can
// exceed its configured limit.
//
// THE WINDOW. The reader's admission and the acknowledgement are two
// separate acts. If the reader checks the acknowledgement and then admits
// WITHOUT holding anything that excludes the acknowledgement, an
// acknowledgement that lands in between is resurrected into the cache.
//
// THE FIX (mechanism A). The reader takes the SAME per-tag shard lock the
// acknowledgement paths take (coldRemovalShards) and holds it across
//
//	(acknowledgement re-check + admission)
//
// and DeleteMessage — the OTHER acknowledgement path — takes that lock too.
// The two are therefore mutually exclusive per tag:
//
//   - the reader first: it admits a tag that is not acknowledged YET; the
//     acknowledgement that follows removes the entry (under the same lock)
//     and acknowledges the WAL, so nothing acknowledged is left resident;
//   - the acknowledgement first: the reader's re-check (under the lock)
//     SEES the acknowledgement and refuses the message, admitting nothing.
//
// THE FIXTURES force the interleaving deterministically: the seam between the
// reader's re-check and its admission is a test-only observer
// (SetColdReadAdmissionForTest). The acknowledgement is issued from another
// goroutine, through the REAL acknowledgement path, so it takes the same
// per-tag lock the reader holds — and is therefore BLOCKED for exactly as
// long as the reader's window is open.

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/maxpert/amqp-go/interfaces"
)

const ackAdmissionQueue = "ack-admission-race-q"

// ackWindowWait bounds how long the fixture waits for the acknowledgement to
// land WHILE the reader is parked in the window.
//
// It is NOT a threshold on any verdict: the verdict comes from what the
// reader and the acknowledgement DID, observed after both have finished. The
// wait only decides whether the fixture has to keep waiting for an
// acknowledgement that CANNOT land yet (because the reader holds the
// per-tag lock) — and in that case the fixture proceeds and asserts the
// final state.
const ackWindowWait = 2 * time.Second

// ackInWindow runs the real acknowledgement path against the tag while the
// reader is parked in its (re-check + admission) window.
//
// It returns a channel that is closed once the acknowledgement has completed,
// and a flag that reports whether the acknowledgement completed WHILE the
// reader was still parked (i.e. the reader's window was open).
func ackInWindow(t *testing.T, ds *StorageEngine, queue string, tag uint64, path string) (done <-chan struct{}, landedInWindow *atomic.Bool) {
	t.Helper()
	ackFinished := make(chan struct{})
	landed := &atomic.Bool{}

	restore := SetColdReadAdmissionForTest(func(q string, gotTag uint64) {
		if q != queue || gotTag != tag {
			return
		}
		// The acknowledgement, through the REAL acknowledgement path, from
		// another goroutine: that path takes the per-tag shard lock, which
		// the reader holds for exactly the window under test.
		go func() {
			defer close(ackFinished)
			switch path {
			case "plain":
				_ = ds.DeleteMessage(queue, tag)
			case "detailed":
				_, _, _ = ds.DeleteMessageIfPresentDetailed(queue, tag)
			}
		}()
		select {
		case <-ackFinished:
			// The acknowledgement landed WHILE the reader was still parked:
			// the reader's window was open.
			landed.Store(true)
		case <-time.After(ackWindowWait):
			// The acknowledgement is blocked (the reader holds the per-tag
			// lock): the window is closed.
			landed.Store(false)
		}
	})
	t.Cleanup(restore)
	return ackFinished, landed
}

// TestDeliveryRead_DoesNotAdmitAnAcknowledgedTag_PlainAckPath forces the
// interleaving on the PLAIN acknowledgement path (DeleteMessage: the hot-cache
// removal and the WAL acknowledgement, both under the per-tag shard lock).
func TestDeliveryRead_DoesNotAdmitAnAcknowledgedTag_PlainAckPath(t *testing.T) {
	ds := newColdReadStorage(t, t.TempDir())

	const tag = uint64(42)
	publishOneColdCandidate(t, ds, ackAdmissionQueue, tag)

	ackFinished, landedInWindow := ackInWindow(t, ds, ackAdmissionQueue, tag, "plain")

	// The reader: it checks the acknowledgement, parks in the window, and
	// then admits.
	_, _ = ds.GetMessageForDelivery(ackAdmissionQueue, tag)

	// The acknowledgement completes (it was blocked until the reader
	// released the per-tag lock).
	select {
	case <-ackFinished:
	case <-time.After(10 * time.Second):
		t.Fatal("the acknowledgement never completed")
	}

	// (1) THE READER MUST NOT LEAVE AN ACKNOWLEDGED ENTRY RESIDENT.
	_, _, resident := ds.hot.GetResident(ackAdmissionQueue, tag)
	require.False(t, resident,
		"an acknowledged tag must never be resident in the hot cache")
	_, _, served := ds.hot.Get(ackAdmissionQueue, tag)
	require.False(t, served,
		"an acknowledged tag must never be served from the hot cache")

	// (2) A SUBSEQUENT READ MUST NOT SERVE IT.
	again, err := ds.GetMessageForDelivery(ackAdmissionQueue, tag)
	require.ErrorIs(t, err, interfaces.ErrMessageNotFound,
		"an acknowledged tag must not be served by a later read")
	require.Nil(t, again)
	peeked, err := ds.GetMessage(ackAdmissionQueue, tag)
	require.ErrorIs(t, err, interfaces.ErrMessageNotFound,
		"an acknowledged tag must not be served by a later peek")
	require.Nil(t, peeked)

	// (3) THE REPORTED COUNT MUST NOT INCLUDE IT.
	count, err := ds.GetQueueMessageCount(ackAdmissionQueue)
	require.NoError(t, err)
	require.Equal(t, 0, count,
		"an acknowledged tag must never be counted by the hot cache")

	// (4) THE QUEUE DEPTH MUST NOT BE DECREMENTED TWICE FOR ONE MESSAGE.
	// The removal reports a win to the broker, and the broker decrements the
	// depth on that win — so an acknowledged tag must NOT report a win.
	removed, wasResident, err := ds.DeleteMessageIfPresentDetailed(ackAdmissionQueue, tag)
	require.NoError(t, err)
	require.False(t, removed,
		"an already-acknowledged tag must not report a removal: the broker would decrement the "+
			"queue depth a second time for one message")
	require.False(t, wasResident,
		"an already-acknowledged tag must not be reported as hot-resident")

	require.False(t, landedInWindow.Load(),
		"the acknowledgement landed while the reader was parked in its (re-check + admission) "+
			"window: the reader's check and admission are not atomic with the acknowledgement")
}

// TestDeliveryRead_DoesNotAdmitAnAcknowledgedTag_DetailedAckPath forces the
// SAME interleaving on the OTHER acknowledgement path
// (DeleteMessageIfPresentDetailed: the broker's deleteIfPresent). It is the
// path the broker uses for a consumer ack, so it is the one whose double
// decrement the queue depth actually suffers.
func TestDeliveryRead_DoesNotAdmitAnAcknowledgedTag_DetailedAckPath(t *testing.T) {
	ds := newColdReadStorage(t, t.TempDir())

	const tag = uint64(43)
	publishOneColdCandidate(t, ds, ackAdmissionQueue, tag)

	ackFinished, landedInWindow := ackInWindow(t, ds, ackAdmissionQueue, tag, "detailed")

	_, _ = ds.GetMessageForDelivery(ackAdmissionQueue, tag)

	select {
	case <-ackFinished:
	case <-time.After(10 * time.Second):
		t.Fatal("the acknowledgement never completed")
	}

	_, _, resident := ds.hot.GetResident(ackAdmissionQueue, tag)
	require.False(t, resident,
		"an acknowledged tag must never be resident in the hot cache")

	count, err := ds.GetQueueMessageCount(ackAdmissionQueue)
	require.NoError(t, err)
	require.Equal(t, 0, count,
		"an acknowledged tag must never be counted by the hot cache")

	// The removal must not report a second win.
	removed, _, err := ds.DeleteMessageIfPresentDetailed(ackAdmissionQueue, tag)
	require.NoError(t, err)
	require.False(t, removed,
		"an already-acknowledged tag must not report a removal: the broker would decrement the "+
			"queue depth a second time for one message")

	require.False(t, landedInWindow.Load(),
		"the acknowledgement landed while the reader was parked in its (re-check + admission) "+
			"window: the reader's check and admission are not atomic with the acknowledgement")
}

// TestDeliveryRead_RefusesAnAlreadyAcknowledgedTag: a tag that is
// ALREADY acknowledged is refused by the delivery read, and nothing is
// admitted.
func TestDeliveryRead_RefusesAnAlreadyAcknowledgedTag(t *testing.T) {
	ds := newColdReadStorage(t, t.TempDir())

	const tag = uint64(45)
	publishOneColdCandidate(t, ds, ackAdmissionQueue, tag)

	// Acknowledge through the shipped entry point.
	ds.wal.Acknowledge(ackAdmissionQueue, tag)
	require.True(t, ds.wal.IsAcknowledged(ackAdmissionQueue, tag), "premise: the tag must be acknowledged")

	got, err := ds.GetMessageForDelivery(ackAdmissionQueue, tag)
	require.ErrorIs(t, err, interfaces.ErrMessageNotFound,
		"an acknowledged tag must not be served")
	require.Nil(t, got)

	_, _, resident := ds.hot.GetResident(ackAdmissionQueue, tag)
	require.False(t, resident, "an acknowledged tag must not be admitted")

	count, err := ds.GetQueueMessageCount(ackAdmissionQueue)
	require.NoError(t, err)
	require.Equal(t, 0, count, "an acknowledged tag must not be counted")
}

// TestDeliveryRead_AdmitsAnUnacknowledgedTag is the POSITIVE CONTROL: with no
// acknowledgement in the window, the delivery read DOES admit what it read.
// Without it, the fixtures above would pass on a reader that admits nothing
// at all.
func TestDeliveryRead_AdmitsAnUnacknowledgedTag(t *testing.T) {
	ds := newColdReadStorage(t, t.TempDir())

	const tag = uint64(44)
	publishOneColdCandidate(t, ds, ackAdmissionQueue, tag)

	restore := SetColdReadAdmissionForTest(func(queue string, gotTag uint64) {
		// NO acknowledgement: the reader must admit.
	})
	defer restore()

	got, err := ds.GetMessageForDelivery(ackAdmissionQueue, tag)
	require.NoError(t, err)
	require.NotNil(t, got)

	_, _, resident := ds.hot.Get(ackAdmissionQueue, tag)
	require.True(t, resident,
		"a delivery read of an unacknowledged message must admit it into the hot cache")

	count, err := ds.GetQueueMessageCount(ackAdmissionQueue)
	require.NoError(t, err)
	require.Equal(t, 1, count,
		"the admitted message is hot-resident, so it is counted")
}
