package storage

// THE WRITE-THROUGH ADMISSION IS ATTRIBUTABLE TO A CONSUMER DELIVERY.
//
// The hot cache's budget is spent on the working set the consumers are
// actually reading. A read that IS a delivery proves the message is in that
// working set, so it admits what it read (write-through). A read that is NOT
// a delivery — a probe, a peek, an existence check — proves nothing about
// the working set, and admitting on it would:
//
//   - fill the cache with messages nobody asked for (the TTL reaper probes
//     every ready tag on every sweep), and
//   - inflate the queue's REPORTED MESSAGE COUNT, which the broker reports
//     for the queue and the reaper itself reads.
//
// So there are TWO reads, and they differ in exactly one thing: whether the
// message they read is admitted.
//
//	DisruptorStorage.GetMessageForDelivery  — a DELIVERY: admits.
//	DisruptorStorage.GetMessage             — a PEEK: does not admit.
//
// The F8 contract (storage/cold_read_count_test.go) pins the peek's half;
// this file pins the delivery's half and the budget's behaviour.

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

// TestDeliveryRead_AdmitsAColdMessage is the write-through contract: a
// CONSUMER DELIVERY of a cold (WAL-only) message admits it, so the message
// is RAM-resident again and the queue's reported count reflects it.
func TestDeliveryRead_AdmitsAColdMessage(t *testing.T) {
	ds := newColdReadStorage(t, t.TempDir())

	const queue = "delivery-admit-q"
	const tag = uint64(21)
	publishOneColdCandidate(t, ds, queue, tag)

	before, err := ds.GetQueueMessageCount(queue)
	require.NoError(t, err)
	require.Equal(t, 0, before, "premise: the message is cold (WAL-only)")

	got, err := ds.GetMessageForDelivery(queue, tag)
	require.NoError(t, err, "the delivery read must serve the durable message")
	require.NotNil(t, got)
	require.Equal(t, []byte("cold-read-body"), got.Body)

	after, err := ds.GetQueueMessageCount(queue)
	require.NoError(t, err)
	require.Equal(t, before+1, after,
		"the delivery read admitted the message: it is RAM-resident again")

	list, err := ds.GetQueueMessages(queue)
	require.NoError(t, err)
	require.Len(t, list, 1, "the admitted message is hot-resident")

	_, _, resident := ds.hot.Get(queue, tag)
	require.True(t, resident, "the delivery read must admit what it read")
}

// TestPeekRead_DoesNotAdmitAColdMessage is the mirror: the SAME cold read
// through the non-admitting read leaves the count and the resident set
// exactly as they were.
func TestPeekRead_DoesNotAdmitAColdMessage(t *testing.T) {
	ds := newColdReadStorage(t, t.TempDir())

	const queue = "peek-no-admit-q"
	const tag = uint64(22)
	publishOneColdCandidate(t, ds, queue, tag)

	before, err := ds.GetQueueMessageCount(queue)
	require.NoError(t, err)
	require.Equal(t, 0, before, "premise: the message is cold (WAL-only)")

	got, err := ds.GetMessage(queue, tag)
	require.NoError(t, err, "the peek must serve the durable message")
	require.NotNil(t, got)

	after, err := ds.GetQueueMessageCount(queue)
	require.NoError(t, err)
	require.Equal(t, before, after,
		"a peek must not change the reported message count")
	_, _, resident := ds.hot.GetResident(queue, tag)
	require.False(t, resident, "a peek must not admit what it read")
}

// TestColdRead_TooBigToAdmitEvictsNothing: a cold read of a message that does
// NOT fit the budget admits nothing — and it must not displace a resident
// message to make room for itself. The admission is best-effort and
// NON-EVICTING.
func TestColdRead_TooBigToAdmitEvictsNothing(t *testing.T) {
	ds, err := NewDisruptorStorageWithEngineConfig(t.TempDir(), interfaces.EngineConfig{
		// A budget one small message fits into, and the big one does not.
		HotCacheQueueBytes: 4096,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	const queue = "too-big-q"
	const smallTag = uint64(31)
	const bigTag = uint64(32)

	require.NoError(t, ds.StoreQueue(&protocol.Queue{Name: queue, Durable: true}))

	// A resident message the read must NOT displace.
	require.NoError(t, ds.StoreMessage(queue, &protocol.Message{
		RoutingKey: queue, Body: []byte("resident"), DeliveryMode: 2, DeliveryTag: smallTag,
	}))
	_, _, resident := ds.hot.Get(queue, smallTag)
	require.True(t, resident, "premise: the small message must be resident")

	// A message that does NOT fit the budget.
	big := make([]byte, 8192)
	require.NoError(t, ds.StoreMessage(queue, &protocol.Message{
		RoutingKey: queue, Body: big, DeliveryMode: 2, DeliveryTag: bigTag,
	}))

	// The read of the big message is COLD (it was never admitted).
	_, _, bigResident := ds.hot.GetResident(queue, bigTag)
	require.False(t, bigResident, "premise: the big message must not be resident")

	// The delivery read: it serves the message, and the admission fails.
	got, err := ds.GetMessageForDelivery(queue, bigTag)
	require.NoError(t, err, "the delivery read must still serve the durable message")
	require.NotNil(t, got)

	_, _, stillBig := ds.hot.GetResident(queue, bigTag)
	require.False(t, stillBig,
		"a message that does not fit the budget must not be admitted")
	_, _, stillSmall := ds.hot.Get(queue, smallTag)
	require.True(t, stillSmall,
		"the failed admission must not displace the resident message")
}
