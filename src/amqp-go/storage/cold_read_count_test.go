package storage

// F8: a COLD READ MUST NOT MUTATE THE HOT CACHE, and therefore must not
// change the queue's reported message count.
//
// The hot cache holds PUBLISHED messages. GetQueueMessages /
// GetQueueMessageCount are hot-resident operations over exactly that set
// (WI3: "Range/count methods remain hot-resident operations AS TODAY" — the
// legacy ring only ever held published records).
//
// GetMessage used to admit the message it had just read from the WAL or the
// segment tier into the hot cache. That turned a READ into a PUBLISH as far as
// the count was concerned: the TTL reaper probes every ready tag on every
// sweep (broker/queue_reaper.go, reapTTLSweep -> storage.GetMessage), so each
// sweep inserted durable records into the cache and inflated the count the
// broker then reported for the queue. The admitted entry was also an eviction
// victim (backed=true), so a probe could displace a published message that was
// still resident.
//
// These tests pin the contract:
//   - a cold read serves the message (it is still readable),
//   - a cold read does NOT admit it,
//   - the count and the message list are UNCHANGED by the read,
//   - an acknowledged cold read is neither served nor admitted,
//   - the cold settle path still works without the admission.

import (
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// newColdReadStorage builds a storage whose hot cache can hold anything, so
// the only reason a read is COLD is that the caller evicted the message (or
// the publish never fit).
func newColdReadStorage(t *testing.T, dataDir string) *DisruptorStorage {
	t.Helper()
	ds, err := NewDisruptorStorageWithEngineConfig(dataDir, interfaces.EngineConfig{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })
	return ds
}

// publishOneColdCandidate publishes one durable message and evicts it from the
// hot cache, so the next read of it is answered by the WAL (a COLD read).
func publishOneColdCandidate(t *testing.T, ds *DisruptorStorage, queue string, tag uint64) *protocol.Message {
	t.Helper()
	msg := &protocol.Message{
		Exchange:     "amq.direct",
		RoutingKey:   queue,
		Body:         []byte("cold-read-body"),
		DeliveryMode: 2,
		DeliveryTag:  tag,
	}
	require.NoError(t, ds.StoreMessage(queue, msg))
	require.NoError(t, ds.StoreQueue(&protocol.Queue{Name: queue, Durable: true}))

	// The publish itself is hot-resident: the count reports published messages.
	count, err := ds.GetQueueMessageCount(queue)
	require.NoError(t, err)
	require.Equal(t, 1, count, "the published message must be counted while it is hot-resident")

	// Evict it: the message stays in the WAL, and the read below is COLD.
	require.True(t, ds.hot.Delete(queue, tag))
	_, _, resident := ds.hot.Get(queue, tag)
	require.False(t, resident, "premise: the message must NOT be hot-resident before the cold read")
	return msg
}

// TestColdRead_DoesNotChangeQueueMessageCount is the F8 regression test: a
// cold (WAL-only) read must leave the queue's reported count exactly as it
// was.
func TestColdRead_DoesNotChangeQueueMessageCount(t *testing.T) {
	ds := newColdReadStorage(t, t.TempDir())

	const queue = "cold-q"
	const tag = uint64(7)
	msg := publishOneColdCandidate(t, ds, queue, tag)

	before, err := ds.GetQueueMessageCount(queue)
	require.NoError(t, err)
	require.Equal(t, 0, before, "the evicted message is not hot-resident, so the count is 0")

	// THE COLD READ. It must succeed — the message is durable and the WAL has
	// it — and it must not touch the hot cache.
	got, err := ds.GetMessage(queue, tag)
	require.NoError(t, err, "the cold read must still serve the durable message")
	require.NotNil(t, got)
	require.Equal(t, msg.Body, got.Body)

	after, err := ds.GetQueueMessageCount(queue)
	require.NoError(t, err)
	require.Equal(t, before, after,
		"a cold read must not change the reported message count: the hot cache holds published messages only")

	list, err := ds.GetQueueMessages(queue)
	require.NoError(t, err)
	require.Empty(t, list,
		"a cold read must not add the message to the hot-resident message list")

	_, _, resident := ds.hot.Get(queue, tag)
	require.False(t, resident,
		"a cold read must not admit the message into the hot cache")
}

// TestColdRead_DoesNotChangeQueueMessageCount_SpilledPublish covers the
// SPILLED case: the publish never fitted in the hot cache at all (so the
// message was never a hot-resident published record), and the read of it is
// cold. The count must stay 0 across the read.
func TestColdRead_DoesNotChangeQueueMessageCount_SpilledPublish(t *testing.T) {
	ds, err := NewDisruptorStorageWithEngineConfig(t.TempDir(), interfaces.EngineConfig{
		// A budget no message can fit into: every publish is WAL-only.
		HotCacheQueueBytes: 1,
	})
	require.NoError(t, err)
	defer ds.Close()

	const queue = "spilled-q"
	const tag = uint64(3)
	require.NoError(t, ds.StoreQueue(&protocol.Queue{Name: queue, Durable: true}))
	require.NoError(t, ds.StoreMessage(queue, &protocol.Message{
		RoutingKey:   queue,
		Body:         []byte("spilled-body"),
		DeliveryMode: 2,
		DeliveryTag:  tag,
	}))

	_, _, resident := ds.hot.Get(queue, tag)
	require.False(t, resident, "premise: the publish must not have been admitted")

	before, err := ds.GetQueueMessageCount(queue)
	require.NoError(t, err)
	require.Equal(t, 0, before)

	got, err := ds.GetMessage(queue, tag)
	require.NoError(t, err, "the spilled message must still be readable from the WAL")
	require.Equal(t, []byte("spilled-body"), got.Body)

	after, err := ds.GetQueueMessageCount(queue)
	require.NoError(t, err)
	require.Equal(t, before, after,
		"reading a spilled message must not change the reported message count")
}

// TestColdRead_ReaperProbeDoesNotChangeQueueMessageCount models the reaper's
// TTL probe: it walks the ready tags and reads each one. Probing a tag that
// is not there (a gap) and probing a tag that IS there must both leave the
// count alone.
func TestColdRead_ReaperProbeDoesNotChangeQueueMessageCount(t *testing.T) {
	ds := newColdReadStorage(t, t.TempDir())

	const queue = "probe-q"
	const tag = uint64(11)
	msg := publishOneColdCandidate(t, ds, queue, tag)

	// Probe a gap tag: nothing is published for it.
	_, err := ds.GetMessage(queue, tag+1000)
	require.Error(t, err, "a gap tag must not be served")

	// Probe the real tag: it is cold, so the probe reads it from the WAL.
	got, err := ds.GetMessage(queue, tag)
	require.NoError(t, err)
	require.Equal(t, msg.Body, got.Body)

	count, err := ds.GetQueueMessageCount(queue)
	require.NoError(t, err)
	require.Equal(t, 0, count,
		"the reaper's probe must not report the probed message as a queued message")
}

// TestColdRead_AcknowledgedMessageIsNeitherServedNorAdmitted pins the
// acknowledgement re-check that survived the removal of the admission: a tag
// acknowledged in the WAL is not served, and the failed read does not admit
// anything either.
func TestColdRead_AcknowledgedMessageIsNeitherServedNorAdmitted(t *testing.T) {
	ds := newColdReadStorage(t, t.TempDir())

	const queue = "acked-q"
	const tag = uint64(5)
	publishOneColdCandidate(t, ds, queue, tag)

	// Acknowledge through the shipped entry point. The WAL's read gate is
	// updated synchronously, so the gate is live from here on.
	ds.wal.Acknowledge(queue, tag)
	require.True(t, ds.wal.IsAcknowledged(queue, tag), "premise: the tag must be acknowledged")

	got, err := ds.GetMessage(queue, tag)
	require.ErrorIs(t, err, interfaces.ErrMessageNotFound,
		"an acknowledged tag must not be served")
	require.Nil(t, got)

	count, err := ds.GetQueueMessageCount(queue)
	require.NoError(t, err)
	require.Equal(t, 0, count, "the refused read must not admit anything")

	_, _, resident := ds.hot.Get(queue, tag)
	require.False(t, resident, "the refused read must not admit the acknowledged message")
}

// TestColdRead_ColdMessageStillSettles guards the removal of the admission
// against the settle path: DeleteMessageIfPresentDetailed reaches the cold
// tier THROUGH GetMessage, and it must still win the removal of a
// WAL-only message.
func TestColdRead_ColdMessageStillSettles(t *testing.T) {
	ds := newColdReadStorage(t, t.TempDir())

	const queue = "settle-q"
	const tag = uint64(9)
	publishOneColdCandidate(t, ds, queue, tag)

	removed, resident, err := ds.DeleteMessageIfPresentDetailed(queue, tag)
	require.NoError(t, err)
	require.True(t, removed, "the cold removal must win: the message was present in the WAL")
	require.False(t, resident, "the message was not hot-resident")

	count, err := ds.GetQueueMessageCount(queue)
	require.NoError(t, err)
	require.Equal(t, 0, count, "settling a cold message must not change the count")

	// And it is gone: the WAL read gate now refuses it.
	_, err = ds.GetMessage(queue, tag)
	require.ErrorIs(t, err, interfaces.ErrMessageNotFound)
}
