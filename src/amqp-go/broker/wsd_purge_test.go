package broker

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPurge_PreservesUnacked_Broker is the broker twin of T4.1: purge removes
// READY messages only — in-flight (delivered-unacked) messages keep their
// bodies and requeue intact. Pre-fix mechanism: storage.PurgeQueue wiped every
// ring slot in the band (unacked included) and reset inflight to 0, so a later
// requeue of an unacked delivery was a gap (lost) and AckAdvance desynced.
func TestPurge_PreservesUnacked_Broker(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	mustDeclareQueue(t, b, "pq")
	qs := b.getOrCreateQueueState("pq")

	for i := 0; i < 5; i++ {
		publishBody(b, "pq", fmt.Sprintf("m%d", i))
	}

	// Deliver 2 with manual-ack basic.get (in-flight, unacked).
	m1, tag1, _, err := b.GetMessageForGet("pq", false)
	require.NoError(t, err)
	require.NotNil(t, m1)
	m2, tag2, _, err := b.GetMessageForGet("pq", false)
	require.NoError(t, err)
	require.NotNil(t, m2)
	require.Equal(t, int64(2), qs.InflightCount())
	require.Equal(t, int64(3), qs.WaitingCount())

	// Purge: exactly the 3 READY messages are removed.
	purged, err := b.PurgeQueue("pq")
	require.NoError(t, err)
	assert.Equal(t, 3, purged, "purge-ok message-count = ready messages removed")
	assert.Equal(t, int64(0), qs.WaitingCount())
	assert.Equal(t, int64(2), qs.InflightCount(), "inflight is untouched by purge")

	// Ack #1 settles cleanly.
	require.NoError(t, b.AcknowledgeGetDelivery(tag1))
	assert.Equal(t, int64(1), qs.InflightCount())

	// Reject+requeue #2: its body survived the purge, so it is redelivered.
	require.NoError(t, b.RejectGetDelivery(tag2, true))
	assert.Equal(t, int64(1), qs.WaitingCount())
	got, _, _, err := b.GetMessageForGet("pq", true)
	require.NoError(t, err)
	require.NotNil(t, got, "the requeued unacked message must still have its body")
	assert.Equal(t, "m1", string(got.Body))
	assert.True(t, got.Redelivered)
}

// TestPurge_InflightCounterIntact (T4.2): the purge path never zeroes
// inflight; acking every in-flight delivery after a purge lands inflight at
// exactly 0 (not clamped from negative).
func TestPurge_InflightCounterIntact(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	declareMaxLen(t, b, "pq2", nil)
	for i := 0; i < 4; i++ {
		publishBody(b, "pq2", fmt.Sprintf("m%d", i))
	}
	_, tag1, _, err := b.GetMessageForGet("pq2", false)
	require.NoError(t, err)
	_, tag2, _, err := b.GetMessageForGet("pq2", false)
	require.NoError(t, err)

	_, err = b.PurgeQueue("pq2")
	require.NoError(t, err)

	ready, inflight, _ := b.QueueCounters("pq2")
	assert.Equal(t, int64(0), ready)
	assert.Equal(t, int64(2), inflight, "purge leaves inflight intact")

	require.NoError(t, b.AcknowledgeGetDelivery(tag1))
	require.NoError(t, b.AcknowledgeGetDelivery(tag2))
	ready, inflight, _ = b.QueueCounters("pq2")
	assert.Equal(t, int64(0), ready)
	assert.Equal(t, int64(0), inflight, "inflight lands at exactly 0 after acking both")
}

// TestPurge_RequeueRingReadyRemoved: ready messages sitting in the REQUEUE
// ring (nacked back) are purged as ready (claim-mechanics (a)).
func TestPurge_RequeueRingReadyRemoved(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	qs := declareMaxLen(t, b, "prq", nil)
	for i := 0; i < 3; i++ {
		publishBody(b, "prq", fmt.Sprintf("m%d", i))
	}
	// Deliver all 3 manual-ack, then nack-requeue 2 of them → 2 ready in the
	// requeue ring, 1 still in flight.
	_, tag1, _, _ := b.GetMessageForGet("prq", false)
	_, tag2, _, _ := b.GetMessageForGet("prq", false)
	_, tag3, _, _ := b.GetMessageForGet("prq", false)
	require.NoError(t, b.RejectGetDelivery(tag1, true))
	require.NoError(t, b.RejectGetDelivery(tag2, true))
	require.Equal(t, int64(2), qs.WaitingCount())
	require.Equal(t, int64(2), qs.RequeueDepth())

	purged, err := b.PurgeQueue("prq")
	require.NoError(t, err)
	assert.Equal(t, 2, purged, "both requeued ready messages are purged")
	assert.Equal(t, int64(0), qs.WaitingCount())
	assert.Equal(t, int64(0), qs.RequeueDepth())
	assert.Equal(t, int64(1), qs.InflightCount())

	// The in-flight one requeues with its body intact.
	require.NoError(t, b.RejectGetDelivery(tag3, true))
	got, _, _, err := b.GetMessageForGet("prq", true)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "m2", string(got.Body))
}

// TestPurge_ConcurrentWithConsumersAndReaper_NoDoubleCount (T4.4, -race):
// purges racing consumers and a TTL reaper never double-count depth. Final
// invariant: ready + inflight == messages drainable, and ready >= 0 always.
func TestPurge_ConcurrentWithConsumersAndReaper_NoDoubleCount(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	qs := declareMaxLen(t, b, "raceq", map[string]interface{}{"x-message-ttl": int32(60000)})

	const publishers = 4
	const perPublisher = 50
	var wg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := 0; i < perPublisher; i++ {
				publishBody(b, "raceq", fmt.Sprintf("p%d-m%d", p, i))
			}
		}(p)
	}

	// Concurrent purger, stopped once publishers and the consumer finish.
	stopPurge := make(chan struct{})
	purgerDone := make(chan struct{})
	go func() {
		defer close(purgerDone)
		for {
			select {
			case <-stopPurge:
				return
			default:
				_, _ = b.PurgeQueue("raceq")
			}
		}
	}()

	// Concurrent no-ack consumer draining.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < publishers*perPublisher/2; i++ {
			_, _, _, _ = b.GetMessageForGet("raceq", true)
		}
	}()

	wg.Wait()
	close(stopPurge)
	<-purgerDone
	_, _ = b.PurgeQueue("raceq")

	// Invariant at quiesce: nothing negative, nothing stranded.
	assert.GreaterOrEqual(t, qs.WaitingCount(), int64(0), "ready never goes negative")
	assert.GreaterOrEqual(t, qs.InflightCount(), int64(0))
	drained := drainBodies(t, b, "raceq")
	ready, inflight, _ := b.QueueCounters("raceq")
	assert.Equal(t, int64(len(drained)), ready, "ready == drainable after quiesce")
	assert.Equal(t, int64(0), inflight)
	assert.Empty(t, drained, "final purge removed everything left")
}

// TestPurge_NonexistentQueue: purging a queue whose state is closed (deleted)
// reports the queue as not found (interfaces.ErrQueueNotFound semantics via
// the broker's typed error).
func TestPurge_NonexistentQueue(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	mustDeclareQueue(t, b, "gone")
	_, err := b.DeleteQueue("gone", false, false)
	require.NoError(t, err)

	_, perr := b.PurgeQueue("gone")
	require.Error(t, perr)
	assert.Contains(t, perr.Error(), "no queue 'gone' in vhost '/'")
}
