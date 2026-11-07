package storage

// TASK 2 — the delivery-path classification counters, and the durability guard
// that a message served from the hot cache is STILL backed by its WAL record.
//
// The counters must classify DELIVERIES, not reads in general: exactly one of
// hit / cold-read per GetMessageForDelivery, and the cold path reports the
// positional reads it issued. The hot-hit ratio the benchmark gates on is
// hits / (hits + cold_reads).

import (
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// TestDeliveryCacheStats_ClassifyHitAndCold pins that a hot-cache delivery is
// counted as a hit and a WAL-backed delivery as a cold read, with the cold
// read's positional reads attributed to the queue.
func TestDeliveryCacheStats_ClassifyHitAndCold(t *testing.T) {
	ds, err := NewDisruptorStorageWithEngineConfig(t.TempDir(), interfaces.EngineConfig{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })

	const queue = "stats-q"
	const tag = uint64(1)
	msg := &protocol.Message{
		Exchange:     "amq.direct",
		RoutingKey:   queue,
		Body:         []byte("delivery-stats-body"),
		DeliveryMode: 2,
		DeliveryTag:  tag,
	}
	require.NoError(t, ds.StoreMessage(queue, msg))

	// The publish admitted the message; its delivery is a HIT.
	beforeHits, beforeCold, beforePreads := ds.DeliveryCacheStats()
	got, err := ds.GetMessageForDelivery(queue, tag)
	require.NoError(t, err)
	require.Equal(t, msg.Body, got.Body)
	hits, cold, preads := ds.DeliveryCacheStats()
	require.Equal(t, beforeHits+1, hits, "a served-from-memory delivery is a hit")
	require.Equal(t, beforeCold, cold, "a hit is not a cold read")
	require.Equal(t, beforePreads, preads, "a hit issues no positional reads")

	// Evict it: the next delivery must read the WAL and be classified COLD,
	// with its two positional reads (header + payload) recorded.
	require.True(t, ds.hot.Delete(queue, tag))
	got, err = ds.GetMessageForDelivery(queue, tag)
	require.NoError(t, err)
	require.Equal(t, msg.Body, got.Body)
	hits2, cold2, preads2 := ds.DeliveryCacheStats()
	require.Equal(t, hits, hits2, "a cold read is not a hit")
	require.Equal(t, cold+1, cold2, "a WAL read for delivery is one cold read")
	require.Equal(t, preads+2, preads2, "the WAL parse issues one header + one payload positional read")
}

// TestHotCacheDelivery_MessageStillRecoversFromWALAfterRestart is the
// durability guard for the hot-cache fix: a message delivered from the hot
// cache is NOT acknowledged, so its WAL record must still be the recovery
// source after the process ends and the store is reopened on the same
// directory.
func TestHotCacheDelivery_MessageStillRecoversFromWALAfterRestart(t *testing.T) {
	dir := t.TempDir()
	ds, err := NewDisruptorStorageWithEngineConfig(dir, interfaces.EngineConfig{})
	require.NoError(t, err)

	const queue = "hot-recover-q"
	const tag = uint64(7)
	body := []byte("hot-delivered-but-unacked")
	require.NoError(t, ds.StoreMessage(queue, &protocol.Message{
		Exchange:     "amq.direct",
		RoutingKey:   queue,
		Body:         body,
		DeliveryMode: 2,
		DeliveryTag:  tag,
	}))

	// Deliver it from memory (a hit) and do NOT acknowledge it.
	beforeHits, _, _ := ds.DeliveryCacheStats()
	delivered, err := ds.GetMessageForDelivery(queue, tag)
	require.NoError(t, err)
	require.Equal(t, body, delivered.Body)
	hits, cold, _ := ds.DeliveryCacheStats()
	require.Equal(t, beforeHits+1, hits, "premise: the delivery must be served from the hot cache")
	require.Zero(t, cold, "premise: the delivery must not be a cold read")

	// The process ends. (Close is a clean shutdown; ack state is not persisted,
	// so an unacked message recovers exactly as after a crash.)
	require.NoError(t, ds.Close())

	reopened, err := NewDisruptorStorageWithEngineConfig(dir, interfaces.EngineConfig{})
	require.NoError(t, err)
	defer reopened.Close()

	recovered, err := reopened.GetRecoverableMessages()
	require.NoError(t, err)
	msgs := recovered[queue]
	require.Len(t, msgs, 1, "the hot-cache-delivered, unacked message must replay from the WAL")
	require.Equal(t, body, msgs[0].Body)
}
