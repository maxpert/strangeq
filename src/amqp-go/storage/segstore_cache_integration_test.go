package storage

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newSegstoreCacheTestStorage(t *testing.T, queueBytes, globalBytes int64) *DisruptorStorage {
	t.Helper()
	ds, err := NewDisruptorStorageWithEngineConfig(t.TempDir(), interfaces.EngineConfig{
		HotCacheQueueBytes:  queueBytes,
		HotCacheGlobalBytes: globalBytes,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, ds.Close()) })
	return ds
}

// evictResidentForCacheTest forces a cold read without acknowledging the tag.
// WI3 replaces the ring operation with hotCache.Delete.
func evictResidentForCacheTest(t *testing.T, ds *DisruptorStorage, queue string, tag uint64) {
	t.Helper()
	require.True(t, ds.hot.Delete(queue, tag), "test premise: message starts hot")
}

func TestSegstoreCacheMissReadsThrough(t *testing.T) {
	ds := newSegstoreCacheTestStorage(t, 16*1024*1024, 256*1024*1024)
	const queue, tag = "cache-miss", uint64(1)
	msg := &protocol.Message{DeliveryTag: tag, DeliveryMode: 2, Body: []byte("durable-on-disk")}
	require.NoError(t, ds.StoreMessage(queue, msg))
	evictResidentForCacheTest(t, ds, queue, tag)

	got, err := ds.GetMessage(queue, tag)
	require.NoError(t, err, "a cache miss must read the durable WAL record")
	assert.Equal(t, msg.Body, got.Body)
	count, err := ds.GetQueueMessageCount(queue)
	require.NoError(t, err)
	// The hot cache holds PUBLISHED, hot-resident messages only. A cold read
	// serves the durable record but must not change the reported count: the
	// TTL reaper probes every ready tag on every sweep, and a probe that
	// inflated the count would make the reaper see phantom queued messages.
	assert.Zero(t, count, "a cold read must not change the reported message count")
}

func TestSegstoreTransientAdmissionFailure(t *testing.T) {
	// Below the 512-byte fixed charge, so every message is rejected by the cache.
	ds := newSegstoreCacheTestStorage(t, 1, 1)
	const queue, tag = "transient-spill", uint64(1)
	msg := &protocol.Message{DeliveryTag: tag, DeliveryMode: 1, Body: []byte("must-reach-wal")}
	require.NoError(t, ds.StoreMessage(queue, msg))

	got, err := ds.wal.Read(queue, tag)
	require.NoError(t, err, "failed transient admission must synchronously write the WAL")
	assert.Equal(t, msg.Body, got.Body)
	count, err := ds.GetQueueMessageCount(queue)
	require.NoError(t, err)
	assert.Zero(t, count, "the rejected transient message must remain disk-only")
}

func TestSegstoreOversizedDurableDiskOnly(t *testing.T) {
	ds := newSegstoreCacheTestStorage(t, 1, 1)
	const queue, tag = "oversized-durable", uint64(1)
	msg := &protocol.Message{DeliveryTag: tag, DeliveryMode: 2, Body: []byte("larger-than-cache")}
	require.NoError(t, ds.StoreMessage(queue, msg))

	count, err := ds.GetQueueMessageCount(queue)
	require.NoError(t, err)
	assert.Zero(t, count, "an oversized durable publish must not fail or enter the cache")
	got, err := ds.GetMessage(queue, tag)
	require.NoError(t, err, "the durable disk copy remains readable")
	assert.Equal(t, msg.Body, got.Body)
}

func TestSegstoreAckInvalidatesAfterColdRead(t *testing.T) {
	ds := newSegstoreCacheTestStorage(t, 16*1024*1024, 256*1024*1024)
	const queue, tag = "cold-repopulated", uint64(1)
	msg := &protocol.Message{DeliveryTag: tag, DeliveryMode: 2, Body: []byte("cold-body")}
	require.NoError(t, ds.StoreMessage(queue, msg))
	evictResidentForCacheTest(t, ds, queue, tag)

	_, err := ds.GetMessage(queue, tag)
	require.NoError(t, err)
	count, err := ds.GetQueueMessageCount(queue)
	require.NoError(t, err)
	require.Zero(t, count, "a cold read must not repopulate the hot cache")

	require.NoError(t, ds.DeleteMessage(queue, tag))
	count, err = ds.GetQueueMessageCount(queue)
	require.NoError(t, err)
	assert.Zero(t, count, "the ack must leave the count at zero")
	// The guard: an acknowledgement that lands AFTER the tier read must make
	// the record unservable. The re-check consults the WAL directly, so it
	// holds whether or not the read was cached.
	_, err = ds.GetMessage(queue, tag)
	assert.ErrorIs(t, err, interfaces.ErrMessageNotFound)
}

func TestSegstoreAsyncFailureNeverCaches(t *testing.T) {
	restore := SetWALGroupCommitFsyncForTest(func(*os.File) error {
		return fmt.Errorf("injected fsync failure")
	})
	defer restore()

	ds := newSegstoreCacheTestStorage(t, 16*1024*1024, 256*1024*1024)
	const queue, tag = "async-failure", uint64(1)
	msg := &protocol.Message{DeliveryTag: tag, DeliveryMode: 2, Body: []byte("not-durable")}
	done := make(chan error, 1)
	require.NoError(t, ds.StoreMessageAsync(queue, msg, func(err error) { done <- err }))

	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("async completion did not fire")
	}
	count, err := ds.GetQueueMessageCount(queue)
	require.NoError(t, err)
	assert.Zero(t, count, "a failed async durable write must never populate the hot cache")
	_, err = ds.GetMessage(queue, tag)
	assert.Error(t, err)
}
