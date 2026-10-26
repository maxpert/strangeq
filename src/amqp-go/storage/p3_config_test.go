package storage

import (
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

func TestP3_HotCacheSizeFromConfig(t *testing.T) {
	t.Parallel()
	ds, err := NewDisruptorStorageWithEngineConfig(t.TempDir(), interfaces.EngineConfig{
		HotCacheQueueBytes:  128 * 1024,
		HotCacheGlobalBytes: 256 * 1024,
	})
	require.NoError(t, err)
	defer ds.Close()
	require.EqualValues(t, 128*1024, ds.hot.queueBytes)
	require.EqualValues(t, 256*1024, ds.hot.globalBytes)
}

func TestP3_MessageRetrieval(t *testing.T) {
	t.Parallel()
	ds, err := NewDisruptorStorageWithEngineConfig(t.TempDir(), interfaces.EngineConfig{})
	require.NoError(t, err)
	defer ds.Close()
	msg := &protocol.Message{DeliveryTag: 256, Body: []byte("wrap-test"), DeliveryMode: 1}
	require.NoError(t, ds.StoreMessage("test-mask-p3", msg))
	retrieved, err := ds.GetMessage("test-mask-p3", 256)
	require.NoError(t, err)
	require.Equal(t, []byte("wrap-test"), retrieved.Body)
}

func TestP3_DefaultsWhenConfigZero(t *testing.T) {
	t.Parallel()
	ds, err := NewDisruptorStorageWithEngineConfig(t.TempDir(), interfaces.EngineConfig{})
	require.NoError(t, err)
	defer ds.Close()
	require.EqualValues(t, 16*1024*1024, ds.hot.queueBytes)
	require.EqualValues(t, 256*1024*1024, ds.hot.globalBytes)
}
