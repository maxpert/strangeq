package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeliveryTagCounter_PersistedAcrossRestart(t *testing.T) {
	tmpDir := t.TempDir()

	store, err := NewPersistentMetadataStore(tmpDir)
	require.NoError(t, err)
	require.NoError(t, store.SaveDeliveryTagCounter(12345))
	require.NoError(t, store.Close())

	store2, err := NewPersistentMetadataStore(tmpDir)
	require.NoError(t, err)
	defer store2.Close()

	tag, err := store2.LoadDeliveryTagCounter()
	require.NoError(t, err)
	assert.Equal(t, uint64(12345), tag)
}

func TestDeliveryTagCounter_DefaultZeroWhenAbsent(t *testing.T) {
	tmpDir := t.TempDir()

	store, err := NewPersistentMetadataStore(tmpDir)
	require.NoError(t, err)
	defer store.Close()

	tag, err := store.LoadDeliveryTagCounter()
	require.NoError(t, err)
	assert.Equal(t, uint64(0), tag, "default should be 0 when no counter file exists")
}

func TestDeliveryTagCounter_OverwriteOnSave(t *testing.T) {
	tmpDir := t.TempDir()

	store, err := NewPersistentMetadataStore(tmpDir)
	require.NoError(t, err)
	defer store.Close()

	require.NoError(t, store.SaveDeliveryTagCounter(100))
	require.NoError(t, store.SaveDeliveryTagCounter(500))

	tag, err := store.LoadDeliveryTagCounter()
	require.NoError(t, err)
	assert.Equal(t, uint64(500), tag, "second save should overwrite first")
}

func TestDeliveryTagCounter_DisruptorStorageDelegates(t *testing.T) {
	tmpDir := t.TempDir()

	ds, err := NewDisruptorStorageWithDataDir(tmpDir)
	require.NoError(t, err)
	defer ds.Close()

	require.NoError(t, ds.SaveDeliveryTagCounter(999))

	tag, err := ds.LoadDeliveryTagCounter()
	require.NoError(t, err)
	assert.Equal(t, uint64(999), tag)
}

func TestDeliveryTagCounter_SurvivesRestartWithSegmentMessages(t *testing.T) {
	tmpDir := t.TempDir()

	engineCfg := interfaces.EngineConfig{
		WALBatchSize:      10,
		WALBatchTimeoutMS: 5,
		WALFileSize:       1024,
		WALChannelBuffer:  5000,
	}

	ds, err := NewDisruptorStorageWithEngineConfig(tmpDir, 5*time.Second, engineCfg)
	require.NoError(t, err)

	queueName := "tag-counter-queue"
	_ = ds.getOrCreateQueueRing(queueName)

	highestTag := uint64(50)
	for i := uint64(1); i <= highestTag; i++ {
		msg := &protocol.Message{
			Exchange:     "amq.direct",
			RoutingKey:   queueName,
			Body:         []byte(fmt.Sprintf("body-%d", i)),
			DeliveryMode: 2,
			DeliveryTag:  i,
		}
		require.NoError(t, ds.StoreMessage(queueName, msg))
	}

	time.Sleep(100 * time.Millisecond)

	ds.wal.sharedWAL.fileMutex.Lock()
	ds.wal.sharedWAL.rollFile()
	ds.wal.sharedWAL.fileMutex.Unlock()
	ds.wal.sharedWAL.performCheckpoint()

	require.NoError(t, ds.SaveDeliveryTagCounter(highestTag))
	ds.Close()

	ds2, err := NewDisruptorStorageWithEngineConfig(tmpDir, 5*time.Second, engineCfg)
	require.NoError(t, err)
	defer ds2.Close()

	persistedTag, err := ds2.LoadDeliveryTagCounter()
	require.NoError(t, err)
	assert.Equal(t, highestTag, persistedTag, "persisted delivery tag counter should survive restart")

	recovered, err := ds2.GetRecoverableMessages()
	require.NoError(t, err)

	messages, ok := recovered[queueName]
	require.True(t, ok)

	var maxRecoveredTag uint64
	for _, msg := range messages {
		if msg.DeliveryTag > maxRecoveredTag {
			maxRecoveredTag = msg.DeliveryTag
		}
	}

	effectiveMax := maxRecoveredTag
	if persistedTag > effectiveMax {
		effectiveMax = persistedTag
	}
	assert.Equal(t, highestTag, effectiveMax, "max(persisted, segments) should equal the highest tag before restart")
}
