package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSegmentResidentMessagesRecoveredAfterRestart(t *testing.T) {
	tmpDir := t.TempDir()

	engineCfg := interfaces.EngineConfig{
		WALBatchSize:      10,
		WALBatchTimeoutMS: 5,
		WALFileSize:       1024,
		WALChannelBuffer:  5000,
	}

	ds, err := NewDisruptorStorageWithEngineConfig(tmpDir, 5*time.Second, engineCfg)
	require.NoError(t, err)

	queueName := "restart-test-queue"
	_ = ds.getOrCreateQueueRing(queueName)

	numMessages := 20
	for i := 1; i <= numMessages; i++ {
		msg := &protocol.Message{
			Exchange:     "amq.direct",
			RoutingKey:   queueName,
			Body:         []byte(fmt.Sprintf("durable-body-%d", i)),
			DeliveryMode: 2,
			DeliveryTag:  uint64(i),
		}
		if err := ds.StoreMessage(queueName, msg); err != nil {
			t.Fatalf("StoreMessage %d: %v", i, err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	ds.wal.sharedWAL.fileMutex.Lock()
	ds.wal.sharedWAL.rollFile()
	ds.wal.sharedWAL.fileMutex.Unlock()
	ds.wal.sharedWAL.performCheckpoint()

	for i := 1; i <= numMessages; i++ {
		msg, err := ds.segments.Read(queueName, uint64(i))
		require.NoError(t, err, "message %d should be in segments before restart", i)
		assert.Contains(t, string(msg.Body), fmt.Sprintf("durable-body-%d", i))
	}

	ds.Close()

	ds2, err := NewDisruptorStorageWithEngineConfig(tmpDir, 5*time.Second, engineCfg)
	require.NoError(t, err)
	defer ds2.Close()

	recovered, err := ds2.GetRecoverableMessages()
	require.NoError(t, err)

	messages, ok := recovered[queueName]
	require.True(t, ok, "queue %s should have recovered messages", queueName)
	require.Len(t, messages, numMessages, "all %d messages should be recovered from segments", numMessages)

	for _, msg := range messages {
		assert.Equal(t, byte(2), msg.DeliveryMode, "recovered message should be durable")
		assert.Contains(t, string(msg.Body), "durable-body-")
	}
}

func TestSegmentRecovery_TornTailTruncated(t *testing.T) {
	tmpDir := t.TempDir()

	sm, err := NewSegmentManager(tmpDir)
	require.NoError(t, err)

	queueName := "torn-tail-queue"
	msgs := make([]*RecoveryMessage, 0, 5)
	for i := 1; i <= 5; i++ {
		msgs = append(msgs, &RecoveryMessage{
			QueueName: queueName,
			Offset:    uint64(i),
			Message: &protocol.Message{
				Exchange:     "amq.direct",
				RoutingKey:   queueName,
				Body:         []byte(fmt.Sprintf("torn-body-%d", i)),
				DeliveryMode: 2,
				DeliveryTag:  uint64(i),
			},
		})
	}
	require.NoError(t, sm.CheckpointBatch(queueName, msgs))
	require.NoError(t, sm.Close())

	segDir := filepath.Join(sm.dataDir, queueName)
	entries, err := os.ReadDir(segDir)
	require.NoError(t, err)
	var segPath string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), SegmentFileExtension) {
			segPath = filepath.Join(segDir, e.Name())
			break
		}
	}
	require.NotEmpty(t, segPath)

	f, err := os.OpenFile(segPath, os.O_WRONLY|os.O_APPEND, 0644)
	require.NoError(t, err)
	_, err = f.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x05})
	require.NoError(t, err)
	f.Close()

	sm2, err := NewSegmentManager(tmpDir)
	require.NoError(t, err)
	defer sm2.Close()

	recovered, err := sm2.RecoverFromSegments()
	require.NoError(t, err)

	queueMsgs, ok := recovered[queueName]
	require.True(t, ok)
	assert.Len(t, queueMsgs, 5, "torn tail should be truncated, 5 valid messages recovered")
	for _, rm := range queueMsgs {
		assert.Contains(t, string(rm.Message.Body), "torn-body-")
	}
}
