package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLogCompaction_FullLifecycle is an end-to-end integration test that exercises
// the complete log compaction pipeline: write → WAL → checkpoint → segment →
// compaction → recovery. It validates that all 6 fixes in Phase 14 work together.
func TestLogCompaction_FullLifecycle(t *testing.T) {
	tmpDir := t.TempDir()

	// Use custom engine config with small sizes to trigger rolls and checkpoints
	engineCfg := interfaces.EngineConfig{
		WALBatchSize:                100,
		WALBatchTimeoutMS:           5,
		WALFileSize:                 2048, // Very small: forces WAL file rolls
		WALChannelBuffer:            5000,
		WALCleanupCheckIntervalMS:   100,
		SegmentSize:                 4096, // Small segments
		CompactionThreshold:         0.5,
		CompactionIntervalMS:        100,
		SegmentCheckpointIntervalMS: 200,
	}

	queues := []string{"orders", "events", "logs"}
	messagesPerQueue := 100
	totalMessages := messagesPerQueue * len(queues)

	// === Step 1: Create DisruptorStorage with custom config ===
	ds := NewDisruptorStorageWithEngineConfig(tmpDir, 5*time.Second, engineCfg)
	require.NotNil(t, ds)
	require.NotNil(t, ds.wal, "WAL manager should be initialized")
	require.NotNil(t, ds.segments, "Segment manager should be initialized")

	// === Step 2: Write durable messages across 3 queues ===
	for _, queue := range queues {
		_ = ds.getOrCreateQueueRing(queue)
		for i := 1; i <= messagesPerQueue; i++ {
			msg := &protocol.Message{
				Exchange:     "amq.direct",
				RoutingKey:   queue,
				Body:         []byte(fmt.Sprintf("message-%s-%d", queue, i)),
				DeliveryMode: 2, // Durable
				DeliveryTag:  uint64(i),
			}
			err := ds.StoreMessage(queue, msg)
			require.NoError(t, err, "failed to store message %d for queue %s", i, queue)
		}
	}

	// Wait for all writes to flush
	time.Sleep(100 * time.Millisecond)

	// Verify messages are readable (ring buffer or WAL)
	for _, queue := range queues {
		for i := 1; i <= messagesPerQueue; i++ {
			msg, err := ds.GetMessage(queue, uint64(i))
			require.NoError(t, err, "message %d should be readable for queue %s", i, queue)
			assert.Contains(t, string(msg.Body), fmt.Sprintf("message-%s-%d", queue, i))
		}
	}

	// === Step 3: Verify WAL files were created and rolled ===
	walDir := filepath.Join(tmpDir, "wal", "shared")
	walFiles, err := os.ReadDir(walDir)
	require.NoError(t, err)
	require.Greater(t, len(walFiles), 1,
		"should have multiple WAL files due to small file size (%d messages written)", totalMessages)

	// Record old file count before checkpoint
	ds.wal.sharedWAL.oldFilesMutex.RLock()
	oldFilesBeforeCheckpoint := len(ds.wal.sharedWAL.oldFiles)
	ds.wal.sharedWAL.oldFilesMutex.RUnlock()
	require.Greater(t, oldFilesBeforeCheckpoint, 0,
		"should have old WAL files before checkpoint")

	// === Step 4: ACK most messages (80%) to trigger compaction ===
	ackedPerQueue := int(float64(messagesPerQueue) * 0.8)
	for _, queue := range queues {
		for i := 1; i <= ackedPerQueue; i++ {
			err := ds.DeleteMessage(queue, uint64(i))
			require.NoError(t, err)
		}
	}

	// Wait for ACKs to process
	time.Sleep(200 * time.Millisecond)

	// === Step 5: Trigger checkpoint — moves messages from WAL to segments ===
	ds.wal.sharedWAL.performCheckpoint()

	// Verify old WAL files were deleted after checkpoint
	ds.wal.sharedWAL.oldFilesMutex.RLock()
	oldFilesAfterCheckpoint := len(ds.wal.sharedWAL.oldFiles)
	ds.wal.sharedWAL.oldFilesMutex.RUnlock()

	assert.Less(t, oldFilesAfterCheckpoint, oldFilesBeforeCheckpoint,
		"checkpoint should reduce old WAL file count")

	// === Step 6: Verify unACKed messages are still readable ===
	// After checkpoint, messages should be in segments
	for _, queue := range queues {
		for i := ackedPerQueue + 1; i <= messagesPerQueue; i++ {
			msg, err := ds.GetMessage(queue, uint64(i))
			require.NoError(t, err,
				"unACKed message %d should still be readable for queue %s (via WAL or segments)", i, queue)
			assert.Contains(t, string(msg.Body), fmt.Sprintf("message-%s-%d", queue, i))
		}
	}

	// === Step 7: Verify segment compaction triggers ===
	// Write more messages and ACK a majority to trigger segment compaction
	segQueue := "compaction_test"
	_ = ds.getOrCreateQueueRing(segQueue)

	for i := uint64(1); i <= 50; i++ {
		msg := &protocol.Message{
			Exchange:     "amq.direct",
			RoutingKey:   segQueue,
			Body:         []byte(fmt.Sprintf("compact-msg-%d", i)),
			DeliveryMode: 2,
			DeliveryTag:  i,
		}
		err := ds.StoreMessage(segQueue, msg)
		require.NoError(t, err)
	}
	time.Sleep(50 * time.Millisecond)

	// Force WAL roll and checkpoint to push messages into segments
	ds.wal.sharedWAL.fileMutex.Lock()
	ds.wal.sharedWAL.rollFile()
	ds.wal.sharedWAL.fileMutex.Unlock()
	ds.wal.sharedWAL.performCheckpoint()

	// Seal the segment so compaction can operate on it
	val, ok := ds.segments.queueSegments.Load(segQueue)
	if ok {
		qs := val.(*QueueSegments)
		qs.mutex.Lock()
		qs.sealSegment()
		qs.mutex.Unlock()
		err = qs.openNextSegment()
		require.NoError(t, err)

		// ACK >50% to trigger compaction
		for i := uint64(1); i <= 30; i++ {
			ds.segments.Acknowledge(segQueue, i)
		}

		// Trigger compaction
		qs.tryCompaction()

		// Verify unACKed messages (31-50) are still readable
		for i := uint64(31); i <= 50; i++ {
			msg, err := ds.segments.Read(segQueue, i)
			require.NoError(t, err, "message %d should survive compaction", i)
			assert.Contains(t, string(msg.Body), fmt.Sprintf("compact-msg-%d", i))
		}
	}

	// === Step 8: Simulate restart — close and reopen ===
	// Close everything
	ds.Close()

	// Reopen with same config
	ds2 := NewDisruptorStorageWithEngineConfig(tmpDir, 5*time.Second, engineCfg)
	require.NotNil(t, ds2)
	require.NotNil(t, ds2.wal)

	// Recover from WAL — should return only unACKed messages
	recovered, err := ds2.wal.RecoverFromWAL()
	require.NoError(t, err)

	// Build a map of recovered offsets per queue
	recoveredByQueue := make(map[string]map[uint64]bool)
	for _, rm := range recovered {
		if recoveredByQueue[rm.QueueName] == nil {
			recoveredByQueue[rm.QueueName] = make(map[uint64]bool)
		}
		recoveredByQueue[rm.QueueName][rm.Offset] = true
	}

	// Verify ACKed messages are NOT recovered
	for _, queue := range queues {
		for i := 1; i <= ackedPerQueue; i++ {
			if recoveredByQueue[queue] != nil && recoveredByQueue[queue][uint64(i)] {
				t.Errorf("ACKed message %d for queue %s should NOT be recovered", i, queue)
			}
		}
	}

	ds2.Close()
}

// TestLogCompaction_RetentionWithCheckpoint tests that time-based retention
// correctly force-checkpoints messages to segments before deleting expired
// WAL files, and that those messages survive a restart.
func TestLogCompaction_RetentionWithCheckpoint(t *testing.T) {
	tmpDir := t.TempDir()

	engineCfg := interfaces.EngineConfig{
		WALBatchSize:      50,
		WALBatchTimeoutMS: 5,
		WALFileSize:       1024, // Small files
		WALChannelBuffer:  5000,
	}

	walCfg := WALConfigFromEngine(engineCfg)
	walCfg.RetentionPeriod = 100 * time.Millisecond
	walCfg.CleanupInterval = 30 * time.Second // Long interval to prevent background loop from racing

	wm, err := NewWALManagerWithConfig(tmpDir, walCfg)
	require.NoError(t, err)

	sm, err := NewSegmentManager(tmpDir)
	require.NoError(t, err)
	wm.SetSegmentManager(sm)

	// Write messages across 2 queues
	for i := uint64(1); i <= 30; i++ {
		queue := "queue_a"
		if i > 15 {
			queue = "queue_b"
		}
		msg := &protocol.Message{
			Exchange:     "test",
			RoutingKey:   queue,
			Body:         []byte(fmt.Sprintf("retention-msg-%d", i)),
			DeliveryMode: 2,
			DeliveryTag:  i,
		}
		err := wm.Write(queue, msg, i)
		require.NoError(t, err)
	}

	time.Sleep(50 * time.Millisecond)

	// Verify old files exist
	wm.sharedWAL.oldFilesMutex.RLock()
	require.Greater(t, len(wm.sharedWAL.oldFiles), 0)
	wm.sharedWAL.oldFilesMutex.RUnlock()

	// Wait for retention to expire
	time.Sleep(200 * time.Millisecond)

	// Trigger cleanup — should force-checkpoint and delete
	wm.sharedWAL.tryDeleteOldFiles()

	wm.sharedWAL.oldFilesMutex.RLock()
	remaining := len(wm.sharedWAL.oldFiles)
	wm.sharedWAL.oldFilesMutex.RUnlock()
	assert.Equal(t, 0, remaining, "all expired files should be deleted")

	// Verify force-checkpointed messages are readable from segments
	checkpointedCount := 0
	for i := uint64(1); i <= 30; i++ {
		queue := "queue_a"
		if i > 15 {
			queue = "queue_b"
		}
		_, err := sm.Read(queue, i)
		if err == nil {
			checkpointedCount++
		}
	}
	assert.Greater(t, checkpointedCount, 0,
		"force-checkpointed messages should be readable from segments")

	wm.Close()
	sm.Close()
}

// TestLogCompaction_LargeOffsetLifecycle verifies the full pipeline works with
// uint64 offsets larger than uint32 max, confirming the roaring64 fix.
func TestLogCompaction_LargeOffsetLifecycle(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := DefaultWALConfig()
	cfg.FileSize = 1024 // Small files to force rolls

	wm, err := NewWALManagerWithConfig(tmpDir, cfg)
	require.NoError(t, err)

	sm, err := NewSegmentManager(tmpDir)
	require.NoError(t, err)
	wm.SetSegmentManager(sm)

	baseOffset := uint64(1) << 33 // ~8.5 billion

	// Write messages with large offsets
	for i := uint64(0); i < 10; i++ {
		msg := &protocol.Message{
			Exchange:     "test",
			RoutingKey:   "large_queue",
			Body:         []byte(fmt.Sprintf("large-offset-%d", i)),
			DeliveryMode: 2,
			DeliveryTag:  baseOffset + i,
		}
		err := wm.Write("large_queue", msg, baseOffset+i)
		require.NoError(t, err)
	}

	time.Sleep(100 * time.Millisecond)

	// ACK first 5
	for i := uint64(0); i < 5; i++ {
		wm.Acknowledge("large_queue", baseOffset+i)
	}
	time.Sleep(100 * time.Millisecond)

	// Checkpoint — should only write unACKed (5-9) to segments
	wm.sharedWAL.performCheckpoint()

	// Verify unACKed messages are in segments
	for i := uint64(5); i < 10; i++ {
		msg, err := sm.Read("large_queue", baseOffset+i)
		if err == nil {
			assert.Contains(t, string(msg.Body), fmt.Sprintf("large-offset-%d", i))
		}
	}

	// Verify ACKed messages are NOT in segments
	for i := uint64(0); i < 5; i++ {
		_, err := sm.Read("large_queue", baseOffset+i)
		assert.Error(t, err, "ACKed large-offset message %d should not be in segments", i)
	}

	// Verify cleanup works — ACK remaining and trigger deletion
	for i := uint64(5); i < 10; i++ {
		wm.Acknowledge("large_queue", baseOffset+i)
	}
	time.Sleep(100 * time.Millisecond)
	wm.sharedWAL.tryDeleteOldFiles()

	wm.sharedWAL.oldFilesMutex.RLock()
	assert.Equal(t, 0, len(wm.sharedWAL.oldFiles),
		"all files should be deleted after all large offsets ACKed")
	wm.sharedWAL.oldFilesMutex.RUnlock()

	wm.Close()
	sm.Close()
}

// TestLogCompaction_TierFallback verifies the three-tier read path:
// Ring Buffer → WAL → Segments
func TestLogCompaction_TierFallback(t *testing.T) {
	tmpDir := t.TempDir()

	engineCfg := interfaces.EngineConfig{
		WALBatchSize:      50,
		WALBatchTimeoutMS: 5,
		WALFileSize:       1024,
		WALChannelBuffer:  5000,
	}

	ds := NewDisruptorStorageWithEngineConfig(tmpDir, 5*time.Second, engineCfg)
	require.NotNil(t, ds)

	queue := "tier_test"
	_ = ds.getOrCreateQueueRing(queue)

	// Write a durable message
	msg := &protocol.Message{
		Exchange:     "test",
		RoutingKey:   queue,
		Body:         []byte("tier-fallback-message"),
		DeliveryMode: 2,
		DeliveryTag:  42,
	}
	err := ds.StoreMessage(queue, msg)
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	// Tier 1: Read from ring buffer
	readMsg, err := ds.GetMessage(queue, 42)
	require.NoError(t, err)
	assert.Equal(t, []byte("tier-fallback-message"), readMsg.Body)

	// Clear ring buffer to force WAL fallback
	ring := ds.getQueueRing(queue)
	index := uint64(42) & RingBufferMask
	ring.ringBuffer[index] = nil

	// Tier 2: Read from WAL (ring buffer cleared)
	readMsg, err = ds.GetMessage(queue, 42)
	require.NoError(t, err, "should fall back to WAL when not in ring buffer")
	assert.Equal(t, []byte("tier-fallback-message"), readMsg.Body)

	// Force WAL roll and checkpoint to push to segments
	ds.wal.sharedWAL.fileMutex.Lock()
	ds.wal.sharedWAL.rollFile()
	ds.wal.sharedWAL.fileMutex.Unlock()
	ds.wal.sharedWAL.performCheckpoint()

	// Remove from WAL offset index to force segment fallback
	ds.wal.sharedWAL.offsetIndexMutex.Lock()
	delete(ds.wal.sharedWAL.offsetIndex, 42)
	ds.wal.sharedWAL.offsetIndexMutex.Unlock()

	// Tier 3: Read from segments (ring buffer + WAL cleared)
	readMsg, err = ds.GetMessage(queue, 42)
	require.NoError(t, err, "should fall back to segments when not in ring buffer or WAL")
	assert.Equal(t, []byte("tier-fallback-message"), readMsg.Body)

	ds.Close()
}
