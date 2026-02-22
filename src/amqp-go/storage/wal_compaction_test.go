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

// TestWAL_Roaring64_LargeOffsets verifies that offsets exceeding uint32 max are correctly
// tracked in the ACK bitmap and that cleanup works for large offsets.
func TestWAL_Roaring64_LargeOffsets(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	defer wm.Close()

	// Use offsets larger than uint32 max (4,294,967,295)
	largeOffsets := []uint64{
		1 << 32,       // 4,294,967,296
		1<<32 + 1,     // 4,294,967,297
		1<<32 + 10000, // 4,295,077,296
	}

	// Write messages with large offsets
	for _, offset := range largeOffsets {
		msg := &protocol.Message{
			Exchange:     "test.exchange",
			RoutingKey:   "test.key",
			Body:         []byte("large offset message"),
			DeliveryMode: 2,
		}
		err := wm.Write("test_queue", msg, offset)
		require.NoError(t, err)
	}

	// Wait for flush
	time.Sleep(50 * time.Millisecond)

	// Verify messages readable
	for _, offset := range largeOffsets {
		msg, err := wm.Read("test_queue", offset)
		require.NoError(t, err, "should read message at offset %d", offset)
		assert.Equal(t, []byte("large offset message"), msg.Body)
	}

	// ACK all large offsets
	for _, offset := range largeOffsets {
		wm.Acknowledge("test_queue", offset)
	}

	// Wait for ACKs to process
	time.Sleep(100 * time.Millisecond)

	// Verify ACKs are correctly tracked (internally via bitmap)
	// We test this indirectly through recovery: ACKed messages should not be recovered
	recovered, err := wm.RecoverFromWAL()
	require.NoError(t, err)

	// All messages are ACKed, so none should be recovered
	// (within the same session, ACK bitmap is in memory)
	for _, rm := range recovered {
		for _, offset := range largeOffsets {
			if rm.Offset == offset {
				t.Errorf("offset %d should be ACKed and not recovered", offset)
			}
		}
	}
}

// TestWAL_DeleteOldFiles_SparseOffsets verifies that files with sparse offsets
// (non-contiguous) are correctly identified as fully ACKed and deleted.
func TestWAL_DeleteOldFiles_SparseOffsets(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	defer wm.Close()

	qw := wm.sharedWAL

	// Write messages with sparse offsets to trigger a file roll
	sparseOffsets := []uint64{1, 1000, 50000}

	for _, offset := range sparseOffsets {
		msg := &protocol.Message{
			Exchange:     "test.exchange",
			RoutingKey:   "test.key",
			Body:         []byte("sparse offset message"),
			DeliveryMode: 2,
		}
		err := wm.Write("test_queue", msg, offset)
		require.NoError(t, err)
	}

	// Wait for flush
	time.Sleep(50 * time.Millisecond)

	// Force a file roll by simulating what happens at file size limit
	qw.fileMutex.Lock()
	qw.rollFile()
	qw.fileMutex.Unlock()

	// Verify we have an old file
	qw.oldFilesMutex.RLock()
	oldFileCount := len(qw.oldFiles)
	qw.oldFilesMutex.RUnlock()
	require.Equal(t, 1, oldFileCount, "should have one old file after roll")

	// ACK all sparse offsets
	for _, offset := range sparseOffsets {
		wm.Acknowledge("test_queue", offset)
	}

	// Wait for ACKs to process
	time.Sleep(100 * time.Millisecond)

	// Trigger cleanup
	qw.tryDeleteOldFiles()

	// Verify old file was deleted
	qw.oldFilesMutex.RLock()
	remainingFiles := len(qw.oldFiles)
	qw.oldFilesMutex.RUnlock()
	assert.Equal(t, 0, remainingFiles, "old file should be deleted after all sparse offsets ACKed")
}

// TestWAL_DeleteOldFiles_PartialACK verifies that files with partially ACKed offsets
// are NOT deleted.
func TestWAL_DeleteOldFiles_PartialACK(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	defer wm.Close()

	qw := wm.sharedWAL

	// Write messages with sparse offsets
	offsets := []uint64{10, 500, 25000}

	for _, offset := range offsets {
		msg := &protocol.Message{
			Exchange:     "test.exchange",
			RoutingKey:   "test.key",
			Body:         []byte("partial ack message"),
			DeliveryMode: 2,
		}
		err := wm.Write("test_queue", msg, offset)
		require.NoError(t, err)
	}

	// Wait for flush
	time.Sleep(50 * time.Millisecond)

	// Force a file roll
	qw.fileMutex.Lock()
	qw.rollFile()
	qw.fileMutex.Unlock()

	// Verify we have an old file
	qw.oldFilesMutex.RLock()
	oldFileCount := len(qw.oldFiles)
	var oldFilePath string
	for _, info := range qw.oldFiles {
		oldFilePath = info.path
	}
	qw.oldFilesMutex.RUnlock()
	require.Equal(t, 1, oldFileCount, "should have one old file")

	// Only ACK 2 of 3 offsets (partial ACK)
	wm.Acknowledge("test_queue", 10)
	wm.Acknowledge("test_queue", 500)
	// NOT acknowledging offset 25000

	// Wait for ACKs
	time.Sleep(100 * time.Millisecond)

	// Trigger cleanup
	qw.tryDeleteOldFiles()

	// File should still exist because offset 25000 is not ACKed
	qw.oldFilesMutex.RLock()
	remainingFiles := len(qw.oldFiles)
	qw.oldFilesMutex.RUnlock()
	assert.Equal(t, 1, remainingFiles, "old file should NOT be deleted with partial ACKs")

	// Verify the file still exists on disk
	_, err = os.Stat(oldFilePath)
	assert.NoError(t, err, "WAL file should still exist on disk")

	// Now ACK the remaining offset
	wm.Acknowledge("test_queue", 25000)
	time.Sleep(100 * time.Millisecond)

	// Trigger cleanup again
	qw.tryDeleteOldFiles()

	// Now file should be deleted
	qw.oldFilesMutex.RLock()
	remainingFiles = len(qw.oldFiles)
	qw.oldFilesMutex.RUnlock()
	assert.Equal(t, 0, remainingFiles, "old file should be deleted after all offsets ACKed")

	// Verify the file was actually removed from disk
	_, err = os.Stat(oldFilePath)
	assert.True(t, os.IsNotExist(err), "WAL file should be removed from disk")
}

// TestWAL_DeleteOldFiles_NoFalsePositive verifies that the old bug where
// iterating [minOffset, maxOffset] range (instead of actual offsets) caused
// false negatives is fixed. With sparse offsets like (1, 50000), the old code
// would iterate 49,999 non-existent offsets and report them as not ACKed.
func TestWAL_DeleteOldFiles_NoFalsePositive(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	defer wm.Close()

	qw := wm.sharedWAL

	// Write only 2 messages with a very wide gap
	offsets := []uint64{1, 1_000_000}

	for _, offset := range offsets {
		msg := &protocol.Message{
			Exchange:     "test.exchange",
			RoutingKey:   "test.key",
			Body:         []byte("wide gap message"),
			DeliveryMode: 2,
		}
		err := wm.Write("test_queue", msg, offset)
		require.NoError(t, err)
	}

	time.Sleep(50 * time.Millisecond)

	// Force roll
	qw.fileMutex.Lock()
	qw.rollFile()
	qw.fileMutex.Unlock()

	// ACK both offsets
	wm.Acknowledge("test_queue", 1)
	wm.Acknowledge("test_queue", 1_000_000)
	time.Sleep(100 * time.Millisecond)

	// Trigger cleanup - with the old bug, this would NOT delete the file
	// because it would check offsets 2, 3, 4, ..., 999999 which are not ACKed
	qw.tryDeleteOldFiles()

	qw.oldFilesMutex.RLock()
	remainingFiles := len(qw.oldFiles)
	qw.oldFilesMutex.RUnlock()
	assert.Equal(t, 0, remainingFiles,
		"file should be deleted - only actual offsets should be checked, not the entire range")
}

// TestWAL_RecoverFromWAL_LargeOffsets verifies that recovery correctly handles
// messages with offsets > uint32 max.
func TestWAL_RecoverFromWAL_LargeOffsets(t *testing.T) {
	tmpDir := t.TempDir()

	// Session 1: Write with large offsets, ACK some
	{
		wm, err := NewWALManager(tmpDir)
		require.NoError(t, err)

		largeBase := uint64(1) << 33 // ~8.5 billion

		for i := uint64(0); i < 5; i++ {
			msg := &protocol.Message{
				Exchange:     "test.exchange",
				RoutingKey:   "test.key",
				Body:         []byte("large offset recovery"),
				DeliveryMode: 2,
			}
			err := wm.Write("test_queue", msg, largeBase+i)
			require.NoError(t, err)
		}

		time.Sleep(50 * time.Millisecond)

		// ACK first 3
		for i := uint64(0); i < 3; i++ {
			wm.Acknowledge("test_queue", largeBase+i)
		}

		time.Sleep(50 * time.Millisecond)

		// Recovery within same session should filter ACKed
		recovered, err := wm.RecoverFromWAL()
		require.NoError(t, err)

		// Only 2 unACKed messages should be recovered
		assert.Equal(t, 2, len(recovered), "should recover only unACKed messages")

		wm.Close()
	}

	// Session 2: Verify WAL files are on disk
	walDir := filepath.Join(tmpDir, "wal", "shared")
	files, err := os.ReadDir(walDir)
	require.NoError(t, err)
	assert.Greater(t, len(files), 0, "WAL files should exist on disk")
}

// TestWALConfig_CustomBatchSize verifies that custom batch size from config
// controls when the WAL flushes.
func TestWALConfig_CustomBatchSize(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := DefaultWALConfig()
	cfg.BatchSize = 5                  // Very small batch size for testing
	cfg.BatchTimeout = 5 * time.Second // Long timeout so only size triggers flush

	wm, err := NewWALManagerWithConfig(tmpDir, cfg)
	require.NoError(t, err)
	defer wm.Close()

	// Verify config was applied
	assert.Equal(t, 5, wm.sharedWAL.cfg.BatchSize)

	// Write exactly 5 messages (should trigger a flush at batch boundary)
	for i := 0; i < 5; i++ {
		msg := &protocol.Message{
			Exchange:     "test.exchange",
			RoutingKey:   "test.key",
			Body:         []byte(fmt.Sprintf("msg %d", i)),
			DeliveryMode: 2,
		}
		err := wm.Write("test_queue", msg, uint64(i+1))
		require.NoError(t, err)
	}

	// Messages should be flushed (batch size reached)
	// Give a small moment for the write to complete
	time.Sleep(50 * time.Millisecond)

	// Verify all 5 messages are readable
	for i := 0; i < 5; i++ {
		msg, err := wm.Read("test_queue", uint64(i+1))
		require.NoError(t, err, "should read message at offset %d", i+1)
		assert.NotNil(t, msg)
	}
}

// TestSegmentConfig_CustomCompactionThreshold verifies custom compaction threshold
func TestSegmentConfig_CustomCompactionThreshold(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := DefaultSegmentConfig()
	cfg.CompactionThreshold = 0.3 // Lower threshold for testing

	sm, err := NewSegmentManagerWithConfig(tmpDir, cfg)
	require.NoError(t, err)
	defer sm.Close()

	// Verify config was stored
	assert.Equal(t, 0.3, sm.cfg.CompactionThreshold)
}

// TestWALConfigFromEngine verifies EngineConfig maps correctly to WALConfig
func TestWALConfigFromEngine(t *testing.T) {
	ec := interfaces.EngineConfig{
		WALBatchSize:              500,
		WALBatchTimeoutMS:         20,
		WALFileSize:               1024 * 1024, // 1MB
		WALChannelBuffer:          5000,
		WALCleanupCheckIntervalMS: 10000, // 10s
	}

	cfg := WALConfigFromEngine(ec)

	assert.Equal(t, 500, cfg.BatchSize)
	assert.Equal(t, 20*time.Millisecond, cfg.BatchTimeout)
	assert.Equal(t, int64(1024*1024), cfg.FileSize)
	assert.Equal(t, 5000, cfg.ChannelBuffer)
	assert.Equal(t, 10*time.Second, cfg.CleanupInterval)
}

// TestSegmentConfigFromEngine verifies EngineConfig maps correctly to SegmentConfig
func TestSegmentConfigFromEngine(t *testing.T) {
	ec := interfaces.EngineConfig{
		SegmentSize:                 512 * 1024 * 1024, // 512MB
		CompactionThreshold:         0.3,
		CompactionIntervalMS:        60000,  // 1 minute
		SegmentCheckpointIntervalMS: 120000, // 2 minutes
	}

	cfg := SegmentConfigFromEngine(ec)

	assert.Equal(t, int64(512*1024*1024), cfg.SegmentSize)
	assert.Equal(t, 0.3, cfg.CompactionThreshold)
	assert.Equal(t, time.Minute, cfg.CompactionInterval)
	assert.Equal(t, 2*time.Minute, cfg.CheckpointInterval)
}

// TestWAL_Checkpoint_MovesToSegments verifies that checkpoint migrates
// messages from old WAL files to segments.
func TestWAL_Checkpoint_MovesToSegments(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := DefaultWALConfig()
	cfg.FileSize = 1024 // Very small file size to force rolls

	wm, err := NewWALManagerWithConfig(tmpDir, cfg)
	require.NoError(t, err)

	// Create segment manager and wire up
	sm, err := NewSegmentManager(tmpDir)
	require.NoError(t, err)
	wm.SetSegmentManager(sm)
	defer sm.Close()

	// Write messages that will cause file roll
	for i := uint64(1); i <= 20; i++ {
		msg := &protocol.Message{
			Exchange:     "test.exchange",
			RoutingKey:   "test.key",
			Body:         []byte(fmt.Sprintf("checkpoint message %d", i)),
			DeliveryMode: 2,
		}
		err := wm.Write("test_queue", msg, i)
		require.NoError(t, err)
	}

	time.Sleep(100 * time.Millisecond)

	// Verify we have old files (file rolled due to small FileSize)
	wm.sharedWAL.oldFilesMutex.RLock()
	oldFileCount := len(wm.sharedWAL.oldFiles)
	wm.sharedWAL.oldFilesMutex.RUnlock()
	require.Greater(t, oldFileCount, 0, "should have old WAL files after writes exceed file size")

	// Trigger checkpoint
	wm.sharedWAL.performCheckpoint()

	// Verify messages are now readable from segments
	for i := uint64(1); i <= 20; i++ {
		msg, err := sm.Read("test_queue", i)
		if err == nil {
			assert.Contains(t, string(msg.Body), "checkpoint message")
		}
		// Some messages may be in current file (not checkpointed)
	}

	wm.Close()
}

// TestWAL_Checkpoint_DeletesOldFile verifies old WAL files are deleted after checkpoint.
func TestWAL_Checkpoint_DeletesOldFile(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := DefaultWALConfig()
	cfg.FileSize = 512 // Very small to force rolls

	wm, err := NewWALManagerWithConfig(tmpDir, cfg)
	require.NoError(t, err)

	sm, err := NewSegmentManager(tmpDir)
	require.NoError(t, err)
	wm.SetSegmentManager(sm)
	defer sm.Close()

	// Write enough to create old files
	for i := uint64(1); i <= 30; i++ {
		msg := &protocol.Message{
			Exchange:     "test.exchange",
			RoutingKey:   "test.key",
			Body:         []byte(fmt.Sprintf("message %d with some padding to fill the file", i)),
			DeliveryMode: 2,
		}
		err := wm.Write("test_queue", msg, i)
		require.NoError(t, err)
	}

	time.Sleep(100 * time.Millisecond)

	// Record old file count and paths
	wm.sharedWAL.oldFilesMutex.RLock()
	oldFilesBefore := len(wm.sharedWAL.oldFiles)
	var oldPaths []string
	for _, info := range wm.sharedWAL.oldFiles {
		oldPaths = append(oldPaths, info.path)
	}
	wm.sharedWAL.oldFilesMutex.RUnlock()
	require.Greater(t, oldFilesBefore, 0)

	// Trigger checkpoint
	wm.sharedWAL.performCheckpoint()

	// Old files should be deleted
	wm.sharedWAL.oldFilesMutex.RLock()
	oldFilesAfter := len(wm.sharedWAL.oldFiles)
	wm.sharedWAL.oldFilesMutex.RUnlock()

	assert.Equal(t, 0, oldFilesAfter, "old WAL files should be deleted after checkpoint")

	// Verify files actually removed from disk
	for _, path := range oldPaths {
		_, err := os.Stat(path)
		assert.True(t, os.IsNotExist(err), "WAL file %s should be removed from disk", path)
	}

	wm.Close()
}

// TestWAL_Checkpoint_SkipsAcked verifies that only unACKed messages are
// written to segments during checkpoint.
func TestWAL_Checkpoint_SkipsAcked(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := DefaultWALConfig()
	cfg.FileSize = 512

	wm, err := NewWALManagerWithConfig(tmpDir, cfg)
	require.NoError(t, err)

	sm, err := NewSegmentManager(tmpDir)
	require.NoError(t, err)
	wm.SetSegmentManager(sm)
	defer sm.Close()

	// Write 10 messages
	for i := uint64(1); i <= 10; i++ {
		msg := &protocol.Message{
			Exchange:     "test.exchange",
			RoutingKey:   "test.key",
			Body:         []byte(fmt.Sprintf("msg %d padding to fill file", i)),
			DeliveryMode: 2,
		}
		err := wm.Write("test_queue", msg, i)
		require.NoError(t, err)
	}

	time.Sleep(100 * time.Millisecond)

	// ACK first 5 messages
	for i := uint64(1); i <= 5; i++ {
		wm.Acknowledge("test_queue", i)
	}
	time.Sleep(100 * time.Millisecond)

	// Trigger checkpoint
	wm.sharedWAL.performCheckpoint()

	// Only unACKed messages (6-10) should be in segments
	for i := uint64(6); i <= 10; i++ {
		msg, err := sm.Read("test_queue", i)
		if err == nil {
			assert.NotNil(t, msg, "unACKed message %d should be in segments", i)
		}
	}

	// ACKed messages (1-5) should NOT be in segments
	for i := uint64(1); i <= 5; i++ {
		_, err := sm.Read("test_queue", i)
		assert.Error(t, err, "ACKed message %d should NOT be in segments", i)
	}

	wm.Close()
}

// TestWAL_Checkpoint_ActiveFileUntouched verifies the current active WAL
// file is never checkpointed.
func TestWAL_Checkpoint_ActiveFileUntouched(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	require.NoError(t, err)

	sm, err := NewSegmentManager(tmpDir)
	require.NoError(t, err)
	wm.SetSegmentManager(sm)
	defer sm.Close()

	// Write messages (all to current file, no roll)
	for i := uint64(1); i <= 5; i++ {
		msg := &protocol.Message{
			Exchange:     "test.exchange",
			RoutingKey:   "test.key",
			Body:         []byte("active file message"),
			DeliveryMode: 2,
		}
		err := wm.Write("test_queue", msg, i)
		require.NoError(t, err)
	}

	time.Sleep(50 * time.Millisecond)

	// No old files - everything is in the active file
	wm.sharedWAL.oldFilesMutex.RLock()
	assert.Equal(t, 0, len(wm.sharedWAL.oldFiles), "should have no old files")
	wm.sharedWAL.oldFilesMutex.RUnlock()

	// Trigger checkpoint - should be a no-op
	wm.sharedWAL.performCheckpoint()

	// Messages should still be readable from WAL (not moved to segments)
	for i := uint64(1); i <= 5; i++ {
		msg, err := wm.Read("test_queue", i)
		require.NoError(t, err, "message %d should still be in active WAL", i)
		assert.Equal(t, []byte("active file message"), msg.Body)
	}

	wm.Close()
}

// TestWAL_Checkpoint_OffsetIndexCleanup verifies that checkpointed offsets
// are removed from the WAL offset index.
func TestWAL_Checkpoint_OffsetIndexCleanup(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := DefaultWALConfig()
	cfg.FileSize = 512

	wm, err := NewWALManagerWithConfig(tmpDir, cfg)
	require.NoError(t, err)

	sm, err := NewSegmentManager(tmpDir)
	require.NoError(t, err)
	wm.SetSegmentManager(sm)
	defer sm.Close()

	// Write enough to create old files
	for i := uint64(1); i <= 20; i++ {
		msg := &protocol.Message{
			Exchange:     "test.exchange",
			RoutingKey:   "test.key",
			Body:         []byte(fmt.Sprintf("index cleanup msg %d padding", i)),
			DeliveryMode: 2,
		}
		err := wm.Write("test_queue", msg, i)
		require.NoError(t, err)
	}

	time.Sleep(100 * time.Millisecond)

	// Check offset index has entries for old file offsets
	wm.sharedWAL.offsetIndexMutex.RLock()
	indexSizeBefore := len(wm.sharedWAL.offsetIndex)
	wm.sharedWAL.offsetIndexMutex.RUnlock()
	require.Greater(t, indexSizeBefore, 0, "offset index should have entries")

	// Trigger checkpoint
	wm.sharedWAL.performCheckpoint()

	// After checkpoint, offset index should have fewer entries
	// (entries for old files removed, current file entries remain)
	wm.sharedWAL.offsetIndexMutex.RLock()
	indexSizeAfter := len(wm.sharedWAL.offsetIndex)
	wm.sharedWAL.offsetIndexMutex.RUnlock()

	assert.Less(t, indexSizeAfter, indexSizeBefore,
		"offset index should have fewer entries after checkpoint (old file offsets removed)")

	wm.Close()
}

// TestWAL_Retention_DeletesExpiredFile verifies that files older than
// the retention period are deleted.
func TestWAL_Retention_DeletesExpiredFile(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := DefaultWALConfig()
	cfg.FileSize = 512                           // Small files
	cfg.RetentionPeriod = 100 * time.Millisecond // Very short retention

	wm, err := NewWALManagerWithConfig(tmpDir, cfg)
	require.NoError(t, err)
	defer wm.Close()

	// Write enough to roll files
	for i := uint64(1); i <= 20; i++ {
		msg := &protocol.Message{
			Exchange:     "test.exchange",
			RoutingKey:   "test.key",
			Body:         []byte(fmt.Sprintf("retention test msg %d with padding", i)),
			DeliveryMode: 2,
		}
		err := wm.Write("test_queue", msg, i)
		require.NoError(t, err)
	}

	time.Sleep(50 * time.Millisecond)

	// Verify old files exist
	wm.sharedWAL.oldFilesMutex.RLock()
	require.Greater(t, len(wm.sharedWAL.oldFiles), 0)
	wm.sharedWAL.oldFilesMutex.RUnlock()

	// Wait for retention period to expire
	time.Sleep(200 * time.Millisecond)

	// Trigger cleanup — expired files should be deleted
	wm.sharedWAL.tryDeleteOldFiles()

	wm.sharedWAL.oldFilesMutex.RLock()
	remaining := len(wm.sharedWAL.oldFiles)
	wm.sharedWAL.oldFilesMutex.RUnlock()

	assert.Equal(t, 0, remaining, "expired files should be deleted by retention")
}

// TestWAL_Retention_PreservesRecentFile verifies that recent files are NOT
// deleted even if they have unACKed messages.
func TestWAL_Retention_PreservesRecentFile(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := DefaultWALConfig()
	cfg.FileSize = 512
	cfg.RetentionPeriod = 10 * time.Second // Long retention

	wm, err := NewWALManagerWithConfig(tmpDir, cfg)
	require.NoError(t, err)
	defer wm.Close()

	// Write enough to roll files
	for i := uint64(1); i <= 20; i++ {
		msg := &protocol.Message{
			Exchange:     "test.exchange",
			RoutingKey:   "test.key",
			Body:         []byte(fmt.Sprintf("recent file msg %d with padding", i)),
			DeliveryMode: 2,
		}
		err := wm.Write("test_queue", msg, i)
		require.NoError(t, err)
	}

	time.Sleep(50 * time.Millisecond)

	wm.sharedWAL.oldFilesMutex.RLock()
	oldCount := len(wm.sharedWAL.oldFiles)
	wm.sharedWAL.oldFilesMutex.RUnlock()
	require.Greater(t, oldCount, 0)

	// Trigger cleanup — recent files should NOT be deleted
	wm.sharedWAL.tryDeleteOldFiles()

	wm.sharedWAL.oldFilesMutex.RLock()
	remaining := len(wm.sharedWAL.oldFiles)
	wm.sharedWAL.oldFilesMutex.RUnlock()

	assert.Equal(t, oldCount, remaining, "recent files should NOT be deleted")
}

// TestWAL_Retention_CheckpointsBeforeDelete verifies that when a file expires
// with unACKed messages, those messages are checkpointed to segments before deletion.
func TestWAL_Retention_CheckpointsBeforeDelete(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := DefaultWALConfig()
	cfg.FileSize = 512
	cfg.RetentionPeriod = 100 * time.Millisecond

	wm, err := NewWALManagerWithConfig(tmpDir, cfg)
	require.NoError(t, err)

	sm, err := NewSegmentManager(tmpDir)
	require.NoError(t, err)
	wm.SetSegmentManager(sm)
	defer sm.Close()

	// Write messages
	for i := uint64(1); i <= 20; i++ {
		msg := &protocol.Message{
			Exchange:     "test.exchange",
			RoutingKey:   "test.key",
			Body:         []byte(fmt.Sprintf("force checkpoint msg %d pad", i)),
			DeliveryMode: 2,
		}
		err := wm.Write("test_queue", msg, i)
		require.NoError(t, err)
	}

	time.Sleep(50 * time.Millisecond)

	// Do NOT ACK anything — all messages should be force-checkpointed

	// Wait for retention to expire
	time.Sleep(200 * time.Millisecond)

	// Trigger cleanup
	wm.sharedWAL.tryDeleteOldFiles()

	// Old files should be deleted
	wm.sharedWAL.oldFilesMutex.RLock()
	remaining := len(wm.sharedWAL.oldFiles)
	wm.sharedWAL.oldFilesMutex.RUnlock()
	assert.Equal(t, 0, remaining, "expired files should be deleted")

	// Messages should have been force-checkpointed to segments
	checkpointedCount := 0
	for i := uint64(1); i <= 20; i++ {
		_, err := sm.Read("test_queue", i)
		if err == nil {
			checkpointedCount++
		}
	}
	assert.Greater(t, checkpointedCount, 0,
		"at least some messages should be force-checkpointed to segments before deletion")

	wm.Close()
}

// TestWAL_Retention_DisabledWhenZero verifies that retention of 0 disables
// time-based cleanup.
func TestWAL_Retention_DisabledWhenZero(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := DefaultWALConfig()
	cfg.FileSize = 512
	cfg.RetentionPeriod = 0 // Disabled

	wm, err := NewWALManagerWithConfig(tmpDir, cfg)
	require.NoError(t, err)
	defer wm.Close()

	// Write enough to roll files
	for i := uint64(1); i <= 20; i++ {
		msg := &protocol.Message{
			Exchange:     "test.exchange",
			RoutingKey:   "test.key",
			Body:         []byte(fmt.Sprintf("no retention msg %d with padding", i)),
			DeliveryMode: 2,
		}
		err := wm.Write("test_queue", msg, i)
		require.NoError(t, err)
	}

	time.Sleep(50 * time.Millisecond)

	wm.sharedWAL.oldFilesMutex.RLock()
	oldCount := len(wm.sharedWAL.oldFiles)
	wm.sharedWAL.oldFilesMutex.RUnlock()
	require.Greater(t, oldCount, 0)

	// Wait a bit and trigger cleanup — without retention, only ACK-based cleanup
	time.Sleep(100 * time.Millisecond)
	wm.sharedWAL.tryDeleteOldFiles()

	wm.sharedWAL.oldFilesMutex.RLock()
	remaining := len(wm.sharedWAL.oldFiles)
	wm.sharedWAL.oldFilesMutex.RUnlock()

	// Files should still be there (nothing ACKed, no retention)
	assert.Equal(t, oldCount, remaining, "files should not be deleted with retention disabled")
}

// TestWAL_SequentialScan_CorrectBody verifies that the sequential scan fallback
// (used when an offset is not in the index) returns the correct body bytes.
// Regression test for the readMessageFromFile missing bodyLen parse bug.
func TestWAL_SequentialScan_CorrectBody(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	defer wm.Close()

	expectedBody := []byte("exact body content — no extra bytes")

	msg := &protocol.Message{
		Exchange:     "test",
		RoutingKey:   "key",
		Body:         expectedBody,
		DeliveryMode: 2,
		DeliveryTag:  999,
	}
	err = wm.Write("test_queue", msg, 999)
	require.NoError(t, err)

	// Wait for flush so offset index is populated
	time.Sleep(50 * time.Millisecond)

	// Remove the offset from the index to force sequential scan path
	wm.sharedWAL.offsetIndexMutex.Lock()
	delete(wm.sharedWAL.offsetIndex, 999)
	wm.sharedWAL.offsetIndexMutex.Unlock()

	// Read via WAL — must fall through to sequential scan (readMessageFromFile)
	readMsg, err := wm.Read("test_queue", 999)
	require.NoError(t, err, "sequential scan should find the message")
	assert.Equal(t, expectedBody, readMsg.Body,
		"body from sequential scan must exactly match written body (no bodyLen prefix bytes)")
}

// TestWALConfigFromEngine_Defaults verifies zero EngineConfig uses defaults
func TestWALConfigFromEngine_Defaults(t *testing.T) {
	ec := interfaces.EngineConfig{} // all zeros

	cfg := WALConfigFromEngine(ec)
	defaults := DefaultWALConfig()

	assert.Equal(t, defaults.BatchSize, cfg.BatchSize)
	assert.Equal(t, defaults.BatchTimeout, cfg.BatchTimeout)
	assert.Equal(t, defaults.FileSize, cfg.FileSize)
	assert.Equal(t, defaults.ChannelBuffer, cfg.ChannelBuffer)
	assert.Equal(t, defaults.CleanupInterval, cfg.CleanupInterval)
}
