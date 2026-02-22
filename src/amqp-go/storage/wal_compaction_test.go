package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

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
