package storage

import (
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSegment_DeletedCountIncrement verifies that acknowledging messages
// correctly increments the deletedCount on the segment containing them.
func TestSegment_DeletedCountIncrement(t *testing.T) {
	tmpDir := t.TempDir()

	sm, err := NewSegmentManager(tmpDir)
	require.NoError(t, err)
	defer sm.Close()

	queueName := "test_queue"

	// Write 100 messages to segments
	for i := uint64(1); i <= 100; i++ {
		msg := &protocol.Message{
			Body:        []byte("test message"),
			DeliveryTag: i,
		}
		err := sm.Write(queueName, msg, i)
		require.NoError(t, err)
	}

	// ACK 60 messages
	for i := uint64(1); i <= 60; i++ {
		sm.Acknowledge(queueName, i)
	}

	// Get the queue segments and check deletedCount
	val, ok := sm.queueSegments.Load(queueName)
	require.True(t, ok)
	qs := val.(*QueueSegments)

	// Check current segment (messages should be there since we didn't seal)
	qs.mutex.Lock()
	currentSeg := qs.currentSegment
	qs.mutex.Unlock()

	require.NotNil(t, currentSeg)

	// Batch ACK is asynchronous, and the segment deletedCount is applied after
	// the ack bitmap — poll the value the test asserts, not an earlier signal,
	// or a loaded machine fails here with a partially applied batch.
	require.Eventually(t, func() bool {
		return currentSeg.deletedCount.Load() >= 60
	}, 5*time.Second, 5*time.Millisecond, "batch ACKs should be applied to segment deletedCount")

	assert.Equal(t, uint64(60), currentSeg.deletedCount.Load(),
		"deletedCount should be 60 after ACKing 60 of 100 messages")
	assert.Equal(t, uint64(100), currentSeg.messageCount.Load(),
		"messageCount should remain 100")
}

// TestSegment_CompactionTriggers verifies that compaction triggers when
// deletion ratio exceeds the threshold.
func TestSegment_CompactionTriggers(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := DefaultSegmentConfig()
	cfg.CompactionThreshold = 0.5
	cfg.CompactionInterval = 100 * time.Millisecond // Fast compaction for testing

	sm, err := NewSegmentManagerWithConfig(tmpDir, cfg)
	require.NoError(t, err)
	defer sm.Close()

	queueName := "test_queue"

	// Write 100 messages
	for i := uint64(1); i <= 100; i++ {
		msg := &protocol.Message{
			Body:        []byte("test message for compaction"),
			DeliveryTag: i,
		}
		err := sm.Write(queueName, msg, i)
		require.NoError(t, err)
	}

	// Get queue segments
	val, ok := sm.queueSegments.Load(queueName)
	require.True(t, ok)
	qs := val.(*QueueSegments)

	// Seal the current segment so compaction can operate on it
	qs.mutex.Lock()
	err = qs.sealSegment()
	qs.mutex.Unlock()
	require.NoError(t, err)
	// openNextSegment takes the mutex internally
	err = qs.openNextSegment()
	require.NoError(t, err)

	// ACK 51 messages (>50% threshold)
	for i := uint64(1); i <= 51; i++ {
		sm.Acknowledge(queueName, i)
	}

	// Batch ACK is asynchronous — wait for ACKs to be applied before compaction
	require.Eventually(t, func() bool {
		qs.bitmapMutex.RLock()
		count := int(qs.ackBitmap.GetCardinality())
		qs.bitmapMutex.RUnlock()
		return count >= 51
	}, 1*time.Second, 5*time.Millisecond, "batch ACKs should be applied before compaction")

	// Wait for compaction loop to run
	time.Sleep(500 * time.Millisecond)

	// After compaction, the sealed segment should have fewer messages
	qs.sealedMutex.RLock()
	compacted := false
	for _, seg := range qs.sealedSegments {
		// If compaction happened, deletedCount should be 0 (reset after compaction)
		// and messageCount should be 49 (100 - 51 ACKed)
		if seg.deletedCount.Load() == 0 && seg.messageCount.Load() == 49 {
			compacted = true
		}
	}
	qs.sealedMutex.RUnlock()

	assert.True(t, compacted, "compaction should have triggered and reduced segment to 49 messages")
}

// TestSegment_CompactionPreservesUnacked verifies that after compaction,
// unACKed messages are still readable.
func TestSegment_CompactionPreservesUnacked(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := DefaultSegmentConfig()
	cfg.CompactionThreshold = 0.3 // Low threshold

	sm, err := NewSegmentManagerWithConfig(tmpDir, cfg)
	require.NoError(t, err)
	defer sm.Close()

	queueName := "test_queue"

	// Write 10 messages with distinct bodies
	for i := uint64(1); i <= 10; i++ {
		msg := &protocol.Message{
			Body:        []byte("unacked message body"),
			DeliveryTag: i,
		}
		err := sm.Write(queueName, msg, i)
		require.NoError(t, err)
	}

	// Get queue segments and seal
	val, ok := sm.queueSegments.Load(queueName)
	require.True(t, ok)
	qs := val.(*QueueSegments)

	qs.mutex.Lock()
	err = qs.sealSegment()
	qs.mutex.Unlock()
	require.NoError(t, err)
	err = qs.openNextSegment()
	require.NoError(t, err)

	// ACK first 4 (40% > 30% threshold)
	for i := uint64(1); i <= 4; i++ {
		sm.Acknowledge(queueName, i)
	}

	// Batch ACK is asynchronous — wait for all 4 ACKs to be applied to the bitmap
	require.Eventually(t, func() bool {
		qs.bitmapMutex.RLock()
		count := int(qs.ackBitmap.GetCardinality())
		qs.bitmapMutex.RUnlock()
		return count >= 4
	}, 1*time.Second, 5*time.Millisecond, "batch ACKs should be applied before compaction")

	// Manually trigger compaction
	qs.tryCompaction()

	// Verify unACKed messages (5-10) are still readable
	for i := uint64(5); i <= 10; i++ {
		msg, err := sm.Read(queueName, i)
		require.NoError(t, err, "unACKed message at offset %d should be readable after compaction", i)
		assert.Equal(t, []byte("unacked message body"), msg.Body)
	}

	// Verify ACKed messages (1-4) are NOT readable after compaction
	for i := uint64(1); i <= 4; i++ {
		_, err := sm.Read(queueName, i)
		assert.Error(t, err, "ACKed message at offset %d should not be readable after compaction", i)
	}
}

// TestSegment_RolloverDuringWrite verifies that writing enough messages to
// exceed SegmentSize triggers an automatic segment rollover without deadlock.
// Regression test for the writeMessage→openNextSegment mutex deadlock.
func TestSegment_RolloverDuringWrite(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := DefaultSegmentConfig()
	cfg.SegmentSize = 32 // Tiny: forces rollover after 2 messages (each ~17 bytes)

	sm, err := NewSegmentManagerWithConfig(tmpDir, cfg)
	require.NoError(t, err)
	defer sm.Close()

	done := make(chan error, 1)
	go func() {
		for i := uint64(1); i <= 10; i++ {
			msg := &protocol.Message{Body: []byte("x")}
			if err := sm.Write("rollover_queue", msg, i); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock detected: writes timed out during segment rollover")
	}

	// All messages should be readable after rollover
	for i := uint64(1); i <= 10; i++ {
		_, err := sm.Read("rollover_queue", i)
		assert.NoError(t, err, "message %d should be readable after rollover", i)
	}
}

// TestSegment_DeletedCountNotIncrementedWithoutFix is a regression test
// verifying that the fix correctly increments deletedCount. Before the fix,
// deletedCount was always 0.
func TestSegment_DeletedCountNotIncrementedWithoutFix(t *testing.T) {
	tmpDir := t.TempDir()

	sm, err := NewSegmentManager(tmpDir)
	require.NoError(t, err)
	defer sm.Close()

	queueName := "test_queue"

	// Write 5 messages
	for i := uint64(1); i <= 5; i++ {
		msg := &protocol.Message{
			Body:        []byte("test"),
			DeliveryTag: i,
		}
		err := sm.Write(queueName, msg, i)
		require.NoError(t, err)
	}

	// ACK 1 message
	sm.Acknowledge(queueName, 1)

	// Get the current segment
	val, ok := sm.queueSegments.Load(queueName)
	require.True(t, ok)
	qs := val.(*QueueSegments)

	// Batch ACK is asynchronous (processed by batchAckLoop every ~10ms).
	// Poll for the expected deletedCount value with a timeout.
	require.Eventually(t, func() bool {
		qs.mutex.Lock()
		seg := qs.currentSegment
		qs.mutex.Unlock()
		return seg != nil && seg.deletedCount.Load() == 1
	}, 1*time.Second, 5*time.Millisecond,
		"deletedCount must be incremented when a message in the segment is ACKed")
}
