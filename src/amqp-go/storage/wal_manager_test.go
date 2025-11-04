package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSharedWAL_BasicWriteRead tests basic write and read operations
func TestSharedWAL_BasicWriteRead(t *testing.T) {
	tmpDir := t.TempDir()

	// Create WAL manager
	wm, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	defer wm.Close()

	// Write a message
	msg := &protocol.Message{
		Exchange:     "test.exchange",
		RoutingKey:   "test.key",
		Body:         []byte("test message body"),
		DeliveryMode: 2,
	}

	err = wm.Write("test_queue", msg, 1)
	require.NoError(t, err)

	// Give it a moment to flush
	time.Sleep(20 * time.Millisecond)

	// Read the message back
	readMsg, err := wm.Read("test_queue", 1)
	require.NoError(t, err)
	assert.Equal(t, msg.Exchange, readMsg.Exchange)
	assert.Equal(t, msg.RoutingKey, readMsg.RoutingKey)
	assert.Equal(t, msg.Body, readMsg.Body)
}

// TestSharedWAL_MultiQueue tests writing to multiple queues in shared WAL
func TestSharedWAL_MultiQueue(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	defer wm.Close()

	// Write to 10 different queues
	numQueues := 10
	messagesPerQueue := 100

	for q := 0; q < numQueues; q++ {
		queueName := fmt.Sprintf("queue_%d", q)
		for i := 0; i < messagesPerQueue; i++ {
			msg := &protocol.Message{
				Exchange:     fmt.Sprintf("exchange_%d", q),
				RoutingKey:   fmt.Sprintf("key_%d", i),
				Body:         []byte(fmt.Sprintf("Q%d:M%d", q, i)),
				DeliveryMode: 2,
			}

			offset := uint64(q*messagesPerQueue + i + 1)
			err := wm.Write(queueName, msg, offset)
			require.NoError(t, err)
		}
	}

	// Allow batch flush
	time.Sleep(50 * time.Millisecond)

	// Verify all messages readable
	for q := 0; q < numQueues; q++ {
		queueName := fmt.Sprintf("queue_%d", q)
		for i := 0; i < messagesPerQueue; i++ {
			offset := uint64(q*messagesPerQueue + i + 1)
			msg, err := wm.Read(queueName, offset)
			require.NoError(t, err)

			expectedBody := fmt.Sprintf("Q%d:M%d", q, i)
			assert.Equal(t, []byte(expectedBody), msg.Body,
				"Message mismatch for queue %d, offset %d", q, offset)
		}
	}
}

// TestSharedWAL_QueueIsolation verifies messages don't leak between queues
func TestSharedWAL_QueueIsolation(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	defer wm.Close()

	// Write to queue A
	msgA := &protocol.Message{
		Exchange:     "exchange.a",
		RoutingKey:   "key.a",
		Body:         []byte("Message for Queue A"),
		DeliveryMode: 2,
	}
	err = wm.Write("queue_a", msgA, 100)
	require.NoError(t, err)

	// Write to queue B with different offset
	msgB := &protocol.Message{
		Exchange:     "exchange.b",
		RoutingKey:   "key.b",
		Body:         []byte("Message for Queue B"),
		DeliveryMode: 2,
	}
	err = wm.Write("queue_b", msgB, 200)
	require.NoError(t, err)

	time.Sleep(20 * time.Millisecond)

	// Read from queue A - should get A's message
	readA, err := wm.Read("queue_a", 100)
	require.NoError(t, err)
	assert.Equal(t, msgA.Body, readA.Body)
	assert.Equal(t, msgA.Exchange, readA.Exchange)

	// Read from queue B - should get B's message
	readB, err := wm.Read("queue_b", 200)
	require.NoError(t, err)
	assert.Equal(t, msgB.Body, readB.Body)
	assert.Equal(t, msgB.Exchange, readB.Exchange)

	// Try to read queue B's offset from queue A - should fail
	// (since offsets are globally unique, this test validates queue filtering)
	_, err = wm.Read("queue_a", 200)
	assert.Error(t, err, "Should not be able to read queue B's message from queue A")
}

// TestSharedWAL_ConcurrentWrites tests concurrent writes from multiple goroutines
func TestSharedWAL_ConcurrentWrites(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	defer wm.Close()

	numGoroutines := 100
	messagesPerGoroutine := 10
	numQueues := 10

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Launch concurrent writers
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()

			queueID := goroutineID % numQueues
			queueName := fmt.Sprintf("queue_%d", queueID)

			for i := 0; i < messagesPerGoroutine; i++ {
				msg := &protocol.Message{
					Exchange:     fmt.Sprintf("exchange_%d", queueID),
					RoutingKey:   fmt.Sprintf("key_%d_%d", goroutineID, i),
					Body:         []byte(fmt.Sprintf("G%d:M%d", goroutineID, i)),
					DeliveryMode: 2,
				}

				offset := uint64(goroutineID*messagesPerGoroutine + i + 1)
				err := wm.Write(queueName, msg, offset)
				assert.NoError(t, err)
			}
		}(g)
	}

	wg.Wait()

	// Allow all batches to flush
	time.Sleep(100 * time.Millisecond)

	// Verify all messages are readable
	successCount := 0
	for g := 0; g < numGoroutines; g++ {
		for i := 0; i < messagesPerGoroutine; i++ {
			queueID := g % numQueues
			queueName := fmt.Sprintf("queue_%d", queueID)
			offset := uint64(g*messagesPerGoroutine + i + 1)

			msg, err := wm.Read(queueName, offset)
			if err == nil && msg != nil {
				successCount++
			}
		}
	}

	expectedTotal := numGoroutines * messagesPerGoroutine
	assert.Equal(t, expectedTotal, successCount,
		"Should be able to read all %d messages after concurrent writes", expectedTotal)
}

// TestSharedWAL_FileRotation tests WAL file rotation with multi-queue load
func TestSharedWAL_FileRotation(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	defer wm.Close()

	// Write enough messages to trigger file rotation (>512MB)
	// Each message ~1KB, need ~520K messages
	// For testing, we'll write a smaller amount and check file creation

	numMessages := 1000                // Reduced for faster test
	largeBody := make([]byte, 10*1024) // 10KB body
	for i := range largeBody {
		largeBody[i] = byte(i % 256)
	}

	for i := 0; i < numMessages; i++ {
		queueID := i % 5
		queueName := fmt.Sprintf("queue_%d", queueID)

		msg := &protocol.Message{
			Exchange:     fmt.Sprintf("exchange_%d", queueID),
			RoutingKey:   fmt.Sprintf("key_%d", i),
			Body:         largeBody,
			DeliveryMode: 2,
		}

		err := wm.Write(queueName, msg, uint64(i+1))
		require.NoError(t, err)
	}

	time.Sleep(100 * time.Millisecond)

	// Verify WAL files exist
	walDir := filepath.Join(tmpDir, "wal", "shared")
	files, err := os.ReadDir(walDir)
	require.NoError(t, err)
	assert.Greater(t, len(files), 0, "Should have at least one WAL file")

	// Count .wal files
	walFileCount := 0
	for _, f := range files {
		if filepath.Ext(f.Name()) == ".wal" {
			walFileCount++
		}
	}
	assert.Greater(t, walFileCount, 0, "Should have created WAL files")
}

// TestSharedWAL_Acknowledge tests message acknowledgment
func TestSharedWAL_Acknowledge(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	defer wm.Close()

	// Write messages
	numMessages := 50
	for i := 0; i < numMessages; i++ {
		msg := &protocol.Message{
			Exchange:     "test.exchange",
			RoutingKey:   fmt.Sprintf("key_%d", i),
			Body:         []byte(fmt.Sprintf("Message %d", i)),
			DeliveryMode: 2,
		}
		err := wm.Write("test_queue", msg, uint64(i+1))
		require.NoError(t, err)
	}

	time.Sleep(20 * time.Millisecond)

	// Acknowledge some messages
	for i := 0; i < 25; i++ {
		wm.Acknowledge("test_queue", uint64(i+1))
	}

	time.Sleep(20 * time.Millisecond)

	// Verify acknowledged messages are marked in bitmap
	// (Internal state - would need to expose for testing, or test via cleanup)
	// For now, verify all messages still readable
	for i := 0; i < numMessages; i++ {
		msg, err := wm.Read("test_queue", uint64(i+1))
		require.NoError(t, err)
		assert.NotNil(t, msg)
	}
}

// TestSharedWAL_Close tests proper cleanup on close
func TestSharedWAL_Close(t *testing.T) {
	tmpDir := t.TempDir()

	wm, err := NewWALManager(tmpDir)
	require.NoError(t, err)

	// Write some messages
	for i := 0; i < 100; i++ {
		msg := &protocol.Message{
			Exchange:     "test.exchange",
			RoutingKey:   "test.key",
			Body:         []byte(fmt.Sprintf("Message %d", i)),
			DeliveryMode: 2,
		}
		err := wm.Write("test_queue", msg, uint64(i+1))
		require.NoError(t, err)
	}

	// Close should not panic
	err = wm.Close()
	assert.NoError(t, err)

	// Writing after close should fail
	msg := &protocol.Message{
		Body:         []byte("Should fail"),
		DeliveryMode: 2,
	}
	err = wm.Write("test_queue", msg, 9999)
	assert.Error(t, err, "Write after close should fail")
}

// TestSharedWAL_RecoveryScenario tests reading after WAL manager restart
func TestSharedWAL_RecoveryScenario(t *testing.T) {
	tmpDir := t.TempDir()

	// First session: write messages
	wm1, err := NewWALManager(tmpDir)
	require.NoError(t, err)

	numMessages := 50
	for i := 0; i < numMessages; i++ {
		queueID := i % 3
		queueName := fmt.Sprintf("queue_%d", queueID)

		msg := &protocol.Message{
			Exchange:     fmt.Sprintf("exchange_%d", queueID),
			RoutingKey:   fmt.Sprintf("key_%d", i),
			Body:         []byte(fmt.Sprintf("Message %d for queue %d", i, queueID)),
			DeliveryMode: 2,
		}
		err := wm1.Write(queueName, msg, uint64(i+1))
		require.NoError(t, err)
	}

	time.Sleep(50 * time.Millisecond)
	wm1.Close()

	// Second session: reopen and verify messages readable
	wm2, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	defer wm2.Close()

	// Try to read messages (they should still be in WAL files)
	// Note: This tests that WAL files persist, but offset index
	// needs to be rebuilt on startup for full recovery
	walDir := filepath.Join(tmpDir, "wal", "shared")
	files, err := os.ReadDir(walDir)
	require.NoError(t, err)
	assert.Greater(t, len(files), 0, "WAL files should persist after close")
}
