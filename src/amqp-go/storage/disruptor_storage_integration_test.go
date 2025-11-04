package storage

import (
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDisruptorStorage_DurableMessageWALIntegration tests that durable messages
// are written to WAL before being stored in the ring buffer
func TestDisruptorStorage_DurableMessageWALIntegration(t *testing.T) {
	tmpDir := t.TempDir()

	// Create WAL manager
	wal, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	defer wal.Close()

	// Create storage with WAL
	storage := NewDisruptorStorageWithDataDir(tmpDir)
	storage.wal = wal

	// Create a queue
	queue := &protocol.Queue{
		Name:    "test_queue",
		Durable: true,
	}
	err = storage.StoreQueue(queue)
	require.NoError(t, err)

	// Publish a durable message (DeliveryMode == 2)
	durableMsg := &protocol.Message{
		Exchange:     "test.exchange",
		RoutingKey:   "test.key",
		Body:         []byte("durable message body"),
		DeliveryMode: 2, // Durable
		DeliveryTag:  1,
	}

	err = storage.StoreMessage("test_queue", durableMsg)
	require.NoError(t, err)

	// Allow WAL to flush
	time.Sleep(50 * time.Millisecond)

	// Verify message can be read from WAL
	readMsg, err := wal.Read("test_queue", 1)
	require.NoError(t, err)
	assert.Equal(t, durableMsg.Exchange, readMsg.Exchange)
	assert.Equal(t, durableMsg.RoutingKey, readMsg.RoutingKey)
	assert.Equal(t, durableMsg.Body, readMsg.Body)
	assert.Equal(t, durableMsg.DeliveryMode, readMsg.DeliveryMode)

	// Verify message is also in ring buffer
	retrievedMsg, err := storage.GetMessage("test_queue", 1)
	require.NoError(t, err)
	assert.Equal(t, durableMsg.Body, retrievedMsg.Body)
}

// TestDisruptorStorage_TransientMessageNoWAL tests that transient messages
// are NOT written to WAL (only to ring buffer)
func TestDisruptorStorage_TransientMessageNoWAL(t *testing.T) {
	tmpDir := t.TempDir()

	// Create WAL manager
	wal, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	defer wal.Close()

	// Create storage with WAL
	storage := NewDisruptorStorageWithDataDir(tmpDir)
	storage.wal = wal

	// Create a queue
	queue := &protocol.Queue{
		Name:    "test_queue",
		Durable: false,
	}
	err = storage.StoreQueue(queue)
	require.NoError(t, err)

	// Publish a transient message (DeliveryMode == 1)
	transientMsg := &protocol.Message{
		Exchange:     "test.exchange",
		RoutingKey:   "test.key",
		Body:         []byte("transient message body"),
		DeliveryMode: 1, // Transient
		DeliveryTag:  1,
	}

	err = storage.StoreMessage("test_queue", transientMsg)
	require.NoError(t, err)

	// Allow WAL to flush
	time.Sleep(50 * time.Millisecond)

	// Verify message is NOT in WAL
	_, err = wal.Read("test_queue", 1)
	assert.Error(t, err, "Transient messages should not be in WAL")

	// Verify message IS in ring buffer
	retrievedMsg, err := storage.GetMessage("test_queue", 1)
	require.NoError(t, err)
	assert.Equal(t, transientMsg.Body, retrievedMsg.Body)
}

// TestDisruptorStorage_DurableMessageAcknowledge tests that ACKs are
// propagated to WAL
func TestDisruptorStorage_DurableMessageAcknowledge(t *testing.T) {
	tmpDir := t.TempDir()

	// Create WAL manager
	wal, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	defer wal.Close()

	// Create storage with WAL
	storage := NewDisruptorStorageWithDataDir(tmpDir)
	storage.wal = wal

	// Create a queue
	queue := &protocol.Queue{
		Name:    "test_queue",
		Durable: true,
	}
	err = storage.StoreQueue(queue)
	require.NoError(t, err)

	// Publish multiple durable messages
	numMessages := 10
	for i := 1; i <= numMessages; i++ {
		msg := &protocol.Message{
			Exchange:     "test.exchange",
			RoutingKey:   "test.key",
			Body:         []byte("message body"),
			DeliveryMode: 2, // Durable
			DeliveryTag:  uint64(i),
		}
		err = storage.StoreMessage("test_queue", msg)
		require.NoError(t, err)
	}

	// Allow WAL to flush
	time.Sleep(50 * time.Millisecond)

	// Verify all messages in WAL
	for i := 1; i <= numMessages; i++ {
		_, err := wal.Read("test_queue", uint64(i))
		require.NoError(t, err, "Message %d should be in WAL", i)
	}

	// Acknowledge some messages
	for i := 1; i <= 5; i++ {
		err := storage.DeleteMessage("test_queue", uint64(i))
		require.NoError(t, err)
	}

	// Allow ACKs to propagate
	time.Sleep(50 * time.Millisecond)

	// Verify ACKed messages are still readable (WAL doesn't delete, just marks as ACKed)
	// This is expected behavior - WAL compaction will clean them up later
	for i := 1; i <= numMessages; i++ {
		_, err := wal.Read("test_queue", uint64(i))
		require.NoError(t, err, "Message %d should still be readable from WAL", i)
	}
}

// TestDisruptorStorage_MultiQueueDurableMessages tests durable message routing
// across multiple queues
func TestDisruptorStorage_MultiQueueDurableMessages(t *testing.T) {
	tmpDir := t.TempDir()

	// Create WAL manager
	wal, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	defer wal.Close()

	// Create storage with WAL
	storage := NewDisruptorStorageWithDataDir(tmpDir)
	storage.wal = wal

	// Create multiple queues
	numQueues := 5
	for q := 0; q < numQueues; q++ {
		queue := &protocol.Queue{
			Name:    "test_queue_" + string(rune('A'+q)),
			Durable: true,
		}
		err = storage.StoreQueue(queue)
		require.NoError(t, err)
	}

	// Publish durable messages to each queue
	messagesPerQueue := 10
	for q := 0; q < numQueues; q++ {
		queueName := "test_queue_" + string(rune('A'+q))
		for i := 1; i <= messagesPerQueue; i++ {
			msg := &protocol.Message{
				Exchange:     "test.exchange",
				RoutingKey:   "test.key",
				Body:         []byte("message body"),
				DeliveryMode: 2, // Durable
				DeliveryTag:  uint64(q*messagesPerQueue + i),
			}
			err = storage.StoreMessage(queueName, msg)
			require.NoError(t, err)
		}
	}

	// Allow WAL to flush
	time.Sleep(100 * time.Millisecond)

	// Verify all messages in WAL
	for q := 0; q < numQueues; q++ {
		queueName := "test_queue_" + string(rune('A'+q))
		for i := 1; i <= messagesPerQueue; i++ {
			offset := uint64(q*messagesPerQueue + i)
			_, err := wal.Read(queueName, offset)
			require.NoError(t, err, "Message %d for queue %s should be in WAL", offset, queueName)
		}
	}
}

// TestDisruptorStorage_WALFallback tests that GetMessage falls back to WAL
// when message is not in ring buffer
func TestDisruptorStorage_WALFallback(t *testing.T) {
	tmpDir := t.TempDir()

	// Create WAL manager
	wal, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	defer wal.Close()

	// Create storage with WAL
	storage := NewDisruptorStorageWithDataDir(tmpDir)
	storage.wal = wal

	// Create a queue
	queue := &protocol.Queue{
		Name:    "test_queue",
		Durable: true,
	}
	err = storage.StoreQueue(queue)
	require.NoError(t, err)

	// Publish a durable message
	msg := &protocol.Message{
		Exchange:     "test.exchange",
		RoutingKey:   "test.key",
		Body:         []byte("durable message"),
		DeliveryMode: 2, // Durable
		DeliveryTag:  1,
	}
	err = storage.StoreMessage("test_queue", msg)
	require.NoError(t, err)

	// Allow WAL to flush
	time.Sleep(50 * time.Millisecond)

	// Clear ring buffer to simulate message eviction
	storage.mutex.Lock()
	ring := storage.queues["test_queue"]
	ring.ringBuffer[1&RingBufferMask] = nil
	storage.mutex.Unlock()

	// Try to retrieve message - should fall back to WAL
	retrievedMsg, err := storage.GetMessage("test_queue", 1)
	require.NoError(t, err, "Should fall back to WAL")
	assert.Equal(t, msg.Exchange, retrievedMsg.Exchange)
	assert.Equal(t, msg.Body, retrievedMsg.Body)
}
