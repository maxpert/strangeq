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
	storage, err := NewDisruptorStorageWithDataDir(tmpDir)
	require.NoError(t, err)
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
	storage, err := NewDisruptorStorageWithDataDir(tmpDir)
	require.NoError(t, err)
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
	storage, err := NewDisruptorStorageWithDataDir(tmpDir)
	require.NoError(t, err)
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

	// Acknowledge the first half only, so the second half is a live control.
	const ackedThrough = 5
	for i := 1; i <= ackedThrough; i++ {
		err := storage.DeleteMessage("test_queue", uint64(i))
		require.NoError(t, err)
	}
	require.Less(t, ackedThrough, numMessages,
		"PREMISE BROKEN: every message was acknowledged, so this fixture has no unacked "+
			"control and cannot tell a correct ack gate from a read path that is simply broken")

	// Allow ACKs to propagate
	time.Sleep(50 * time.Millisecond)

	// Split at the acknowledgement boundary and assert BOTH directions.
	//
	// This replaces an undiscriminating "all 10 still readable". That assertion's
	// own rationale was about PHYSICAL retention — "WAL doesn't delete, just marks
	// as ACKed… compaction will clean them up later" — which remains true, because
	// nothing here deletes a record. But it stated physical retention as LOGICAL
	// readability, and the module already disagreed: RecoverFromWAL filters on
	// ackBitmap.Contains, and broker/queue_reaper.go reapTTLSweep probes acked tags
	// on every sweep documenting that they must read back empty.
	for i := 1; i <= ackedThrough; i++ {
		_, rerr := wal.Read("test_queue", uint64(i))
		require.Error(t, rerr,
			"ACKNOWLEDGED TAG STILL SERVED: message %d was acknowledged through "+
				"DisruptorStorage.DeleteMessage, so the WAL must not return it as a live "+
				"message. Serving it re-expires and re-dead-letters the same record without "+
				"bound and stalls the queue tail", i)
	}
	for i := ackedThrough + 1; i <= numMessages; i++ {
		_, rerr := wal.Read("test_queue", uint64(i))
		require.NoError(t, rerr,
			"UNACKNOWLEDGED TAG REFUSED: message %d was never acknowledged and must still be "+
				"readable from the WAL. Refusing it loses confirmed durable data silently", i)
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
	storage, err := NewDisruptorStorageWithDataDir(tmpDir)
	require.NoError(t, err)
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
	storage, err := NewDisruptorStorageWithDataDir(tmpDir)
	require.NoError(t, err)
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
	storage.hot.Delete("test_queue", 1)

	// Try to retrieve message - should fall back to WAL
	retrievedMsg, err := storage.GetMessage("test_queue", 1)
	require.NoError(t, err, "Should fall back to WAL")
	assert.Equal(t, msg.Exchange, retrievedMsg.Exchange)
	assert.Equal(t, msg.Body, retrievedMsg.Body)
}
