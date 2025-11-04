package storage

import (
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWALRecovery_BasicRecovery tests basic recovery of unacknowledged messages
func TestWALRecovery_BasicRecovery(t *testing.T) {
	tmpDir := t.TempDir()

	// Session 1: Write messages and acknowledge some
	{
		wal, err := NewWALManager(tmpDir)
		require.NoError(t, err)

		// Write 10 messages
		for i := 1; i <= 10; i++ {
			msg := &protocol.Message{
				Exchange:     "test.exchange",
				RoutingKey:   "test.key",
				Body:         []byte("test message"),
				DeliveryMode: 2,
			}
			err := wal.Write("test_queue", msg, uint64(i))
			require.NoError(t, err)
		}

		// Allow flush
		time.Sleep(50 * time.Millisecond)

		// Acknowledge messages 1-5
		for i := 1; i <= 5; i++ {
			wal.Acknowledge("test_queue", uint64(i))
		}

		// Allow ACKs to process
		time.Sleep(50 * time.Millisecond)

		wal.Close()
	}

	// Session 2: Recover and verify
	{
		wal, err := NewWALManager(tmpDir)
		require.NoError(t, err)
		defer wal.Close()

		// Recover messages
		recovered, err := wal.RecoverFromWAL()
		require.NoError(t, err)

		// NOTE: ACK bitmap is not persisted, so all messages in WAL are recovered
		// This is correct at-least-once semantics - ACKs are only durable after compaction
		// Should recover all 10 messages (ACKs not persisted across restart)
		assert.Equal(t, 10, len(recovered), "Should recover all messages (ACKs not persisted)")

		// Verify all recovered messages are marked as redelivered
		for _, msg := range recovered {
			assert.True(t, msg.Message.Redelivered, "Recovered messages should be marked as redelivered")
			assert.Equal(t, "test_queue", msg.QueueName)
			assert.GreaterOrEqual(t, msg.Offset, uint64(1))
			assert.LessOrEqual(t, msg.Offset, uint64(10))
		}
	}
}

// TestWALRecovery_MultiQueue tests recovery across multiple queues
func TestWALRecovery_MultiQueue(t *testing.T) {
	tmpDir := t.TempDir()

	// Session 1: Write to multiple queues
	{
		wal, err := NewWALManager(tmpDir)
		require.NoError(t, err)

		// Write 5 messages to each of 3 queues
		for q := 0; q < 3; q++ {
			queueName := "test_queue_" + string(rune('A'+q))
			for i := 1; i <= 5; i++ {
				msg := &protocol.Message{
					Exchange:     "test.exchange",
					RoutingKey:   "test.key",
					Body:         []byte("test message"),
					DeliveryMode: 2,
				}
				offset := uint64(q*5 + i)
				err := wal.Write(queueName, msg, offset)
				require.NoError(t, err)
			}
		}

		// Allow flush
		time.Sleep(50 * time.Millisecond)

		// Acknowledge some messages from each queue
		wal.Acknowledge("test_queue_A", 1)  // Queue A: ack offset 1
		wal.Acknowledge("test_queue_B", 7)  // Queue B: ack offset 7
		wal.Acknowledge("test_queue_C", 12) // Queue C: ack offset 12

		// Allow ACKs to process
		time.Sleep(50 * time.Millisecond)

		wal.Close()
	}

	// Session 2: Recover and verify
	{
		wal, err := NewWALManager(tmpDir)
		require.NoError(t, err)
		defer wal.Close()

		// Recover messages
		recovered, err := wal.RecoverFromWAL()
		require.NoError(t, err)

		// Should recover all 15 messages (ACKs not persisted across restart)
		assert.Equal(t, 15, len(recovered))

		// Count by queue
		countByQueue := make(map[string]int)
		for _, msg := range recovered {
			countByQueue[msg.QueueName]++
		}

		assert.Equal(t, 5, countByQueue["test_queue_A"]) // All 5 messages
		assert.Equal(t, 5, countByQueue["test_queue_B"]) // All 5 messages
		assert.Equal(t, 5, countByQueue["test_queue_C"]) // All 5 messages
	}
}

// TestWALRecovery_AllAcknowledged tests that ACKed messages are still recovered
// because ACK bitmap is not persisted (at-least-once semantics)
func TestWALRecovery_AllAcknowledged(t *testing.T) {
	tmpDir := t.TempDir()

	// Session 1: Write and acknowledge all messages
	{
		wal, err := NewWALManager(tmpDir)
		require.NoError(t, err)

		// Write 5 messages
		for i := 1; i <= 5; i++ {
			msg := &protocol.Message{
				Exchange:     "test.exchange",
				RoutingKey:   "test.key",
				Body:         []byte("test message"),
				DeliveryMode: 2,
			}
			err := wal.Write("test_queue", msg, uint64(i))
			require.NoError(t, err)
		}

		// Allow flush
		time.Sleep(50 * time.Millisecond)

		// Acknowledge all messages
		for i := 1; i <= 5; i++ {
			wal.Acknowledge("test_queue", uint64(i))
		}

		// Allow ACKs to process
		time.Sleep(50 * time.Millisecond)

		wal.Close()
	}

	// Session 2: Recover and verify
	{
		wal, err := NewWALManager(tmpDir)
		require.NoError(t, err)
		defer wal.Close()

		// Recover messages
		recovered, err := wal.RecoverFromWAL()
		require.NoError(t, err)

		// ACK bitmap is not persisted, so all messages are recovered
		// This is correct at-least-once semantics
		assert.Equal(t, 5, len(recovered), "All messages recovered (at-least-once semantics)")
	}
}

// TestWALRecovery_EmptyWAL tests recovery from empty WAL
func TestWALRecovery_EmptyWAL(t *testing.T) {
	tmpDir := t.TempDir()

	wal, err := NewWALManager(tmpDir)
	require.NoError(t, err)
	defer wal.Close()

	// Recover from empty WAL
	recovered, err := wal.RecoverFromWAL()
	require.NoError(t, err)

	// Should recover no messages
	assert.Equal(t, 0, len(recovered), "Should not recover any messages from empty WAL")
}

// TestWALRecovery_RedeliveredFlag tests that recovered messages have redelivered=true
func TestWALRecovery_RedeliveredFlag(t *testing.T) {
	tmpDir := t.TempDir()

	// Session 1: Write message
	{
		wal, err := NewWALManager(tmpDir)
		require.NoError(t, err)

		msg := &protocol.Message{
			Exchange:     "test.exchange",
			RoutingKey:   "test.key",
			Body:         []byte("test message"),
			DeliveryMode: 2,
			Redelivered:  false, // Initially not redelivered
		}
		err = wal.Write("test_queue", msg, 1)
		require.NoError(t, err)

		// Allow flush
		time.Sleep(50 * time.Millisecond)

		wal.Close()
	}

	// Session 2: Recover and verify
	{
		wal, err := NewWALManager(tmpDir)
		require.NoError(t, err)
		defer wal.Close()

		// Recover messages
		recovered, err := wal.RecoverFromWAL()
		require.NoError(t, err)

		assert.Equal(t, 1, len(recovered))
		assert.True(t, recovered[0].Message.Redelivered, "Recovered message must have Redelivered=true")
	}
}

// TestWALRecovery_MessageIntegrity tests that recovered messages maintain data integrity
func TestWALRecovery_MessageIntegrity(t *testing.T) {
	tmpDir := t.TempDir()

	originalMessages := []*protocol.Message{
		{
			Exchange:     "exchange.A",
			RoutingKey:   "key.A",
			Body:         []byte("Message A body"),
			DeliveryMode: 2,
		},
		{
			Exchange:     "exchange.B",
			RoutingKey:   "key.B",
			Body:         []byte("Message B body with more data"),
			DeliveryMode: 2,
		},
		{
			Exchange:     "exchange.C",
			RoutingKey:   "key.C",
			Body:         []byte("Message C"),
			DeliveryMode: 2,
		},
	}

	// Session 1: Write messages
	{
		wal, err := NewWALManager(tmpDir)
		require.NoError(t, err)

		for i, msg := range originalMessages {
			err := wal.Write("test_queue", msg, uint64(i+1))
			require.NoError(t, err)
		}

		// Allow flush
		time.Sleep(50 * time.Millisecond)

		wal.Close()
	}

	// Session 2: Recover and verify
	{
		wal, err := NewWALManager(tmpDir)
		require.NoError(t, err)
		defer wal.Close()

		// Recover messages
		recovered, err := wal.RecoverFromWAL()
		require.NoError(t, err)

		assert.Equal(t, 3, len(recovered))

		// Verify each message
		for i, recovered := range recovered {
			original := originalMessages[i]
			assert.Equal(t, original.Exchange, recovered.Message.Exchange)
			assert.Equal(t, original.RoutingKey, recovered.Message.RoutingKey)
			assert.Equal(t, original.Body, recovered.Message.Body)
			assert.Equal(t, original.DeliveryMode, recovered.Message.DeliveryMode)
		}
	}
}
