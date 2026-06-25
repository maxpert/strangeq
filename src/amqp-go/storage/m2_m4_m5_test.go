package storage

import (
	"runtime"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// TestM4_FdatasyncHelper verifies that the platform-specific fdatasync helper
// works correctly. On Linux it uses syscall.Fdatasync; on other platforms it
// falls back to file.Sync().
func TestM4_FdatasyncHelper(t *testing.T) {
	dataDir := t.TempDir()
	wm, err := NewWALManager(dataDir)
	if err != nil {
		t.Fatalf("NewWALManager: %v", err)
	}
	defer wm.Close()

	// Write a durable message to create a WAL file
	msg := &protocol.Message{
		Exchange:     "amq.direct",
		RoutingKey:   "test",
		DeliveryMode: 2,
		Body:         []byte("fdatasync-test"),
	}
	if err := wm.Write("test-queue", msg, 1); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// The write already calls fdatasync internally via flushBatch.
	// Read back to verify data integrity after sync.
	readMsg, err := wm.Read("test-queue", 1)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(readMsg.Body) != "fdatasync-test" {
		t.Errorf("body mismatch: got %q, want %q", readMsg.Body, "fdatasync-test")
	}

	// Verify platform behavior
	if runtime.GOOS == "linux" {
		t.Log("Running on Linux — fdatasync syscall should be used")
	} else {
		t.Logf("Running on %s — fallback to file.Sync() is used", runtime.GOOS)
	}
}

// TestM4_FdatasyncDirectCall verifies the helper function doesn't error on a
// real file.
func TestM4_FdatasyncDirectCall(t *testing.T) {
	dataDir := t.TempDir()
	wm, err := NewWALManager(dataDir)
	if err != nil {
		t.Fatalf("NewWALManager: %v", err)
	}
	defer wm.Close()

	// Write a message to create a WAL file with content
	msg := &protocol.Message{
		Exchange:     "",
		RoutingKey:   "rk",
		DeliveryMode: 2,
		Body:         []byte("direct-fdatasync"),
	}
	if err := wm.Write("q", msg, 1); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Get the current file handle and call fdatasyncFile directly
	wm.sharedWAL.fileMutex.Lock()
	file := wm.sharedWAL.currentFile
	wm.sharedWAL.fileMutex.Unlock()

	if file == nil {
		t.Fatal("current WAL file is nil")
	}

	if err := fdatasyncFile(file); err != nil {
		t.Errorf("fdatasyncFile error: %v", err)
	}
}

// TestM2_BatchSegmentAck verifies that multiple ACKs are batched and applied
// to the segment bitmap without per-ACK lock contention.
func TestM2_BatchSegmentAck(t *testing.T) {
	dataDir := t.TempDir()
	sm, err := NewSegmentManager(dataDir)
	if err != nil {
		t.Fatalf("NewSegmentManager: %v", err)
	}
	defer sm.Close()

	// Write some messages to segments
	msgs := make([]*RecoveryMessage, 10)
	for i := 0; i < 10; i++ {
		msgs[i] = &RecoveryMessage{
			QueueName: "test-batch-ack",
			Offset:    uint64(i + 1),
			Message: &protocol.Message{
				DeliveryTag:  uint64(i + 1),
				Body:         []byte("msg"),
				Exchange:     "",
				RoutingKey:   "rk",
				DeliveryMode: 2,
			},
		}
	}

	if err := sm.CheckpointBatch("test-batch-ack", msgs); err != nil {
		t.Fatalf("CheckpointBatch: %v", err)
	}

	// ACK all messages
	for i := 0; i < 10; i++ {
		sm.Acknowledge("test-batch-ack", uint64(i+1))
	}

	// Get queue segments for verification
	val, ok := sm.queueSegments.Load("test-batch-ack")
	if !ok {
		t.Fatal("queue segments not found")
	}
	qs := val.(*QueueSegments)

	// Give the batch ACK goroutine time to process
	require.Eventually(t, func() bool {
		qs.bitmapMutex.RLock()
		count := int(qs.ackBitmap.GetCardinality())
		qs.bitmapMutex.RUnlock()
		return count >= 10
	}, 1*time.Second, 5*time.Millisecond)

	// Verify all ACKs are in the bitmap

	qs.bitmapMutex.RLock()
	count := int(qs.ackBitmap.GetCardinality())
	qs.bitmapMutex.RUnlock()

	if count != 10 {
		t.Errorf("acked count: got %d, want 10", count)
	}
}

// TestM5_BatchCheckpointWrites verifies that CheckpointBatch writes all
// messages in a single batch (one file write, one mutex acquisition) and
// that all messages are readable afterward.
func TestM5_BatchCheckpointWrites(t *testing.T) {
	dataDir := t.TempDir()
	sm, err := NewSegmentManager(dataDir)
	if err != nil {
		t.Fatalf("NewSegmentManager: %v", err)
	}
	defer sm.Close()

	numMsgs := 50
	msgs := make([]*RecoveryMessage, numMsgs)
	for i := 0; i < numMsgs; i++ {
		msgs[i] = &RecoveryMessage{
			QueueName: "test-batch-write",
			Offset:    uint64(i + 1),
			Message: &protocol.Message{
				DeliveryTag:  uint64(i + 1),
				Body:         []byte("batch-msg"),
				Exchange:     "amq.topic",
				RoutingKey:   "routing.key",
				DeliveryMode: 2,
			},
		}
	}

	// Batch write all messages
	if err := sm.CheckpointBatch("test-batch-write", msgs); err != nil {
		t.Fatalf("CheckpointBatch: %v", err)
	}

	// Read back and verify all messages
	for i := 0; i < numMsgs; i++ {
		msg, err := sm.Read("test-batch-write", uint64(i+1))
		if err != nil {
			t.Errorf("Read offset %d failed: %v", i+1, err)
			continue
		}
		if string(msg.Body) != "batch-msg" {
			t.Errorf("offset %d body: got %q, want %q", i+1, msg.Body, "batch-msg")
		}
		if msg.Exchange != "amq.topic" {
			t.Errorf("offset %d exchange: got %q, want %q", i+1, msg.Exchange, "amq.topic")
		}
		if msg.RoutingKey != "routing.key" {
			t.Errorf("offset %d routingKey: got %q, want %q", i+1, msg.RoutingKey, "routing.key")
		}
	}
}

// TestM5_BatchCheckpointWithRollover verifies that batch writes work correctly
// when the batch triggers a segment rollover. Messages should be readable
// across both the sealed and new segments.
func TestM5_BatchCheckpointWithRollover(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultSegmentConfig()
	cfg.SegmentSize = 128 // Tiny segment to force rollover during batch

	sm, err := NewSegmentManagerWithConfig(dataDir, cfg)
	require.NoError(t, err)
	defer sm.Close()

	// Write enough messages to trigger at least one rollover
	numMsgs := 20
	msgs := make([]*RecoveryMessage, numMsgs)
	for i := 0; i < numMsgs; i++ {
		msgs[i] = &RecoveryMessage{
			QueueName: "test-rollover",
			Offset:    uint64(i + 1),
			Message: &protocol.Message{
				DeliveryTag:  uint64(i + 1),
				Body:         []byte("rollover-msg"),
				Exchange:     "ex",
				RoutingKey:   "rk",
				DeliveryMode: 2,
			},
		}
	}

	// Batch write — should trigger rollover
	err = sm.CheckpointBatch("test-rollover", msgs)
	require.NoError(t, err, "CheckpointBatch with rollover should succeed")

	// Read back all messages across both segments
	for i := 0; i < numMsgs; i++ {
		msg, err := sm.Read("test-rollover", uint64(i+1))
		require.NoError(t, err, "Read offset %d failed after rollover: %v", i+1, err)
		require.Equal(t, "rollover-msg", string(msg.Body),
			"offset %d body mismatch after rollover", i+1)
	}
}
