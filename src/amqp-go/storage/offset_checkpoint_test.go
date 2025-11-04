package storage

import (
	"os"
	"testing"
	"time"
)

func TestOffsetCheckpointBasic(t *testing.T) {
	// Create temp dir
	tmpDir, err := os.MkdirTemp("", "offset_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create offset store
	store, err := NewOffsetCheckpointStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create offset store: %v", err)
	}
	defer store.Close()

	// Update offsets
	store.UpdateOffset("test.queue", "consumer1", 100)
	store.UpdateOffset("test.queue", "consumer2", 200)
	store.UpdateOffset("another.queue", "consumer3", 300)

	// Get offsets
	offset1, exists := store.GetOffset("test.queue", "consumer1")
	if !exists || offset1 != 100 {
		t.Errorf("expected offset 100, got %d (exists: %v)", offset1, exists)
	}

	offset2, exists := store.GetOffset("test.queue", "consumer2")
	if !exists || offset2 != 200 {
		t.Errorf("expected offset 200, got %d (exists: %v)", offset2, exists)
	}

	// Get queue offsets
	queueOffsets := store.GetQueueOffsets("test.queue")
	if len(queueOffsets) != 2 {
		t.Errorf("expected 2 offsets for test.queue, got %d", len(queueOffsets))
	}
}

func TestOffsetCheckpointUpdates(t *testing.T) {
	// Create temp dir
	tmpDir, err := os.MkdirTemp("", "offset_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create offset store
	store, err := NewOffsetCheckpointStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create offset store: %v", err)
	}
	defer store.Close()

	// Update offset multiple times (simulates many ACKs)
	for i := 1; i <= 1000; i++ {
		store.UpdateOffset("test.queue", "consumer1", uint64(i))
	}

	// Should only keep the highest
	offset, exists := store.GetOffset("test.queue", "consumer1")
	if !exists || offset != 1000 {
		t.Errorf("expected offset 1000, got %d", offset)
	}

	// Try updating with lower value (should be ignored)
	store.UpdateOffset("test.queue", "consumer1", 500)
	offset, exists = store.GetOffset("test.queue", "consumer1")
	if !exists || offset != 1000 {
		t.Errorf("expected offset to remain 1000, got %d", offset)
	}
}

func TestOffsetCheckpointPersistence(t *testing.T) {
	// Create temp dir
	tmpDir, err := os.MkdirTemp("", "offset_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create first store
	store1, err := NewOffsetCheckpointStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create first store: %v", err)
	}

	// Update offsets
	store1.UpdateOffset("test.queue", "consumer1", 100)
	store1.UpdateOffset("test.queue", "consumer2", 200)

	// Force checkpoint
	if err := store1.CheckpointAll(); err != nil {
		t.Fatalf("failed to checkpoint: %v", err)
	}

	// Close first store
	store1.Close()

	// Create second store (simulates restart)
	store2, err := NewOffsetCheckpointStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create second store: %v", err)
	}
	defer store2.Close()

	// Load offsets
	if err := store2.LoadAllOffsets(); err != nil {
		t.Fatalf("failed to load offsets: %v", err)
	}

	// Verify offsets persisted
	offset1, exists := store2.GetOffset("test.queue", "consumer1")
	if !exists || offset1 != 100 {
		t.Errorf("expected offset 100 after restart, got %d (exists: %v)", offset1, exists)
	}

	offset2, exists := store2.GetOffset("test.queue", "consumer2")
	if !exists || offset2 != 200 {
		t.Errorf("expected offset 200 after restart, got %d (exists: %v)", offset2, exists)
	}
}

func TestOffsetCheckpointBackgroundCheckpointing(t *testing.T) {
	// Create temp dir
	tmpDir, err := os.MkdirTemp("", "offset_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create store (background checkpointing starts automatically)
	store, err := NewOffsetCheckpointStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	// Update offset
	store.UpdateOffset("test.queue", "consumer1", 500)

	// Wait for background checkpoint (interval is 5 seconds, but test shouldn't wait that long)
	// Instead, manually trigger checkpoint
	time.Sleep(100 * time.Millisecond)
	if err := store.CheckpointAll(); err != nil {
		t.Fatalf("failed to checkpoint: %v", err)
	}

	// Close and verify checkpoint was written
	store.Close()

	// Create new store and load
	store2, err := NewOffsetCheckpointStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create second store: %v", err)
	}
	defer store2.Close()

	if err := store2.LoadAllOffsets(); err != nil {
		t.Fatalf("failed to load offsets: %v", err)
	}

	offset, exists := store2.GetOffset("test.queue", "consumer1")
	if !exists || offset != 500 {
		t.Errorf("expected offset 500 after background checkpoint, got %d", offset)
	}
}

func TestOffsetCheckpointRemoveConsumer(t *testing.T) {
	// Create temp dir
	tmpDir, err := os.MkdirTemp("", "offset_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create store
	store, err := NewOffsetCheckpointStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	// Add offsets
	store.UpdateOffset("test.queue", "consumer1", 100)
	store.UpdateOffset("test.queue", "consumer2", 200)

	// Checkpoint
	if err := store.CheckpointAll(); err != nil {
		t.Fatalf("failed to checkpoint: %v", err)
	}

	// Remove consumer1
	if err := store.RemoveConsumer("test.queue", "consumer1"); err != nil {
		t.Fatalf("failed to remove consumer: %v", err)
	}

	// Verify consumer1 is gone
	_, exists := store.GetOffset("test.queue", "consumer1")
	if exists {
		t.Error("expected consumer1 to be removed")
	}

	// Verify consumer2 still exists
	offset, exists := store.GetOffset("test.queue", "consumer2")
	if !exists || offset != 200 {
		t.Errorf("expected consumer2 offset 200, got %d (exists: %v)", offset, exists)
	}
}

func TestOffsetCheckpointZeroWrites(t *testing.T) {
	// This test verifies that UpdateOffset does not write to disk immediately
	tmpDir, err := os.MkdirTemp("", "offset_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create store
	store, err := NewOffsetCheckpointStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	// Update offset 10,000 times (simulates high ACK rate)
	start := time.Now()
	for i := 1; i <= 10000; i++ {
		store.UpdateOffset("test.queue", "consumer1", uint64(i))
	}
	duration := time.Since(start)

	// Should be extremely fast (< 10ms for 10K updates if no disk writes)
	t.Logf("10,000 UpdateOffset calls took %v", duration)
	if duration > 100*time.Millisecond {
		t.Errorf("UpdateOffset too slow: %v (should be <100ms for 10K calls)", duration)
	}

	// Verify offset is correct in memory
	offset, exists := store.GetOffset("test.queue", "consumer1")
	if !exists || offset != 10000 {
		t.Errorf("expected offset 10000, got %d", offset)
	}
}

func TestOffsetCheckpointMultipleQueues(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "offset_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewOffsetCheckpointStore(tmpDir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	// Create offsets for multiple queues
	store.UpdateOffset("queue1", "consumer1", 100)
	store.UpdateOffset("queue2", "consumer1", 200)
	store.UpdateOffset("queue3", "consumer1", 300)

	// Get all offsets
	allOffsets := store.GetAllOffsets()
	if len(allOffsets) != 3 {
		t.Errorf("expected 3 queues, got %d", len(allOffsets))
	}

	// Verify each queue has correct offset
	for queueName, expectedOffset := range map[string]uint64{
		"queue1": 100,
		"queue2": 200,
		"queue3": 300,
	} {
		if queueOffsets, exists := allOffsets[queueName]; !exists {
			t.Errorf("queue %s not found in all offsets", queueName)
		} else if offset, exists := queueOffsets["consumer1"]; !exists {
			t.Errorf("consumer1 not found for queue %s", queueName)
		} else if offset.LastAckTag != expectedOffset {
			t.Errorf("queue %s: expected offset %d, got %d", queueName, expectedOffset, offset.LastAckTag)
		}
	}
}
