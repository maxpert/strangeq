package storage

import (
	"os"
	"testing"
	"time"
)

// TestOffsetCheckpointConfigurableInterval tests different checkpoint intervals
func TestOffsetCheckpointConfigurableInterval(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "offset_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	tests := []struct {
		name       string
		interval   time.Duration
		wantTicker bool
	}{
		{
			name:       "Default 5 second interval",
			interval:   5 * time.Second,
			wantTicker: true,
		},
		{
			name:       "Custom 1 second interval",
			interval:   1 * time.Second,
			wantTicker: true,
		},
		{
			name:       "Disabled checkpointing (0)",
			interval:   0,
			wantTicker: false,
		},
		{
			name:       "High performance mode (30s)",
			interval:   30 * time.Second,
			wantTicker: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create store with custom interval
			store, err := NewOffsetCheckpointStoreWithInterval(tmpDir, tt.interval)
			if err != nil {
				t.Fatalf("failed to create store: %v", err)
			}
			defer store.Close()

			// Check if ticker is created correctly
			hasTicker := store.ticker != nil
			if hasTicker != tt.wantTicker {
				t.Errorf("ticker existence = %v, want %v (interval=%v)", hasTicker, tt.wantTicker, tt.interval)
			}

			// Test that updates work regardless of interval
			store.UpdateOffset("test.queue", "consumer1", 100)
			offset, exists := store.GetOffset("test.queue", "consumer1")
			if !exists || offset != 100 {
				t.Errorf("expected offset 100, got %d (exists: %v)", offset, exists)
			}

			// Manual checkpoint should work even when background is disabled
			if err := store.CheckpointAll(); err != nil {
				t.Errorf("manual checkpoint failed: %v", err)
			}
		})
	}
}

// TestOffsetCheckpointDisabledBackgroundCheckpointing verifies that when interval=0,
// no background goroutine is started
func TestOffsetCheckpointDisabledBackgroundCheckpointing(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "offset_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create store with checkpointing disabled
	store, err := NewOffsetCheckpointStoreWithInterval(tmpDir, 0)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	// Update offset
	store.UpdateOffset("test.queue", "consumer1", 200)

	// Wait a bit to ensure no background checkpoint happens
	time.Sleep(100 * time.Millisecond)

	// Create new store - should have no checkpoints
	store2, err := NewOffsetCheckpointStoreWithInterval(tmpDir, 0)
	if err != nil {
		t.Fatalf("failed to create second store: %v", err)
	}
	defer store2.Close()

	if err := store2.LoadAllOffsets(); err != nil {
		t.Fatalf("failed to load offsets: %v", err)
	}

	// Should not find any offset since background checkpointing was disabled
	_, exists := store2.GetOffset("test.queue", "consumer1")
	if exists {
		t.Error("expected no checkpoint when background checkpointing is disabled")
	}

	// Now manually checkpoint from first store
	if err := store.CheckpointAll(); err != nil {
		t.Fatalf("failed to manual checkpoint: %v", err)
	}

	// Create third store and verify manual checkpoint worked
	store3, err := NewOffsetCheckpointStoreWithInterval(tmpDir, 0)
	if err != nil {
		t.Fatalf("failed to create third store: %v", err)
	}
	defer store3.Close()

	if err := store3.LoadAllOffsets(); err != nil {
		t.Fatalf("failed to load offsets: %v", err)
	}

	offset, exists := store3.GetOffset("test.queue", "consumer1")
	if !exists || offset != 200 {
		t.Errorf("expected manual checkpoint to persist offset 200, got %d (exists: %v)", offset, exists)
	}
}
