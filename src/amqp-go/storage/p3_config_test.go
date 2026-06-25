package storage

import (
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

// TestP3_RingBufferSizeFromConfig verifies that the ring buffer is created with
// the size specified in EngineConfig, not the hardcoded DefaultRingBufferSize.
func TestP3_RingBufferSizeFromConfig(t *testing.T) {
	t.Parallel()

	customSize := 128 // Small power-of-2 for testing
	ec := interfaces.EngineConfig{
		RingBufferSize:        customSize,
		SpillThresholdPercent: 75,
	}

	ds := NewDisruptorStorageWithEngineConfig(t.TempDir(), 5000, ec)

	ring := ds.getOrCreateQueueRing("test-queue-p3")

	if got := len(ring.ringBuffer); got != customSize {
		t.Errorf("ring buffer size: got %d, want %d (from EngineConfig)", got, customSize)
	}

	if got := ring.ringMask; got != customSize-1 {
		t.Errorf("ring buffer mask: got %d, want %d", got, customSize-1)
	}

	// Verify spill threshold is computed from percent: 75% of 128 = 96
	expectedSpill := uint64(float64(customSize) * 0.75)
	if got := ds.spillThreshold; got != expectedSpill {
		t.Errorf("spill threshold: got %d, want %d (75%% of %d)", got, expectedSpill, customSize)
	}
}

// TestP3_RingBufferMaskWorks verifies that the mask correctly wraps around
// for the configured ring buffer size (not the default).
func TestP3_RingBufferMaskWorks(t *testing.T) {
	t.Parallel()

	customSize := 256
	ec := interfaces.EngineConfig{
		RingBufferSize:        customSize,
		SpillThresholdPercent: 80,
	}

	ds := NewDisruptorStorageWithEngineConfig(t.TempDir(), 5000, ec)
	ring := ds.getOrCreateQueueRing("test-mask-p3")

	// Store a message with delivery tag = customSize (should wrap to index 0)
	msg := &protocol.Message{
		DeliveryTag:  uint64(customSize),
		Body:         []byte("wrap-test"),
		Exchange:     "amq.direct",
		RoutingKey:   "rk",
		DeliveryMode: 1,
	}

	index := msg.DeliveryTag & uint64(ring.ringMask)
	if int(index) != 0 {
		t.Errorf("mask wrap: tag=%d, mask=%d, index=%d, want 0",
			msg.DeliveryTag, ring.ringMask, index)
	}

	// Actually store and retrieve the message
	if err := ds.StoreMessage("test-mask-p3", msg); err != nil {
		t.Fatalf("StoreMessage failed: %v", err)
	}

	retrieved, err := ds.GetMessage("test-mask-p3", uint64(customSize))
	if err != nil {
		t.Fatalf("GetMessage failed: %v", err)
	}

	if retrieved.Body == nil || string(retrieved.Body) != "wrap-test" {
		t.Errorf("retrieved message body mismatch: got %v", retrieved.Body)
	}
}

// TestP3_DefaultsWhenConfigZero verifies that default values are used when
// EngineConfig fields are zero.
func TestP3_DefaultsWhenConfigZero(t *testing.T) {
	t.Parallel()

	ec := interfaces.EngineConfig{} // All zeros

	ds := NewDisruptorStorageWithEngineConfig(t.TempDir(), 5000, ec)

	// Should fall back to DefaultRingBufferSize
	if ds.ringBufferSize != DefaultRingBufferSize {
		t.Errorf("default ring size: got %d, want %d", ds.ringBufferSize, DefaultRingBufferSize)
	}

	// Default spill threshold should be 80% of default ring size
	defaultSize := DefaultRingBufferSize
	expectedSpill := uint64(float64(defaultSize) * 0.8)
	if ds.spillThreshold != expectedSpill {
		t.Errorf("default spill threshold: got %d, want %d", ds.spillThreshold, expectedSpill)
	}
}

// TestP3_SpillThresholdAtCorrectPercent verifies that transient messages start
// spilling to WAL when the ring buffer exceeds the configured spill threshold.
// The spill condition is strict-greater: messageCount > spillThreshold.
func TestP3_SpillThresholdAtCorrectPercent(t *testing.T) {
	t.Parallel()

	customSize := 64 // Small ring buffer
	ec := interfaces.EngineConfig{
		RingBufferSize:        customSize,
		SpillThresholdPercent: 50, // Spill at 50% = 32 messages
	}

	ds := NewDisruptorStorageWithEngineConfig(t.TempDir(), 5000, ec)
	defer ds.Close()

	expectedSpill := uint64(float64(customSize) * 0.5)
	if ds.spillThreshold != expectedSpill {
		t.Fatalf("spill threshold: got %d, want %d", ds.spillThreshold, expectedSpill)
	}

	// Fill ring buffer up to exactly the threshold (32 messages, tags 0–31).
	// The spill condition is messageCount > spillThreshold (strict >),
	// so at count=32 the next message should still go to the ring.
	for i := uint64(0); i < expectedSpill; i++ {
		msg := &protocol.Message{
			DeliveryTag:  i,
			Body:         []byte("fill"),
			DeliveryMode: 1, // Transient (no WAL write)
		}
		if err := ds.StoreMessage("test-spill-p3", msg); err != nil {
			t.Fatalf("StoreMessage failed at tag %d: %v", i, err)
		}
	}

	ring := ds.getQueueRing("test-spill-p3")
	countAtThreshold := ring.messageCount.Load()
	if countAtThreshold != expectedSpill {
		t.Fatalf("ring count at threshold: got %d, want %d", countAtThreshold, expectedSpill)
	}

	// At count == threshold, next message should still go to ring (not > threshold)
	msg := &protocol.Message{
		DeliveryTag:  expectedSpill,
		Body:         []byte("at-threshold"),
		DeliveryMode: 1,
	}
	if err := ds.StoreMessage("test-spill-p3", msg); err != nil {
		t.Fatalf("StoreMessage at threshold failed: %v", err)
	}
	countAfterAtThreshold := ring.messageCount.Load()
	if countAfterAtThreshold != countAtThreshold+1 {
		t.Errorf("at threshold (count==threshold), message should go to ring: got count %d, want %d",
			countAfterAtThreshold, countAtThreshold+1)
	}

	// Now count = threshold+1 = 33 > threshold = 32. Next message should spill to WAL.
	spillMsg := &protocol.Message{
		DeliveryTag:  expectedSpill + 1,
		Body:         []byte("spilled"),
		DeliveryMode: 1, // Transient — will be written to WAL via spill path
	}
	if err := ds.StoreMessage("test-spill-p3", spillMsg); err != nil {
		t.Fatalf("StoreMessage above threshold failed: %v", err)
	}
	countAfterSpill := ring.messageCount.Load()

	// Ring count should NOT have increased — message was spilled to WAL
	if countAfterSpill != countAfterAtThreshold {
		t.Errorf("above threshold, message should spill to WAL: ring count got %d, want %d (unchanged)",
			countAfterSpill, countAfterAtThreshold)
	}

	// Verify the spilled message is retrievable from WAL
	retrieved, err := ds.GetMessage("test-spill-p3", expectedSpill+1)
	if err != nil {
		t.Fatalf("GetMessage for spilled message failed: %v", err)
	}
	if string(retrieved.Body) != "spilled" {
		t.Errorf("spilled message body: got %q, want %q", retrieved.Body, "spilled")
	}
}
