package storage

import (
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

func TestP3_RingBufferSizeFromConfig(t *testing.T) {
	t.Parallel()

	customSize := 128
	ec := interfaces.EngineConfig{
		RingBufferSize:        customSize,
		SpillThresholdPercent: 75,
	}

	ds := NewDisruptorStorageWithEngineConfig(t.TempDir(), 5000, ec)

	ring := ds.getOrCreateQueueRing("test-queue-p3")

	if got := ring.ring.Size(); got != customSize {
		t.Errorf("ring buffer size: got %d, want %d (from EngineConfig)", got, customSize)
	}

	expectedSpill := uint64(customSize) * 75 / 100
	if got := ds.spillThreshold; got != expectedSpill {
		t.Errorf("spill threshold: got %d, want %d (75%% of %d)", got, expectedSpill, customSize)
	}
}

func TestP3_RingBufferMaskWorks(t *testing.T) {
	t.Parallel()

	customSize := 256
	ec := interfaces.EngineConfig{
		RingBufferSize:        customSize,
		SpillThresholdPercent: 80,
	}

	ds := NewDisruptorStorageWithEngineConfig(t.TempDir(), 5000, ec)

	msg := &protocol.Message{
		DeliveryTag:  uint64(customSize),
		Body:         []byte("wrap-test"),
		Exchange:     "amq.direct",
		RoutingKey:   "rk",
		DeliveryMode: 1,
	}

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

func TestP3_DefaultsWhenConfigZero(t *testing.T) {
	t.Parallel()

	ec := interfaces.EngineConfig{}

	ds := NewDisruptorStorageWithEngineConfig(t.TempDir(), 5000, ec)

	if ds.ringBufferSize != DefaultRingBufferSize {
		t.Errorf("default ring size: got %d, want %d", ds.ringBufferSize, DefaultRingBufferSize)
	}

	defaultSize := DefaultRingBufferSize
	expectedSpill := uint64(defaultSize) * uint64(DefaultSpillThresholdPercent) / 100
	if ds.spillThreshold != expectedSpill {
		t.Errorf("default spill threshold: got %d, want %d", ds.spillThreshold, expectedSpill)
	}
}

func TestP3_SpillThresholdAtCorrectPercent(t *testing.T) {
	t.Parallel()

	customSize := 64
	ec := interfaces.EngineConfig{
		RingBufferSize:        customSize,
		SpillThresholdPercent: 50,
	}

	ds := NewDisruptorStorageWithEngineConfig(t.TempDir(), 5000, ec)
	defer ds.Close()

	expectedSpill := uint64(customSize) * 50 / 100
	if ds.spillThreshold != expectedSpill {
		t.Fatalf("spill threshold: got %d, want %d", ds.spillThreshold, expectedSpill)
	}

	for i := uint64(0); i < expectedSpill; i++ {
		msg := &protocol.Message{
			DeliveryTag:  i,
			Body:         []byte("fill"),
			DeliveryMode: 1,
		}
		if err := ds.StoreMessage("test-spill-p3", msg); err != nil {
			t.Fatalf("StoreMessage failed at tag %d: %v", i, err)
		}
	}

	ring := ds.getQueueRing("test-spill-p3")
	countAtThreshold := ring.ring.Count()

	msg := &protocol.Message{
		DeliveryTag:  expectedSpill,
		Body:         []byte("at-threshold"),
		DeliveryMode: 1,
	}
	if err := ds.StoreMessage("test-spill-p3", msg); err != nil {
		t.Fatalf("StoreMessage at threshold failed: %v", err)
	}
	countAfterAtThreshold := ring.ring.Count()

	if countAfterAtThreshold != countAtThreshold+1 {
		t.Errorf("at threshold (count==threshold), message should go to ring: got count %d, want %d",
			countAfterAtThreshold, countAtThreshold+1)
	}

	spillMsg := &protocol.Message{
		DeliveryTag:  expectedSpill + 1,
		Body:         []byte("spilled"),
		DeliveryMode: 1,
	}
	if err := ds.StoreMessage("test-spill-p3", spillMsg); err != nil {
		t.Fatalf("StoreMessage above threshold failed: %v", err)
	}
	countAfterSpill := ring.ring.Count()

	if countAfterSpill != countAfterAtThreshold {
		t.Errorf("above threshold, message should spill to WAL: ring count got %d, want %d (unchanged)",
			countAfterSpill, countAfterAtThreshold)
	}
}
