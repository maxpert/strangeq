package storage

import (
	"fmt"

	"github.com/maxpert/amqp-go/protocol"
)

// UnifiedStore provides a clean abstraction over the three-tier storage architecture
// It hides the complexity of Ring Buffer → WAL → Segments from callers
type UnifiedStore struct {
	ring     *[DefaultRingBufferSize]*protocol.Message
	wal      *WALManager
	segments *SegmentManager
}

// NewUnifiedStore creates a new unified storage abstraction
func NewUnifiedStore(ring *[DefaultRingBufferSize]*protocol.Message, wal *WALManager, segments *SegmentManager) *UnifiedStore {
	return &UnifiedStore{
		ring:     ring,
		wal:      wal,
		segments: segments,
	}
}

// Get retrieves a message by offset, checking all tiers
// Returns the message from the hottest tier available
func (us *UnifiedStore) Get(queueName string, offset uint64) (*protocol.Message, error) {
	// Tier 1: Try ring buffer (hot, O(1))
	if us.ring != nil {
		index := offset & RingBufferMask
		msg := us.ring[index]
		if msg != nil && msg.DeliveryTag == offset {
			return msg, nil
		}
	}

	// Tier 2: Try WAL (warm, sequential scan)
	if us.wal != nil {
		msg, err := us.wal.Read(queueName, offset)
		if err == nil {
			return msg, nil
		}
	}

	// Tier 3: Try segments (cold, with index)
	if us.segments != nil {
		msg, err := us.segments.Read(queueName, offset)
		if err == nil {
			return msg, nil
		}
	}

	return nil, ErrMessageNotFound
}

// Put stores a message in the appropriate tier
// Hot messages go to ring buffer, spilled messages go to WAL
func (us *UnifiedStore) Put(queueName string, message *protocol.Message, ringCount uint64) error {
	// Check if ring buffer has space (< 80% full)
	if ringCount < SpillThreshold {
		// Store in ring buffer
		// Caller handles ring buffer writes
		return nil
	}

	// Ring buffer full - write to WAL
	if us.wal != nil {
		return us.wal.Write(queueName, message, message.DeliveryTag)
	}

	return nil
}

// Acknowledge marks a message as ACKed across all tiers
func (us *UnifiedStore) Acknowledge(queueName string, offset uint64) {
	// Notify WAL for cleanup
	if us.wal != nil {
		us.wal.Acknowledge(queueName, offset)
	}

	// Notify segments for compaction
	if us.segments != nil {
		us.segments.Acknowledge(queueName, offset)
	}
}

// Close closes all storage tiers
func (us *UnifiedStore) Close() error {
	if us.wal != nil {
		_ = us.wal.Close()
	}
	if us.segments != nil {
		_ = us.segments.Close()
	}
	return nil
}

// ErrMessageNotFound is returned when a message is not found in any tier
var ErrMessageNotFound = fmt.Errorf("message not found")
