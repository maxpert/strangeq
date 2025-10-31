package protocol

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/Workiva/go-datastructures/queue"
)

// FrameMailbox represents an unbounded mailbox for AMQP frames.
// This matches RabbitMQ's Erlang mailbox architecture where each subsystem
// has its own unbounded queue and handler goroutine.
//
// The reader NEVER blocks on enqueue, allowing the system to handle
// bursts of frames without blocking frame reception. Memory grows dynamically
// under load rather than blocking the reader.
type FrameMailbox struct {
	queue          *queue.Queue // Unbounded queue (Workiva/go-datastructures)
	closed         atomic.Bool  // Atomic closed flag
	memoryEstimate atomic.Int64 // Estimated memory usage in bytes for memory alarms
	name           string       // Mailbox name for logging/debugging
}

// ConnectionMailboxes represents the three separate mailboxes used in RabbitMQ architecture.
// Each mailbox has its own handler goroutine that processes frames independently.
//
// Architecture (matching RabbitMQ):
// - Heartbeat Mailbox: Processes heartbeat frames (FrameHeartbeat)
// - Channel Mailbox: Processes channel data frames (FrameMethod, FrameHeader, FrameBody)
// - Connection Mailbox: Processes connection control frames (Connection.Close, etc.)
//
// The TCP reader routes frames by type to the appropriate mailbox, ensuring:
// 1. Reader never blocks (all mailboxes are unbounded)
// 2. Heartbeats are processed independently (never starved by data frames)
// 3. Connection control is separated from data processing
type ConnectionMailboxes struct {
	Heartbeat  *FrameMailbox // Heartbeat frames (highest priority, separate handler)
	Channel    *FrameMailbox // Channel data frames (method, header, body)
	Connection *FrameMailbox // Connection control frames
	closed     atomic.Bool   // Overall closed state
}

var (
	// ErrMailboxClosed is returned when attempting to enqueue to a closed mailbox
	ErrMailboxClosed = errors.New("mailbox is closed")

	// Estimated frame size for memory calculations (conservative estimate)
	// Typical frame: 7-byte header + payload + 1-byte end marker
	// Average payload size: ~1KB for data frames
	estimatedFrameSize int64 = 1024

	// System memory threshold (configurable via MemoryLimitPercent or MemoryLimitBytes)
	// Calculated once at startup and cached
	memoryThresholdBytes int64
	memoryThresholdOnce  sync.Once

	// Configuration values set by SetMemoryConfig
	configMemoryPercent int
	configMemoryBytes   int64
)

// SetMemoryConfig sets memory configuration from server config.
// Must be called before any mailboxes are created.
func SetMemoryConfig(memoryPercent int, memoryBytes int64) {
	configMemoryPercent = memoryPercent
	configMemoryBytes = memoryBytes
}

// getSystemMemoryThreshold calculates and caches the memory threshold.
// Uses configuration values (MemoryLimitPercent or MemoryLimitBytes).
// This is calculated once at startup for efficiency.
func getSystemMemoryThreshold() int64 {
	memoryThresholdOnce.Do(func() {
		// If absolute limit is configured, use it
		if configMemoryBytes > 0 {
			memoryThresholdBytes = configMemoryBytes
			return
		}

		// Use percentage-based limit
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		// Get total system memory from runtime
		// Use Sys which is total memory obtained from OS
		systemMemory := int64(m.Sys)

		// If we can't detect system memory reliably, fall back to a conservative default
		if systemMemory == 0 {
			// Default to 16GB for modern systems
			systemMemory = 16 * 1024 * 1024 * 1024
		}

		// Apply configured percentage (default 60%)
		percent := configMemoryPercent
		if percent <= 0 || percent > 100 {
			percent = 60 // Default to 60% if invalid
		}
		memoryThresholdBytes = int64(float64(systemMemory) * float64(percent) / 100.0)

		// Ensure minimum threshold of 1GB even on small systems
		const minThreshold = 1 * 1024 * 1024 * 1024 // 1GB
		if memoryThresholdBytes < minThreshold {
			memoryThresholdBytes = minThreshold
		}
	})
	return memoryThresholdBytes
}

// NewFrameMailbox creates a new unbounded frame mailbox.
// initialCapacity: Initial capacity hint for the unbounded queue (not a limit)
func NewFrameMailbox(name string, initialCapacity int) *FrameMailbox {
	m := &FrameMailbox{
		queue: queue.New(int64(initialCapacity)),
		name:  name,
	}
	m.closed.Store(false)
	m.memoryEstimate.Store(0)
	return m
}

// NewConnectionMailboxes creates the three mailboxes for RabbitMQ-style architecture.
// Each mailbox is unbounded and has its own handler goroutine.
func NewConnectionMailboxes() *ConnectionMailboxes {
	return &ConnectionMailboxes{
		Heartbeat:  NewFrameMailbox("heartbeat", 100),  // Small initial size, heartbeats are rare
		Channel:    NewFrameMailbox("channel", 100000), // Large initial size for data frames
		Connection: NewFrameMailbox("connection", 100), // Small initial size, control frames are rare
	}
}

// ===== FrameMailbox Methods =====

// Enqueue adds a frame to the mailbox. This method NEVER blocks - it always succeeds (RabbitMQ-style).
// Returns ErrMailboxClosed if the mailbox has been closed.
//
// Memory grows dynamically under load. Use memory alarms to trigger backpressure
// at the application level (block publishers) rather than blocking the reader.
func (m *FrameMailbox) Enqueue(frame *Frame) error {
	if m.closed.Load() {
		return ErrMailboxClosed
	}

	// Enqueue to unbounded queue - never blocks!
	err := m.queue.Put(frame)
	if err != nil {
		return err
	}

	// Update memory estimate for memory alarms
	m.memoryEstimate.Add(estimatedFrameSize)

	return nil
}

// Dequeue removes and returns the next frame from the mailbox, blocking if empty.
// Returns (nil, false) if the mailbox is closed and empty.
func (m *FrameMailbox) Dequeue() (*Frame, bool) {
	if m.closed.Load() && m.queue.Empty() {
		return nil, false
	}

	items, err := m.queue.Get(1)
	if err != nil || len(items) == 0 {
		return nil, false
	}

	frame := items[0].(*Frame)
	m.memoryEstimate.Add(-estimatedFrameSize)
	return frame, true
}

// Close closes the mailbox and marks it as closed.
// After calling Close, Enqueue will return ErrMailboxClosed.
// Dequeue will continue to return frames until the queue is drained,
// then return (nil, false).
//
// It is safe to call Close multiple times.
func (m *FrameMailbox) Close() {
	if m.closed.Swap(true) {
		// Already closed
		return
	}

	m.queue.Dispose() // Dispose of unbounded queue
}

// Len returns the number of frames in the mailbox.
// This is useful for monitoring and debugging.
// Note: The result may be stale by the time you use it due to concurrent access.
func (m *FrameMailbox) Len() int {
	return int(m.queue.Len())
}

// Usage returns the mailbox usage based on estimated memory consumption.
// For unbounded queues, this is based on estimated memory (bytes) rather than capacity.
//
// Memory thresholds:
// - A value > 0.9 (90%) indicates memory alarm should trigger
// - A value < 0.8 (80%) indicates memory alarm should clear (hysteresis)
//
// This provides RabbitMQ-style memory pressure detection.
// RabbitMQ uses 60% of available RAM by default - we use the same approach.
func (m *FrameMailbox) Usage() float64 {
	// Get dynamic memory threshold (60% of system RAM, calculated once at startup)
	threshold := getSystemMemoryThreshold()

	estimatedMemory := m.memoryEstimate.Load()
	if estimatedMemory <= 0 {
		return 0.0
	}

	usage := float64(estimatedMemory) / float64(threshold)
	if usage > 1.0 {
		return 1.0 // Cap at 100%
	}
	return usage
}

// MemoryEstimate returns the estimated memory usage in bytes.
// This is used by memory alarms to trigger backpressure.
func (m *FrameMailbox) MemoryEstimate() int64 {
	return m.memoryEstimate.Load()
}

// ===== ConnectionMailboxes Methods =====

// Close closes all three mailboxes and marks the overall state as closed.
// It is safe to call Close multiple times.
func (cm *ConnectionMailboxes) Close() {
	if cm.closed.Swap(true) {
		// Already closed
		return
	}

	cm.Heartbeat.Close()
	cm.Channel.Close()
	cm.Connection.Close()
}
