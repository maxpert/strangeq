package protocol

import (
	"sync"
	"time"
)

// MessageState represents the state of a message in the queue
type MessageState uint8

const (
	// StateReady indicates message is in cache and ready for delivery
	StateReady MessageState = iota
	// StateUnacked indicates message has been delivered and awaiting acknowledgment
	StateUnacked
	// StatePaged indicates message body is on disk and needs to be loaded
	StatePaged
)

// String returns the string representation of MessageState
func (s MessageState) String() string {
	switch s {
	case StateReady:
		return "READY"
	case StateUnacked:
		return "UNACKED"
	case StatePaged:
		return "PAGED"
	default:
		return "UNKNOWN"
	}
}

// MessageIndex represents metadata about a message in the queue.
// This is kept in memory to provide fast lookups without loading the full message body.
// Typical size: ~64-80 bytes per message (vs 1KB+ for full message)
type MessageIndex struct {
	// Core identifiers
	DeliveryTag uint64 // Unique identifier for this message delivery

	// Hot metadata (frequently accessed)
	RoutingKey string // Routing key used for this message
	Exchange   string // Exchange the message was published to

	// Size and state
	Size  uint32       // Message body size in bytes
	State MessageState // Current state (ready/unacked/paged)

	// Storage
	CacheKey   string // Key for cache lookup (usually: queueName:deliveryTag)
	DiskOffset uint64 // Storage offset for paged messages

	// LRU tracking
	Timestamp  int64 // Unix timestamp for LRU eviction
	LastAccess int64 // Last access time for cache policy

	// AMQP properties
	Priority     uint8 // Message priority (0-9)
	DeliveryMode uint8 // 1=non-persistent, 2=persistent

	// Flags
	Redelivered bool // Was this message redelivered?
}

// NewMessageIndex creates a new message index entry from a message
func NewMessageIndex(deliveryTag uint64, message *Message, queueName string) *MessageIndex {
	now := time.Now().Unix()
	return &MessageIndex{
		DeliveryTag:  deliveryTag,
		RoutingKey:   message.RoutingKey,
		Exchange:     message.Exchange,
		Size:         uint32(len(message.Body)),
		State:        StateReady,
		CacheKey:     makeCacheKey(queueName, deliveryTag),
		Timestamp:    now,
		LastAccess:   now,
		Priority:     message.Priority,
		DeliveryMode: message.DeliveryMode,
		Redelivered:  message.Redelivered,
	}
}

// Touch updates the last access time for LRU tracking
func (idx *MessageIndex) Touch() {
	idx.LastAccess = time.Now().Unix()
}

// MarkPaged marks the message as paged to disk
func (idx *MessageIndex) MarkPaged(diskOffset uint64) {
	idx.State = StatePaged
	idx.DiskOffset = diskOffset
}

// MarkReady marks the message as ready in cache
func (idx *MessageIndex) MarkReady() {
	idx.State = StateReady
	idx.Touch()
}

// MarkUnacked marks the message as delivered and awaiting ACK
func (idx *MessageIndex) MarkUnacked() {
	idx.State = StateUnacked
}

// IsPaged returns true if the message body is on disk
func (idx *MessageIndex) IsPaged() bool {
	return idx.State == StatePaged
}

// IsReady returns true if the message is ready for delivery
func (idx *MessageIndex) IsReady() bool {
	return idx.State == StateReady
}

// IsUnacked returns true if the message is awaiting acknowledgment
func (idx *MessageIndex) IsUnacked() bool {
	return idx.State == StateUnacked
}

// makeCacheKey generates a cache key from queue name and delivery tag
func makeCacheKey(queueName string, deliveryTag uint64) string {
	// Pre-allocate buffer to avoid allocations
	// Format: "queueName:deliveryTag"
	return queueName + ":" + string(rune(deliveryTag))
}

// MessageIndexManager manages a collection of message indices for a queue
// This provides O(1) lookups and maintains FIFO ordering
type MessageIndexManager struct {
	mu sync.RWMutex

	// Ordered list of indices (FIFO)
	indices []*MessageIndex

	// Fast lookup map: deliveryTag -> array index
	indexMap map[uint64]int

	// Statistics
	totalSize int64 // Total size of all indexed messages
	count     int   // Number of messages
}

// NewMessageIndexManager creates a new index manager
func NewMessageIndexManager() *MessageIndexManager {
	return &MessageIndexManager{
		indices:  make([]*MessageIndex, 0, 1024), // Pre-allocate for 1K messages
		indexMap: make(map[uint64]int, 1024),
	}
}

// Add appends a message index to the queue (FIFO)
func (m *MessageIndexManager) Add(idx *MessageIndex) {
	m.mu.Lock()
	defer m.mu.Unlock()

	position := len(m.indices)
	m.indices = append(m.indices, idx)
	m.indexMap[idx.DeliveryTag] = position
	m.totalSize += int64(idx.Size)
	m.count++
}

// Get retrieves a message index by delivery tag
func (m *MessageIndexManager) Get(deliveryTag uint64) (*MessageIndex, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	pos, exists := m.indexMap[deliveryTag]
	if !exists || pos >= len(m.indices) {
		return nil, false
	}

	idx := m.indices[pos]
	if idx != nil {
		idx.Touch() // Update LRU timestamp
	}
	return idx, true
}

// Remove deletes a message index by delivery tag
func (m *MessageIndexManager) Remove(deliveryTag uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	pos, exists := m.indexMap[deliveryTag]
	if !exists || pos >= len(m.indices) {
		return false
	}

	// Update statistics
	idx := m.indices[pos]
	if idx != nil {
		m.totalSize -= int64(idx.Size)
		m.count--
	}

	// Remove from map
	delete(m.indexMap, deliveryTag)

	// Mark slot as nil (preserve positions for other indices)
	m.indices[pos] = nil

	// If this is the first element, compact the array
	if pos == 0 {
		m.compactFront()
	}

	return true
}

// Peek returns the first message index without removing it
func (m *MessageIndexManager) Peek() (*MessageIndex, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Find first non-nil index
	for _, idx := range m.indices {
		if idx != nil {
			return idx, true
		}
	}

	return nil, false
}

// compactFront removes nil entries from the front of the array
// Must be called with lock held
func (m *MessageIndexManager) compactFront() {
	// Find first non-nil entry
	firstValid := 0
	for i, idx := range m.indices {
		if idx != nil {
			firstValid = i
			break
		}
	}

	if firstValid > 0 {
		// Shift array
		newIndices := m.indices[firstValid:]
		m.indices = make([]*MessageIndex, len(newIndices))
		copy(m.indices, newIndices)

		// Rebuild index map
		m.indexMap = make(map[uint64]int, len(m.indices))
		for i, idx := range m.indices {
			if idx != nil {
				m.indexMap[idx.DeliveryTag] = i
			}
		}
	}
}

// Len returns the number of messages in the index
func (m *MessageIndexManager) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.count
}

// TotalSize returns the total size of all indexed messages
func (m *MessageIndexManager) TotalSize() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.totalSize
}

// GetAll returns all message indices (for iteration)
// Returns a copy to avoid holding the lock
func (m *MessageIndexManager) GetAll() []*MessageIndex {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]*MessageIndex, 0, m.count)
	for _, idx := range m.indices {
		if idx != nil {
			result = append(result, idx)
		}
	}
	return result
}

// GetByIndex returns the message index at the given position (not delivery tag)
func (m *MessageIndexManager) GetByIndex(pos int) *MessageIndex {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if pos < 0 || pos >= len(m.indices) {
		return nil
	}
	return m.indices[pos]
}

// Clear removes all indices
func (m *MessageIndexManager) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.indices = make([]*MessageIndex, 0, 1024)
	m.indexMap = make(map[uint64]int, 1024)
	m.totalSize = 0
	m.count = 0
}
