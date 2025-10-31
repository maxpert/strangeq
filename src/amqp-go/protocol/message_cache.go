package protocol

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

// Default cache configuration
const (
	DefaultMaxCacheSize = 128 * 1024 * 1024 // 128MB per queue
	MinCacheSize        = 1 * 1024 * 1024   // 1MB minimum
)

// CacheEntry represents a cached message with LRU metadata
type CacheEntry struct {
	Message     *Message      // The actual message
	Size        uint32        // Message body size
	Element     *list.Element // Position in LRU doubly-linked list
	LastAccess  int64         // Unix timestamp of last access
	DeliveryTag uint64        // For quick identification
}

// MessageCache implements an LRU cache for message bodies
// This keeps frequently accessed messages in memory while evicting cold messages
type MessageCache struct {
	mu          sync.RWMutex
	cache       map[string]*CacheEntry // cacheKey -> entry
	lruList     *list.List             // Doubly-linked list for LRU ordering
	maxBytes    int64                  // Maximum cache size in bytes
	currentSize int64                  // Current size in bytes (atomic for stats)

	// Statistics
	hits      uint64 // Cache hits
	misses    uint64 // Cache misses
	evictions uint64 // Number of evictions
}

// NewMessageCache creates a new LRU message cache
func NewMessageCache(maxBytes int64) *MessageCache {
	if maxBytes < MinCacheSize {
		maxBytes = MinCacheSize
	}

	return &MessageCache{
		cache:    make(map[string]*CacheEntry, 1024),
		lruList:  list.New(),
		maxBytes: maxBytes,
	}
}

// Put adds a message to the cache
// If the cache is full, it evicts the least recently used message
func (c *MessageCache) Put(cacheKey string, deliveryTag uint64, message *Message) {
	c.mu.Lock()
	defer c.mu.Unlock()

	size := uint32(len(message.Body))

	// Check if message already exists (update case)
	if existing, exists := c.cache[cacheKey]; exists {
		// Update existing entry
		existing.Message = message
		existing.LastAccess = time.Now().Unix()
		// Move to front of LRU list (most recently used)
		c.lruList.MoveToFront(existing.Element)

		// Update size if changed
		oldSize := existing.Size
		existing.Size = size
		atomic.AddInt64(&c.currentSize, int64(size)-int64(oldSize))
		return
	}

	// Evict entries if we would exceed maxBytes
	for atomic.LoadInt64(&c.currentSize)+int64(size) > c.maxBytes && c.lruList.Len() > 0 {
		c.evictOldest()
	}

	// Create new cache entry
	entry := &CacheEntry{
		Message:     message,
		Size:        size,
		LastAccess:  time.Now().Unix(),
		DeliveryTag: deliveryTag,
	}

	// Add to front of LRU list (most recently used)
	entry.Element = c.lruList.PushFront(cacheKey)

	// Add to cache map
	c.cache[cacheKey] = entry

	// Update size
	atomic.AddInt64(&c.currentSize, int64(size))
}

// Get retrieves a message from the cache
// Returns the message and true if found, nil and false otherwise
func (c *MessageCache) Get(cacheKey string) (*Message, bool) {
	c.mu.RLock()
	entry, exists := c.cache[cacheKey]
	c.mu.RUnlock()

	if !exists {
		atomic.AddUint64(&c.misses, 1)
		return nil, false
	}

	// Update LRU (requires write lock)
	c.mu.Lock()
	entry.LastAccess = time.Now().Unix()
	c.lruList.MoveToFront(entry.Element)
	c.mu.Unlock()

	atomic.AddUint64(&c.hits, 1)
	return entry.Message, true
}

// Remove deletes a message from the cache
func (c *MessageCache) Remove(cacheKey string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, exists := c.cache[cacheKey]
	if !exists {
		return false
	}

	// Remove from LRU list
	c.lruList.Remove(entry.Element)

	// Remove from cache map
	delete(c.cache, cacheKey)

	// Update size
	atomic.AddInt64(&c.currentSize, -int64(entry.Size))

	return true
}

// evictOldest removes the least recently used entry from the cache
// Must be called with write lock held
func (c *MessageCache) evictOldest() {
	if c.lruList.Len() == 0 {
		return
	}

	// Get oldest element (back of list)
	oldest := c.lruList.Back()
	if oldest == nil {
		return
	}

	cacheKey := oldest.Value.(string)
	entry, exists := c.cache[cacheKey]
	if !exists {
		// Shouldn't happen, but handle gracefully
		c.lruList.Remove(oldest)
		return
	}

	// Remove from list and map
	c.lruList.Remove(oldest)
	delete(c.cache, cacheKey)

	// Update size
	atomic.AddInt64(&c.currentSize, -int64(entry.Size))
	atomic.AddUint64(&c.evictions, 1)
}

// EvictN evicts up to N oldest entries from the cache
// Returns the number of entries actually evicted
func (c *MessageCache) EvictN(n int) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	evicted := 0
	for i := 0; i < n && c.lruList.Len() > 0; i++ {
		c.evictOldest()
		evicted++
	}

	return evicted
}

// EvictBytes evicts entries until we free up at least targetBytes
// Returns the number of bytes actually freed
func (c *MessageCache) EvictBytes(targetBytes int64) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	freed := int64(0)
	startSize := atomic.LoadInt64(&c.currentSize)

	for freed < targetBytes && c.lruList.Len() > 0 {
		// Check current size before eviction
		beforeSize := atomic.LoadInt64(&c.currentSize)
		c.evictOldest()
		afterSize := atomic.LoadInt64(&c.currentSize)
		freed += (beforeSize - afterSize)
	}

	return startSize - atomic.LoadInt64(&c.currentSize)
}

// Clear removes all entries from the cache
func (c *MessageCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache = make(map[string]*CacheEntry, 1024)
	c.lruList = list.New()
	atomic.StoreInt64(&c.currentSize, 0)
}

// Len returns the number of entries in the cache
func (c *MessageCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.cache)
}

// CurrentSize returns the current cache size in bytes
func (c *MessageCache) CurrentSize() int64 {
	return atomic.LoadInt64(&c.currentSize)
}

// MaxSize returns the maximum cache size in bytes
func (c *MessageCache) MaxSize() int64 {
	return c.maxBytes
}

// Usage returns the current cache usage as a percentage (0.0 - 1.0)
func (c *MessageCache) Usage() float64 {
	current := float64(atomic.LoadInt64(&c.currentSize))
	max := float64(c.maxBytes)
	if max == 0 {
		return 0
	}
	return current / max
}

// Stats returns cache statistics
type CacheStats struct {
	Entries   int     // Number of entries
	Size      int64   // Current size in bytes
	MaxSize   int64   // Maximum size in bytes
	Usage     float64 // Usage percentage (0.0 - 1.0)
	Hits      uint64  // Cache hits
	Misses    uint64  // Cache misses
	HitRate   float64 // Hit rate percentage (0.0 - 1.0)
	Evictions uint64  // Number of evictions
}

// GetStats returns current cache statistics
func (c *MessageCache) GetStats() CacheStats {
	c.mu.RLock()
	entries := len(c.cache)
	c.mu.RUnlock()

	hits := atomic.LoadUint64(&c.hits)
	misses := atomic.LoadUint64(&c.misses)
	total := hits + misses

	hitRate := 0.0
	if total > 0 {
		hitRate = float64(hits) / float64(total)
	}

	return CacheStats{
		Entries:   entries,
		Size:      atomic.LoadInt64(&c.currentSize),
		MaxSize:   c.maxBytes,
		Usage:     c.Usage(),
		Hits:      hits,
		Misses:    misses,
		HitRate:   hitRate,
		Evictions: atomic.LoadUint64(&c.evictions),
	}
}

// SetMaxSize updates the maximum cache size
// This may trigger evictions if the new size is smaller
func (c *MessageCache) SetMaxSize(maxBytes int64) {
	if maxBytes < MinCacheSize {
		maxBytes = MinCacheSize
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.maxBytes = maxBytes

	// Evict entries if we're over the new limit
	for atomic.LoadInt64(&c.currentSize) > maxBytes && c.lruList.Len() > 0 {
		c.evictOldest()
	}
}

// Compact performs cache maintenance
// - Evicts entries that haven't been accessed in the last N seconds
// - Useful for long-running queues with bursty access patterns
func (c *MessageCache) Compact(maxAgeSec int64) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now().Unix()
	evicted := 0

	// Walk from back (oldest) to front
	for element := c.lruList.Back(); element != nil; {
		cacheKey := element.Value.(string)
		entry := c.cache[cacheKey]

		// Check if entry is too old
		if entry != nil && (now-entry.LastAccess) > maxAgeSec {
			// Save next element before removing current
			next := element.Prev()

			// Remove this entry
			c.lruList.Remove(element)
			delete(c.cache, cacheKey)
			atomic.AddInt64(&c.currentSize, -int64(entry.Size))
			evicted++

			element = next
		} else {
			// Stop when we hit a recent entry (list is ordered by access time)
			break
		}
	}

	return evicted
}
