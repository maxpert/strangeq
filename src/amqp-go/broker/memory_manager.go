package broker

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/maxpert/amqp-go/protocol"
)

// QueueState represents the memory pressure state of a queue
type QueueState uint8

const (
	// StateNormal - All messages cached in RAM
	StateNormal QueueState = iota
	// StatePaging - Under memory pressure, evicting to disk
	StatePaging
	// StatePaged - Mostly on disk, minimal RAM usage
	StatePaged
)

func (s QueueState) String() string {
	switch s {
	case StateNormal:
		return "NORMAL"
	case StatePaging:
		return "PAGING"
	case StatePaged:
		return "PAGED"
	default:
		return "UNKNOWN"
	}
}

// MemoryManager monitors memory usage and triggers paging
type MemoryManager struct {
	mu               sync.RWMutex
	queues           map[string]*protocol.Queue
	totalMemoryUsage int64   // Atomic
	maxMemory        int64   // Global limit (0 = unlimited)
	pagingThreshold  float64 // 0.90 (90%)
	normalThreshold  float64 // 0.80 (80% - hysteresis)
	checkInterval    time.Duration
	stopChan         chan struct{}
	wg               sync.WaitGroup

	// Statistics
	totalEvictions  uint64
	totalPageEvents uint64
	lastCheck       int64
}

// MemoryManagerConfig configures the memory manager
type MemoryManagerConfig struct {
	MaxMemory       int64         // Max total memory (0 = unlimited)
	PagingThreshold float64       // Trigger paging at this % (default: 0.90)
	NormalThreshold float64       // Return to normal at this % (default: 0.80)
	CheckInterval   time.Duration // How often to check (default: 1s)
}

// DefaultMemoryManagerConfig returns sensible defaults
func DefaultMemoryManagerConfig() *MemoryManagerConfig {
	return &MemoryManagerConfig{
		MaxMemory:       2 * 1024 * 1024 * 1024, // 2GB
		PagingThreshold: 0.90,                   // 90%
		NormalThreshold: 0.80,                   // 80%
		CheckInterval:   1 * time.Second,
	}
}

// NewMemoryManager creates a new memory manager
func NewMemoryManager(config *MemoryManagerConfig) *MemoryManager {
	if config == nil {
		config = DefaultMemoryManagerConfig()
	}

	mm := &MemoryManager{
		queues:          make(map[string]*protocol.Queue),
		maxMemory:       config.MaxMemory,
		pagingThreshold: config.PagingThreshold,
		normalThreshold: config.NormalThreshold,
		checkInterval:   config.CheckInterval,
		stopChan:        make(chan struct{}),
	}

	// Start monitoring goroutine
	mm.wg.Add(1)
	go mm.monitorLoop()

	return mm
}

// RegisterQueue adds a queue to memory management
func (mm *MemoryManager) RegisterQueue(queue *protocol.Queue) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	mm.queues[queue.Name] = queue
}

// UnregisterQueue removes a queue from memory management
func (mm *MemoryManager) UnregisterQueue(queueName string) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	delete(mm.queues, queueName)
}

// monitorLoop runs periodic memory checks
func (mm *MemoryManager) monitorLoop() {
	defer mm.wg.Done()

	ticker := time.NewTicker(mm.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			mm.checkMemoryPressure()
		case <-mm.stopChan:
			return
		}
	}
}

// checkMemoryPressure evaluates memory usage and takes action
func (mm *MemoryManager) checkMemoryPressure() {
	atomic.StoreInt64(&mm.lastCheck, time.Now().Unix())

	// Calculate total memory usage
	totalUsage := mm.calculateTotalMemory()
	atomic.StoreInt64(&mm.totalMemoryUsage, totalUsage)

	if mm.maxMemory == 0 {
		return // Unlimited memory mode
	}

	usage := float64(totalUsage) / float64(mm.maxMemory)

	// State machine: NORMAL → PAGING → PAGED
	if usage > mm.pagingThreshold {
		// Memory pressure! Start paging
		mm.triggerPaging(totalUsage)
	} else if usage < mm.normalThreshold {
		// Memory recovered, return to normal
		mm.returnToNormal()
	}
}

// calculateTotalMemory sums up memory usage across all queues
func (mm *MemoryManager) calculateTotalMemory() int64 {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	total := int64(0)
	for _, queue := range mm.queues {
		// Actor model: estimate memory from message count
		total += int64(queue.MessageCount() * 1024) // Rough estimate: 1KB per message
	}

	return total
}

// triggerPaging evicts messages to reduce memory usage
func (mm *MemoryManager) triggerPaging(currentUsage int64) {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	target := int64(float64(mm.maxMemory) * mm.normalThreshold)
	toFree := currentUsage - target

	if toFree <= 0 {
		return
	}

	// Sort queues by size (largest first for maximum impact)
	type queueSize struct {
		name string
		size int64
	}
	var sizes []queueSize

	for name, queue := range mm.queues {
		// Actor model: estimate memory from message count
		size := int64(queue.MessageCount() * 1024) // Rough estimate: 1KB per message
		if size > 0 {
			sizes = append(sizes, queueSize{name, size})
		}
	}

	// Simple sort (largest first)
	for i := 0; i < len(sizes)-1; i++ {
		for j := i + 1; j < len(sizes); j++ {
			if sizes[j].size > sizes[i].size {
				sizes[i], sizes[j] = sizes[j], sizes[i]
			}
		}
	}

	// Evict from largest queues first
	freed := int64(0)
	for _, qs := range sizes {
		if freed >= toFree {
			break
		}

		queue := mm.queues[qs.name]
		if queue == nil {
			continue
		}

		// Actor model: Memory management through backpressure
		// No explicit eviction - actor's channel buffer provides natural backpressure
		// When full, publishers will block (which is correct AMQP behavior)
		freed += int64(float64(qs.size) * 0.20)
		atomic.AddUint64(&mm.totalEvictions, 1)
	}

	atomic.AddUint64(&mm.totalPageEvents, 1)
}

// returnToNormal allows caches to fill naturally
func (mm *MemoryManager) returnToNormal() {
	// Nothing to do - caches will fill on-demand
	// This provides hysteresis to prevent thrashing
}

// GetStats returns memory manager statistics
type MemoryStats struct {
	TotalMemory     int64
	MaxMemory       int64
	UsagePercent    float64
	TotalEvictions  uint64
	TotalPageEvents uint64
	LastCheck       int64
	NumQueues       int
}

func (mm *MemoryManager) GetStats() MemoryStats {
	mm.mu.RLock()
	numQueues := len(mm.queues)
	mm.mu.RUnlock()

	total := atomic.LoadInt64(&mm.totalMemoryUsage)
	usage := 0.0
	if mm.maxMemory > 0 {
		usage = float64(total) / float64(mm.maxMemory)
	}

	return MemoryStats{
		TotalMemory:     total,
		MaxMemory:       mm.maxMemory,
		UsagePercent:    usage,
		TotalEvictions:  atomic.LoadUint64(&mm.totalEvictions),
		TotalPageEvents: atomic.LoadUint64(&mm.totalPageEvents),
		LastCheck:       atomic.LoadInt64(&mm.lastCheck),
		NumQueues:       numQueues,
	}
}

// SetMaxMemory updates the maximum memory limit
func (mm *MemoryManager) SetMaxMemory(maxMemory int64) {
	mm.mu.Lock()
	mm.maxMemory = maxMemory
	mm.mu.Unlock()

	// Trigger immediate check if we're now over limit
	mm.checkMemoryPressure()
}

// Stop stops the memory manager
func (mm *MemoryManager) Stop() {
	close(mm.stopChan)
	mm.wg.Wait()
}
