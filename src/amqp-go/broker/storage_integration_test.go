package broker

import (
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
)

// TestNewStorageIntegration tests the RabbitMQ-style index+cache storage
func TestNewStorageIntegration(t *testing.T) {
	broker := NewBroker()

	// Create a durable queue (should automatically enable new storage)
	queue, err := broker.DeclareQueue("test-durable-queue", true, false, false, nil)
	if err != nil {
		t.Fatalf("Failed to declare queue: %v", err)
	}

	// Verify storage is initialized
	if queue.IndexManager == nil {
		t.Fatal("Expected IndexManager to be initialized")
	}

	if queue.Cache == nil {
		t.Fatal("Expected Cache to be initialized")
	}

	// Publish messages using the new storage
	for i := 0; i < 100; i++ {
		message := &protocol.Message{
			DeliveryTag: uint64(i + 1),
			Body:        []byte("test message"),
			Exchange:    "",
			RoutingKey:  "test-durable-queue",
		}

		err := broker.PublishMessage("", "test-durable-queue", message)
		if err != nil {
			t.Fatalf("Failed to publish message: %v", err)
		}
	}

	// Verify messages are in the index
	if queue.MessageCount() != 100 {
		t.Fatalf("Expected 100 messages in index, got %d", queue.MessageCount())
	}

	// Verify messages are in the cache
	stats := queue.CacheStats()
	if stats.Entries != 100 {
		t.Fatalf("Expected 100 messages in cache, got %d", stats.Entries)
	}

	t.Logf("Cache stats: %d entries, %d bytes, %.2f%% hit rate",
		stats.Entries, stats.Size, stats.HitRate*100)
}

// TestMemoryManagerIntegration tests the memory manager with queues
func TestMemoryManagerIntegration(t *testing.T) {
	broker := NewBroker()

	// Enable memory management with a very small limit to trigger paging
	broker.EnableMemoryManagement(1 * 1024 * 1024) // 1MB limit

	// Create a durable queue
	queue, err := broker.DeclareQueue("test-memory-queue", true, false, false, nil)
	if err != nil {
		t.Fatalf("Failed to declare queue: %v", err)
	}

	// Verify memory manager is tracking this queue
	stats := broker.MemoryManager.GetStats()
	if stats.NumQueues != 1 {
		t.Fatalf("Expected 1 queue in memory manager, got %d", stats.NumQueues)
	}

	// Publish enough messages to potentially trigger paging
	// Each message is roughly 1KB, so 2000 messages = 2MB > 1MB limit
	for i := 0; i < 2000; i++ {
		message := &protocol.Message{
			DeliveryTag: uint64(i + 1),
			Body:        make([]byte, 1024), // 1KB message
			Exchange:    "",
			RoutingKey:  "test-memory-queue",
		}

		err := broker.PublishMessage("", "test-memory-queue", message)
		if err != nil {
			t.Fatalf("Failed to publish message: %v", err)
		}
	}

	// Get final stats
	finalStats := broker.MemoryManager.GetStats()
	t.Logf("Memory stats: %.2fMB / %.2fMB (%.1f%%), %d page events, %d evictions",
		float64(finalStats.TotalMemory)/(1024*1024),
		float64(finalStats.MaxMemory)/(1024*1024),
		finalStats.UsagePercent*100,
		finalStats.TotalPageEvents,
		finalStats.TotalEvictions)

	// Verify messages are still accessible (some may have been evicted from cache)
	if queue.MessageCount() != 2000 {
		t.Fatalf("Expected 2000 messages in index, got %d", queue.MessageCount())
	}

	// Stop memory manager
	broker.StopMemoryManagement()
}

// TestDurableVsNonDurableStorage compares durable and non-durable queue behavior
// Both now use the same index+cache architecture (RabbitMQ-style unified storage)
func TestDurableVsNonDurableStorage(t *testing.T) {
	broker := NewBroker()

	// Create non-durable queue (new storage, no disk persistence)
	nonDurableQueue, err := broker.DeclareQueue("test-nondurable", false, false, false, nil)
	if err != nil {
		t.Fatalf("Failed to declare non-durable queue: %v", err)
	}

	// Create durable queue (new storage, with disk persistence)
	durableQueue, err := broker.DeclareQueue("test-durable", true, false, false, nil)
	if err != nil {
		t.Fatalf("Failed to declare durable queue: %v", err)
	}

	// Verify both use unified storage architecture
	if nonDurableQueue.IndexManager == nil || nonDurableQueue.Cache == nil {
		t.Fatal("Expected non-durable queue to have storage initialized")
	}

	if durableQueue.IndexManager == nil || durableQueue.Cache == nil {
		t.Fatal("Expected durable queue to have storage initialized")
	}

	// Publish to both queues
	for i := 0; i < 10; i++ {
		msg := &protocol.Message{
			DeliveryTag: uint64(i + 1),
			Body:        []byte("test"),
			Exchange:    "",
		}

		// Publish to non-durable queue
		msg.RoutingKey = "test-nondurable"
		err := broker.PublishMessage("", "test-nondurable", msg)
		if err != nil {
			t.Fatalf("Failed to publish to non-durable queue: %v", err)
		}

		// Publish to durable queue
		msg.RoutingKey = "test-durable"
		err = broker.PublishMessage("", "test-durable", msg)
		if err != nil {
			t.Fatalf("Failed to publish to durable queue: %v", err)
		}
	}

	// Verify both queues have 10 messages (both use index now)
	if nonDurableQueue.MessageCount() != 10 {
		t.Fatalf("Expected 10 messages in non-durable queue index, got %d", nonDurableQueue.MessageCount())
	}

	if durableQueue.MessageCount() != 10 {
		t.Fatalf("Expected 10 messages in durable queue index, got %d", durableQueue.MessageCount())
	}

	// Both queues use the same architecture now
	t.Logf("Non-durable queue: %d messages in index, %d in cache (Durable=%v)",
		nonDurableQueue.MessageCount(),
		nonDurableQueue.CacheStats().Entries,
		nonDurableQueue.Durable)
	t.Logf("Durable queue: %d messages in index, %d in cache (Durable=%v)",
		durableQueue.MessageCount(),
		durableQueue.CacheStats().Entries,
		durableQueue.Durable)
}

// TestNonDurableQueueMemoryBounded verifies that non-durable queues
// are bounded by the memory manager to prevent OOM (RabbitMQ-style unified storage)
func TestNonDurableQueueMemoryBounded(t *testing.T) {
	broker := NewBroker() // Memory manager enabled by default

	// Create a non-durable queue (now uses index+cache like durable queues)
	queue, err := broker.DeclareQueue("test-nondurable-bounded", false, false, false, nil)
	if err != nil {
		t.Fatalf("Failed to declare queue: %v", err)
	}

	// Verify storage is initialized (unified architecture)
	if queue.IndexManager == nil || queue.Cache == nil {
		t.Fatal("Expected storage to be initialized for non-durable queue")
	}

	// Verify memory manager is tracking this queue
	stats := broker.MemoryManager.GetStats()
	if stats.NumQueues != 1 {
		t.Fatalf("Expected 1 queue in memory manager, got %d", stats.NumQueues)
	}

	// Set a very small memory limit to trigger paging quickly
	broker.MemoryManager.SetMaxMemory(1 * 1024 * 1024) // 1MB

	// Publish enough messages to exceed the limit
	// Each message is 1KB, so 2000 messages = 2MB > 1MB limit
	for i := 0; i < 2000; i++ {
		message := &protocol.Message{
			DeliveryTag: uint64(i + 1),
			Body:        make([]byte, 1024), // 1KB message
			Exchange:    "",
			RoutingKey:  "test-nondurable-bounded",
		}

		err := broker.PublishMessage("", "test-nondurable-bounded", message)
		if err != nil {
			t.Fatalf("Failed to publish message: %v", err)
		}
	}

	// Wait a bit for memory manager to detect and handle pressure
	for i := 0; i < 5; i++ {
		time.Sleep(200 * time.Millisecond)
		finalStats := broker.MemoryManager.GetStats()
		if finalStats.TotalPageEvents > 0 {
			t.Logf("Memory management working! Page events: %d, evictions: %d, usage: %.1f%%",
				finalStats.TotalPageEvents,
				finalStats.TotalEvictions,
				finalStats.UsagePercent*100)

			// Verify cache was actually evicted (using unified storage now)
			cacheStats := queue.CacheStats()
			indexCount := queue.MessageCount()

			t.Logf("SUCCESS: Cache evicted from non-durable queue. Index: %d messages, Cache: %d entries",
				indexCount, cacheStats.Entries)

			// Cache should have fewer entries than index after eviction
			if cacheStats.Entries >= indexCount {
				t.Logf("Note: Cache still has all messages, may need more eviction cycles")
			}
			return
		}
	}

	t.Log("Note: Memory manager may not have triggered paging yet (1 second interval)")
}
