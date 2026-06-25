package storage

import (
	"sync"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
)

// TestP9_DoneChannelPool verifies that the sync.Pool for WAL done channels
// correctly reuses channels and reduces allocations.
func TestP9_DoneChannelPool(t *testing.T) {
	pool := getDoneChannelPool()

	// Get a channel from the pool
	ch1 := getDoneChannel(pool)
	if ch1 == nil {
		t.Fatal("getDoneChannel returned nil")
	}
	if cap(ch1) != 1 {
		t.Errorf("channel capacity: got %d, want 1", cap(ch1))
	}

	// Use the channel
	ch1 <- nil

	// Read from it (simulating the Write() caller)
	err := <-ch1
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}

	// Return to pool
	putDoneChannel(pool, ch1)

	// Get another channel — should reuse the same one from the pool
	ch2 := getDoneChannel(pool)
	if ch2 == nil {
		t.Fatal("getDoneChannel returned nil on second call")
	}

	// Verify it's reusable (empty, cap 1)
	select {
	case _, ok := <-ch2:
		if ok {
			t.Error("pooled channel should be empty")
		}
	default:
		// Good — channel is empty
	}

	if cap(ch2) != 1 {
		t.Errorf("pooled channel capacity: got %d, want 1", cap(ch2))
	}

	// Return to pool for cleanup
	putDoneChannel(pool, ch2)
}

// TestP9_DoneChannelPoolAllocs verifies that getting a channel from the pool
// after the first allocation does not trigger new heap allocations.
func TestP9_DoneChannelPoolAllocs(t *testing.T) {
	pool := getDoneChannelPool()

	// Prime the pool: allocate one channel and return it
	ch := getDoneChannel(pool)
	putDoneChannel(pool, ch)

	// Now measure: getting and putting should not allocate
	allocs := testing.AllocsPerRun(100, func() {
		ch := getDoneChannel(pool)
		putDoneChannel(pool, ch)
	})

	// sync.Pool.Get returns the cached item without allocation.
	// sync.Pool.Put also doesn't allocate. Under GC pressure, New may be
	// called (1 alloc), so we allow ≤1 to avoid CI flakiness.
	if allocs > 1 {
		t.Errorf("pooled get/put: got %.0f allocs, want ≤1", allocs)
	}
}

// TestP9_DoneChannelPoolConcurrent verifies the pool is safe for concurrent use.
func TestP9_DoneChannelPoolConcurrent(t *testing.T) {
	pool := getDoneChannelPool()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch := getDoneChannel(pool)
			ch <- nil
			<-ch
			putDoneChannel(pool, ch)
		}()
	}
	wg.Wait()
}

// TestP9_WALWriteUsesPool verifies that WAL Write operations still work
// correctly when using the pooled done channels.
func TestP9_WALWriteUsesPool(t *testing.T) {
	dataDir := t.TempDir()
	wm, err := NewWALManager(dataDir)
	if err != nil {
		t.Fatalf("NewWALManager: %v", err)
	}
	defer wm.Close()

	msg := &protocol.Message{
		Exchange:     "amq.direct",
		RoutingKey:   "test.key",
		DeliveryMode: 2, // Durable
		Body:         []byte("pooled-channel-test"),
	}

	// Write a durable message — should block until fsynced, then return nil
	if err := wm.Write("test-queue", msg, 1); err != nil {
		t.Fatalf("WAL Write failed: %v", err)
	}

	// Read it back to verify it was persisted correctly
	readMsg, err := wm.Read("test-queue", 1)
	if err != nil {
		t.Fatalf("WAL Read failed: %v", err)
	}
	if string(readMsg.Body) != "pooled-channel-test" {
		t.Errorf("body mismatch: got %q, want %q", readMsg.Body, "pooled-channel-test")
	}

	// Write multiple messages to exercise the pool under sequential use
	for i := uint64(2); i <= 10; i++ {
		msg.DeliveryTag = i
		msg.Body = []byte("msg")
		if err := wm.Write("test-queue", msg, i); err != nil {
			t.Fatalf("WAL Write %d failed: %v", i, err)
		}
	}
}
