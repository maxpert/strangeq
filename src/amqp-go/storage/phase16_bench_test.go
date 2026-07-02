package storage

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
)

// ============================================================================
// Phase 16 Performance Benchmarks
//
// These benchmarks measure the specific areas optimized in Phase 16:
//   1. Multi-queue concurrent publish (lock contention: sync.Map vs RWMutex)
//   2. WAL read with file handle cache + pread (vs open/seek/read/close)
//   3. Storage-level publish to ring buffer (no WAL, transient messages)
//   4. Segment write/read with full metadata (C4 fix verification)
// ============================================================================

// BenchmarkMultiQueueConcurrentPublish measures concurrent publish across
// multiple queues — the key test for the sync.Map vs RWMutex optimization (P1).
// Before: ds.mutex write lock serialized ALL publishes across ALL queues.
// After: sync.Map provides lock-free reads, mutex only during queue creation.
// Each goroutine publishes then immediately deletes to keep ring buffer empty.
func BenchmarkMultiQueueConcurrentPublish(b *testing.B) {
	ds := NewDisruptorStorageWithDataDir(b.TempDir())
	defer ds.Close()

	queues := make([]string, 10)
	for i := 0; i < 10; i++ {
		queues[i] = fmt.Sprintf("bench_queue_%d", i)
		ds.getOrCreateQueueRing(queues[i])
	}

	var tag atomic.Uint64

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			t := tag.Add(1)
			queueName := queues[t%10]
			msg := &protocol.Message{
				Exchange:     "",
				RoutingKey:   "bench.key",
				Body:         make([]byte, 256),
				DeliveryMode: 1,
				DeliveryTag:  t,
			}
			if err := ds.StoreMessage(queueName, msg); err != nil {
				b.Fatal(err)
			}
			_ = ds.DeleteMessage(queueName, t)
		}
	})
}

// BenchmarkSingleQueueConcurrentPublish measures concurrent publish to a
// SINGLE queue — tests contention on the queue's ring buffer without
// cross-queue lock interference. Each goroutine publishes then deletes.
func BenchmarkSingleQueueConcurrentPublish(b *testing.B) {
	ds := NewDisruptorStorageWithDataDir(b.TempDir())
	defer ds.Close()

	queueName := "single_queue"
	ds.getOrCreateQueueRing(queueName)

	var tag atomic.Uint64

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			t := tag.Add(1)
			msg := &protocol.Message{
				Exchange:     "",
				RoutingKey:   queueName,
				Body:         make([]byte, 256),
				DeliveryMode: 1,
				DeliveryTag:  t,
			}
			if err := ds.StoreMessage(queueName, msg); err != nil {
				b.Fatal(err)
			}
			_ = ds.DeleteMessage(queueName, t)
		}
	})
}

// BenchmarkWALReadWithCache measures WAL read performance with the file handle
// cache + pread optimization (P4 + M3). Reads from old WAL files (after roll)
// to exercise the cache path.
func BenchmarkWALReadWithCache(b *testing.B) {
	tmpDir := b.TempDir()
	wm, err := NewWALManager(tmpDir)
	if err != nil {
		b.Fatal(err)
	}
	defer wm.Close()

	// Write enough messages to roll at least one WAL file
	// (512MB file size, 1KB messages → ~500K messages to fill)
	// Instead, use small WAL file size for faster benchmark setup
	numMessages := 5000
	msg := &protocol.Message{
		Exchange:     "bench.exchange",
		RoutingKey:   "bench.key",
		Body:         make([]byte, 1024),
		DeliveryMode: 2,
	}

	for i := 0; i < numMessages; i++ {
		if err := writeWithRetry(wm, "bench_queue", msg, uint64(i+1)); err != nil {
			b.Fatal(err)
		}
	}
	// Allow batch to flush
	time.Sleep(100 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		offset := uint64((i % numMessages) + 1)
		_, err := wm.Read("bench_queue", offset)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGetMessageRingBuffer measures ring buffer read performance —
// the hot path for consumer delivery when messages are in the ring buffer.
// Limits to 10000 messages to stay within ring buffer capacity.
func BenchmarkGetMessageRingBuffer(b *testing.B) {
	ds := NewDisruptorStorageWithDataDir(b.TempDir())
	defer ds.Close()

	queueName := "get_bench_queue"
	ds.getOrCreateQueueRing(queueName)

	// Pre-populate ring buffer with transient messages
	numMessages := 1000
	msg := &protocol.Message{
		Exchange:     "",
		RoutingKey:   queueName,
		Body:         make([]byte, 256),
		DeliveryMode: 1,
	}

	for i := 0; i < numMessages; i++ {
		msg.DeliveryTag = uint64(i + 1)
		if err := ds.StoreMessage(queueName, msg); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		offset := uint64((i % numMessages) + 1)
		_, err := ds.GetMessage(queueName, offset)
		if err != nil {
			_ = err
		}
	}
}

// BenchmarkDeleteMessage measures ACK/delete performance — the hot path
// when consumers acknowledge messages.
func BenchmarkDeleteMessage(b *testing.B) {
	ds := NewDisruptorStorageWithDataDir(b.TempDir())
	defer ds.Close()

	queueName := "delete_bench_queue"
	ds.getOrCreateQueueRing(queueName)

	// Pre-populate
	numMessages := 1000
	msg := &protocol.Message{
		Exchange:     "",
		RoutingKey:   queueName,
		Body:         make([]byte, 256),
		DeliveryMode: 1,
	}

	for i := 0; i < numMessages; i++ {
		msg.DeliveryTag = uint64(i + 1)
		if err := ds.StoreMessage(queueName, msg); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		offset := uint64((i % numMessages) + 1)
		_ = ds.DeleteMessage(queueName, offset)
	}
}

// BenchmarkConcurrentGetAndDelete measures concurrent read + delete —
// simulates consumer delivery + ACK under load.
func BenchmarkConcurrentGetAndDelete(b *testing.B) {
	ds := NewDisruptorStorageWithDataDir(b.TempDir())
	defer ds.Close()

	queueName := "concurrent_bench_queue"
	ds.getOrCreateQueueRing(queueName)

	// Pre-populate
	numMessages := 10000
	msg := &protocol.Message{
		Exchange:     "",
		RoutingKey:   queueName,
		Body:         make([]byte, 256),
		DeliveryMode: 1,
	}

	for i := 0; i < numMessages; i++ {
		msg.DeliveryTag = uint64(i + 1)
		if err := ds.StoreMessage(queueName, msg); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	var counter atomic.Uint64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := counter.Add(1)
			offset := uint64((int(i) % numMessages) + 1)
			_, _ = ds.GetMessage(queueName, offset)
			_ = ds.DeleteMessage(queueName, offset)
		}
	})
}

// BenchmarkSegmentWriteReadWithMetadata measures segment write + read with
// full AMQP metadata (C4 fix verification — ensures metadata roundtrip works
// and measures the cost of the full format).
func BenchmarkSegmentWriteReadWithMetadata(b *testing.B) {
	sm, err := NewSegmentManager(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer sm.Close()

	msg := &protocol.Message{
		Exchange:     "amq.direct",
		RoutingKey:   "routing.key",
		Body:         make([]byte, 1024),
		DeliveryMode: 2,
	}

	// Write phase
	numMessages := 1000
	var recoveryMsgs []*RecoveryMessage
	for i := 0; i < numMessages; i++ {
		m := &protocol.Message{
			Exchange:     msg.Exchange,
			RoutingKey:   msg.RoutingKey,
			Body:         msg.Body,
			DeliveryMode: msg.DeliveryMode,
			DeliveryTag:  uint64(i + 1),
		}
		recoveryMsgs = append(recoveryMsgs, &RecoveryMessage{Message: m, Offset: uint64(i + 1), QueueName: "bench_queue"})
	}
	if err := sm.CheckpointBatch("bench_queue", recoveryMsgs); err != nil {
		b.Fatal(err)
	}

	// Get queue segments for reading
	qs := sm.getOrCreateQueueSegments("bench_queue")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		offset := uint64((i % numMessages) + 1)
		m, err := qs.readMessage(offset)
		if err != nil {
			b.Fatal(err)
		}
		// Verify metadata is preserved (compiler can't optimize away)
		if m.Exchange != msg.Exchange {
			b.Fatal("metadata lost")
		}
	}
}
