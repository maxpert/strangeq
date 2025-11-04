package storage

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
)

// writeWithRetry writes a message with exponential backoff on channel full
func writeWithRetry(wm *WALManager, queueName string, msg *protocol.Message, offset uint64) error {
	backoff := time.Microsecond
	for {
		err := wm.Write(queueName, msg, offset)
		if err == nil {
			return nil
		}
		// If channel is full, back off and retry
		if err.Error() == "shared WAL write channel full" {
			time.Sleep(backoff)
			backoff *= 2
			if backoff > 10*time.Millisecond {
				backoff = 10 * time.Millisecond
			}
			continue
		}
		return err
	}
}

// BenchmarkSharedWAL_SingleQueueWrites benchmarks sequential writes to a single queue
func BenchmarkSharedWAL_SingleQueueWrites(b *testing.B) {
	tmpDir := b.TempDir()
	wm, err := NewWALManager(tmpDir)
	if err != nil {
		b.Fatal(err)
	}
	defer wm.Close()

	msg := &protocol.Message{
		Exchange:     "bench.exchange",
		RoutingKey:   "bench.key",
		Body:         make([]byte, 1024), // 1KB message
		DeliveryMode: 2,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := writeWithRetry(wm, "bench_queue", msg, uint64(i+1))
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSharedWAL_MultiQueueWrites benchmarks writes to multiple queues
func BenchmarkSharedWAL_MultiQueueWrites(b *testing.B) {
	tmpDir := b.TempDir()
	wm, err := NewWALManager(tmpDir)
	if err != nil {
		b.Fatal(err)
	}
	defer wm.Close()

	numQueues := 10
	msg := &protocol.Message{
		Exchange:     "bench.exchange",
		RoutingKey:   "bench.key",
		Body:         make([]byte, 1024), // 1KB message
		DeliveryMode: 2,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		queueID := i % numQueues
		queueName := fmt.Sprintf("bench_queue_%d", queueID)
		err := writeWithRetry(wm, queueName, msg, uint64(i+1))
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSharedWAL_ConcurrentWrites benchmarks concurrent writes
func BenchmarkSharedWAL_ConcurrentWrites(b *testing.B) {
	tmpDir := b.TempDir()
	wm, err := NewWALManager(tmpDir)
	if err != nil {
		b.Fatal(err)
	}
	defer wm.Close()

	msg := &protocol.Message{
		Exchange:     "bench.exchange",
		RoutingKey:   "bench.key",
		Body:         make([]byte, 1024), // 1KB message
		DeliveryMode: 2,
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := uint64(0)
		for pb.Next() {
			i++
			queueID := i % 10
			queueName := fmt.Sprintf("bench_queue_%d", queueID)
			err := writeWithRetry(wm, queueName, msg, i)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkSharedWAL_Read benchmarks read performance (with offset index)
func BenchmarkSharedWAL_Read(b *testing.B) {
	tmpDir := b.TempDir()
	wm, err := NewWALManager(tmpDir)
	if err != nil {
		b.Fatal(err)
	}
	defer wm.Close()

	// Pre-populate with messages
	numMessages := 10000
	msg := &protocol.Message{
		Exchange:     "bench.exchange",
		RoutingKey:   "bench.key",
		Body:         make([]byte, 1024), // 1KB message
		DeliveryMode: 2,
	}

	for i := 0; i < numMessages; i++ {
		err := writeWithRetry(wm, "bench_queue", msg, uint64(i+1))
		if err != nil {
			b.Fatal(err)
		}
	}
	// Allow batch to flush
	time.Sleep(50 * time.Millisecond)

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

// BenchmarkSharedWAL_Acknowledge benchmarks acknowledgment performance
func BenchmarkSharedWAL_Acknowledge(b *testing.B) {
	tmpDir := b.TempDir()
	wm, err := NewWALManager(tmpDir)
	if err != nil {
		b.Fatal(err)
	}
	defer wm.Close()

	// Pre-populate with messages
	numMessages := 10000
	msg := &protocol.Message{
		Exchange:     "bench.exchange",
		RoutingKey:   "bench.key",
		Body:         make([]byte, 1024), // 1KB message
		DeliveryMode: 2,
	}

	for i := 0; i < numMessages; i++ {
		err := writeWithRetry(wm, "bench_queue", msg, uint64(i+1))
		if err != nil {
			b.Fatal(err)
		}
	}
	// Allow batch to flush
	time.Sleep(50 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		offset := uint64((i % numMessages) + 1)
		wm.Acknowledge("bench_queue", offset)
	}
}

// BenchmarkSharedWAL_MessageSizes benchmarks different message sizes
func BenchmarkSharedWAL_MessageSizes(b *testing.B) {
	sizes := []int{
		100,     // 100 bytes
		1024,    // 1 KB
		10240,   // 10 KB
		102400,  // 100 KB
		1048576, // 1 MB
	}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size_%dB", size), func(b *testing.B) {
			tmpDir := b.TempDir()
			wm, err := NewWALManager(tmpDir)
			if err != nil {
				b.Fatal(err)
			}
			defer wm.Close()

			msg := &protocol.Message{
				Exchange:     "bench.exchange",
				RoutingKey:   "bench.key",
				Body:         make([]byte, size),
				DeliveryMode: 2,
			}

			b.SetBytes(int64(size))
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				err := writeWithRetry(wm, "bench_queue", msg, uint64(i+1))
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSharedWAL_Throughput measures sustained throughput
func BenchmarkSharedWAL_Throughput(b *testing.B) {
	tmpDir := b.TempDir()
	wm, err := NewWALManager(tmpDir)
	if err != nil {
		b.Fatal(err)
	}
	defer wm.Close()

	numWorkers := 10
	msg := &protocol.Message{
		Exchange:     "bench.exchange",
		RoutingKey:   "bench.key",
		Body:         make([]byte, 1024), // 1KB message
		DeliveryMode: 2,
	}

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	messagesPerWorker := b.N / numWorkers

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			queueName := fmt.Sprintf("bench_queue_%d", workerID)
			for i := 0; i < messagesPerWorker; i++ {
				offset := uint64(workerID*messagesPerWorker + i + 1)
				err := writeWithRetry(wm, queueName, msg, offset)
				if err != nil {
					b.Error(err)
					return
				}
			}
		}(w)
	}

	wg.Wait()
}

// BenchmarkSharedWAL_QueueScaling benchmarks scaling with different queue counts
func BenchmarkSharedWAL_QueueScaling(b *testing.B) {
	queueCounts := []int{1, 10, 100, 1000}

	for _, numQueues := range queueCounts {
		b.Run(fmt.Sprintf("Queues_%d", numQueues), func(b *testing.B) {
			tmpDir := b.TempDir()
			wm, err := NewWALManager(tmpDir)
			if err != nil {
				b.Fatal(err)
			}
			defer wm.Close()

			msg := &protocol.Message{
				Exchange:     "bench.exchange",
				RoutingKey:   "bench.key",
				Body:         make([]byte, 1024), // 1KB message
				DeliveryMode: 2,
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				queueID := i % numQueues
				queueName := fmt.Sprintf("bench_queue_%d", queueID)
				err := writeWithRetry(wm, queueName, msg, uint64(i+1))
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
