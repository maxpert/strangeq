package storage

import (
	"sync/atomic"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
)

// Benchmarks for the synchronous group-commit WAL write path (the durable
// publish fsync path). Run with default WALConfig so numbers reflect
// production defaults (BatchSize=1000, BatchTimeout=10ms).

func newBenchWALManager(b *testing.B) *WALManager {
	b.Helper()
	wm, err := NewWALManagerWithConfig(b.TempDir(), DefaultWALConfig())
	if err != nil {
		b.Fatalf("NewWALManagerWithConfig: %v", err)
	}
	b.Cleanup(func() { _ = wm.Close() })
	return wm
}

// BenchmarkWALWrite_Sequential measures a lone synchronous writer — the shape
// of a single fast publisher (durable publish, or a transient publisher in
// the spill path).
func BenchmarkWALWrite_Sequential(b *testing.B) {
	wm := newBenchWALManager(b)
	body := make([]byte, 256)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		msg := &protocol.Message{
			Body:         body,
			RoutingKey:   "bench-q",
			DeliveryMode: 2,
			DeliveryTag:  uint64(i + 1),
		}
		if err := wm.Write("bench-q", msg, uint64(i+1)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWALWrite_Parallel measures concurrent synchronous writers — the
// group-commit case where many publisher connections share one fsync.
func BenchmarkWALWrite_Parallel(b *testing.B) {
	wm := newBenchWALManager(b)
	body := make([]byte, 256)
	var tag atomic.Uint64

	b.ReportAllocs()
	b.SetParallelism(16) // 16*GOMAXPROCS concurrent writers
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			t := tag.Add(1)
			msg := &protocol.Message{
				Body:         body,
				RoutingKey:   "bench-q",
				DeliveryMode: 2,
				DeliveryTag:  t,
			}
			if err := wm.Write("bench-q", msg, t); err != nil {
				b.Fatal(err)
			}
		}
	})
}
