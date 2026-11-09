package storage

// Focused N1 benchmark: the cold read of a record in the ACTIVE WAL file, with
// and without a committer driving durable (fsynced) group commits on another
// queue. It is deliberately narrow — it profiles readMessage against
// commitBatch, it does not measure end-to-end broker throughput (that is the
// owner's load matrix).
//
// Run with a CPU profile of the read path:
//
//	go test ./storage -run '^$' -bench 'BenchmarkWALActiveRead_Committing$' \
//	    -benchtime 3s -cpuprofile cpu.prof
//	go tool pprof -top cpu.prof
//
// The writers perform REAL durable WAL writes (their own fdatasync/F_FULLFSYNC);
// there is no synthetic load goroutine.

import (
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
)

const (
	activeReadBenchQueue  = "active.read.bench"
	activeReadWriterQueue = "active.read.writer"
)

// benchActiveReadFixture pre-writes records that stay in the ACTIVE file (a
// huge FileSize keeps the whole benchmark in file 1) and returns the manager.
func benchActiveReadFixture(b *testing.B, preWritten int) (*WALManager, uint64) {
	b.Helper()
	const fileSize = 4 << 30 // 4 GiB: no roll for the duration of the benchmark
	cfg := DefaultWALConfig()
	cfg.FileSize = fileSize
	cfg.CleanupInterval = time.Hour
	cfg.CompactionInterval = time.Hour
	cfg.RetentionPeriod = 0

	wm, err := NewWALManagerWithConfig(b.TempDir(), cfg)
	if err != nil {
		b.Fatal(err)
	}

	body := make([]byte, 1024)
	for i := range body {
		body[i] = byte(i)
	}
	for i := 0; i < preWritten; i++ {
		if err := wm.Write(activeReadBenchQueue, &protocol.Message{
			RoutingKey:   activeReadBenchQueue,
			Body:         body,
			DeliveryMode: 2,
			DeliveryTag:  uint64(i + 1),
			MessageID:    "bench",
		}, uint64(i+1)); err != nil {
			b.Fatal(err)
		}
	}
	if ar := wm.sharedWAL.activeRead.Load(); ar == nil || ar.num != wm.sharedWAL.fileNum.Load() {
		b.Fatalf("premise: the benchmark records must be in the ACTIVE file (fileNum=%d)",
			wm.sharedWAL.fileNum.Load())
	}
	return wm, uint64(preWritten)
}

// startDurableWriters starts n goroutines that append durable records forever
// (until stop is closed), keeping the committer's group-commit critical section
// continuously occupied. Returns a stop function that joins them.
func startDurableWriters(wm *WALManager, n int) (stop func()) {
	var stopCh = make(chan struct{})
	var wg sync.WaitGroup
	var next atomic.Uint64
	for w := 0; w < n; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			body := make([]byte, 1024)
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				off := uint64(1) << 40
				off += next.Add(1)
				if err := wm.Write(activeReadWriterQueue, &protocol.Message{
					RoutingKey:   activeReadWriterQueue,
					Body:         body,
					DeliveryMode: 2,
					DeliveryTag:  off,
					MessageID:    "w",
				}, off); err != nil {
					return
				}
			}
		}()
	}
	return func() { close(stopCh); wg.Wait() }
}

// benchActiveRead is the shared body: read `preWritten` records in the active
// file under b.RunParallel, timing each read, while `writers` durable writers
// (0 for the idle arm) keep the committer busy.
func benchActiveRead(b *testing.B, writers int, preWritten int) {
	wm, n := benchActiveReadFixture(b, preWritten)
	defer func() { _ = wm.Close() }()

	var stop func()
	if writers > 0 {
		stop = startDurableWriters(wm, writers)
	}

	var mu sync.Mutex
	var samples []time.Duration

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		local := make([]time.Duration, 0, 4096)
		i := uint64(0)
		for pb.Next() {
			tag := (i % n) + 1
			i++
			start := time.Now()
			if _, err := wm.Read(activeReadBenchQueue, tag); err != nil {
				b.Errorf("read %d: %v", tag, err)
				return
			}
			local = append(local, time.Since(start))
		}
		mu.Lock()
		samples = append(samples, local...)
		mu.Unlock()
	})
	b.StopTimer()

	if stop != nil {
		stop()
	}

	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	pct := func(q float64) time.Duration {
		if len(samples) == 0 {
			return 0
		}
		idx := int(q * float64(len(samples)))
		if idx >= len(samples) {
			idx = len(samples) - 1
		}
		return samples[idx]
	}
	b.ReportMetric(float64(pct(0.50).Nanoseconds()), "p50-ns")
	b.ReportMetric(float64(pct(0.99).Nanoseconds()), "p99-ns")
	b.ReportMetric(float64(pct(0.999).Nanoseconds()), "p999-ns")
}

func BenchmarkWALActiveRead_Idle(b *testing.B) {
	benchActiveRead(b, 0, 8192)
}

func BenchmarkWALActiveRead_Committing(b *testing.B) {
	benchActiveRead(b, 4, 8192)
}
