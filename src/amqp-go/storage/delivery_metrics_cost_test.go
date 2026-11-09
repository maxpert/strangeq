package storage

import (
	"sync"
	"testing"

	"github.com/maxpert/amqp-go/metrics"
	"github.com/maxpert/amqp-go/protocol"
)

// benchDeliveryCollector is created ONCE per test binary: the collector
// registers on the default registry, so a second creation with the same
// namespace panics.
var benchDeliveryCollector = sync.OnceValue(func() *metrics.Collector {
	return metrics.NewCollector("bench_delivery_metrics_hotpath")
})

// BenchmarkDeliveryMetricsHotPath measures GetMessageForDelivery on a
// hot-cached transient message with a REAL prometheus collector bound, in
// parallel. It is the shape whose label-vector resolution (hash + the vector's
// internal RWMutex reader count) T4 removes from every delivered message.
//
// It does real work per iteration (the hot-cache Get plus the metric update);
// it is not a spin loop. Every goroutine adds the SAME series, which is the
// contended case the audit measured at 117-124 ns/op with 8 goroutines.
func BenchmarkDeliveryMetricsHotPath(b *testing.B) {
	ds, err := NewStorageEngineWithDataDir(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer ds.Close()
	ds.SetMetrics(benchDeliveryCollector())

	const q = "dm-bench-q"
	msg := &protocol.Message{Exchange: "", RoutingKey: q, Body: make([]byte, 256), DeliveryMode: 1, DeliveryTag: 1}
	if err := ds.StoreMessage(q, msg); err != nil {
		b.Fatal(err)
	}
	if _, err := ds.GetMessageForDelivery(q, 1); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := ds.GetMessageForDelivery(q, 1); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
