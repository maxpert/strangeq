package server

import (
	"net"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
)

// These benchmarks isolate the consumer-side delivery encode+write for a 64 KiB
// body, contrasting the pre-change contiguous path against the new vectored
// path on the SAME body. They exist to produce the local pprof/alloc proof that
// the two byte-proportional costs the diagnosis flagged — the batch make() and
// the protocol.AppendFrame body memmove — are gone at 64 KiB (native macOS
// throughput is fsync-masked, so this alloc/CPU delta, not wall-clock, is the
// signal). Run with:
//
//	go test ./server/ -run x -bench 'BenchmarkDelivery64KB' -benchmem \
//	    -memprofile mem.out -cpuprofile cpu.out
//	go tool pprof -top mem.out   # make()/AppendFrame body copy present only in Contiguous
//	go tool pprof -top cpu.out   # runtime.memmove present only in Contiguous

// benchDiscardConn discards writes without blocking (mirrors discardConn from
// the correctness tests; redeclared here so the bench file is self-contained).
type benchDiscardConn struct{ net.Conn }

func (benchDiscardConn) Write(p []byte) (int, error) { return len(p), nil }

func benchDelivery64KB() *protocol.Delivery {
	return makeDelivery(1, seededBody(64*1024))
}

// BenchmarkDelivery64KB_Contiguous replicates the pre-change hot path: encode
// the batch into one contiguous buffer via serializeDeliveryInto (which drives
// protocol.AppendFrame's body memmove) and write it once. estimatedSize here is
// below maxPoolSize for a single delivery, so this uses the pool; the make()
// site is exercised by the 3x variant. It is the "before" profile baseline.
func BenchmarkDelivery64KB_Contiguous(b *testing.B) {
	srv := makeTestServer()
	conn := protocol.NewConnection(benchDiscardConn{})
	d := benchDelivery64KB()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := protocol.GetBufferForSize(deliveryOverheadEstimate + len(d.Message.Body))
		_ = srv.serializeDeliveryInto(buf, 1, "c", d.DeliveryTag, false, d.Exchange, d.RoutingKey, d.Message)
		conn.WriteMutex.Lock()
		_, _ = conn.Conn.Write(*buf)
		conn.WriteMutex.Unlock()
		protocol.PutBufferForSize(buf)
	}
}

// BenchmarkDelivery64KB_ContiguousBatch3 replicates the >=3x64KB batch case that
// hit the unpooled make([]byte,0,estimatedSize) at batch_delivery.go — the 46%
// alloc site. estimatedSize = 3*(256+65536) > 131*1024, so this allocates the
// big non-pooled buffer AND memmoves three bodies into it.
func BenchmarkDelivery64KB_ContiguousBatch3(b *testing.B) {
	srv := makeTestServer()
	conn := protocol.NewConnection(benchDiscardConn{})
	deliveries := []*protocol.Delivery{benchDelivery64KB(), benchDelivery64KB(), benchDelivery64KB()}
	est := 0
	for _, d := range deliveries {
		est += deliveryOverheadEstimate + len(d.Message.Body)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := make([]byte, 0, est) // the unpooled make() the diagnosis flagged
		for _, d := range deliveries {
			_ = srv.serializeDeliveryInto(&buf, 1, "c", d.DeliveryTag, false, d.Exchange, d.RoutingKey, d.Message)
		}
		conn.WriteMutex.Lock()
		_, _ = conn.Conn.Write(buf)
		conn.WriteMutex.Unlock()
	}
}

// BenchmarkDelivery64KB_Vectored is the new zero-copy path (single delivery).
func BenchmarkDelivery64KB_Vectored(b *testing.B) {
	srv := makeTestServer()
	conn := protocol.NewConnection(benchDiscardConn{})
	deliveries := []*protocol.Delivery{benchDelivery64KB()}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = srv.sendBatchedDeliveries(conn, 1, "c", deliveries)
	}
}

// BenchmarkDelivery64KB_VectoredBatch3 is the new zero-copy path for the 3x64KB
// batch (the make()-hitting case above).
func BenchmarkDelivery64KB_VectoredBatch3(b *testing.B) {
	srv := makeTestServer()
	conn := protocol.NewConnection(benchDiscardConn{})
	deliveries := []*protocol.Delivery{benchDelivery64KB(), benchDelivery64KB(), benchDelivery64KB()}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = srv.sendBatchedDeliveries(conn, 1, "c", deliveries)
	}
}
