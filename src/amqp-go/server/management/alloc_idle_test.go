package management

import (
	"context"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/server"
	"go.uber.org/zap"
)

// TestNoPerMessageAllocation_WithManagementIdle pins the zero-cost contract of
// the control plane on the MESSAGE path (spec test 87): constructing the
// management surface (enabled) and leaving it idle must not add a single
// allocation to a publish/get round trip. The control plane is a separate HTTP
// listener and must never hook the per-message path. The comparison is against
// an identically built broker with the surface disabled, so any per-message
// coupling would show up as a different AllocsPerOp.
func TestNoPerMessageAllocation_WithManagementIdle(t *testing.T) {
	measure := func(managementEnabled bool) testing.BenchmarkResult {
		return testing.Benchmark(func(b *testing.B) {
			cfg := config.DefaultConfig()
			cfg.Storage.Path = b.TempDir()
			cfg.Server.LogLevel = "silent"
			cfg.Management.Enabled = managementEnabled
			cfg.Management.Authentication = config.ManagementAuthenticationNone
			cfg.Management.Address = "127.0.0.1"

			srv, err := server.NewServerBuilder().WithConfig(cfg).WithZapLogger("silent").Build()
			if err != nil {
				b.Fatalf("build broker: %v", err)
			}
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = srv.Shutdown(ctx)
			}()

			if managementEnabled {
				// Constructed but never Started: an IDLE control plane.
				if _, err := New(Options{Server: srv, Config: cfg, Logger: zap.NewNop()}); err != nil {
					b.Fatalf("management.New: %v", err)
				}
			}

			if _, err := srv.Broker.DeclareQueue("idle-bench", false, false, false, nil); err != nil {
				b.Fatalf("DeclareQueue: %v", err)
			}
			body := make([]byte, 256)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				msg := &protocol.Message{Body: body, RoutingKey: "idle-bench"}
				if err := srv.Broker.PublishMessage("", "idle-bench", msg); err != nil {
					b.Fatal(err)
				}
				// Drain each iteration so the queue stays bounded and the
				// measurement never swaps onto a different (WAL) code path.
				if got, tag, _, err := srv.Broker.GetMessageForGet("idle-bench", false); err == nil && got != nil {
					_ = srv.Broker.AcknowledgeGetDelivery(tag)
				}
			}
		})
	}

	on := measure(true)
	off := measure(false)
	t.Logf("management idle ON:  %d allocs/op, %d ns/op", on.AllocsPerOp(), on.NsPerOp())
	t.Logf("management idle OFF: %d allocs/op, %d ns/op", off.AllocsPerOp(), off.NsPerOp())
	if off.N <= 0 || on.N <= 0 {
		t.Fatalf("benchmarks produced no iterations: on=%d off=%d", on.N, off.N)
	}
	if on.AllocsPerOp() != off.AllocsPerOp() {
		t.Fatalf("an idle management surface must not change per-message allocations: on=%d off=%d allocs/op",
			on.AllocsPerOp(), off.AllocsPerOp())
	}
}
