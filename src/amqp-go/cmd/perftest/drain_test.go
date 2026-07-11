package main

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/server"
)

// ============================================================================
// Drain / loss-metric regression tests (iter7).
//
// These exercise runBenchmark against a real embedded StrangeQ server (the
// same client library and code path the CLI uses), NOT a mock. They pin the
// two invariants of the loss metric:
//
//   1. In-flight messages at the publish-window cutoff (broker queue backlog +
//      amqp091-go client prefetch buffers) MUST be drained and counted, not
//      reported as "Lost". This was the measurement artifact iter7 fixes: the
//      old code shared one context between producers and consumers, so at the
//      cutoff consumers returned immediately with no drain and the pipeline
//      depth was miscounted as loss (TestDrainRecoversBacklog).
//
//   2. The drain is bounded by drainTimeout, so a genuinely un-consumed
//      strand still surfaces as real loss and never hangs
//      (TestDrainTimeoutSurfacesRealLoss).
//
// The embedded-server setup mirrors versus_bench_test.go's versusURI.
// ============================================================================

// startEmbeddedServer boots an in-process StrangeQ server on a free loopback
// port with storage in a temp dir, and returns its AMQP URL plus a stop func.
func startEmbeddedServer(t *testing.T) (string, func()) {
	t.Helper()

	addr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	cfg := config.DefaultConfig()
	cfg.Network.Address = addr
	cfg.Storage.Path = t.TempDir()
	// Silence the embedded server's logger: go test folds the test binary's
	// stdout and stderr into one stream, and the unconditional connection-close
	// log on srv.Stop() would otherwise interleave with test output.
	cfg.Server.LogLevel = "silent"

	srv, err := server.NewServerBuilder().WithConfig(cfg).Build()
	if err != nil {
		t.Fatalf("build embedded server: %v", err)
	}
	go func() {
		if err := srv.Start(); err != nil {
			t.Logf("embedded server stopped: %v", err)
		}
	}()
	for i := 0; i < 100; i++ {
		if srv.IsListening() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !srv.IsListening() {
		srv.Stop()
		t.Fatal("embedded server listener not ready")
	}
	return fmt.Sprintf("amqp://guest:guest@%s/", addr), func() { srv.Stop() }
}

// freePort asks the OS for an unused loopback TCP port and returns it. The
// tiny reuse window is acceptable for a test fixture and avoids collisions
// across parallel test binaries far more reliably than a fixed base+counter.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve free port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		t.Fatalf("release free port: %v", err)
	}
	return port
}

// TestDrainRecoversBacklog is the RED→GREEN anchor for the iter7 fix.
//
// It publishes a FIXED, hardware-independent number of messages (maxMessages)
// with 3 producers, against 1 consumer throttled to ~1 msg/ms. Producers exit
// on the count cap well before the 10s window timer, so `target` is ~maxMessages
// on every machine regardless of publish speed. The throttled consumer cannot
// keep pace (maxMessages*consumerDelay of consume-time >> the sub-second publish
// time), so a backlog is GUARANTEED at the window cutoff. Before the fix that
// backlog is reported as loss (lost > 0, delivered < target); after the fix the
// bounded drain empties the queue (delivered == target, lost == 0), and because
// target is bounded the drain (~maxMessages*consumerDelay ≈ a few seconds)
// always finishes with a large fixed margin under drainTimeout on ANY hardware.
//
// The consumedInWindow < target assertion is the anti-vacuity guard: it proves
// a real backlog existed at cutoff, so the test genuinely distinguishes the
// drained path from the broken one rather than passing trivially.
func TestDrainRecoversBacklog(t *testing.T) {
	url, stop := startEmbeddedServer(t)
	defer stop()

	const maxMessages = 4000
	cfg := benchConfig{
		url:           url,
		queue:         "iter7-drain-recover",
		exchange:      "",
		producers:     3,
		consumers:     1,
		size:          64,
		prefetch:      100,
		outstanding:   1000,
		duration:      10 * time.Second, // producers exit on the cap, not this timer
		confirmGrace:  15 * time.Second,
		drainTimeout:  30 * time.Second,
		durable:       true,
		confirm:       true,
		consumerDelay: time.Millisecond, // throttle: guarantees a backlog vs the cap
		maxMessages:   maxMessages,      // fixed target => hardware-independent
	}

	start := time.Now()
	result := runBenchmark(context.Background(), cfg, newStats())
	drainElapsed := time.Since(start) - result.windowElapsed

	t.Logf("consumedInWindow=%d target=%d delivered=%d lost=%d | window=%s drain=%s (drainTimeout=%s)",
		result.consumedInWindow, result.target, result.delivered, result.lost,
		result.windowElapsed.Round(time.Millisecond), drainElapsed.Round(time.Millisecond), cfg.drainTimeout)

	if result.target == 0 {
		t.Fatalf("no messages broker-accepted (target=0); publish/confirm setup is broken")
	}
	// Anti-vacuity guard: without a backlog at cutoff this test cannot tell the
	// fixed path from the broken one.
	if result.consumedInWindow >= result.target {
		t.Fatalf("no backlog at window close (consumedInWindow=%d >= target=%d): "+
			"scenario did not reproduce, test would be vacuous",
			result.consumedInWindow, result.target)
	}
	if result.unconfirmed != 0 || result.nacked != 0 {
		t.Errorf("confirm accounting: unconfirmed=%d nacked=%d, want 0/0",
			result.unconfirmed, result.nacked)
	}
	if result.delivered != result.target {
		t.Errorf("drain incomplete: delivered=%d, want target=%d", result.delivered, result.target)
	}
	if result.lost != 0 {
		t.Errorf("backlog miscounted as loss: lost=%d (target=%d delivered=%d consumedInWindow=%d)",
			result.lost, result.target, result.delivered, result.consumedInWindow)
	}
}

// TestDrainTimeoutSurfacesRealLoss guards the other direction: the bounded
// drain must never mask a genuine strand. With no consumer at all, every
// broker-accepted message is un-consumed; after the short drain deadline the
// remainder MUST be reported as loss (and the run must not hang).
func TestDrainTimeoutSurfacesRealLoss(t *testing.T) {
	url, stop := startEmbeddedServer(t)
	defer stop()

	cfg := benchConfig{
		url:          url,
		queue:        "iter7-drain-loss",
		exchange:     "",
		producers:    1,
		consumers:    0, // no consumer: nothing can be drained
		size:         64,
		prefetch:     100,
		outstanding:  1000,
		duration:     200 * time.Millisecond,
		confirmGrace: 5 * time.Second,
		drainTimeout: 500 * time.Millisecond,
		durable:      true,
		confirm:      true,
	}

	result := runBenchmark(context.Background(), cfg, newStats())

	if result.target == 0 {
		t.Fatalf("no messages broker-accepted (target=0); publish/confirm setup is broken")
	}
	if result.delivered != 0 {
		t.Errorf("no consumer was running, want delivered=0, got %d", result.delivered)
	}
	if result.lost != result.target {
		t.Errorf("bounded drain masked real loss: lost=%d, want target=%d", result.lost, result.target)
	}
	if result.lost <= 0 {
		t.Errorf("expected a genuine strand to surface as loss, got lost=%d", result.lost)
	}
}
