package server

// DURABLE-MODE PACING, end to end through the real publish path.
//
// The broker-level tests (broker/hot_cache_pacing_durable_test.go) prove WHICH
// durable crash models return a hot-cache pace gate. These tests prove the
// consequence at the server:
//
//   - crash_model: force_flush (the full flush) — even when the queue's
//     hot-cache byte budget is exceeded, the producer-only connection's
//     depth-backpressure flag is never armed and no publisher reader park is
//     counted: the batch size is the throughput limiter there.
//   - crash_model: barrier (the shipped durable default, darwin) — the SAME
//     condition DOES arm the connection and count a park, because the ordering
//     fence is cheap and cold WAL reads are the limiter.
//
// WHY THE SPLIT: the original inertness (campaign ab-runC, F3 64 KiB 1p/1c
// durable+confirm) held because the ~16 ms F_FULLFSYNC per batch made
// msgs/fsync the throughput, and parking the reader halved the batch
// (499 -> 221 msgs/fsync, ~33% throughput). Under the ~71 µs
// F_BARRIERFSYNC fence the cycle is ~1.3 ms and the stager self-sizes batches
// to ~25-30, so parking no longer starves the batch — while it removes the
// 84-88% cold WAL reads that now dominate.

import (
	"testing"
	"time"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/maxpert/amqp-go/transaction"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// newHotBudgetServer builds a server whose hot-cache byte budget is tiny and
// whose message-count depth HWM is huge, so the ONLY thing that could arm
// reader backpressure is the hot-cache pace gate. syncDisabled selects the
// durability none level (true) vs a fenced level (false, the shipped default);
// crashModel selects the confirm fence within the fenced levels
// (interfaces.CrashModelBarrier -> barrier, CrashModelForceFlush -> flush).
// Returns the server and its storage so the test can read the resident-byte
// mirror.
func newHotBudgetServer(t *testing.T, syncDisabled bool, crashModel string) (*Server, *storage.DisruptorStorage) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Storage.Path = t.TempDir()
	switch {
	case syncDisabled:
		cfg.Storage.Durability = interfaces.DurabilityNone
	case crashModel == interfaces.CrashModelForceFlush:
		cfg.Storage.Durability = interfaces.DurabilityFlush
	default:
		cfg.Storage.Durability = interfaces.DurabilityBarrier
	}

	eng := cfg.GetEngine()
	eng.HotCacheQueueBytes = 2048 // tiny: binds after a dozen small bodies
	eng.HotCacheGlobalBytes = 1 << 20
	eng.DepthHighWM = 52428 // depth HWM: never reached here

	storageImpl, err := storage.NewDisruptorStorageWithEngineConfig(cfg.Storage.Path, eng)
	require.NoError(t, err)
	t.Cleanup(func() { _ = storageImpl.Close() })

	storageBroker := broker.NewStorageBroker(storageImpl, eng)
	unifiedBroker := NewStorageBrokerAdapter(storageBroker)
	tm := transaction.NewTransactionManager()
	tm.SetExecutor(transaction.NewUnifiedBrokerExecutor(unifiedBroker))

	return &Server{
		Addr:               ":0",
		Connections:        make(map[string]*protocol.Connection),
		Log:                zap.NewNop(),
		Broker:             unifiedBroker,
		Config:             cfg,
		MetricsCollector:   &NoOpMetricsCollector{},
		TransactionManager: tm,
	}, storageImpl
}

// TestForceFlushModePublishNeverParksReaderAtHotBudget drives the real durable
// publish path past the hot-cache byte budget under crash_model: force_flush
// and asserts the producer-only connection is NEVER armed for reader
// backpressure and NO park is counted. There the confirm fence is the ~16 ms
// full drive-cache flush, so the group-commit batch size IS the throughput
// limiter and pacing the publisher would starve it.
func TestForceFlushModePublishNeverParksReaderAtHotBudget(t *testing.T) {
	const queue = "durable-nopace-q"
	// ~64-byte bodies so the 2048-byte budget is spent within a handful of
	// publishes; the loop publishes far past that.
	body := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	srv, store := newHotBudgetServer(t, false, interfaces.CrashModelForceFlush) // DURABLE, full flush
	conn, frameCh := newPipeConnWithFrames(t)
	sq5Setup(t, srv, conn, frameCh, queue)

	done := make(chan struct{})
	go srv.confirmFlusher(conn, done)
	defer func() { <-done }()
	defer close(conn.Done)

	conn.HeartbeatSec.Store(0)
	conn.HasPublished.Store(true) // producer-only

	// Publish past the hot budget, PACED so the asynchronous hot-cache
	// admissions land between publishes and each gate evaluation sees the
	// filled cache — exactly the harness the fsync:false pacing test uses. The
	// depth HWM is 52,428, so the ONLY thing that could arm this connection is
	// the hot-cache pace gate.
	deadline := time.After(5 * time.Second)
	for i := 0; i < 200; i++ {
		select {
		case <-deadline:
			t.Fatalf("durable-mode publish loop stalled after %d publishes", i)
		default:
		}
		require.NoError(t, srv.processCompleteMessage(conn, 1, makeDurablePendingMessage("", queue, body)))
		require.False(t, conn.DepthBackpressure.Load(),
			"force_flush mode must never arm reader backpressure for hot-cache pacing (publish %d)", i)
		time.Sleep(time.Millisecond)
	}

	require.GreaterOrEqual(t, store.HotCacheResidentBytes(queue), int64(1500),
		"fixture premise: the hot budget must be substantially spent")
	require.False(t, conn.DepthBackpressure.Load(),
		"the producer-only connection must stay unpaced in durable mode")

	count, _, current := srv.PublisherParkStats()
	require.Zero(t, count, "durable mode must count zero publisher reader parks")
	require.Zero(t, current, "durable mode must never have a park in progress")
}

// TestNonDurableModePublishParksReaderAtHotBudget is the matching guard: with
// the WAL fsync barrier DISABLED (fsync:false) the SAME condition DOES arm the
// producer-only connection, proving the hot-cache pace wiring is retained and
// the durable-mode inertness above is not simply dead code.
func TestNonDurableModePublishParksReaderAtHotBudget(t *testing.T) {
	const queue = "nondur-pace-q"
	body := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	srv, _ := newHotBudgetServer(t, true, "") // fsync:false: pacing wired
	conn, frameCh := newPipeConnWithFrames(t)
	sq5Setup(t, srv, conn, frameCh, queue)

	done := make(chan struct{})
	go srv.confirmFlusher(conn, done)
	defer func() { <-done }()
	defer close(conn.Done)

	conn.HeartbeatSec.Store(0)
	conn.HasPublished.Store(true) // producer-only

	deadline := time.After(5 * time.Second)
	for i := 0; i < 200 && !conn.DepthBackpressure.Load(); i++ {
		select {
		case <-deadline:
			t.Fatalf("hot-cache pacing never armed after %d publishes in fsync:false mode", i)
		default:
		}
		require.NoError(t, srv.processCompleteMessage(conn, 1, makeDurablePendingMessage("", queue, body)))
		time.Sleep(time.Millisecond)
	}
	require.True(t, conn.DepthBackpressure.Load(),
		"fsync:false mode must pace the publisher at the hot-cache byte budget")
}

// TestBarrierModePublishParksReaderAtHotBudget is the durable counterpart: under
// crash_model: barrier (the shipped default) with fsync ON, the SAME
// byte-budget-exceeded condition DOES arm the producer-only connection and
// count a park, because the ~71 µs ordering fence is not the throughput
// limiter and cold WAL reads are. Guarded on darwin: on Linux and other
// platforms the barrier IS a full drive-cache flush, so pacing stays off
// (TestForceFlushModePublishNeverParksReaderAtHotBudget covers that wiring via
// force_flush).
func TestBarrierModePublishParksReaderAtHotBudget(t *testing.T) {
	if !storage.ConfirmFenceOrdersOnly {
		t.Skip("crash_model: barrier is a full flush on this platform; pacing stays off")
	}
	const queue = "barrier-pace-q"
	body := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	srv, _ := newHotBudgetServer(t, false, interfaces.CrashModelBarrier) // DURABLE, ordering fence
	conn, frameCh := newPipeConnWithFrames(t)
	sq5Setup(t, srv, conn, frameCh, queue)

	done := make(chan struct{})
	go srv.confirmFlusher(conn, done)
	defer func() { <-done }()
	defer close(conn.Done)

	conn.HeartbeatSec.Store(0)
	conn.HasPublished.Store(true) // producer-only

	deadline := time.After(5 * time.Second)
	for i := 0; i < 200 && !conn.DepthBackpressure.Load(); i++ {
		select {
		case <-deadline:
			t.Fatalf("hot-cache pacing never armed after %d publishes under crash_model: barrier", i)
		default:
		}
		require.NoError(t, srv.processCompleteMessage(conn, 1, makeDurablePendingMessage("", queue, body)))
		time.Sleep(time.Millisecond)
	}
	require.True(t, conn.DepthBackpressure.Load(),
		"durable + crash_model barrier must pace the publisher at the hot-cache byte budget")
}
