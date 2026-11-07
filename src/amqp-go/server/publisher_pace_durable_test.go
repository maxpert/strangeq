package server

// DURABLE-MODE INERTNESS, end to end through the real publish path.
//
// The broker-level test (broker/hot_cache_pacing_durable_test.go) proves the
// broker never RETURNS a hot-cache pace gate in durable mode. This test proves
// the consequence at the server: even when the queue's hot-cache byte budget is
// exceeded, the producer-only connection's depth-backpressure flag is never
// armed and no publisher reader park is ever counted — the reader path is wired
// exactly as on main.
//
// WHY (campaign ab-runC, F3 64 KiB 1p/1c durable+confirm): in durable mode the
// WAL group-commit batch size is the throughput limiter; parking the publisher
// reader creates arrival gaps that halve the batch, 499 -> 221 msgs/fsync,
// costing ~33% throughput. fsync's own confirm latency already paces the
// publisher, so the gate is not wired in durable mode.

import (
	"testing"
	"time"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/maxpert/amqp-go/transaction"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// newHotBudgetServer builds a server whose hot-cache byte budget is tiny and
// whose message-count depth HWM is huge, so the ONLY thing that could arm
// reader backpressure is the hot-cache pace gate. syncDisabled selects
// fsync:false (true) vs durable (false, the shipped default). Returns the
// server and its storage so the test can read the resident-byte mirror.
func newHotBudgetServer(t *testing.T, syncDisabled bool) (*Server, *storage.DisruptorStorage) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Storage.Path = t.TempDir()
	fsyncOn := !syncDisabled
	cfg.Storage.Fsync = &fsyncOn // tri-state: explicit fsync for this mode

	eng := cfg.GetEngine()
	eng.HotCacheQueueBytes = 2048 // tiny: binds after a dozen small bodies
	eng.HotCacheGlobalBytes = 1 << 20
	eng.RingBufferSize = 65536
	eng.SpillThresholdPercent = 80 // depth HWM = 52,428: never reached here

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

// TestDurableModePublishNeverParksReaderAtHotBudget drives the real durable
// publish path past the hot-cache byte budget and asserts the producer-only
// connection is NEVER armed for reader backpressure and NO park is counted.
func TestDurableModePublishNeverParksReaderAtHotBudget(t *testing.T) {
	const queue = "durable-nopace-q"
	// ~64-byte bodies so the 2048-byte budget is spent within a handful of
	// publishes; the loop publishes far past that.
	body := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

	srv, store := newHotBudgetServer(t, false) // DURABLE: fsync enabled
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
			"durable mode must never arm reader backpressure for hot-cache pacing (publish %d)", i)
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

	srv, _ := newHotBudgetServer(t, true) // fsync:false: pacing wired
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
