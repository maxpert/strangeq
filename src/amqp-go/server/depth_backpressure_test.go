package server

import (
	"sync/atomic"
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

// fakeDepthGate is a controllable protocol.DepthGate for unit-testing the
// reader-level queue-depth backpressure park in isolation from the broker.
type fakeDepthGate struct{ hwm atomic.Bool }

func (f *fakeDepthGate) AtHighWaterMark() bool { return f.hwm.Load() }

func newDepthParkServer() *Server {
	return &Server{
		Log:                     zap.NewNop(),
		Config:                  config.DefaultConfig(),
		MetricsCollector:        &NoOpMetricsCollector{},
		readerBackpressureProbe: time.Millisecond,      // fast depth re-check
		depthLivenessProbe:      50 * time.Millisecond, // fast liveness probe
	}
}

// TestDepthPark_ParksAtHWMResumesAndClearsOnDrain: the park must block while the
// gate reports the queue at HWM (TCP backpressure) and resume — clearing the
// connection's gate so the reader's fast path returns — the moment a consumer
// drains it below HWM, WITHOUT depending on any socket read (a blocked, silent
// client must not stall the resume).
func TestDepthPark_ParksAtHWMResumesAndClearsOnDrain(t *testing.T) {
	s := newDepthParkServer()
	conn := protocol.NewConnection(nil)
	g := &fakeDepthGate{}
	g.hwm.Store(true)
	conn.SetDepthGate(g)
	require.True(t, conn.DepthBackpressure.Load(), "arming the gate must set the fast-path flag")

	done := make(chan bool, 1)
	go func() { done <- s.parkReaderWhileDepthBackpressured(conn) }()

	select {
	case <-done:
		t.Fatal("reader resumed while the queue is still at HWM")
	case <-time.After(30 * time.Millisecond):
	}

	g.hwm.Store(false)
	select {
	case probing := <-done:
		require.False(t, probing, "a drain resume is not a liveness probe")
	case <-time.After(2 * time.Second):
		t.Fatal("reader did not resume after the queue drained below HWM")
	}
	require.False(t, conn.DepthBackpressure.Load(), "the gate must be cleared once the queue drains")
}

// TestDepthPark_SkipsConnectionWithConsumer: depth backpressure must never pause
// a connection that has an active consumer — it must keep reading to deliver its
// own basic.ack frames (which drain the very queue), so pausing could deadlock.
func TestDepthPark_SkipsConnectionWithConsumer(t *testing.T) {
	s := newDepthParkServer()
	conn := protocol.NewConnection(nil)

	ch := protocol.NewChannel(1, conn)
	ch.Mutex.Lock()
	ch.Consumers["c-1"] = &protocol.Consumer{Tag: "c-1"}
	ch.Mutex.Unlock()
	conn.Channels.Store(uint16(1), ch)

	g := &fakeDepthGate{}
	g.hwm.Store(true)
	conn.SetDepthGate(g)

	done := make(chan bool, 1)
	go func() { done <- s.parkReaderWhileDepthBackpressured(conn) }()
	select {
	case probing := <-done:
		require.False(t, probing)
	case <-time.After(time.Second):
		t.Fatal("park did not return for a connection with a consumer (deadlock risk)")
	}
	require.False(t, conn.DepthBackpressure.Load(), "a consuming connection must not stay depth-gated")
}

// TestDepthPark_ReturnsOnConnectionDone: a parked reader must unblock when the
// connection tears down, even if the queue never drains (consumers gone).
func TestDepthPark_ReturnsOnConnectionDone(t *testing.T) {
	s := newDepthParkServer()
	conn := protocol.NewConnection(nil)
	g := &fakeDepthGate{}
	g.hwm.Store(true)
	conn.SetDepthGate(g)

	done := make(chan bool, 1)
	go func() { done <- s.parkReaderWhileDepthBackpressured(conn) }()
	time.Sleep(20 * time.Millisecond)
	close(conn.Done)
	select {
	case probing := <-done:
		require.False(t, probing)
	case <-time.After(time.Second):
		t.Fatal("park did not return on connection teardown")
	}
}

// TestDepthPark_LivenessProbeWhenPinned: when the queue stays pinned at HWM with
// no drain (all consumers gone), the park must periodically break out
// (probing=true) so the reader can burst-drain buffered RX and detect a client
// close, rather than polling a dead queue forever.
func TestDepthPark_LivenessProbeWhenPinned(t *testing.T) {
	s := newDepthParkServer()
	conn := protocol.NewConnection(nil)
	g := &fakeDepthGate{}
	g.hwm.Store(true) // never drains
	conn.SetDepthGate(g)

	done := make(chan bool, 1)
	go func() { done <- s.parkReaderWhileDepthBackpressured(conn) }()
	select {
	case probing := <-done:
		require.True(t, probing, "a pinned queue must break out to a liveness probe, not block forever")
	case <-time.After(2 * time.Second):
		t.Fatal("liveness probe never fired on a pinned-at-HWM queue")
	}
	require.True(t, conn.DepthBackpressure.Load())
}

// TestDepthPark_LivenessBurstDrainsBufferedWindow (option A — teardown strand
// fix). When the depth-park is pinned at HWM (consumers gone at teardown), the
// producer's buffered-but-unread final window must still be READ so it durablizes
// and confirms within grace. This drives readFrames over a pipe with a gate that
// is ALWAYS at HWM, feeds a window of N publish frames, and requires ALL of them
// to reach FrameQueue in ~one liveness episode. Without the burst-drain (reading
// only ONE frame per liveness tick) the window strands and this times out.
func TestDepthPark_LivenessBurstDrainsBufferedWindow(t *testing.T) {
	srv := makeTestServer()
	srv.Connections = map[string]*protocol.Connection{}
	srv.readerBackpressureProbe = 5 * time.Millisecond
	srv.depthLivenessProbe = 50 * time.Millisecond
	conn, clientConn := registerPauseConn(t, srv)
	conn.HeartbeatSec.Store(0)
	conn.HasPublished.Store(true) // producer-only

	// Queue pinned at HWM forever (teardown: consumers gone) — the reader parks and
	// only its liveness burst-drain can move the buffered window.
	g := &fakeDepthGate{}
	g.hwm.Store(true)
	conn.SetDepthGate(g)

	readerDone := make(chan struct{})
	go srv.readFrames(conn, readerDone)

	const n = 50
	go func() {
		for i := 0; i < n; i++ {
			if _, err := clientConn.Write(nonAckPublishFrameBytes(t, 16)); err != nil {
				return
			}
		}
	}()

	// One-per-liveness would need n*50ms = 2.5s; the burst-drain does it in ~one
	// episode. Bound well under 2.5s to distinguish.
	deadline := time.After(1500 * time.Millisecond)
	got := 0
	for got < n {
		select {
		case f := <-conn.FrameQueue:
			if f == nil {
				t.Fatal("FrameQueue closed before the buffered window drained")
			}
			got++
		case <-readerDone:
			t.Fatal("reader exited before draining the buffered window")
		case <-deadline:
			t.Fatalf("buffered window stranded at HWM: drained %d/%d — liveness burst-drain missing?", got, n)
		}
	}

	close(conn.Done)
	clientConn.Close()
	<-readerDone
}

// newTinyRingServer builds a server whose broker high-water mark is tiny (ring 8
// * spill 50% = 4), so a handful of publishes drives a queue over HWM. Storage
// keeps its own (large) ring; the broker's depth counter is what gates.
func newTinyRingServer(t *testing.T) *Server {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Storage.Path = t.TempDir()
	cfg.Engine.RingBufferSize = 8
	cfg.Engine.SpillThresholdPercent = 50 // HWM = 8 * 50 / 100 = 4

	storageImpl, err := storage.NewDisruptorStorageWithDataDir(cfg.Storage.Path)
	require.NoError(t, err)
	storageBroker := broker.NewStorageBroker(storageImpl, cfg.GetEngine())
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
	}
}

// TestDepthWiring_ArmsGateForProducerOnlyConnAtHWM: publishing durable+confirm
// past the (tiny) HWM on a producer-only connection must arm the connection's
// depth-backpressure flag, and every one of those publishes must still confirm
// (no strand) — the whole point of moving the backpressure to the reader.
func TestDepthWiring_ArmsGateForProducerOnlyConnAtHWM(t *testing.T) {
	srv := newTinyRingServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	sq5Setup(t, srv, conn, frameCh, "bp-wire-q")

	done := make(chan struct{})
	go srv.confirmFlusher(conn, done)
	defer func() { <-done }()
	defer close(conn.Done)

	// Depth is incremented asynchronously by each durable copy's fsync completion,
	// so keep publishing until the gate arms — no consumer drains, so depth only
	// climbs and the arm is inevitable.
	published := 0
	deadline := time.After(5 * time.Second)
	for !conn.DepthBackpressure.Load() {
		select {
		case <-deadline:
			t.Fatalf("gate never armed after %d producer-only publishes past HWM", published)
		default:
		}
		require.NoError(t, srv.processCompleteMessage(conn, 1, makeDurablePendingMessage("", "bp-wire-q", "m")))
		published++
		time.Sleep(time.Millisecond)
	}

	require.True(t, conn.DepthBackpressure.Load(),
		"a producer-only connection publishing past HWM must arm reader depth backpressure")

	got := highestConfirmAck(t, frameCh, uint64(published), 5*time.Second)
	require.Equal(t, uint64(published), got, "all admitted publishes must confirm; got %d of %d", got, published)
}

// TestDepthWiring_DoesNotArmForConnectionWithConsumer: a connection with an
// active consumer must NEVER be depth-gated (it must keep reading its own acks).
func TestDepthWiring_DoesNotArmForConnectionWithConsumer(t *testing.T) {
	srv := newTinyRingServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	ch := sq5Setup(t, srv, conn, frameCh, "bp-consumer-q")

	ch.Mutex.Lock()
	ch.Consumers["cons-1"] = &protocol.Consumer{Tag: "cons-1"}
	ch.Mutex.Unlock()
	conn.Channels.Store(uint16(1), ch)

	deadline := time.After(1 * time.Second)
	for {
		select {
		case <-deadline:
			require.False(t, conn.DepthBackpressure.Load(),
				"a connection with a consumer must never be depth-gated (deadlock safety)")
			return
		default:
		}
		require.NoError(t, srv.processCompleteMessage(conn, 1, makeDurablePendingMessage("", "bp-consumer-q", "m")))
		require.False(t, conn.DepthBackpressure.Load(),
			"a connection with a consumer must never be depth-gated (deadlock safety)")
		time.Sleep(time.Millisecond)
	}
}
