package server

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Item 4 (Opus): a connection whose reader is parked in a lease wait when the
// server closes it must have the wait cancelled (failConnection → leaseCancel)
// and keep reading so the close handshake completes.
func TestLease_GatedWaitCancelledForCloseHandshake(t *testing.T) {
	release := gateWALFsync(t)
	srv, addr, _, _, _ := leaseServer(t, 1<<20, nil, nil)

	// A: one durable publish holds the whole budget behind the gated fsync.
	a := dialRaw(t, addr)
	a.handshake("guest", "guest", 4, 0)
	a.openChannel(1)
	a.declareQueue(1, "sl4c-q")
	leaseConfirmSelect(t, a, 1)
	leaseSendPublish(t, a, 1, "", "sl4c-q", leaseBody(700<<10), true)
	require.Eventually(t, func() bool { return leaseUsed(srv) >= 717744 }, 30*time.Second, time.Millisecond)

	// A's second header parks in the lease wait.
	leaseSendMethod(t, a, 1, "", "sl4c-q", false)
	leaseSendHeader(t, a, 1, 700<<10, true, "")
	require.Eventually(t, func() bool { return leaseWaiters(srv) == 1 }, 30*time.Second, time.Millisecond)

	// The server closes A (any failConnection trigger) while A is parked.
	srv.Mutex.RLock()
	var aConn *protocol.Connection
	for _, c := range srv.Connections {
		aConn = c
	}
	srv.Mutex.RUnlock()
	require.NotNil(t, aConn)
	rt := srv.runtimeFor(aConn)
	go srv.failConnection(rt, 320, "CONNECTION_FORCED - test", 0, 0)

	// A reads connection.close, replies close-ok; the cancelled wait must let
	// the reader keep reading it, and the close handshake completes (EOF).
	a.expectConnectionClose(320)
	a.send(0, 10, 51, []byte{}) // connection.close-ok
	a.expectEOF()
	require.Eventually(t, func() bool { return leaseWaiters(srv) == 0 }, 30*time.Second, time.Millisecond)
	release()
}

// Item 4: an invalid frame on a healthy connection closes it with a clean
// close handshake (close → close-ok → EOF), no lease interference.
func TestLease_InvalidFrameCloseHandshakeClean(t *testing.T) {
	srv, addr, _, _, _ := leaseServer(t, 0, nil, nil)

	b := dialRaw(t, addr)
	b.handshake("guest", "guest", 4, 0)
	b.openChannel(1)
	// Unknown class 99 on a channel → FRAME_ERROR connection close.
	b.send(1, 99, 99, []byte{1, 2, 3})
	b.expectConnectionClose(501)
	b.send(0, 10, 51, []byte{})
	b.expectEOF()
	assert.Equal(t, 0, leaseWaiters(srv))
	assert.Equal(t, int64(0), leaseUsed(srv))
}

// wedgingBroker wedges the FIRST PublishMessageAsyncConfirm call on a gate so
// the frame processor stalls mid-publish (SL-23).
type wedgingBroker struct {
	UnifiedBroker
	gate    chan struct{}
	once    sync.Once
	waiting atomic.Bool
}

// PublishMessageAdmitted forwards to the wrapped broker so Build's
// lease-enabled NonBlockingPublisher check passes.
func (w *wedgingBroker) PublishMessageAdmitted(exchangeName, routingKey string, message *protocol.Message) (protocol.DepthGate, error) {
	return w.UnifiedBroker.(NonBlockingPublisher).PublishMessageAdmitted(exchangeName, routingKey, message)
}

func (w *wedgingBroker) PublishMessageAsyncConfirm(exchangeName, routingKey string, message *protocol.Message, onDurable func(error), leaseDone protocol.PublishLeaseToken) (bool, protocol.DepthGate, error) {
	w.once.Do(func() {
		w.waiting.Store(true)
		<-w.gate
	})
	return w.UnifiedBroker.PublishMessageAsyncConfirm(exchangeName, routingKey, message, onDurable, leaseDone)
}

// SL-23 (Opus): with the processor wedged mid-publish and content queued, a
// header's lease wait is cancelled by the stall guard (no processor progress
// for writeTimeout) and the connection closes with 506.
func TestLease_ProcessorStallCloses506(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := storage.NewDisruptorStorageWithDataDir(tmpDir)
	require.NoError(t, err)
	wedge := &wedgingBroker{UnifiedBroker: NewStorageBrokerAdapter(broker.NewStorageBroker(store, config.DefaultConfig().GetEngine())), gate: make(chan struct{})}
	var unwedge sync.Once
	t.Cleanup(func() { unwedge.Do(func() { close(wedge.gate) }) }) // unwedge so shutdown can finish

	cfg := config.DefaultConfig()
	cfg.Network.Address = "127.0.0.1:0"
	cfg.Storage.Path = tmpDir
	cfg.Server.LogLevel = "silent"
	cfg.Network.PublishInflightBudgetBytes = 1 << 20
	srv, err := NewServerBuilder().WithConfig(cfg).WithStorage(store).WithUnifiedBroker(wedge).Build()
	require.NoError(t, err)
	srv.writeTimeoutOverride = 200 * time.Millisecond
	probe := &leaseMetricProbe{}
	srv.MetricsCollector = probe

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Start() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	deadline := time.Now().Add(2 * time.Second)
	for !srv.IsListening() {
		select {
		case err := <-errCh:
			require.NoError(t, err)
			require.FailNow(t, "server Start returned without listening")
		default:
		}
		require.False(t, time.Now().After(deadline), "listener not ready")
		time.Sleep(10 * time.Millisecond)
	}
	srv.Mutex.RLock()
	addr := srv.Listener.Addr().String()
	srv.Mutex.RUnlock()

	a := dialRaw(t, addr)
	a.handshake("guest", "guest", 4, 0)
	a.openChannel(1)
	a.declareQueue(1, "sl23-q")
	leaseConfirmSelect(t, a, 1)

	// msg1: the processor wedges INSIDE the broker call (holding the lease).
	leaseSendPublish(t, a, 1, "", "sl23-q", leaseBody(700<<10), true)
	require.Eventually(t, func() bool { return wedge.waiting.Load() }, 30*time.Second, time.Millisecond)

	// msg2: the reader's lease Begin cannot grant (717744+717744 > 1 MiB) and waits.
	leaseSendMethod(t, a, 1, "", "sl23-q", false)
	leaseSendHeader(t, a, 1, 700<<10, true, "")
	require.Eventually(t, func() bool { return leaseWaiters(srv) == 1 }, 30*time.Second, time.Millisecond)

	// The processor is wedged with msg2's method frame queued behind it: the
	// stall guard cancels the wait and closes with 506 (frame observed or
	// socket closed — the best-effort close write races teardown).
	a.c.SetReadDeadline(time.Now().Add(30 * time.Second))
	f, err := protocol.ReadFrameOptimizedWithLimit(a.r, 131072+8)
	if err == nil {
		require.Equal(t, byte(protocol.FrameMethod), f.Type)
		require.Equal(t, uint16(10), uint16(f.Payload[0])<<8|uint16(f.Payload[1]))
		require.Equal(t, uint16(50), uint16(f.Payload[2])<<8|uint16(f.Payload[3]))
		m := &protocol.ConnectionCloseMethod{}
		require.NoError(t, m.Deserialize(f.Payload[4:]))
		assert.Equal(t, uint16(506), m.ReplyCode, "RESOURCE_ERROR")
	}
	require.Eventually(t, func() bool { return probe.overbudgetCloses.Load() == 1 }, 30*time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return leaseWaiters(srv) == 0 }, 30*time.Second, time.Millisecond)
	unwedge.Do(func() { close(wedge.gate) }) // let the wedged processor finish before Shutdown
}

// panicUnregisterBroker panics inside cleanupConnection (via
// UnregisterConsumer) — AFTER the connection's leases should already have
// been drained (item 3: the once-only drain runs FIRST so a later
// panic/deadlock cannot leak budget).
type panicUnregisterBroker struct {
	UnifiedBroker
}

func (p *panicUnregisterBroker) UnregisterConsumer(consumerID string) error {
	panic("panicUnregisterBroker: UnregisterConsumer")
}

// PublishMessageAdmitted forwards to the wrapped broker so Build's
// lease-enabled NonBlockingPublisher check passes.
func (p *panicUnregisterBroker) PublishMessageAdmitted(exchangeName, routingKey string, message *protocol.Message) (protocol.DepthGate, error) {
	return p.UnifiedBroker.(NonBlockingPublisher).PublishMessageAdmitted(exchangeName, routingKey, message)
}

func TestLease_CleanupPanicCannotLeakLeases(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := storage.NewDisruptorStorageWithDataDir(tmpDir)
	require.NoError(t, err)
	cfg := config.DefaultConfig()
	cfg.Network.Address = "127.0.0.1:0"
	cfg.Storage.Path = tmpDir
	cfg.Server.LogLevel = "silent"
	pb := &panicUnregisterBroker{UnifiedBroker: NewStorageBrokerAdapter(broker.NewStorageBroker(store, cfg.GetEngine()))}
	srv, err := NewServerBuilder().WithConfig(cfg).WithStorage(store).WithUnifiedBroker(pb).Build()
	require.NoError(t, err)
	created := &atomic.Int64{}
	released := &atomic.Int64{}
	srv.leaseCreated = created
	srv.leaseReleased = released

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Start() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	deadline := time.Now().Add(2 * time.Second)
	for !srv.IsListening() {
		require.False(t, time.Now().After(deadline), "listener not ready")
		time.Sleep(10 * time.Millisecond)
	}
	srv.Mutex.RLock()
	addr := srv.Listener.Addr().String()
	srv.Mutex.RUnlock()

	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 4, 0)
	r.openChannel(1)
	r.declareQueue(1, "slpanic-q")
	r.consume(1, "slpanic-q", "c1") // registers a consumer → cleanup panics

	// A half-received publish: the lease sits in PendingMessages at teardown.
	leaseSendMethod(t, r, 1, "", "slpanic-q", false)
	leaseSendHeader(t, r, 1, 100<<10, true, "")
	require.Eventually(t, func() bool { return created.Load() == 1 }, 30*time.Second, time.Millisecond)

	_ = r.c.Close()
	require.Eventually(t, func() bool { return released.Load() == 1 }, 30*time.Second, time.Millisecond,
		"the lease must be released BEFORE the panicking unregister")
	assert.Equal(t, int64(1), created.Load())
	assert.Equal(t, int64(0), leaseUsed(srv))
}
