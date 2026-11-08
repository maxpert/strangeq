package server

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/metrics"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// publishErrBroker wraps the real broker and forces a chosen publish error, so
// the refusal branches are reachable with no queue-delete race.
type publishErrBroker struct {
	UnifiedBroker
	err error
}

func (b *publishErrBroker) PublishMessageAdmitted(string, string, *protocol.Message) (protocol.DepthGate, error) {
	return nil, b.err
}

func (b *publishErrBroker) PublishMessageAsyncConfirm(string, string, *protocol.Message, func(error), protocol.PublishLeaseToken) (bool, protocol.DepthGate, error) {
	return false, nil, b.err
}

// noConsumerBroker answers every consumer lookup with "not found" so a requeue
// attempt is a no-op after it has been counted.
type noConsumerBroker struct{ UnifiedBroker }

func (noConsumerBroker) GetConsumerForDelivery(uint64) (string, bool) { return "", false }

// wireSpy records the spec 06 server-side hot-path families through the
// MetricsCollector surface (the methods this spy does not override fall
// through to NoOpMetricsCollector).
type wireSpy struct {
	NoOpMetricsCollector
	pubRefusedMaxLen  atomic.Int64
	pubRefusedClosed  atomic.Int64
	depthGateArmed    atomic.Int64
	consumerRegErrors atomic.Int64
	dropped           atomic.Int64
	droppedByReason   sync.Map // string -> *atomic.Int64
	requeued          atomic.Int64
	sendErrors        atomic.Int64
	closedByCause     atomic.Int64
	causes            sync.Map // string -> *atomic.Int64
	shutdowns         atomic.Int64
	storageCloseErrs  atomic.Int64
	alarmTrips        atomic.Int64
	panics            atomic.Int64
}

// counter returns the per-key counter, creating it on first use.
func counter(m *sync.Map, key string) *atomic.Int64 {
	v, _ := m.LoadOrStore(key, &atomic.Int64{})
	return v.(*atomic.Int64)
}

// counterValue reads a per-key counter, or 0.
func counterValue(m *sync.Map, key string) int64 {
	if v, ok := m.Load(key); ok {
		return v.(*atomic.Int64).Load()
	}
	return 0
}

// causeCount is the number of closes recorded for one bounded cause.
func (s *wireSpy) causeCount(cause string) int64 { return counterValue(&s.causes, cause) }

// droppedCount is the number of deliveries dropped for one bounded reason.
func (s *wireSpy) droppedCount(reason string) int64 { return counterValue(&s.droppedByReason, reason) }

func (s *wireSpy) RecordPublishRefused(reason string) {
	switch reason {
	case PublishRefusedMaxLength:
		s.pubRefusedMaxLen.Add(1)
	case PublishRefusedQueueClosed:
		s.pubRefusedClosed.Add(1)
	}
}
func (s *wireSpy) RecordDepthGateArmed()        { s.depthGateArmed.Add(1) }
func (s *wireSpy) RecordConsumerRegisterError() { s.consumerRegErrors.Add(1) }
func (s *wireSpy) RecordDeliveriesDropped(reason string, n int) {
	s.dropped.Add(int64(n))
	counter(&s.droppedByReason, reason).Add(int64(n))
}
func (s *wireSpy) RecordDeliveriesRequeued(_ string, n int) { s.requeued.Add(int64(n)) }
func (s *wireSpy) RecordDeliverySendError()                 { s.sendErrors.Add(1) }
func (s *wireSpy) RecordConnectionClosedByCause(cause string) {
	s.closedByCause.Add(1)
	counter(&s.causes, cause).Add(1)
}
func (s *wireSpy) RecordShutdown(string, float64) { s.shutdowns.Add(1) }
func (s *wireSpy) RecordStorageCloseError()       { s.storageCloseErrs.Add(1) }
func (s *wireSpy) RecordResourceAlarmTrip(string) { s.alarmTrips.Add(1) }
func (s *wireSpy) RecordPanic(string)             { s.panics.Add(1) }

// TestRecoveryFaultSink_Forwards pins the recovery sink wiring (spec 06 §3.2):
// a fault and a discarded-record count pass through to the sink by value.
func TestRecoveryFaultSink_Forwards(t *testing.T) {
	var gotFaults []string
	var gotDiscarded int
	sink := &recordingRecoverySink{
		fault: func(stage, outcome string) { gotFaults = append(gotFaults, stage+"/"+outcome) },
		disc:  func(stage string, n int) { gotDiscarded += n },
	}
	rm := &RecoveryManager{}
	rm.SetRecoveryFaultSink(sink)

	rm.addFault(interfaces.DegradedFault(RecoveryStageQueue, "queue q", "detail", "cost", nil))
	rm.addFault(interfaces.FatalFault(RecoveryStageQueue, "queue q", "detail", "cost", nil))
	rm.recordDiscarded(RecoveryStageQueue, 3)

	assert.Equal(t, []string{"queue-recovery/degraded", "queue-recovery/fatal"}, gotFaults)
	assert.Equal(t, 3, gotDiscarded)
}

type recordingRecoverySink struct {
	fault func(stage, outcome string)
	disc  func(stage string, n int)
}

func (r *recordingRecoverySink) RecordRecoveryFault(stage, outcome string) { r.fault(stage, outcome) }
func (r *recordingRecoverySink) RecordRecoveryDiscardedRecords(stage string, n int) {
	r.disc(stage, n)
}

// TestApplyAlarmBits_TripCounter (spec 06 §5 test #22): the 0->nonzero edge
// counts one trip per active resource; a repeat inside the alarmed state and a
// clear do not.
func TestApplyAlarmBits_TripCounter(t *testing.T) {
	spy := &wireSpy{}
	s := &Server{Log: zap.NewNop(), MetricsCollector: spy}

	s.applyAlarmBits(AlarmMemory)
	assert.Equal(t, int64(1), spy.alarmTrips.Load(), "the 0->memory edge counts one trip")

	s.applyAlarmBits(AlarmMemory)
	assert.Equal(t, int64(1), spy.alarmTrips.Load(), "no re-trip while already alarmed")

	s.applyAlarmBits(0)
	s.applyAlarmBits(AlarmDisk)
	assert.Equal(t, int64(2), spy.alarmTrips.Load(), "a second resource edge counts another trip")

	s.applyAlarmBits(0)
	s.applyAlarmBits(AlarmMemory | AlarmDisk)
	assert.Equal(t, int64(4), spy.alarmTrips.Load(), "both resources ticking on one edge counts two trips")
}

// TestPanicsCounter_Component (spec 06 §5 test #23): a recovered panic is
// counted under its bounded component; an unknown goroutine name maps to
// "other" rather than leaking the raw name.
func TestPanicsCounter_Component(t *testing.T) {
	spy := &wireSpy{}
	s := &Server{Log: zap.NewNop(), MetricsCollector: spy}

	s.notePanic("reader", "c1", "boom")
	assert.Equal(t, int64(1), spy.panics.Load())

	s.notePanic("ack-processor", "c1", "boom")
	assert.Equal(t, int64(2), spy.panics.Load(), "an unmapped goroutine still counts under a bounded component")

	assert.Equal(t, PanicComponentReader, panicComponent("reader"))
	assert.Equal(t, PanicComponentOther, panicComponent("ack-processor"))
}

// TestPublishRefused_MaxLength (spec 06 §5 test #11): publishing past
// x-max-length:1 with reject-publish counts a bounded refusal, once.
func TestPublishRefused_MaxLength(t *testing.T) {
	srv := newTinyRingServer(t)
	spy := &wireSpy{}
	srv.MetricsCollector = spy
	conn, frameCh := newPipeConnWithFrames(t)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)
	decl := &protocol.QueueDeclareMethod{
		Queue:     "maxlen-q",
		Arguments: map[string]interface{}{"x-max-length": int64(1), "x-overflow": "reject-publish"},
	}
	payload, err := decl.Serialize()
	require.NoError(t, err)
	require.NoError(t, srv.handleQueueDeclare(conn, 1, payload))
	drainFrame(t, frameCh)

	require.NoError(t, srv.processCompleteMessage(conn, 1, makeDurablePendingMessage("", "maxlen-q", "m1")))
	_ = srv.processCompleteMessage(conn, 1, makeDurablePendingMessage("", "maxlen-q", "m2"))

	require.Eventually(t, func() bool { return spy.pubRefusedMaxLen.Load() == 1 }, 3*time.Second, 5*time.Millisecond,
		"the second publish must be counted as an x_max_length refusal")
}

// TestPublishRefused_QueueClosed (spec 06 §5 test #12): a publish refused with
// ErrQueueClosed after routing counts the bounded queue_closed reason.
func TestPublishRefused_QueueClosed(t *testing.T) {
	spy := &wireSpy{}
	srv := newTinyRingServer(t)
	srv.Broker = &publishErrBroker{UnifiedBroker: srv.Broker, err: broker.ErrQueueClosed}
	srv.MetricsCollector = spy
	conn, _ := newPipeConnWithFrames(t)
	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	_ = srv.processCompleteMessage(conn, 1, makeDurablePendingMessage("", "q", "m"))
	assert.Equal(t, int64(1), spy.pubRefusedClosed.Load())
	assert.Equal(t, int64(0), spy.pubRefusedMaxLen.Load())
}

// TestDeliveriesRequeued_Counter (spec 06 §5 test #15, counter half): a
// vanished consumer's deliveries are DROPPED, not counted as requeued. The
// requeue counter counts only deliveries whose RejectMessage actually landed.
func TestDeliveriesRequeued_Counter(t *testing.T) {
	spy := &wireSpy{}
	s := &Server{Log: zap.NewNop(), MetricsCollector: spy, Broker: noConsumerBroker{}}
	s.requeueFailedDeliveries(
		[]*protocol.Delivery{{DeliveryTag: 1}, {DeliveryTag: 2}}, nil)
	assert.Equal(t, int64(0), spy.requeued.Load(),
		"a vanished consumer must not be counted as a requeue")
	assert.Equal(t, int64(2), spy.droppedCount(DeliveryDropConsumerRemoved),
		"the stranded deliveries must be counted as consumer_removed drops")
	assert.Equal(t, int64(2), spy.dropped.Load())
}

// TestConsumerRegisterError (spec 06 §5 test #16): a refused basic.consume
// registration is counted.
func TestConsumerRegisterError(t *testing.T) {
	srv := newTransactionTestServer(t)
	spy := &wireSpy{}
	srv.MetricsCollector = spy
	conn, _ := newPipeConnWithFrames(t)
	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	// No queue declared -> RegisterConsumer fails.
	payload := encodeBasicConsume(t, "no-such-queue", "ct", false)
	require.Error(t, srv.handleBasicConsume(conn, 1, payload))
	assert.Equal(t, int64(1), spy.consumerRegErrors.Load())
}

// TestConnectionsClosedByCause_ForcedShutdown (spec 06 §5 test #19): a live
// connection torn down by broker shutdown records the forced_shutdown cause,
// and a failed socket write records exactly one write_error cause.
func TestConnectionsClosedByCause_ForcedShutdown(t *testing.T) {
	srv := newTransactionTestServer(t)
	spy := &wireSpy{}
	srv.MetricsCollector = spy
	conn, _ := newPipeConnWithFrames(t)
	srv.Mutex.Lock()
	srv.Connections[conn.ID] = conn
	srv.Mutex.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)

	assert.GreaterOrEqual(t, spy.causeCount(ConnectionCloseForcedShutdown), int64(1),
		"a connection closed by shutdown must record the forced_shutdown cause")

	// A hard write failure is its own bounded cause, recorded exactly once by
	// the once-guarded close helper even though writeLocked is called again.
	writeSrv := &Server{Log: zap.NewNop(), MetricsCollector: spy}
	wconn, _ := newPipeConnWithFrames(t)
	require.NoError(t, wconn.Conn.Close()) // any subsequent write fails
	require.Error(t, writeSrv.writeFrame(wconn, &protocol.Frame{Type: protocol.FrameHeartbeat}))
	// A second failed write must not double-count.
	_ = writeSrv.writeFrame(wconn, &protocol.Frame{Type: protocol.FrameHeartbeat})
	assert.Equal(t, int64(1), spy.causeCount(ConnectionCloseWriteError),
		"a failed write must record exactly one write_error cause")
}

// TestDepthGateArmed_CounterAndGauge (spec 06 §5 test #13): arming the
// queue-depth reader gate on a producer-only connection increments the counter
// and the live gauge reads the gated-connection count.
func TestDepthGateArmed_CounterAndGauge(t *testing.T) {
	srv := newTinyRingServer(t)
	spy := &wireSpy{}
	srv.MetricsCollector = spy
	conn, frameCh := newPipeConnWithFrames(t)
	sq5Setup(t, srv, conn, frameCh, "tele-depth-q")

	done := make(chan struct{})
	go srv.confirmFlusher(conn, done)
	defer func() { <-done }()
	defer close(conn.Done)

	deadline := time.After(5 * time.Second)
	for !conn.DepthBackpressure.Load() {
		select {
		case <-deadline:
			t.Fatal("depth gate never armed")
		default:
		}
		require.NoError(t, srv.processCompleteMessage(conn, 1, makeDurablePendingMessage("", "tele-depth-q", "m")))
		time.Sleep(time.Millisecond)
	}

	assert.GreaterOrEqual(t, spy.depthGateArmed.Load(), int64(1))
	assert.Equal(t, int64(1), srv.DepthGatedCurrent(), "exactly one connection is gated")
}

// errCloser is a storage whose Close fails.
type errCloser struct {
	interfaces.Storage
	err error
}

func (e errCloser) Close() error { return e.err }

// TestStorageCloseError_Counter (spec 06 §5 test #21): a storage Close failure
// during shutdown increments the storage-close-error counter.
func TestStorageCloseError_Counter(t *testing.T) {
	srv := newTransactionTestServer(t)
	spy := &wireSpy{}
	srv.MetricsCollector = spy
	srv.storage = errCloser{err: assert.AnError}
	srv.ownsStorage = true

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.shutdown(ctx)

	assert.Equal(t, int64(1), spy.storageCloseErrs.Load())
}

// TestShutdownMetrics_Recorded pins the shutdown telemetry site (spec 06
// §3.5): a Shutdown records exactly one result.
func TestShutdownMetrics_Recorded(t *testing.T) {
	srv := newTransactionTestServer(t)
	spy := &wireSpy{}
	srv.MetricsCollector = spy

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)

	assert.Equal(t, int64(1), spy.shutdowns.Load(), "exactly one shutdown result is recorded")
}

// The production collector must satisfy the recovery sink interface, so a
// build that drops a method fails here rather than at runtime (a no-op boot).
var _ RecoveryFaultSink = (*metrics.Collector)(nil)

// The production collector must also satisfy the three live-gauge binders.
var (
	_ DepthGateStatsBinder        = (*metrics.Collector)(nil)
	_ ReaderPausedStatsBinder     = (*metrics.Collector)(nil)
	_ MessagesInflightStatsBinder = (*metrics.Collector)(nil)
)
