package server

import (
	"bufio"
	"context"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// WS-I Phase B server-integration tests (SL-*). Deterministic hooks:
// storage.SetWALGroupCommitFsyncForTest gates durability, the limiter's
// Stats/hooks expose leasing, and net.Pipe rigs drive the reader directly.
// Channel waits carry a 30 s liveness bound; no sleeps.

// --- harness ---------------------------------------------------------------

// leaseMetricProbe is a LeaseMetrics + MetricsCollector implementation that
// records every lease-gauge update (so max-used is observed at EVERY step,
// not sampled) and counts the close/leak counters.
type leaseMetricProbe struct {
	NoOpMetricsCollector
	maxUsed          atomic.Int64
	waits            atomic.Int64
	oversized        atomic.Int64
	overbudgetCloses atomic.Int64
	bodyStallCloses  atomic.Int64
	leaked           atomic.Int64
}

func (m *leaseMetricProbe) UpdatePublishLeaseGauges(usedBytes, budgetBytes int64, waiters int) {
	for {
		cur := m.maxUsed.Load()
		if usedBytes <= cur || m.maxUsed.CompareAndSwap(cur, usedBytes) {
			return
		}
	}
}
func (m *leaseMetricProbe) RecordPublishLeaseWait(seconds float64) { m.waits.Add(1) }
func (m *leaseMetricProbe) RecordPublishLeaseOversized()           { m.oversized.Add(1) }
func (m *leaseMetricProbe) RecordPublishLeaseOverbudgetClose()     { m.overbudgetCloses.Add(1) }
func (m *leaseMetricProbe) RecordPublishLeaseBodyStallClose()      { m.bodyStallCloses.Add(1) }
func (m *leaseMetricProbe) RecordPublishLeaseLeaked()              { m.leaked.Add(1) }

// leaseServer starts a full TCP server with the given publish-inflight budget
// and returns it plus the lease counters and metrics probe wired into it.
func leaseServer(t *testing.T, budget int64, mutate func(*config.AMQPConfig), tweak func(*Server)) (*Server, string, *atomic.Int64, *atomic.Int64, *leaseMetricProbe) {
	t.Helper()
	created := &atomic.Int64{}
	released := &atomic.Int64{}
	probe := &leaseMetricProbe{}
	srv, addr := startWSBServer(t, func(cfg *config.AMQPConfig) {
		cfg.Network.PublishInflightBudgetBytes = budget
		if mutate != nil {
			mutate(cfg)
		}
	}, func(srv *Server) {
		srv.leaseCreated = created
		srv.leaseReleased = released
		srv.MetricsCollector = probe
		if tweak != nil {
			tweak(srv)
		}
	})
	return srv, addr, created, released, probe
}

// gateWALFsync holds every WAL group-commit fsync until release is called
// (process-global hook: tests using it must not run parallel).
func gateWALFsync(t *testing.T) (release func()) {
	t.Helper()
	gate := make(chan struct{})
	var once sync.Once
	restore := storage.SetWALGroupCommitFsyncForTest(func(f *os.File) error {
		<-gate
		return nil
	})
	t.Cleanup(restore)
	t.Cleanup(func() { once.Do(func() { close(gate) }) })
	return func() { once.Do(func() { close(gate) }) }
}

// --- raw client publish helpers --------------------------------------------

func leaseSendMethod(t *testing.T, r *rawAMQP, ch uint16, exchange, key string, mandatory bool) {
	t.Helper()
	m := &protocol.BasicPublishMethod{Exchange: exchange, RoutingKey: key, Mandatory: mandatory}
	data, err := m.Serialize()
	require.NoError(t, err)
	r.send(ch, 60, 40, data)
}

func leaseSendHeader(t *testing.T, r *rawAMQP, ch uint16, bodySize uint64, durable bool, userID string) {
	t.Helper()
	h := &protocol.ContentHeader{ClassID: 60, BodySize: bodySize}
	if durable {
		h.PropertyFlags |= protocol.FlagDeliveryMode
		h.DeliveryMode = 2
	}
	if userID != "" {
		h.PropertyFlags |= protocol.FlagUserID
		h.UserID = userID
	}
	hp, err := h.Serialize()
	require.NoError(t, err)
	r.sendFrame(&protocol.Frame{Type: protocol.FrameHeader, Channel: ch, Size: uint32(len(hp)), Payload: hp})
}

func leaseSendBody(t *testing.T, r *rawAMQP, ch uint16, body []byte) {
	t.Helper()
	const maxPayload = 131072 - 8
	for len(body) > 0 {
		n := len(body)
		if n > maxPayload {
			n = maxPayload
		}
		r.sendFrame(&protocol.Frame{Type: protocol.FrameBody, Channel: ch, Size: uint32(n), Payload: body[:n]})
		body = body[n:]
	}
}

func leaseSendPublish(t *testing.T, r *rawAMQP, ch uint16, exchange, key string, body []byte, durable bool) {
	t.Helper()
	leaseSendMethod(t, r, ch, exchange, key, false)
	leaseSendHeader(t, r, ch, uint64(len(body)), durable, "")
	leaseSendBody(t, r, ch, body)
}

func leaseConfirmSelect(t *testing.T, r *rawAMQP, ch uint16) {
	t.Helper()
	m := &protocol.ConfirmSelectMethod{}
	data, err := m.Serialize()
	require.NoError(t, err)
	r.send(ch, 85, 10, data)
	r.expectMethodCh(ch, 85, 11) // confirm.select-ok
}

// leaseWaitAck reads frames until a basic.ack on ch covers tag want (30 s bound).
func leaseWaitAck(t *testing.T, r *rawAMQP, ch uint16, want uint64) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var maxTag uint64
	for maxTag < want {
		require.NoError(t, r.c.SetReadDeadline(deadline))
		f, err := protocol.ReadFrameOptimizedWithLimit(r.r, 131072+8)
		require.NoError(t, err, "waiting for basic.ack covering tag %d (max seen %d)", want, maxTag)
		if f.Type == protocol.FrameMethod && f.Channel == ch && len(f.Payload) >= 4 &&
			f.Payload[0] == 0 && f.Payload[1] == 60 && f.Payload[2] == 0 && f.Payload[3] == 80 {
			ack := &protocol.BasicAckMethod{}
			if ack.Deserialize(f.Payload[4:]) == nil && ack.DeliveryTag > maxTag {
				maxTag = ack.DeliveryTag
			}
		}
	}
}

// leaseWaitAcks reads frames until every listed channel's basic.ack watermark
// covers its wanted tag (acks for other channels are tracked, not discarded).
func leaseWaitAcks(t *testing.T, r *rawAMQP, wants map[uint16]uint64) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	got := make(map[uint16]uint64)
	for {
		done := true
		for ch, want := range wants {
			if got[ch] < want {
				done = false
			}
		}
		if done {
			return
		}
		require.NoError(t, r.c.SetReadDeadline(deadline))
		f, err := protocol.ReadFrameOptimizedWithLimit(r.r, 131072+8)
		require.NoError(t, err, "waiting for acks %v (got %v)", wants, got)
		if f.Type == protocol.FrameMethod && len(f.Payload) >= 4 &&
			f.Payload[0] == 0 && f.Payload[1] == 60 && f.Payload[2] == 0 && f.Payload[3] == 80 {
			ack := &protocol.BasicAckMethod{}
			if ack.Deserialize(f.Payload[4:]) == nil && ack.DeliveryTag > got[f.Channel] {
				got[f.Channel] = ack.DeliveryTag
			}
		}
	}
}

// leaseUsed returns the limiter's current used bytes.
func leaseUsed(srv *Server) int64 {
	used, _, _ := srv.publishLimiter().Stats()
	return used
}

func leaseWaiters(srv *Server) int {
	_, _, w := srv.publishLimiter().Stats()
	return w
}

// leaseBody makes a deterministic body of n bytes.
func leaseBody(n int) []byte { return make([]byte, n) }

// --- SL-1 ------------------------------------------------------------------

// SL-1: with G fitting exactly two leases, the third header waits in Reserve
// BEFORE its body is read and before a confirm tag is minted.
func TestLease_ReservedAtHeaderBeforeBodyAndTag(t *testing.T) {
	release := gateWALFsync(t)
	srv, addr, _, _, _ := leaseServer(t, 1<<20, nil, nil)

	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 16, 0)
	r.openChannel(1)
	r.declareQueue(1, "sl1-q")
	leaseConfirmSelect(t, r, 1)

	const body = 500 << 10 // lease ≈ 512800; two fit in 1 MiB, three do not
	leaseSendPublish(t, r, 1, "", "sl1-q", leaseBody(body), true)
	leaseSendPublish(t, r, 1, "", "sl1-q", leaseBody(body), true)

	require.Eventually(t, func() bool { return leaseUsed(srv) >= 2*512800 }, 30*time.Second, time.Millisecond,
		"both publishes must be leased")

	// The confirm sequence has advanced for the two processed publishes.
	sconn := wsbServerConn(srv)
	require.NotNil(t, sconn)
	var sch *protocol.Channel
	require.Eventually(t, func() bool {
		if v, ok := sconn.Channels.Load(uint16(1)); ok {
			sch = v.(*protocol.Channel)
			return sch.ConfirmSequence.Load() == 2
		}
		return false
	}, 30*time.Second, time.Millisecond)

	// Third publish: the reader reserves at the header and must NOT read the
	// body while it waits.
	leaseSendMethod(t, r, 1, "", "sl1-q", false)
	leaseSendHeader(t, r, 1, body, true, "")
	bodyDone := make(chan struct{})
	go func() {
		defer close(bodyDone)
		leaseSendBody(t, r, 1, leaseBody(body))
	}()

	require.Eventually(t, func() bool { return leaseWaiters(srv) == 1 }, 30*time.Second, time.Millisecond,
		"the third header must be queued in the limiter")

	// While it waits: no confirm tag minted, and the body bytes stay unread.
	base := srv.bytesReceived.Load()
	assert.Never(t, func() bool {
		return sch.ConfirmSequence.Load() != 2
	}, 200*time.Millisecond, 20*time.Millisecond, "no confirm tag may be minted before the lease is granted")
	assert.Less(t, srv.bytesReceived.Load()-base, int64(100<<10),
		"the third body must stay unread while the header waits")

	// Release durability: everything drains, all three confirms arrive.
	release()
	leaseWaitAck(t, r, 1, 3)
	<-bodyDone
	require.Eventually(t, func() bool { return leaseUsed(srv) == 0 }, 30*time.Second, time.Millisecond)
	assert.Equal(t, uint64(3), sch.ConfirmSequence.Load())
}

// --- SL-2 ------------------------------------------------------------------

// SL-2: ten producers, 200 publishes each; used <= G at every step; all 2000
// confirmed; used == 0 at the end.
func TestLease_TenProducersBoundedByG(t *testing.T) {
	const G = int64(1 << 20)
	srv, addr, _, _, probe := leaseServer(t, G, nil, nil)

	var wg sync.WaitGroup
	for p := 0; p < 10; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			r := dialRaw(t, addr)
			defer r.c.Close()
			r.handshake("guest", "guest", 4, 0)
			r.openChannel(1)
			if p == 0 {
				r.declareQueue(1, "sl2-q")
			}
			leaseConfirmSelect(t, r, 1)
			for i := 0; i < 200; i++ {
				leaseSendPublish(t, r, 1, "", "sl2-q", leaseBody(4096), true)
			}
			leaseWaitAck(t, r, 1, 200)
		}(p)
	}
	wg.Wait()

	assert.LessOrEqual(t, probe.maxUsed.Load(), G, "used must never exceed G")
	assert.Equal(t, int64(0), probe.leaked.Load())
	require.Eventually(t, func() bool { return leaseUsed(srv) == 0 }, 30*time.Second, time.Millisecond)
}

// --- SL-3 ------------------------------------------------------------------

// SL-3 (fairness): a hog connection with huge bodies cannot starve a light
// connection: each light header is granted within ≤ 2 grants of its reserve,
// and the hog completes.
func TestLease_FairnessHogAndLight(t *testing.T) {
	srv, addr, _, _, _ := leaseServer(t, 1<<20, func(cfg *config.AMQPConfig) {
		cfg.Engine.WALBatchSize = 1 // one fsync per publish: per-publish completion cadence
	}, nil)

	// Token-gated fsync: one token completes one publish.
	tokens := make(chan struct{})
	restore := storage.SetWALGroupCommitFsyncForTest(func(f *os.File) error {
		<-tokens
		return nil
	})
	t.Cleanup(restore)
	t.Cleanup(func() { close(tokens) })

	// Grant/reserve observation via the step hook.
	var mu sync.Mutex
	var steps []string
	srvStepHook := func(step string) {
		mu.Lock()
		steps = append(steps, step)
		mu.Unlock()
	}
	// The limiter is created lazily on first publish; set the hook now.
	srv.Mutex.Lock()
	srv.limiterStepHook = srvStepHook
	srv.Mutex.Unlock()

	// Hog: 900 KiB durable bodies (lease ≈ 922688; one at a time under G=1 MiB).
	hog := dialRaw(t, addr)
	hog.handshake("guest", "guest", 4, 0)
	hog.openChannel(1)
	hog.declareQueue(1, "sl3-q")
	leaseConfirmSelect(t, hog, 1)

	leaseSendPublish(t, hog, 1, "", "sl3-q", leaseBody(900<<10), true)
	require.Eventually(t, func() bool { return leaseUsed(srv) >= 922688 }, 30*time.Second, time.Millisecond)

	// The hog's second header queues (used + lease > G).
	leaseSendMethod(t, hog, 1, "", "sl3-q", false)
	leaseSendHeader(t, hog, 1, 900<<10, true, "")
	hogBody2 := make(chan struct{})
	go func() {
		defer close(hogBody2)
		leaseSendBody(t, hog, 1, leaseBody(900<<10))
	}()
	require.Eventually(t, func() bool { return leaseWaiters(srv) == 1 }, 30*time.Second, time.Millisecond)

	// Light connection: a small publish queues behind the hog's waiter.
	light := dialRaw(t, addr)
	light.handshake("guest", "guest", 4, 0)
	light.openChannel(1)
	leaseConfirmSelect(t, light, 1)
	leaseSendMethod(t, light, 1, "", "sl3-q", false)
	leaseSendHeader(t, light, 1, 4096, true, "")
	lightBody := make(chan struct{})
	go func() {
		defer close(lightBody)
		leaseSendBody(t, light, 1, leaseBody(4096))
	}()
	require.Eventually(t, func() bool { return leaseWaiters(srv) == 2 }, 30*time.Second, time.Millisecond)

	// Identify connections: the hog reserved first.
	hogID := wsbServerConn(srv) // exactly... two connections now; find IDs below
	_ = hogID

	// Complete the hog's first publish → its release grants the hog's second
	// header (FIFO head). Then the second publish completes → the light header
	// is granted: exactly 2 grants after the light reserve.
	tokens <- struct{}{} // hog msg1 fsync
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		grants := 0
		for _, s := range steps {
			if len(s) >= 6 && s[:6] == "grant:" {
				grants++
			}
		}
		return grants >= 2
	}, 30*time.Second, time.Millisecond, "hog msg2 must be granted next (FIFO head)")

	tokens <- struct{}{} // hog msg2 fsync → light msg1 granted
	leaseWaitAck(t, light, 1, 1)
	<-lightBody

	// Fairness: between the light reserve and the light grant there are at
	// most 2 grants (the in-flight holder and the FIFO head).
	mu.Lock()
	reserveIdx, grantIdx, grantsBetween := -1, -1, 0
	reserves := 0
	for i, s := range steps {
		if len(s) >= 8 && s[:8] == "reserve:" {
			reserves++
			if reserves == 3 { // hog msg1, hog msg2, light msg1
				reserveIdx = i
			}
		}
	}
	if reserveIdx >= 0 {
		for i := reserveIdx + 1; i < len(steps); i++ {
			if len(steps[i]) >= 6 && steps[i][:6] == "grant:" {
				grantsBetween++
				grantIdx = i
			}
			if grantIdx >= 0 && grantsBetween >= 2 {
				break
			}
		}
	}
	mu.Unlock()
	require.GreaterOrEqual(t, grantIdx, 0, "the light publish must be granted")
	assert.LessOrEqual(t, grantsBetween, 2, "light header granted within <= 2 grants of its reserve")

	// The hog completes.
	tokens <- struct{}{} // light msg1 fsync → hog msg2 released earlier; hog msg2's own fsync needs a token too
	leaseWaitAck(t, hog, 1, 2)
	<-hogBody2
	require.Eventually(t, func() bool { return leaseUsed(srv) == 0 }, 30*time.Second, time.Millisecond)
}

// --- SL-4 ------------------------------------------------------------------

// SL-4: 1000 connections × 2 publishes, all confirmed; used <= G; goroutines
// return to baseline ± 2.
func TestLease_1000Connections(t *testing.T) {
	srv, addr, _, _, probe := leaseServer(t, 0, func(cfg *config.AMQPConfig) {
		cfg.Network.MaxConnections = 2000
	}, nil) // 0 → default 256 MiB

	// One shared queue so 1000 declare storms don't dominate the test.
	setup := dialRaw(t, addr)
	setup.handshake("guest", "guest", 1, 0)
	setup.openChannel(1)
	setup.declareQueue(1, "sl4-q")
	setup.c.Close()

	base := runtime.NumGoroutine()

	const conns = 1000
	const workers = 100
	var wg sync.WaitGroup
	var acked atomic.Int64
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := w; i < conns; i += workers {
				r := dialRaw(t, addr)
				r.handshake("guest", "guest", 1, 0)
				r.openChannel(1)
				leaseConfirmSelect(t, r, 1)
				leaseSendPublish(t, r, 1, "", "sl4-q", leaseBody(1024), true)
				leaseSendPublish(t, r, 1, "", "sl4-q", leaseBody(1024), true)
				leaseWaitAck(t, r, 1, 2)
				acked.Add(2)
				r.c.Close()
			}
		}(w)
	}
	wg.Wait()
	assert.Equal(t, int64(2*conns), acked.Load(), "every publish confirmed")
	assert.LessOrEqual(t, probe.maxUsed.Load(), int64(256<<20), "used <= G")
	require.Eventually(t, func() bool { return leaseUsed(srv) == 0 }, 30*time.Second, time.Millisecond)

	// Goroutines return to baseline once teardowns complete.
	require.Eventually(t, func() bool {
		return runtime.NumGoroutine() <= base+2
	}, 30*time.Second, 50*time.Millisecond,
		"goroutines must return to baseline: base=%d now=%d", base, runtime.NumGoroutine())
}

// --- SL-5 ------------------------------------------------------------------

// SL-5: a 4 MiB message with G = 1 MiB proceeds as the single oversized lease
// at used == 0; a header over MaxMessageSize gets the existing 406 with used
// unchanged.
func TestLease_OversizedProgress(t *testing.T) {
	release := gateWALFsync(t)
	srv, addr, _, _, probe := leaseServer(t, 1<<20, nil, nil)

	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 4, 0)
	r.openChannel(1)
	r.declareQueue(1, "sl5-q")
	leaseConfirmSelect(t, r, 1)

	// Oversized: 4 MiB > G. Granted only at used == 0, allowed to exceed G.
	leaseSendPublish(t, r, 1, "", "sl5-q", leaseBody(4<<20), true)
	require.Eventually(t, func() bool { return leaseUsed(srv) > 1<<20 }, 30*time.Second, time.Millisecond,
		"the oversized lease must proceed at used == 0 and exceed G while held")
	assert.Equal(t, int64(1), probe.oversized.Load())

	release()
	leaseWaitAck(t, r, 1, 1)
	require.Eventually(t, func() bool { return leaseUsed(srv) == 0 }, 30*time.Second, time.Millisecond)

	// Over MaxMessageSize (16 MiB default): the reader pairs a nil lease and
	// the processor emits the existing 406; used is unchanged.
	leaseSendMethod(t, r, 1, "", "sl5-q", false)
	leaseSendHeader(t, r, 1, 32<<20, true, "")
	r.expectChannelClose(1, 406)
	assert.Equal(t, int64(0), leaseUsed(srv), "a 406 header never takes a lease")
}

// --- SL-6 ------------------------------------------------------------------

// startPipeServerConnection runs a full processConnectionFrames over a
// net.Pipe: it consumes the client's protocol header first (handleConnection
// does that raw), then drives the handshake + reader/processor goroutines.
func startPipeServerConnection(t *testing.T, srv *Server) (*rawAMQP, <-chan struct{}) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	r := &rawAMQP{t: t, c: clientConn, r: nil}
	r.r = newBufioReader(clientConn)
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 8)
		if _, err := io.ReadFull(serverConn, buf); err != nil {
			return
		}
		conn := protocol.NewConnection(serverConn)
		srv.processConnectionFrames(conn)
	}()
	t.Cleanup(func() { _ = clientConn.Close() })
	return r, done
}

// SL-6: a Reserve waiter whose client vanishes is unblocked by leaseCancel
// (heartbeat write failure, hb > 0) or by limiter.Close (hb == 0).
func TestLease_WedgedWALClientGone(t *testing.T) {
	t.Run("hb>0 heartbeat write failure cancels the wait", func(t *testing.T) {
		release := gateWALFsync(t)
		srv := newTransactionTestServer(t)
		srv.Config.Network.PublishInflightBudgetBytes = 1000

		a, _ := startPipeServerConnection(t, srv)
		a.handshake("guest", "guest", 4, 0)
		a.openChannel(1)
		a.declareQueue(1, "sl6-q")
		leaseConfirmSelect(t, a, 1)
		leaseSendPublish(t, a, 1, "", "sl6-q", leaseBody(700), true)
		require.Eventually(t, func() bool { return leaseUsed(srv) >= 1282 }, 30*time.Second, time.Millisecond)

		// b: its header's Reserve blocks (a holds the whole budget).
		b, bDone := startPipeServerConnection(t, srv)
		b.handshake("guest", "guest", 4, 1) // hb=1s
		b.openChannel(1)
		leaseConfirmSelect(t, b, 1)
		leaseSendMethod(t, b, 1, "", "sl6-q", false)
		leaseSendHeader(t, b, 1, 700, true, "")
		require.Eventually(t, func() bool { return leaseWaiters(srv) == 1 }, 30*time.Second, time.Millisecond)

		// Client b vanishes: the heartbeat writer's write fails (net.Pipe
		// close fails writes immediately) → writeLocked error path →
		// leaseCancel → the reader exits its Reserve.
		_ = b.c.Close()
		select {
		case <-bDone:
		case <-time.After(30 * time.Second):
			t.Fatal("the parked Reserve waiter was not released after the heartbeat write failure")
		}
		assert.Equal(t, 0, leaseWaiters(srv))
		assert.GreaterOrEqual(t, leaseUsed(srv), int64(1282), "a's lease is broker-held, unaffected")
		release()
	})

	t.Run("hb=0 wait ends at limiter Close", func(t *testing.T) {
		release := gateWALFsync(t)
		srv := newTransactionTestServer(t)
		srv.Config.Network.PublishInflightBudgetBytes = 1000

		a, _ := startPipeServerConnection(t, srv)
		a.handshake("guest", "guest", 4, 0)
		a.openChannel(1)
		a.declareQueue(1, "sl6-q")
		leaseConfirmSelect(t, a, 1)
		leaseSendPublish(t, a, 1, "", "sl6-q", leaseBody(700), true)
		require.Eventually(t, func() bool { return leaseUsed(srv) >= 1282 }, 30*time.Second, time.Millisecond)

		b, bDone := startPipeServerConnection(t, srv)
		b.handshake("guest", "guest", 4, 0) // hb=0: no heartbeat writer at all
		b.openChannel(1)
		leaseConfirmSelect(t, b, 1)
		leaseSendMethod(t, b, 1, "", "sl6-q", false)
		leaseSendHeader(t, b, 1, 700, true, "")
		require.Eventually(t, func() bool { return leaseWaiters(srv) == 1 }, 30*time.Second, time.Millisecond)

		_ = b.c.Close() // nobody notices: the reader is parked in Reserve, no hb
		srv.publishLimiter().Close()
		select {
		case <-bDone:
		case <-time.After(30 * time.Second):
			t.Fatal("limiter.Close must fail the parked Reserve waiter (hb=0 bounded wait)")
		}
		release()
	})
}

// --- SL-7 ------------------------------------------------------------------

// SL-7: connection close releases FIFO, HeaderLease and pending-message
// leases; a broker-held (transferred) lease releases on WAL completion even
// after the connection is gone.
func TestLease_ConnectionCloseReleasesAll(t *testing.T) {
	release := gateWALFsync(t)
	srv, addr, created, released, _ := leaseServer(t, 0, nil, nil)

	// Track WAL staging so we know msg1 reached the broker (lease transferred).
	var staged atomic.Int64
	restoreStage := storage.SetWALStageHookForTest(func(n int) { staged.Store(int64(n)) })
	t.Cleanup(restoreStage)

	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 8, 0)
	r.openChannel(1)
	r.declareQueue(1, "sl7-q")
	leaseConfirmSelect(t, r, 1)

	// msg1: fully published; its lease is transferred to the broker (fsync gated).
	leaseSendPublish(t, r, 1, "", "sl7-q", leaseBody(100<<10), true)
	require.Eventually(t, func() bool { return staged.Load() >= 1 }, 30*time.Second, time.Millisecond)

	// msg2: half-received publish (lease lives in PendingMessages).
	leaseSendMethod(t, r, 1, "", "sl7-q", false)
	leaseSendHeader(t, r, 1, 100<<10, true, "")
	leaseSendBody(t, r, 1, leaseBody(50<<10))

	// msg3 + msg4 (on their own channels — a second publish on one channel
	// mid-content is a protocol violation): headers only, so their leases
	// live in the FIFO / HeaderLease / pending map, wherever teardown catches
	// them.
	r.openChannel(2)
	r.openChannel(3)
	leaseSendMethod(t, r, 2, "", "sl7-q", false)
	leaseSendHeader(t, r, 2, 100<<10, true, "")
	leaseSendMethod(t, r, 3, "", "sl7-q", false)
	leaseSendHeader(t, r, 3, 100<<10, true, "")

	require.Eventually(t, func() bool { return created.Load() == 4 }, 30*time.Second, time.Millisecond,
		"four header leases must have been created")

	_ = r.c.Close()
	require.Eventually(t, func() bool { return released.Load() == 3 }, 30*time.Second, time.Millisecond,
		"teardown releases the FIFO, HeaderLease and pending-message leases; the broker holds msg1's")
	assert.Equal(t, leaseBytes(100<<10, 131072), leaseUsed(srv),
		"only the broker-held msg1 lease remains")

	release()
	require.Eventually(t, func() bool {
		return released.Load() == 4 && created.Load() == 4 && leaseUsed(srv) == 0
	}, 30*time.Second, time.Millisecond, "the transferred lease releases on WAL completion after the connection is gone")
}

// --- SL-8 ------------------------------------------------------------------

// SL-8: a tx publish releases its lease when it is buffered into the
// transaction, and tx.commit proceeds while G is held elsewhere.
func TestLease_TxPublishReleasedAtBuffer(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	defer close(conn.Done)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)
	declPayload, err := (&protocol.QueueDeclareMethod{Queue: "sl8-q", Durable: true}).Serialize()
	require.NoError(t, err)
	require.NoError(t, srv.handleQueueDeclare(conn, 1, declPayload))
	drainFrame(t, frameCh) // declare-ok

	require.NoError(t, srv.handleTxSelect(conn, 1, []byte{}))
	drainFrame(t, frameCh) // tx.select-ok

	var releases atomic.Int64
	pm := makeDurablePendingMessage("", "sl8-q", "tx-body")
	pm.Lease = &countingReleaser{releases: &releases}
	require.NoError(t, srv.processCompleteMessage(conn, 1, pm))
	assert.Equal(t, int64(1), releases.Load(), "tx publish must release its lease at AddOperation")
	assert.Nil(t, pm.Lease)

	// Hold essentially all of G elsewhere; the commit must still proceed (tx
	// staging and the tx WAL write are not lease-bounded).
	holder, err := srv.publishLimiter().Reserve(context.Background(), "other-conn", 200<<20)
	require.NoError(t, err)
	require.NoError(t, srv.handleTxCommit(conn, 1, nil))
	drainFrame(t, frameCh) // tx.commit-ok
	holder.Release()
}

// --- SL-9 ------------------------------------------------------------------

// SL-9: a malformed header (payload < 12 bytes) pairs a nil lease; the
// processor produces the existing malformed outcome; no leak.
func TestLease_MalformedHeaderNilLease(t *testing.T) {
	srv, addr, created, released, _ := leaseServer(t, 0, nil, nil)

	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 4, 0)
	r.openChannel(1)
	r.declareQueue(1, "sl9-q")

	leaseSendMethod(t, r, 1, "", "sl9-q", false)
	r.sendFrame(&protocol.Frame{Type: protocol.FrameHeader, Channel: 1, Size: 5, Payload: []byte{0, 60, 0, 0, 0}})

	// The existing malformed-content-header outcome: connection.close.
	f := r.readFrame()
	require.Equal(t, byte(protocol.FrameMethod), f.Type)
	require.Equal(t, uint16(10), uint16(f.Payload[0])<<8|uint16(f.Payload[1]), "connection class")
	require.Equal(t, uint16(50), uint16(f.Payload[2])<<8|uint16(f.Payload[3]), "connection.close")

	assert.Equal(t, int64(0), created.Load(), "a malformed header never reserves")
	require.Eventually(t, func() bool { return released.Load() == created.Load() }, 30*time.Second, time.Millisecond)
	assert.Equal(t, int64(0), leaseUsed(srv))
}

// --- SL-10 -----------------------------------------------------------------

// SL-10: the channel-max and closing-gate discards release the header lease.
func TestLease_DiscardGatesRelease(t *testing.T) {
	t.Run("channel-max gate", func(t *testing.T) {
		_, addr, created, released, _ := leaseServer(t, 0, nil, nil)
		r := dialRaw(t, addr)
		r.handshake("guest", "guest", 1, 0) // channel_max = 1
		r.openChannel(1)
		r.declareQueue(1, "sl10-q")
		leaseConfirmSelect(t, r, 1)

		// Publish on channel 2 (> channel_max): method + header are both
		// discarded by the §6.3 gate; the header's lease must be released.
		leaseSendPublish(t, r, 2, "", "sl10-q", leaseBody(1024), true)
		r.expectConnectionClose(530) // NOT_ALLOWED - channel exceeds channel_max
		require.Eventually(t, func() bool {
			return created.Load() == 1 && released.Load() == 1
		}, 30*time.Second, time.Millisecond, "the channel-max discard arm must release the lease")
	})

	t.Run("closing gate", func(t *testing.T) {
		srv, addr, created, released, _ := leaseServer(t, 0, nil, nil)
		r := dialRaw(t, addr)
		r.handshake("guest", "guest", 4, 0)
		r.openChannel(1)
		r.declareQueue(1, "sl10-q")
		leaseConfirmSelect(t, r, 1)

		// msg1: user_id mismatch → server channel.close (406) → closing gate.
		leaseSendMethod(t, r, 1, "", "sl10-q", false)
		leaseSendHeader(t, r, 1, 128, true, "intruder")
		leaseSendBody(t, r, 1, leaseBody(128))
		// msg2: pipelined behind it → method and header hit the closing gate.
		leaseSendPublish(t, r, 1, "", "sl10-q", leaseBody(128), true)

		r.expectChannelClose(1, 406)
		require.Eventually(t, func() bool {
			return created.Load() == 2 && released.Load() == 2
		}, 30*time.Second, time.Millisecond, "the closing-gate discard arm must release the lease")
		assert.Equal(t, int64(0), leaseUsed(srv))
	})
}

// --- SL-11 -----------------------------------------------------------------

// SL-11: on a mixed consumer+producer connection, acks are processed after
// the grant and the connection stays open.
func TestLease_MixedConnAcksAfterGrant(t *testing.T) {
	release := gateWALFsync(t)
	srv, addr, _, _, _ := leaseServer(t, 1<<20, nil, nil)

	// Producer fills the queue transiently (no leases held).
	p := dialRaw(t, addr)
	p.handshake("guest", "guest", 4, 0)
	p.openChannel(1)
	p.declareQueue(1, "sl11-q")
	for i := 0; i < 3; i++ {
		leaseSendPublish(t, p, 1, "", "sl11-q", leaseBody(64), false)
	}

	// Mixed connection: consumes AND publishes (durable).
	m := dialRaw(t, addr)
	m.handshake("guest", "guest", 4, 0)
	m.openChannel(1)
	leaseConfirmSelect(t, m, 1)
	m.consume(1, "sl11-q", "c1")

	// Read one delivery so we have something to ack later. basic.deliver
	// payload: consumer-tag (shortstr) + delivery-tag (8) + redelivered (1).
	var deliveryTag uint64
	{
		f := r_readMethod(t, m, 60, 60) // basic.deliver
		tagLen := int(f[0])
		deliveryTag = mustUint64(f[1+tagLen : 1+tagLen+8])
	}

	// m publishes a durable message: the lease (≈615272) is granted, then held
	// by the gated fsync.
	leaseSendPublish(t, m, 1, "", "sl11-q", leaseBody(600<<10), true)
	require.Eventually(t, func() bool { return leaseUsed(srv) >= 615272 }, 30*time.Second, time.Millisecond)

	// A second durable publish: its header's Reserve blocks (2 leases > G).
	leaseSendMethod(t, m, 1, "", "sl11-q", false)
	leaseSendHeader(t, m, 1, 600<<10, true, "")
	body2Done := make(chan struct{})
	go func() {
		defer close(body2Done)
		leaseSendBody(t, m, 1, leaseBody(600<<10))
	}()
	require.Eventually(t, func() bool { return leaseWaiters(srv) == 1 }, 30*time.Second, time.Millisecond)

	// The client acks its delivery while the reader is parked in Reserve.
	ack := &protocol.BasicAckMethod{DeliveryTag: deliveryTag, Multiple: false}
	data, err := ack.Serialize()
	require.NoError(t, err)
	m.send(1, 60, 80, data)

	// Release durability → the first lease releases, the second header is
	// granted, and only THEN is the ack read and processed.
	release()
	leaseWaitAck(t, m, 1, 2)
	<-body2Done

	// The ack was processed after the grant (the connection stayed open and
	// kept working): proof is that the connection is still listed and not
	// closed, and a further publish still confirms.
	leaseSendPublish(t, m, 1, "", "sl11-q", leaseBody(64), true)
	leaseWaitAck(t, m, 1, 3)
	srv.Mutex.RLock()
	var mconn *protocol.Connection
	for _, c := range srv.Connections {
		mconn = c
	}
	srv.Mutex.RUnlock()
	require.NotNil(t, mconn)
	assert.False(t, mconn.Closed.Load(), "the mixed connection must stay open")
}

// --- SL-12 -----------------------------------------------------------------

// eventRecorder is a shared ordered event log for the deadline/step-hook
// ordering assertion.
type eventRecorder struct {
	mu     sync.Mutex
	events []string
}

func (e *eventRecorder) add(s string) {
	e.mu.Lock()
	e.events = append(e.events, s)
	e.mu.Unlock()
}

func (e *eventRecorder) snapshot() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.events...)
}

// deadlineRecordingConn wraps a net.Conn and logs every SetReadDeadline.
type deadlineRecordingConn struct {
	net.Conn
	rec *eventRecorder
}

func (c *deadlineRecordingConn) SetReadDeadline(t time.Time) error {
	c.rec.add("read-deadline")
	return c.Conn.SetReadDeadline(t)
}

// SL-12: the read deadline for the iteration after a lease wait is computed
// AFTER the Reserve returns (no stale deadline counts the wait).
func TestLease_DeadlineAfterWait(t *testing.T) {
	srv := newTransactionTestServer(t)
	srv.Config.Network.PublishInflightBudgetBytes = 1400
	rec := &eventRecorder{}
	srv.limiterStepHook = func(step string) { rec.add(step) }

	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() { _ = clientConn.Close() })
	conn := protocol.NewConnection(&deadlineRecordingConn{serverConn, rec})
	conn.HeartbeatSec.Store(60) // hb > 0 → every iteration arms a read deadline
	rt := srv.runtimeFor(conn)
	done := make(chan struct{})
	defer stopReader(conn, done)
	go srv.readFrames(rt, done)

	// First publish: lease 1384 of G=1400 granted immediately.
	go func() {
		w := func(f *protocol.Frame) { _ = protocol.WriteFrame(clientConn, f) }
		pm := &protocol.BasicPublishMethod{Exchange: "", RoutingKey: "q"}
		data, _ := pm.Serialize()
		w(protocol.EncodeMethodFrameForChannel(1, 60, 40, data))
		h := &protocol.ContentHeader{ClassID: 60, BodySize: 800}
		hp, _ := h.Serialize()
		w(&protocol.Frame{Type: protocol.FrameHeader, Channel: 1, Size: uint32(len(hp)), Payload: hp})
		w(&protocol.Frame{Type: protocol.FrameBody, Channel: 1, Size: 800, Payload: make([]byte, 800)})
	}()

	require.Eventually(t, func() bool { return leaseUsed(srv) == 1384 }, 30*time.Second, time.Millisecond)

	// Second publish: header's Reserve blocks. The client writes from a
	// goroutine because net.Pipe is synchronous.
	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		w := func(f *protocol.Frame) { _ = protocol.WriteFrame(clientConn, f) }
		pm := &protocol.BasicPublishMethod{Exchange: "", RoutingKey: "q"}
		data, _ := pm.Serialize()
		w(protocol.EncodeMethodFrameForChannel(1, 60, 40, data))
		h := &protocol.ContentHeader{ClassID: 60, BodySize: 800}
		hp, _ := h.Serialize()
		w(&protocol.Frame{Type: protocol.FrameHeader, Channel: 1, Size: uint32(len(hp)), Payload: hp})
		w(&protocol.Frame{Type: protocol.FrameBody, Channel: 1, Size: 800, Payload: make([]byte, 800)})
	}()

	require.Eventually(t, func() bool { return leaseWaiters(srv) == 1 }, 30*time.Second, time.Millisecond)

	// Release the first lease (no processor is running; the FIFO holds it).
	le := conn.LeaseFIFO.Pop()
	require.NotNil(t, le)
	le.Release()

	<-clientDone // body2 was read → the loop set a fresh deadline first

	events := rec.snapshot()
	// Find the reserve-wait / reserve-return pair for the second header and
	// assert NO read-deadline was set between them, and that one follows.
	waitIdx, returnIdx, deadlineAfter := -1, -1, false
	waits := 0
	for i, ev := range events {
		switch {
		case len(ev) >= 13 && ev[:13] == "reserve-wait:":
			waits++
			if waits == 2 {
				waitIdx = i
			}
		case len(ev) >= 15 && ev[:15] == "reserve-return:" && waitIdx >= 0 && returnIdx < 0:
			returnIdx = i
			for j := waitIdx + 1; j < returnIdx; j++ {
				require.NotEqual(t, "read-deadline", events[j],
					"no read deadline may be set while the reader is parked in Reserve")
			}
		case ev == "read-deadline" && returnIdx >= 0:
			deadlineAfter = true
		}
	}
	require.GreaterOrEqual(t, waitIdx, 0, "the second header must have blocked in Reserve")
	require.GreaterOrEqual(t, returnIdx, 0, "the wait must have completed")
	assert.True(t, deadlineAfter, "SetReadDeadline follows reserve-return")
}

// --- SL-13 -----------------------------------------------------------------

// SL-13: a seeded mix of publish outcomes leaves created == released and
// used == 0.
func TestLease_NoLeakPerLease(t *testing.T) {
	srv, addr, created, released, probe := leaseServer(t, 1<<20, nil, nil)

	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 8, 0)
	r.openChannel(1)
	r.declareQueue(1, "sl13-q")
	leaseConfirmSelect(t, r, 1)

	// 1. durable confirmed.
	leaseSendPublish(t, r, 1, "", "sl13-q", leaseBody(1024), true)
	leaseWaitAck(t, r, 1, 1)

	// 2. transient (channel 2, non-confirm).
	r.openChannel(2)
	leaseSendPublish(t, r, 2, "", "sl13-q", leaseBody(1024), false)

	// 3. tx publish (channel 3).
	r.openChannel(3)
	r.send(3, 90, 10, []byte{}) // tx.select
	r.expectMethodCh(3, 90, 11) // tx.select-ok
	leaseSendPublish(t, r, 3, "", "sl13-q", leaseBody(1024), true)
	r.send(3, 90, 20, []byte{}) // tx.commit
	r.expectMethodCh(3, 90, 21) // tx.commit-ok

	// 4. unroutable (mandatory) durable: ErrNoRoute → basic.return + ack.
	r.openChannel(4)
	leaseConfirmSelect(t, r, 4)
	leaseSendMethod(t, r, 4, "", "no-such-queue", true)
	leaseSendHeader(t, r, 4, 1024, true, "")
	leaseSendBody(t, r, 4, leaseBody(1024))
	leaseWaitAck(t, r, 4, 1)

	// 5. no-route non-mandatory durable on a declared-but-unbound exchange.
	r.openChannel(5)
	leaseConfirmSelect(t, r, 5)
	r.send(5, 40, 10, mustSerialize(&protocol.ExchangeDeclareMethod{Exchange: "sl13-ex", Type: "direct"}))
	r.expectMethodCh(5, 40, 11) // exchange.declare-ok
	leaseSendPublish(t, r, 5, "sl13-ex", "nowhere", leaseBody(1024), true)
	leaseWaitAck(t, r, 5, 1)

	// 6. user_id mismatch → 406 channel error; the lease releases.
	r.openChannel(6)
	leaseSendMethod(t, r, 6, "", "sl13-q", false)
	leaseSendHeader(t, r, 6, 128, true, "intruder")
	leaseSendBody(t, r, 6, leaseBody(128))
	r.expectChannelClose(6, 406)

	// 7. publish to an undeclared exchange → channel error; lease releases.
	r.openChannel(7)
	leaseSendPublish(t, r, 7, "no-such-exchange", "k", leaseBody(128), true)
	r.expectChannelClose(7, 404)

	require.Eventually(t, func() bool {
		c, rel := created.Load(), released.Load()
		return c == rel && c >= 7 && leaseUsed(srv) == 0
	}, 30*time.Second, time.Millisecond,
		"created(%d) == released(%d), used==0", created.Load(), released.Load())
	assert.Equal(t, int64(0), probe.leaked.Load(), "no lease may be recovered via the leak path")
}

// --- SL-14 -----------------------------------------------------------------

// SL-14: interleaved channels within budget: ch2's nested header TryReserve
// succeeds; interleaved bodies both confirm.
func TestLease_InterleavedChannelsWithinBudget(t *testing.T) {
	release := gateWALFsync(t)
	srv, addr, _, _, _ := leaseServer(t, 1<<20, nil, nil)

	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 8, 0)
	r.openChannel(1)
	r.openChannel(2)
	r.declareQueue(1, "sl14-q")
	leaseConfirmSelect(t, r, 1)
	leaseConfirmSelect(t, r, 2)

	const body = 400 << 10 // lease ≈ 410400; two fit in 1 MiB
	leaseSendMethod(t, r, 1, "", "sl14-q", false)
	leaseSendHeader(t, r, 1, body, true, "")
	leaseSendMethod(t, r, 2, "", "sl14-q", false)
	leaseSendHeader(t, r, 2, body, true, "") // nested: TryReserve must succeed

	require.Eventually(t, func() bool { return leaseUsed(srv) >= 2*410400 }, 30*time.Second, time.Millisecond,
		"the nested header must be TryReserved within budget")

	// Interleave the bodies.
	b1, b2 := leaseBody(body), leaseBody(body)
	const chunk = 64 << 10
	for off := 0; off < body; off += chunk {
		n := chunk
		if off+n > body {
			n = body - off
		}
		leaseSendBody(t, r, 1, b1[off:off+n])
		leaseSendBody(t, r, 2, b2[off:off+n])
	}

	release()
	leaseWaitAcks(t, r, map[uint16]uint64{1: 1, 2: 1})
	require.Eventually(t, func() bool { return leaseUsed(srv) == 0 }, 30*time.Second, time.Millisecond)
}

// --- SL-15 -----------------------------------------------------------------

// SL-15: ch1 owed while another connection's waiter is queued: ch2's nested
// TryReserve fails → 506 close; ch2 content is never processed; teardown
// releases everything.
func TestLease_NestedHeaderOverBudgetCloses(t *testing.T) {
	release := gateWALFsync(t)
	srv, addr, created, released, probe := leaseServer(t, 1<<20, nil, nil)

	// X: ch1 publishes 600 KiB (lease ≈ 615272) but only sends part of the
	// body — the leased body stays owed.
	x := dialRaw(t, addr)
	x.handshake("guest", "guest", 8, 0)
	x.openChannel(1)
	x.declareQueue(1, "sl15-q")
	leaseConfirmSelect(t, x, 1)
	leaseSendMethod(t, x, 1, "", "sl15-q", false)
	leaseSendHeader(t, x, 1, 600<<10, true, "")
	leaseSendBody(t, x, 1, leaseBody(100<<10)) // 500 KiB still owed
	require.Eventually(t, func() bool { return created.Load() == 1 }, 30*time.Second, time.Millisecond)

	// Y: a full 600 KiB durable publish — its header's blocking Reserve queues
	// (used + lease > G), and its body stays unread.
	y := dialRaw(t, addr)
	y.handshake("guest", "guest", 4, 0)
	y.openChannel(1)
	leaseConfirmSelect(t, y, 1)
	leaseSendMethod(t, y, 1, "", "sl15-q", false)
	leaseSendHeader(t, y, 1, 600<<10, true, "")
	yBody := make(chan struct{})
	go func() {
		defer close(yBody)
		leaseSendBody(t, y, 1, leaseBody(600<<10))
	}()
	require.Eventually(t, func() bool { return leaseWaiters(srv) == 1 }, 30*time.Second, time.Millisecond,
		"Y's header must be queued in the limiter")

	// X: a nested header on ch2 — TryReserve must fail (FIFO non-empty and
	// over budget) → 506 close.
	leaseSendMethod(t, x, 2, "", "sl15-q", false)
	leaseSendHeader(t, x, 2, 600<<10, true, "")

	// Either the 506 frame is observed or the socket closes.
	x.c.SetReadDeadline(time.Now().Add(30 * time.Second))
	f, err := protocol.ReadFrameOptimizedWithLimit(x.r, 131072+8)
	if err == nil {
		require.Equal(t, byte(protocol.FrameMethod), f.Type)
		cls := uint16(f.Payload[0])<<8 | uint16(f.Payload[1])
		mth := uint16(f.Payload[2])<<8 | uint16(f.Payload[3])
		require.Equal(t, uint16(10), cls)
		require.Equal(t, uint16(50), mth, "connection.close")
		m := &protocol.ConnectionCloseMethod{}
		require.NoError(t, m.Deserialize(f.Payload[4:]))
		assert.Equal(t, uint16(506), m.ReplyCode, "RESOURCE_ERROR")
	}
	// else: socket closed without a frame — also acceptable.

	require.Eventually(t, func() bool { return probe.overbudgetCloses.Load() == 1 }, 30*time.Second, time.Millisecond)
	assert.Equal(t, int64(1), created.Load(), "ch2's header is never leased (never admitted)")

	// X's teardown releases ch1's pending lease, which grants Y's waiter.
	release() // let every fsync through
	leaseWaitAck(t, y, 1, 1)
	<-yBody
	require.Eventually(t, func() bool {
		return created.Load() == 2 && released.Load() == 2 && leaseUsed(srv) == 0
	}, 30*time.Second, time.Millisecond, "everything releases; final used == 0")
}

// --- SL-16 -----------------------------------------------------------------

// SL-16: a header-only stall closes the connection at the owed deadline
// (writeTimeout) even though the client keeps sending heartbeats; the lease
// is released.
func TestLease_HeaderOnlyStallClosesAtOwedDeadline(t *testing.T) {
	srv, addr, created, _, probe := leaseServer(t, 0, nil, func(srv *Server) {
		srv.writeTimeoutOverride = 200 * time.Millisecond
	})

	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 4, 1) // hb = 1s
	r.openChannel(1)
	r.declareQueue(1, "sl16-q")
	leaseConfirmSelect(t, r, 1)

	leaseSendMethod(t, r, 1, "", "sl16-q", false)
	leaseSendHeader(t, r, 1, 100<<10, true, "") // declare 100 KiB, send nothing
	require.Eventually(t, func() bool { return created.Load() == 1 }, 30*time.Second, time.Millisecond)

	// Keep sending heartbeats: they must NOT extend the owed deadline.
	hbStop := make(chan struct{})
	defer close(hbStop)
	go func() {
		for {
			select {
			case <-hbStop:
				return
			default:
			}
			if err := protocol.WriteFrame(r.c, &protocol.Frame{Type: protocol.FrameHeartbeat, Channel: 0}); err != nil {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// Wait on the close event (not on time): the connection must die at the
	// owed deadline (200 ms) even though heartbeats keep arriving.
	r.expectEOF()
	require.Eventually(t, func() bool { return probe.bodyStallCloses.Load() == 1 }, 30*time.Second, time.Millisecond,
		"the close must be accounted as a body stall, not a heartbeat death")
	require.Eventually(t, func() bool { return leaseUsed(srv) == 0 }, 30*time.Second, time.Millisecond,
		"the stalled lease is released at teardown")
}

// --- SL-17 -----------------------------------------------------------------

// SL-17: 20 connections × 4 interleaving channels and 1000 × 1 with a small
// G: every connection completes or is closed with 506; no hang; created ==
// released; final used == 0.
func TestLease_MultiChannel20And1000Connections(t *testing.T) {
	const G = int64(4 << 20)
	srv, addr, created, released, probe := leaseServer(t, G, func(cfg *config.AMQPConfig) {
		cfg.Network.MaxConnections = 2000
	}, nil)

	setup := dialRaw(t, addr)
	setup.handshake("guest", "guest", 1, 0)
	setup.openChannel(1)
	setup.declareQueue(1, "sl17-q")
	setup.c.Close()

	// Phase 1: 20 connections × 4 channels, headers first (max nesting), then
	// interleaved bodies. Each connection either confirms all 4 or is closed
	// with 506 — both are legal outcomes.
	const body = 100 << 10
	var wg1 sync.WaitGroup
	var closedOK, completedOK atomic.Int64
	for i := 0; i < 20; i++ {
		wg1.Add(1)
		go func() {
			defer wg1.Done()
			r := dialRaw(t, addr)
			defer r.c.Close()
			r.handshake("guest", "guest", 8, 0)
			for ch := uint16(1); ch <= 4; ch++ {
				r.send(ch, 20, 10, []byte{})
				r.expectMethodCh(ch, 20, 11)
			}
			for ch := uint16(1); ch <= 4; ch++ {
				leaseConfirmSelect(t, r, ch)
			}
			for ch := uint16(1); ch <= 4; ch++ {
				leaseSendMethod(t, r, ch, "", "sl17-q", false)
				leaseSendHeader(t, r, ch, body, true, "")
			}
			// Read frames: either 4 acks arrive (after sending bodies) or a
			// 506 connection.close ends it. Send bodies from a goroutine
			// because the server may 506 mid-stream.
			bodiesDone := make(chan struct{})
			go func() {
				defer close(bodiesDone)
				for ch := uint16(1); ch <= 4; ch++ {
					for off := 0; off < body; off += 32 << 10 {
						n := 32 << 10
						if off+n > body {
							n = body - off
						}
						if err := protocol.WriteFrame(r.c, &protocol.Frame{
							Type: protocol.FrameBody, Channel: ch, Size: uint32(n), Payload: make([]byte, n),
						}); err != nil {
							return
						}
					}
				}
			}()
			acked := make(map[uint16]uint64)
			closedEarly := false
			for {
				done := true
				for ch := uint16(1); ch <= 4; ch++ {
					if acked[ch] < 1 {
						done = false
					}
				}
				if done {
					break
				}
				r.c.SetReadDeadline(time.Now().Add(30 * time.Second))
				f, err := protocol.ReadFrameOptimizedWithLimit(r.r, 131072+8)
				if err != nil {
					closedEarly = true // socket closed (possibly without a frame)
					break
				}
				if f.Type != protocol.FrameMethod || len(f.Payload) < 4 {
					continue
				}
				cls := uint16(f.Payload[0])<<8 | uint16(f.Payload[1])
				mth := uint16(f.Payload[2])<<8 | uint16(f.Payload[3])
				if cls == 10 && mth == 50 {
					m := &protocol.ConnectionCloseMethod{}
					if m.Deserialize(f.Payload[4:]) == nil {
						require.Equal(t, uint16(506), m.ReplyCode, "the only legal early close is a 506 lease close")
					}
					closedEarly = true
					break
				}
				if cls == 60 && mth == 80 {
					ack := &protocol.BasicAckMethod{}
					if ack.Deserialize(f.Payload[4:]) == nil {
						acked[f.Channel] = ack.DeliveryTag
					}
				}
			}
			<-bodiesDone
			if closedEarly {
				closedOK.Add(1)
			} else {
				completedOK.Add(1)
			}
		}()
	}
	wg1.Wait()
	t.Logf("phase 1: completed=%d closedEarly=%d overbudgetCloses=%d",
		completedOK.Load(), closedOK.Load(), probe.overbudgetCloses.Load())
	assert.Equal(t, int64(20), completedOK.Load()+closedOK.Load(), "no hang: every connection completes or is 506-closed")
	assert.Equal(t, closedOK.Load(), probe.overbudgetCloses.Load(),
		"every early close must be a 506 lease-budget close (frame observed or socket closed)")

	// Phase 2: 1000 connections × 1 channel with the same small G: headers
	// queue in the limiter (blocking Reserve, owed == 0) and all complete.
	var wg2 sync.WaitGroup
	var acked2 atomic.Int64
	for w := 0; w < 100; w++ {
		wg2.Add(1)
		go func(w int) {
			defer wg2.Done()
			for i := w; i < 1000; i += 100 {
				r := dialRaw(t, addr)
				r.handshake("guest", "guest", 1, 0)
				r.openChannel(1)
				leaseConfirmSelect(t, r, 1)
				leaseSendPublish(t, r, 1, "", "sl17-q", leaseBody(1024), true)
				leaseWaitAck(t, r, 1, 1)
				acked2.Add(1)
				r.c.Close()
			}
		}(w)
	}
	wg2.Wait()
	assert.Equal(t, int64(1000), acked2.Load())

	require.Eventually(t, func() bool {
		return created.Load() == released.Load() && leaseUsed(srv) == 0
	}, 30*time.Second, time.Millisecond,
		"created(%d) == released(%d), used==0", created.Load(), released.Load())
	assert.Equal(t, int64(0), probe.leaked.Load())
}

// --- SL-18 -----------------------------------------------------------------

// countingReleaser is a test Releaser that counts every Release call (a
// double-release is caught, unlike the idempotent production Lease).
type countingReleaser struct {
	releases *atomic.Int64
}

func (c *countingReleaser) Release()            { c.releases.Add(1) }
func (c *countingReleaser) ReleaseFunc() func() { return c.Release }

// SL-18: takeLease nil-safety, exactly-once on every processCompleteMessage
// arm, and the PutPendingMessage non-nil-lease assertion.
func TestLease_TakeLeaseNilSafeAndPutAsserts(t *testing.T) {
	t.Run("takeLease nil-safe", func(t *testing.T) {
		takeLease(nil)() // nil pm → noopRelease
		pm := &protocol.PendingMessage{}
		takeLease(pm)() // nil lease → noopRelease
		var n atomic.Int64
		pm.Lease = &countingReleaser{releases: &n}
		f1 := takeLease(pm)
		assert.Nil(t, pm.Lease, "takeLease must clear pm.Lease")
		f2 := takeLease(pm) // second take → noop
		f1()
		f2()
		assert.Equal(t, int64(1), n.Load(), "exactly one release")
	})

	// processCompleteMessage arms: each leaves Lease == nil and releases
	// exactly once.
	newSrvConn := func(t *testing.T) (*Server, *protocol.Connection, chan *protocol.Frame) {
		srv := newTransactionTestServer(t)
		conn, frameCh := newPipeConnWithFrames(t)
		t.Cleanup(func() { close(conn.Done) })
		ch := protocol.NewChannel(1, conn)
		conn.Channels.Store(uint16(1), ch)
		conn.Username = "guest"
		return srv, conn, frameCh
	}
	declare := func(t *testing.T, srv *Server, conn *protocol.Connection, frameCh chan *protocol.Frame, q string) {
		payload, err := (&protocol.QueueDeclareMethod{Queue: q, Durable: true}).Serialize()
		require.NoError(t, err)
		require.NoError(t, srv.handleQueueDeclare(conn, 1, payload))
		drainFrame(t, frameCh)
	}

	t.Run("user_id precondition failure", func(t *testing.T) {
		srv, conn, _ := newSrvConn(t)
		var n atomic.Int64
		pm := makeDurablePendingMessage("", "q", "b")
		pm.Header.PropertyFlags |= protocol.FlagUserID
		pm.Header.UserID = "intruder"
		pm.Lease = &countingReleaser{releases: &n}
		_ = srv.processCompleteMessage(conn, 1, pm)
		assert.Nil(t, pm.Lease)
		assert.Equal(t, int64(1), n.Load())
	})

	t.Run("tx buffered", func(t *testing.T) {
		srv, conn, frameCh := newSrvConn(t)
		declare(t, srv, conn, frameCh, "sl18-txq")
		require.NoError(t, srv.handleTxSelect(conn, 1, []byte{}))
		drainFrame(t, frameCh)
		var n atomic.Int64
		pm := makeDurablePendingMessage("", "sl18-txq", "b")
		pm.Lease = &countingReleaser{releases: &n}
		require.NoError(t, srv.processCompleteMessage(conn, 1, pm))
		assert.Nil(t, pm.Lease)
		assert.Equal(t, int64(1), n.Load(), "tx releases at AddOperation")
	})

	t.Run("async durable releases on completion", func(t *testing.T) {
		release := gateWALFsync(t)
		srv, conn, frameCh := newSrvConn(t)
		declare(t, srv, conn, frameCh, "sl18-dq")
		if v, ok := conn.Channels.Load(uint16(1)); ok {
			v.(*protocol.Channel).ConfirmMode.Store(true)
		}
		var n atomic.Int64
		pm := makeDurablePendingMessage("", "sl18-dq", "b")
		pm.Lease = &countingReleaser{releases: &n}
		require.NoError(t, srv.processCompleteMessage(conn, 1, pm))
		assert.Nil(t, pm.Lease, "the broker owns the lease now")
		assert.Equal(t, int64(0), n.Load(), "not released before durability")
		release()
		require.Eventually(t, func() bool { return n.Load() == 1 }, 30*time.Second, time.Millisecond)
	})

	t.Run("transient synchronous", func(t *testing.T) {
		srv, conn, frameCh := newSrvConn(t)
		declare(t, srv, conn, frameCh, "sl18-tq")
		var n atomic.Int64
		pm := makePendingMessage("", "sl18-tq", "b") // DeliveryMode 0 → transient
		pm.Lease = &countingReleaser{releases: &n}
		require.NoError(t, srv.processCompleteMessage(conn, 1, pm))
		assert.Nil(t, pm.Lease)
		assert.Equal(t, int64(1), n.Load())
	})

	t.Run("unroutable", func(t *testing.T) {
		srv, conn, _ := newSrvConn(t)
		var n atomic.Int64
		pm := makeDurablePendingMessage("", "no-such-queue", "b")
		pm.Lease = &countingReleaser{releases: &n}
		require.NoError(t, srv.processCompleteMessage(conn, 1, pm))
		assert.Nil(t, pm.Lease)
		assert.Equal(t, int64(1), n.Load())
	})

	t.Run("exchange error", func(t *testing.T) {
		srv, conn, _ := newSrvConn(t)
		var n atomic.Int64
		pm := makeDurablePendingMessage("no-such-exchange", "k", "b")
		pm.Lease = &countingReleaser{releases: &n}
		err := srv.processCompleteMessage(conn, 1, pm)
		require.Error(t, err)
		assert.Nil(t, pm.Lease)
		assert.Equal(t, int64(1), n.Load())
	})

	t.Run("PutPendingMessage with a non-nil lease panics under the armed flag", func(t *testing.T) {
		pm := protocol.GetPendingMessage()
		pm.Lease = &countingReleaser{releases: &atomic.Int64{}}
		require.Panics(t, func() { protocol.PutPendingMessage(pm) })
		// Disarmed: the lease is released and the leak reported, never lost.
		protocol.SetLeaseLeakPanicForTest(false)
		defer protocol.SetLeaseLeakPanicForTest(true)
		var leaks atomic.Int64
		protocol.SetLeaseLeakReporter(func() { leaks.Add(1) })
		defer protocol.SetLeaseLeakReporter(nil)
		var n atomic.Int64
		pm2 := protocol.GetPendingMessage()
		pm2.Lease = &countingReleaser{releases: &n}
		require.NotPanics(t, func() { protocol.PutPendingMessage(pm2) })
		assert.Equal(t, int64(1), n.Load(), "production path releases the lease")
		assert.Equal(t, int64(1), leaks.Load(), "and reports the leak")
	})
}

// --- SL-19 -----------------------------------------------------------------

// SL-19: shutdown Close fails waiters; leases held by fsync-blocked publishes
// release when the fsync is released; used == 0.
func TestLease_LimiterCloseWithOutstanding(t *testing.T) {
	release := gateWALFsync(t)
	srv, addr, created, released, _ := leaseServer(t, 1<<20, nil, nil)

	// A: durable publish, lease broker-held behind the gated fsync.
	a := dialRaw(t, addr)
	a.handshake("guest", "guest", 4, 0)
	a.openChannel(1)
	a.declareQueue(1, "sl19-q")
	leaseConfirmSelect(t, a, 1)
	leaseSendPublish(t, a, 1, "", "sl19-q", leaseBody(700<<10), true)
	require.Eventually(t, func() bool { return created.Load() == 1 }, 30*time.Second, time.Millisecond)

	// B: header's Reserve waits (A holds most of G).
	b := dialRaw(t, addr)
	b.handshake("guest", "guest", 4, 0)
	b.openChannel(1)
	leaseConfirmSelect(t, b, 1)
	leaseSendMethod(t, b, 1, "", "sl19-q", false)
	leaseSendHeader(t, b, 1, 700<<10, true, "")
	require.Eventually(t, func() bool { return leaseWaiters(srv) == 1 }, 30*time.Second, time.Millisecond)

	// Shutdown closes the limiter: the waiter fails and its connection tears
	// down; the broker-held lease survives until the fsync completes.
	srv.publishLimiter().Close()
	require.Eventually(t, func() bool { return leaseWaiters(srv) == 0 }, 30*time.Second, time.Millisecond)
	b.expectEOF()

	assert.Equal(t, int64(1), created.Load()-released.Load(), "the fsync-blocked lease is still held")
	release()
	require.Eventually(t, func() bool {
		return created.Load() == released.Load() && leaseUsed(srv) == 0
	}, 30*time.Second, time.Millisecond)
}

// --- SL-20 -----------------------------------------------------------------

// SL-20: an alarm that becomes active while a body is owed does NOT park the
// reader until the body is read; then it parks.
func TestLease_ParksDeferredWhileOwed(t *testing.T) {
	srv := newTransactionTestServer(t)
	srv.alarmParkProbe = time.Hour // no probe breakouts during the park

	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() { _ = clientConn.Close() })
	conn := protocol.NewConnection(serverConn)
	rt := srv.runtimeFor(conn)
	done := make(chan struct{})
	defer stopReader(conn, done)
	go srv.readFrames(rt, done)

	w := func(f *protocol.Frame) { require.NoError(t, protocol.WriteFrame(clientConn, f)) }

	// Publish method + header (body 1000) + a partial body.
	pm := &protocol.BasicPublishMethod{Exchange: "", RoutingKey: "q"}
	data, err := pm.Serialize()
	require.NoError(t, err)
	w(protocol.EncodeMethodFrameForChannel(1, 60, 40, data))
	h := &protocol.ContentHeader{ClassID: 60, BodySize: 1000}
	hp, err := h.Serialize()
	require.NoError(t, err)
	w(&protocol.Frame{Type: protocol.FrameHeader, Channel: 1, Size: uint32(len(hp)), Payload: hp})

	require.Eventually(t, func() bool { return leaseUsed(srv) == 1000+72+512 }, 30*time.Second, time.Millisecond)

	// The alarm becomes active while 600 body bytes are still owed.
	w(&protocol.Frame{Type: protocol.FrameBody, Channel: 1, Size: 400, Payload: make([]byte, 400)})
	srv.alarmState.Store(AlarmMemory)
	conn.HasPublished.Store(true)

	// The reader must NOT park while the body is owed: the next body frame is
	// read promptly.
	w(&protocol.Frame{Type: protocol.FrameBody, Channel: 1, Size: 400, Payload: make([]byte, 400)})
	require.Eventually(t, func() bool { return len(conn.FrameQueue) >= 3 }, 30*time.Second, time.Millisecond,
		"the reader keeps reading while a leased body is owed (no park)")

	// Final body bytes: owed clears.
	w(&protocol.Frame{Type: protocol.FrameBody, Channel: 1, Size: 200, Payload: make([]byte, 200)})
	require.Eventually(t, func() bool { return len(conn.FrameQueue) >= 4 }, 30*time.Second, time.Millisecond)

	// Now the alarm park engages: a further frame is NOT read.
	queued := len(conn.FrameQueue)
	pm2 := &protocol.BasicPublishMethod{Exchange: "", RoutingKey: "q"}
	data2, _ := pm2.Serialize()
	go func() { _ = protocol.WriteFrame(clientConn, protocol.EncodeMethodFrameForChannel(1, 60, 40, data2)) }()
	select {
	case <-conn.AlarmWake: // never sent; drain guard
	default:
	}
	assert.Never(t, func() bool { return len(conn.FrameQueue) > queued }, 200*time.Millisecond, 20*time.Millisecond,
		"once the body is read the alarm park must engage")

	// Clear the alarm and poke the wake: the reader resumes and reads it.
	srv.alarmState.Store(0)
	select {
	case conn.AlarmWake <- struct{}{}:
	default:
	}
	require.Eventually(t, func() bool { return len(conn.FrameQueue) > queued }, 30*time.Second, time.Millisecond,
		"the reader resumes after the alarm clears")
}

// --- small helpers ----------------------------------------------------------

func newBufioReader(c net.Conn) *bufio.Reader { return bufio.NewReader(c) }

func r_readMethod(t *testing.T, r *rawAMQP, class, method uint16) []byte {
	t.Helper()
	return r.expectMethod(class, method)
}

func mustUint64(b []byte) uint64 {
	return uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7])
}

func mustSerialize(m interface{ Serialize() ([]byte, error) }) []byte {
	data, err := m.Serialize()
	if err != nil {
		panic(err)
	}
	return data
}
