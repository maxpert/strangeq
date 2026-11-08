package server

import (
	"bytes"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// recordingReader drains a client connection and records every byte received,
// so a test can WAIT for a specific message body to reach the wire instead of
// sleeping a fixed window (which races the delivery loop under load).
type recordingReader struct {
	mu  sync.Mutex
	buf []byte
}

func newRecordingReader(clientConn net.Conn) (*recordingReader, <-chan struct{}) {
	rr := &recordingReader{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		b := make([]byte, 4096)
		for {
			n, err := clientConn.Read(b)
			if err != nil {
				return
			}
			rr.mu.Lock()
			rr.buf = append(rr.buf, b[:n]...)
			rr.mu.Unlock()
		}
	}()
	return rr, done
}

// received reports whether the given literal has appeared on the wire so far.
func (rr *recordingReader) received(sub string) bool {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	return bytes.Contains(rr.buf, []byte(sub))
}

// TestA7_DirtyFlagGatesRediscovery verifies that the delivery loop skips
// consumer discovery when ConsumersDirty is false, and re-scans when true.
func TestA7_DirtyFlagGatesRediscovery(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Engine.ConsumerSelectTimeoutMS = 10000

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	conn := protocol.NewConnection(serverConn)
	srv := &Server{
		Log:              zap.NewNop(),
		Config:           cfg,
		MetricsCollector: &NoOpMetricsCollector{},
	}

	rr, readerDone := newRecordingReader(clientConn)

	ch := protocol.NewChannel(1, conn)
	ch.Mutex.Lock()
	ch.Consumers["c1"] = &protocol.Consumer{
		ID:       "c1",
		Tag:      "c1",
		Queue:    "q1",
		Messages: make(chan *protocol.Delivery, 10),
		Cancel:   make(chan struct{}),
	}
	ch.Mutex.Unlock()
	conn.Channels.Store(uint16(1), ch)

	conn.ConsumersDirty.Store(true)

	done := make(chan struct{})
	go func() { srv.consumerDeliveryLoop(srv.runtimeFor(conn)); close(done) }()
	defer func() {
		conn.Closed.Store(true)
		// The loop's blocking select watches fan-in, not conn.Closed, so poke it
		// with a c1 delivery to make the next iteration observe the close and
		// exit — avoids waiting out the 10s select timeout at teardown.
		wake := &protocol.Delivery{
			Message:    &protocol.Message{DeliveryTag: 9, Body: []byte("teardown-wake"), RoutingKey: "q1"},
			ConsumerID: "c1",
			Exchange:   "",
			RoutingKey: "q1",
		}
		select {
		case ch.Consumers["c1"].Messages <- wake:
		case <-time.After(time.Second):
		}
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("delivery loop did not exit on teardown")
		}
		clientConn.Close()
		<-readerDone
	}()

	// Deterministic synchronization point: the loop clears ConsumersDirty when
	// it scans, and the 10s select timeout means the flag is not self-set again
	// within this test. Waiting on the cleared flag — not a 100ms sleep — is what
	// guarantees c1 was discovered BEFORE c2 is registered. If c2 were added
	// before the first scan it would be discovered too (the CI flake).
	require.Eventually(t, func() bool { return !conn.ConsumersDirty.Load() },
		5*time.Second, time.Millisecond, "delivery loop must complete its initial discovery")

	// Prove the loop is live and routing: a c1 body reaches the wire.
	ch.Consumers["c1"].Messages <- &protocol.Delivery{
		Message:     &protocol.Message{DeliveryTag: 1, Body: []byte("c1-warm"), Exchange: "", RoutingKey: "q1"},
		DeliveryTag: 1,
		ConsumerID:  "c1",
		Exchange:    "",
		RoutingKey:  "q1",
	}
	require.Eventually(t, func() bool { return rr.received("c1-warm") },
		5*time.Second, time.Millisecond, "the discovered consumer's delivery must reach the wire")

	// c2 is registered while the dirty flag is CLEAR: the gated loop must not
	// rediscover it, so its message can never be forwarded.
	ch.Mutex.Lock()
	ch.Consumers["c2"] = &protocol.Consumer{
		ID:       "c2",
		Tag:      "c2",
		Queue:    "q2",
		Messages: make(chan *protocol.Delivery, 10),
		Cancel:   make(chan struct{}),
	}
	ch.Mutex.Unlock()

	ch.Consumers["c2"].Messages <- &protocol.Delivery{
		Message:     &protocol.Message{DeliveryTag: 2, Body: []byte("c2-must-not-flow"), Exchange: "", RoutingKey: "q2"},
		DeliveryTag: 2,
		ConsumerID:  "c2",
		Exchange:    "",
		RoutingKey:  "q2",
	}

	// No forwarder exists for c2, so this is a deterministic check (not a window):
	// had the loop rediscovered, c2's body would already be on the wire.
	assert.False(t, rr.received("c2-must-not-flow"),
		"a consumer registered while the dirty flag is clear must not be discovered")

	// Setting the flag makes the loop rescan and route c2. The loop only checks
	// the flag after its blocking select returns, so a c1 delivery (already
	// discovered) wakes it: the next iteration re-scans, discovers c2, and its
	// stranded message is delivered.
	conn.ConsumersDirty.Store(true)
	ch.Consumers["c1"].Messages <- &protocol.Delivery{
		Message:     &protocol.Message{DeliveryTag: 3, Body: []byte("c1-wake"), Exchange: "", RoutingKey: "q1"},
		DeliveryTag: 3,
		ConsumerID:  "c1",
		Exchange:    "",
		RoutingKey:  "q1",
	}
	require.Eventually(t, func() bool { return rr.received("c2-must-not-flow") },
		5*time.Second, time.Millisecond, "setting the dirty flag must rediscover c2 and deliver its message")
}

// TestA7_RegisterConsumerSetsDirtyFlag verifies that handleBasicConsume sets
// the ConsumersDirty flag on the connection.
func TestA7_RegisterConsumerSetsDirtyFlag(t *testing.T) {
	srv := newB4Server(t, false, nil)
	conn := newB4Conn(t, nil, "")

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	srv.handleQueueDeclare(conn, 1, encodeMethod(t, 50, 10, &protocol.QueueDeclareMethod{Queue: "test-q"}))

	if conn.ConsumersDirty.Load() {
		t.Fatal("dirty flag should be false before consume")
	}

	method := &protocol.BasicConsumeMethod{Queue: "test-q", ConsumerTag: "ctag-test"}
	err := srv.handleBasicConsume(conn, 1, encodeMethod(t, 60, 20, method))
	if err != nil {
		t.Fatalf("handleBasicConsume failed: %v", err)
	}

	if !conn.ConsumersDirty.Load() {
		t.Error("dirty flag should be true after registering consumer")
	}
}

// TestA7_CancelConsumerSetsDirtyFlag verifies that handleBasicCancel sets
// the ConsumersDirty flag on the connection.
func TestA7_CancelConsumerSetsDirtyFlag(t *testing.T) {
	srv := newB4Server(t, false, nil)
	conn := newB4Conn(t, nil, "")

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	srv.handleQueueDeclare(conn, 1, encodeMethod(t, 50, 10, &protocol.QueueDeclareMethod{Queue: "test-q"}))
	srv.handleBasicConsume(conn, 1, encodeMethod(t, 60, 20, &protocol.BasicConsumeMethod{Queue: "test-q", ConsumerTag: "ctag-test"}))

	conn.ConsumersDirty.Store(false)

	err := srv.handleBasicCancel(conn, 1, encodeMethod(t, 60, 30, &protocol.BasicCancelMethod{ConsumerTag: "ctag-test"}))
	if err != nil {
		t.Fatalf("handleBasicCancel failed: %v", err)
	}

	if !conn.ConsumersDirty.Load() {
		t.Error("dirty flag should be true after cancelling consumer")
	}
}

// TestA7_TimeoutFallbackSelfHeals verifies that the periodic timeout fallback
// re-discovers consumers even when the dirty flag was never set.
func TestA7_TimeoutFallbackSelfHeals(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Engine.ConsumerSelectTimeoutMS = 5

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	conn := protocol.NewConnection(serverConn)
	srv := &Server{
		Log:              zap.NewNop(),
		Config:           cfg,
		MetricsCollector: &NoOpMetricsCollector{},
	}

	rr, readerDone := newRecordingReader(clientConn)

	ch := protocol.NewChannel(1, conn)
	ch.Mutex.Lock()
	ch.Consumers["c1"] = &protocol.Consumer{
		ID:       "c1",
		Tag:      "c1",
		Queue:    "q1",
		Messages: make(chan *protocol.Delivery, 10),
		Cancel:   make(chan struct{}),
	}
	ch.Mutex.Unlock()
	conn.Channels.Store(uint16(1), ch)

	conn.ConsumersDirty.Store(true)

	done := make(chan struct{})
	go func() { srv.consumerDeliveryLoop(srv.runtimeFor(conn)); close(done) }()
	defer func() {
		conn.Closed.Store(true)
		<-done
		clientConn.Close()
		<-readerDone
	}()

	// Wait for the loop to be live and routing c1 (its initial scan). With the
	// select timeout at 5ms, waiting on the dirty flag would race the self-set;
	// waiting on an actual c1 delivery is a real synchronization point.
	ch.Consumers["c1"].Messages <- &protocol.Delivery{
		Message:     &protocol.Message{DeliveryTag: 1, Body: []byte("c1-warm"), Exchange: "", RoutingKey: "q1"},
		DeliveryTag: 1,
		ConsumerID:  "c1",
		Exchange:    "",
		RoutingKey:  "q1",
	}
	require.Eventually(t, func() bool { return rr.received("c1-warm") },
		5*time.Second, time.Millisecond, "the delivery loop must be live and routing c1")

	// c2 is registered with the dirty flag NOT set by the test: only the
	// periodic select-timeout fallback can rediscover it. Wait for the actual
	// delivery instead of a fixed 300ms window.
	ch.Mutex.Lock()
	ch.Consumers["c2"] = &protocol.Consumer{
		ID:       "c2",
		Tag:      "c2",
		Queue:    "q2",
		Messages: make(chan *protocol.Delivery, 10),
		Cancel:   make(chan struct{}),
	}
	ch.Mutex.Unlock()

	ch.Consumers["c2"].Messages <- &protocol.Delivery{
		Message:     &protocol.Message{DeliveryTag: 2, Body: []byte("c2-timeout-fallback"), Exchange: "", RoutingKey: "q2"},
		DeliveryTag: 2,
		ConsumerID:  "c2",
		Exchange:    "",
		RoutingKey:  "q2",
	}

	require.Eventually(t, func() bool { return rr.received("c2-timeout-fallback") },
		5*time.Second, time.Millisecond, "the timeout fallback must rediscover c2 and deliver its message")
}

// TestA7_NoAllocationsInSteadyState verifies that discoverConsumers does not
// heap-allocate when there are no new or removed consumers (the activeTags
// map is reused, not reallocated).
func TestA7_NoAllocationsInSteadyState(t *testing.T) {
	_, serverConn := net.Pipe()
	defer serverConn.Close()

	conn := protocol.NewConnection(serverConn)
	srv := &Server{
		Log:              zap.NewNop(),
		Config:           config.DefaultConfig(),
		MetricsCollector: &NoOpMetricsCollector{},
	}

	ch := protocol.NewChannel(1, conn)
	ch.Mutex.Lock()
	for i := 0; i < 3; i++ {
		tag := string(rune('a' + i))
		ch.Consumers[tag] = &protocol.Consumer{
			ID:       tag,
			Tag:      tag,
			Queue:    "q-" + tag,
			Messages: make(chan *protocol.Delivery, 10),
			Cancel:   make(chan struct{}),
		}
	}
	ch.Mutex.Unlock()
	conn.Channels.Store(uint16(1), ch)

	consumerInfos := make(map[string]*consumerInfo)
	forwarders := make(map[string]chan struct{})
	activeTags := make(map[string]struct{})
	fanIn := make(chan *protocol.Delivery, 100)

	srv.discoverConsumers(srv.runtimeFor(conn), consumerInfos, forwarders, activeTags, fanIn)

	defer func() {
		for _, stop := range forwarders {
			close(stop)
		}
	}()

	allocs := testing.AllocsPerRun(100, func() {
		srv.discoverConsumers(srv.runtimeFor(conn), consumerInfos, forwarders, activeTags, fanIn)
	})

	if allocs != 0 {
		t.Errorf("expected 0 allocations in steady state, got %v", allocs)
	}
}
