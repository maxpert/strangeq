package server

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

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

	var readBytes int64
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		buf := make([]byte, 4096)
		for {
			n, err := clientConn.Read(buf)
			if err != nil {
				return
			}
			atomic.AddInt64(&readBytes, int64(n))
		}
	}()

	ch := protocol.NewChannel(1, conn)
	ch.Mutex.Lock()
	ch.Consumers["c1"] = &protocol.Consumer{
		Tag:      "c1",
		Queue:    "q1",
		Messages: make(chan *protocol.Delivery, 10),
		Cancel:   make(chan struct{}),
	}
	ch.Mutex.Unlock()
	conn.Channels.Store(uint16(1), ch)

	conn.ConsumersDirty.Store(true)

	done := make(chan struct{})
	go srv.consumerDeliveryLoop(conn, done)
	defer func() {
		conn.Closed.Store(true)
		<-done
		clientConn.Close()
		<-readerDone
	}()

	time.Sleep(100 * time.Millisecond)

	ch.Mutex.Lock()
	ch.Consumers["c2"] = &protocol.Consumer{
		Tag:      "c2",
		Queue:    "q2",
		Messages: make(chan *protocol.Delivery, 10),
		Cancel:   make(chan struct{}),
	}
	ch.Mutex.Unlock()

	ch.Consumers["c2"].Messages <- &protocol.Delivery{
		Message:     &protocol.Message{DeliveryTag: 1, Body: []byte("c2-msg"), Exchange: "", RoutingKey: "q2"},
		DeliveryTag: 1,
		ConsumerTag: "c2",
		Exchange:    "",
		RoutingKey:  "q2",
	}

	time.Sleep(200 * time.Millisecond)
	phase1 := atomic.LoadInt64(&readBytes)
	if phase1 > 0 {
		t.Errorf("expected no delivery when dirty=false, got %d bytes", phase1)
	}

	conn.ConsumersDirty.Store(true)
	ch.Consumers["c1"].Messages <- &protocol.Delivery{
		Message:     &protocol.Message{DeliveryTag: 2, Body: []byte("c1-trigger"), Exchange: "", RoutingKey: "q1"},
		DeliveryTag: 2,
		ConsumerTag: "c1",
		Exchange:    "",
		RoutingKey:  "q1",
	}

	time.Sleep(300 * time.Millisecond)
	phase2 := atomic.LoadInt64(&readBytes)
	if phase2 <= 0 {
		t.Errorf("expected delivery when dirty=true, got 0 bytes")
	}
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

	var readBytes int64
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		buf := make([]byte, 4096)
		for {
			n, err := clientConn.Read(buf)
			if err != nil {
				return
			}
			atomic.AddInt64(&readBytes, int64(n))
		}
	}()

	ch := protocol.NewChannel(1, conn)
	ch.Mutex.Lock()
	ch.Consumers["c1"] = &protocol.Consumer{
		Tag:      "c1",
		Queue:    "q1",
		Messages: make(chan *protocol.Delivery, 10),
		Cancel:   make(chan struct{}),
	}
	ch.Mutex.Unlock()
	conn.Channels.Store(uint16(1), ch)

	conn.ConsumersDirty.Store(true)

	done := make(chan struct{})
	go srv.consumerDeliveryLoop(conn, done)
	defer func() {
		conn.Closed.Store(true)
		<-done
		clientConn.Close()
		<-readerDone
	}()

	time.Sleep(100 * time.Millisecond)

	ch.Mutex.Lock()
	ch.Consumers["c2"] = &protocol.Consumer{
		Tag:      "c2",
		Queue:    "q2",
		Messages: make(chan *protocol.Delivery, 10),
		Cancel:   make(chan struct{}),
	}
	ch.Mutex.Unlock()

	ch.Consumers["c2"].Messages <- &protocol.Delivery{
		Message:     &protocol.Message{DeliveryTag: 1, Body: []byte("c2-msg"), Exchange: "", RoutingKey: "q2"},
		DeliveryTag: 1,
		ConsumerTag: "c2",
		Exchange:    "",
		RoutingKey:  "q2",
	}

	time.Sleep(300 * time.Millisecond)
	phase := atomic.LoadInt64(&readBytes)
	if phase <= 0 {
		t.Errorf("expected delivery via timeout fallback, got 0 bytes")
	}
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

	srv.discoverConsumers(conn, consumerInfos, forwarders, activeTags, fanIn)

	defer func() {
		for _, stop := range forwarders {
			close(stop)
		}
	}()

	allocs := testing.AllocsPerRun(100, func() {
		srv.discoverConsumers(conn, consumerInfos, forwarders, activeTags, fanIn)
	})

	if allocs != 0 {
		t.Errorf("expected 0 allocations in steady state, got %v", allocs)
	}
}
