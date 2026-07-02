package server

import (
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

type recordedAck struct {
	consumerTag string
	deliveryTag uint64
	multiple    bool
}

type recordedNack struct {
	consumerTag string
	deliveryTag uint64
	multiple    bool
	requeue     bool
}

type recordedReject struct {
	consumerTag string
	deliveryTag uint64
	requeue     bool
}

type ackTestBroker struct {
	mu sync.Mutex

	ackCalls    []recordedAck
	nackCalls   []recordedNack
	rejectCalls []recordedReject

	pubCount    atomic.Int64
	ackCount    atomic.Int64
	nackCount   atomic.Int64
	rejectCount atomic.Int64

	pubBlock      chan struct{}
	pubEntered    chan struct{}
	ackEntered    chan struct{}
	nackEntered   chan struct{}
	rejectEntered chan struct{}

	deliveryTags map[uint64]string
}

func newAckTestBroker() *ackTestBroker {
	return &ackTestBroker{
		pubBlock:      make(chan struct{}),
		pubEntered:    make(chan struct{}),
		ackEntered:    make(chan struct{}, 256),
		nackEntered:   make(chan struct{}, 256),
		rejectEntered: make(chan struct{}, 256),
		deliveryTags:  make(map[uint64]string),
	}
}

func (b *ackTestBroker) setDeliveryTag(tag uint64, consumerTag string) {
	b.mu.Lock()
	b.deliveryTags[tag] = consumerTag
	b.mu.Unlock()
}

func (b *ackTestBroker) getAcks() []recordedAck {
	b.mu.Lock()
	defer b.mu.Unlock()
	copies := make([]recordedAck, len(b.ackCalls))
	copy(copies, b.ackCalls)
	return copies
}

func (b *ackTestBroker) getNacks() []recordedNack {
	b.mu.Lock()
	defer b.mu.Unlock()
	copies := make([]recordedNack, len(b.nackCalls))
	copy(copies, b.nackCalls)
	return copies
}

func (b *ackTestBroker) getRejects() []recordedReject {
	b.mu.Lock()
	defer b.mu.Unlock()
	copies := make([]recordedReject, len(b.rejectCalls))
	copy(copies, b.rejectCalls)
	return copies
}

func (b *ackTestBroker) DeclareExchange(name, exchangeType string, durable, autoDelete, internal bool, arguments map[string]interface{}) error {
	return nil
}
func (b *ackTestBroker) DeleteExchange(name string, ifUnused bool) error { return nil }
func (b *ackTestBroker) DeclareQueue(name string, durable, autoDelete, exclusive bool, arguments map[string]interface{}) (*protocol.Queue, error) {
	return &protocol.Queue{Name: name}, nil
}
func (b *ackTestBroker) DeleteQueue(name string, ifUnused, ifEmpty bool) error { return nil }
func (b *ackTestBroker) BindQueue(queueName, exchangeName, routingKey string, arguments map[string]interface{}) error {
	return nil
}
func (b *ackTestBroker) UnbindQueue(queueName, exchangeName, routingKey string) error { return nil }

func (b *ackTestBroker) PublishMessage(exchangeName, routingKey string, message *protocol.Message) error {
	b.pubCount.Add(1)
	close(b.pubEntered)
	<-b.pubBlock
	return nil
}

func (b *ackTestBroker) RegisterConsumer(queueName, consumerTag string, consumer *protocol.Consumer) error {
	return nil
}
func (b *ackTestBroker) UnregisterConsumer(consumerTag string) error { return nil }

func (b *ackTestBroker) AcknowledgeMessage(consumerTag string, deliveryTag uint64, multiple bool) error {
	b.mu.Lock()
	b.ackCalls = append(b.ackCalls, recordedAck{consumerTag, deliveryTag, multiple})
	b.mu.Unlock()
	b.ackCount.Add(1)
	select {
	case b.ackEntered <- struct{}{}:
	default:
	}
	return nil
}

func (b *ackTestBroker) RejectMessage(consumerTag string, deliveryTag uint64, requeue bool) error {
	b.mu.Lock()
	b.rejectCalls = append(b.rejectCalls, recordedReject{consumerTag, deliveryTag, requeue})
	b.mu.Unlock()
	b.rejectCount.Add(1)
	select {
	case b.rejectEntered <- struct{}{}:
	default:
	}
	return nil
}

func (b *ackTestBroker) NacknowledgeMessage(consumerTag string, deliveryTag uint64, multiple, requeue bool) error {
	b.mu.Lock()
	b.nackCalls = append(b.nackCalls, recordedNack{consumerTag, deliveryTag, multiple, requeue})
	b.mu.Unlock()
	b.nackCount.Add(1)
	select {
	case b.nackEntered <- struct{}{}:
	default:
	}
	return nil
}

func (b *ackTestBroker) GetConsumerForDelivery(deliveryTag uint64) (string, bool) {
	b.mu.Lock()
	tag, ok := b.deliveryTags[deliveryTag]
	b.mu.Unlock()
	return tag, ok
}

func (b *ackTestBroker) RecoverQueue(queueName string, minTag, maxTag uint64)        {}
func (b *ackTestBroker) AdvanceDeliveryTag(tag uint64)                               {}
func (b *ackTestBroker) RebuildDeliveryIndex(deliveryTag uint64, consumerTag string) {}
func (b *ackTestBroker) GetQueues() map[string]*protocol.Queue                       { return nil }
func (b *ackTestBroker) GetExchanges() map[string]*protocol.Exchange                 { return nil }
func (b *ackTestBroker) GetConsumers() map[string]*protocol.Consumer                 { return nil }

func newAckTestServer(broker *ackTestBroker) *Server {
	return &Server{
		Log:              zap.NewNop(),
		Broker:           broker,
		Config:           config.DefaultConfig(),
		MetricsCollector: &NoOpMetricsCollector{},
	}
}

func encodeAckFrame(t *testing.T, channelID uint16, deliveryTag uint64, multiple bool) *protocol.Frame {
	t.Helper()
	m := &protocol.BasicAckMethod{DeliveryTag: deliveryTag, Multiple: multiple}
	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("serialize basic.ack: %v", err)
	}
	return protocol.EncodeMethodFrameForChannel(channelID, 60, protocol.BasicAck, data)
}

func encodeNackFrame(t *testing.T, channelID uint16, deliveryTag uint64, multiple, requeue bool) *protocol.Frame {
	t.Helper()
	m := &protocol.BasicNackMethod{DeliveryTag: deliveryTag, Multiple: multiple, Requeue: requeue}
	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("serialize basic.nack: %v", err)
	}
	return protocol.EncodeMethodFrameForChannel(channelID, 60, protocol.BasicNack, data)
}

func encodeRejectFrame(t *testing.T, channelID uint16, deliveryTag uint64, requeue bool) *protocol.Frame {
	t.Helper()
	m := &protocol.BasicRejectMethod{DeliveryTag: deliveryTag, Requeue: requeue}
	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("serialize basic.reject: %v", err)
	}
	return protocol.EncodeMethodFrameForChannel(channelID, 60, protocol.BasicReject, data)
}

func encodePublishFrame(t *testing.T, channelID uint16, exchange, routingKey string) *protocol.Frame {
	t.Helper()
	m := &protocol.BasicPublishMethod{Exchange: exchange, RoutingKey: routingKey}
	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("serialize basic.publish: %v", err)
	}
	return protocol.EncodeMethodFrameForChannel(channelID, 60, protocol.BasicPublish, data)
}

func encodeEmptyHeaderFrame(t *testing.T, channelID uint16) *protocol.Frame {
	t.Helper()
	h := &protocol.ContentHeader{ClassID: 60, BodySize: 0, PropertyFlags: 0}
	payload, err := h.Serialize()
	if err != nil {
		t.Fatalf("serialize content header: %v", err)
	}
	return &protocol.Frame{
		Type:    protocol.FrameHeader,
		Channel: channelID,
		Size:    uint32(len(payload)),
		Payload: payload,
	}
}

func drainAckConn(c net.Conn) {
	buf := make([]byte, 4096)
	for {
		if _, err := c.Read(buf); err != nil {
			return
		}
	}
}

func TestA2_AckFrameRoutedToAckQueue(t *testing.T) {
	broker := newAckTestBroker()
	srv := newAckTestServer(broker)
	_, serverConn := net.Pipe()
	defer serverConn.Close()
	conn := protocol.NewConnection(serverConn)
	conn.Channels.Store(uint16(1), protocol.NewChannel(1, conn))

	ackFrame := encodeAckFrame(t, 1, 42, false)

	if err := srv.processFrame(conn, ackFrame); err != nil {
		t.Fatalf("processFrame returned error: %v", err)
	}

	select {
	case f := <-conn.AckQueue:
		if f != ackFrame {
			t.Fatal("frame from AckQueue is not the same ACK frame")
		}
	case <-time.After(time.Second):
		t.Fatal("ACK frame was not routed to AckQueue")
	}

	if broker.ackCount.Load() != 0 {
		t.Fatal("AcknowledgeMessage should not be called during routing (deferred to ackProcessor)")
	}
}

func TestA2_NackFrameRoutedToAckQueue(t *testing.T) {
	broker := newAckTestBroker()
	srv := newAckTestServer(broker)
	_, serverConn := net.Pipe()
	defer serverConn.Close()
	conn := protocol.NewConnection(serverConn)

	nackFrame := encodeNackFrame(t, 1, 42, false, true)

	if err := srv.processFrame(conn, nackFrame); err != nil {
		t.Fatalf("processFrame returned error: %v", err)
	}

	select {
	case <-conn.AckQueue:
	case <-time.After(time.Second):
		t.Fatal("NACK frame was not routed to AckQueue")
	}

	if broker.nackCount.Load() != 0 {
		t.Fatal("NacknowledgeMessage should not be called during routing")
	}
}

func TestA2_RejectFrameRoutedToAckQueue(t *testing.T) {
	broker := newAckTestBroker()
	srv := newAckTestServer(broker)
	_, serverConn := net.Pipe()
	defer serverConn.Close()
	conn := protocol.NewConnection(serverConn)

	rejectFrame := encodeRejectFrame(t, 1, 42, true)

	if err := srv.processFrame(conn, rejectFrame); err != nil {
		t.Fatalf("processFrame returned error: %v", err)
	}

	select {
	case <-conn.AckQueue:
	case <-time.After(time.Second):
		t.Fatal("REJECT frame was not routed to AckQueue")
	}

	if broker.rejectCount.Load() != 0 {
		t.Fatal("RejectMessage should not be called during routing")
	}
}

func TestA2_NonAckFrameNotRoutedToAckQueue(t *testing.T) {
	broker := newAckTestBroker()
	srv := newAckTestServer(broker)
	_, serverConn := net.Pipe()
	defer serverConn.Close()
	conn := protocol.NewConnection(serverConn)
	conn.Channels.Store(uint16(1), protocol.NewChannel(1, conn))

	publishFrame := encodePublishFrame(t, 1, "", "test")

	done := make(chan struct{})
	go func() {
		srv.processFrame(conn, publishFrame)
		close(done)
	}()

	select {
	case <-conn.AckQueue:
		t.Fatal("non-ACK frame should not be routed to AckQueue")
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("processFrame did not complete for non-ACK frame")
	}
}

func TestA2_AckProcessorProcessesAck(t *testing.T) {
	broker := newAckTestBroker()
	broker.setDeliveryTag(42, "test-consumer")
	srv := newAckTestServer(broker)
	_, serverConn := net.Pipe()
	defer serverConn.Close()
	conn := protocol.NewConnection(serverConn)

	ackFrame := encodeAckFrame(t, 1, 42, false)
	conn.AckQueue <- ackFrame

	done := make(chan struct{})
	go srv.ackProcessor(conn, done)

	select {
	case <-broker.ackEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("ackProcessor did not process ACK frame")
	}

	conn.Closed.Store(true)
	close(conn.AckQueue)
	<-done

	acks := broker.getAcks()
	if len(acks) != 1 {
		t.Fatalf("expected 1 ack call, got %d", len(acks))
	}
	if acks[0].consumerTag != "test-consumer" {
		t.Errorf("expected consumer tag 'test-consumer', got %q", acks[0].consumerTag)
	}
	if acks[0].deliveryTag != 42 {
		t.Errorf("expected delivery tag 42, got %d", acks[0].deliveryTag)
	}
	if acks[0].multiple != false {
		t.Error("expected multiple=false")
	}
}

func TestA2_AckProcessorProcessesNack(t *testing.T) {
	broker := newAckTestBroker()
	broker.setDeliveryTag(42, "test-consumer")
	srv := newAckTestServer(broker)
	_, serverConn := net.Pipe()
	defer serverConn.Close()
	conn := protocol.NewConnection(serverConn)

	nackFrame := encodeNackFrame(t, 1, 42, true, true)
	conn.AckQueue <- nackFrame

	done := make(chan struct{})
	go srv.ackProcessor(conn, done)

	select {
	case <-broker.nackEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("ackProcessor did not process NACK frame")
	}

	conn.Closed.Store(true)
	close(conn.AckQueue)
	<-done

	nacks := broker.getNacks()
	if len(nacks) != 1 {
		t.Fatalf("expected 1 nack call, got %d", len(nacks))
	}
	if nacks[0].consumerTag != "test-consumer" {
		t.Errorf("expected consumer tag 'test-consumer', got %q", nacks[0].consumerTag)
	}
	if nacks[0].deliveryTag != 42 {
		t.Errorf("expected delivery tag 42, got %d", nacks[0].deliveryTag)
	}
	if nacks[0].multiple != true {
		t.Error("expected multiple=true")
	}
	if nacks[0].requeue != true {
		t.Error("expected requeue=true")
	}
}

func TestA2_AckProcessorProcessesReject(t *testing.T) {
	broker := newAckTestBroker()
	broker.setDeliveryTag(42, "test-consumer")
	srv := newAckTestServer(broker)
	_, serverConn := net.Pipe()
	defer serverConn.Close()
	conn := protocol.NewConnection(serverConn)

	rejectFrame := encodeRejectFrame(t, 1, 42, true)
	conn.AckQueue <- rejectFrame

	done := make(chan struct{})
	go srv.ackProcessor(conn, done)

	select {
	case <-broker.rejectEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("ackProcessor did not process REJECT frame")
	}

	conn.Closed.Store(true)
	close(conn.AckQueue)
	<-done

	rejects := broker.getRejects()
	if len(rejects) != 1 {
		t.Fatalf("expected 1 reject call, got %d", len(rejects))
	}
	if rejects[0].consumerTag != "test-consumer" {
		t.Errorf("expected consumer tag 'test-consumer', got %q", rejects[0].consumerTag)
	}
	if rejects[0].deliveryTag != 42 {
		t.Errorf("expected delivery tag 42, got %d", rejects[0].deliveryTag)
	}
	if rejects[0].requeue != true {
		t.Error("expected requeue=true")
	}
}

func TestA2_AckProcessorDrainsOnClose(t *testing.T) {
	broker := newAckTestBroker()
	for i := uint64(1); i <= 5; i++ {
		broker.setDeliveryTag(i, "test-consumer")
	}
	srv := newAckTestServer(broker)
	_, serverConn := net.Pipe()
	defer serverConn.Close()
	conn := protocol.NewConnection(serverConn)

	for i := uint64(1); i <= 5; i++ {
		conn.AckQueue <- encodeAckFrame(t, 1, i, false)
	}

	done := make(chan struct{})
	go srv.ackProcessor(conn, done)

	close(conn.AckQueue)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ackProcessor did not exit after AckQueue close")
	}

	if got := broker.ackCount.Load(); got != 5 {
		t.Fatalf("expected 5 acks drained, got %d", got)
	}
	acks := broker.getAcks()
	if len(acks) != 5 {
		t.Fatalf("expected 5 recorded acks, got %d", len(acks))
	}
	for i, a := range acks {
		expected := uint64(i + 1)
		if a.deliveryTag != expected {
			t.Errorf("ack[%d]: expected delivery tag %d, got %d", i, expected, a.deliveryTag)
		}
	}
}

func TestA2_AckProcessorNoGoroutineLeak(t *testing.T) {
	broker := newAckTestBroker()
	srv := newAckTestServer(broker)
	_, serverConn := net.Pipe()
	defer serverConn.Close()
	conn := protocol.NewConnection(serverConn)

	before := runtime.NumGoroutine()

	done := make(chan struct{})
	go srv.ackProcessor(conn, done)

	close(conn.AckQueue)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ackProcessor did not exit")
	}

	time.Sleep(100 * time.Millisecond)

	after := runtime.NumGoroutine()
	if leaked := after - before; leaked > 1 {
		t.Errorf("goroutine leak: before=%d, after=%d, leaked=%d", before, after, leaked)
	}
}

func TestA2_AckConcurrentWithPublish(t *testing.T) {
	broker := newAckTestBroker()
	broker.setDeliveryTag(99, "test-consumer")
	srv := newAckTestServer(broker)

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	go drainAckConn(clientConn)

	conn := protocol.NewConnection(serverConn)
	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	processorDone := make(chan struct{})
	go srv.processFrames(conn, processorDone)

	ackDone := make(chan struct{})
	go srv.ackProcessor(conn, ackDone)

	conn.FrameQueue <- encodeAckFrame(t, 1, 99, false)
	conn.FrameQueue <- encodePublishFrame(t, 1, "", "test")
	conn.FrameQueue <- encodeEmptyHeaderFrame(t, 1)

	select {
	case <-broker.pubEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("PublishMessage was not called (publish did not reach broker)")
	}

	select {
	case <-broker.ackEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("ACK was not processed while publish was blocked — head-of-line blocking NOT eliminated")
	}

	if broker.pubCount.Load() != 1 {
		t.Fatalf("expected exactly 1 publish in flight, got %d", broker.pubCount.Load())
	}
	if broker.ackCount.Load() != 1 {
		t.Fatal("ACK was not processed by ackProcessor")
	}

	close(broker.pubBlock)
	close(conn.FrameQueue)

	select {
	case <-processorDone:
	case <-time.After(5 * time.Second):
		t.Fatal("processFrames did not exit after unblocking publish")
	}

	close(conn.AckQueue)
	<-ackDone
}
