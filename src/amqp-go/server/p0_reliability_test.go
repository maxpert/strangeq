package server

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

// ============================================================================
// P0 Reliability Tests
//
// These tests verify fixes for three critical reliability bugs:
//   PUB-16/PUB-2: conn.Mutex leak in processBodyFrame (deadlock on overflow)
//   ACK-9: ack error closes entire connection on consumer unregister race
//   CON-14: messages dropped on TCP write error without requeue
// ============================================================================

// --- PUB-16/PUB-2: Mutex leak / unnecessary mutex in publish path ---

// TestP0_BodyOverflowNoDeadlock verifies that processBodyFrame returns an error
// (and does NOT deadlock) when the body payload exceeds the expected BodySize.
// Before the fix, conn.Mutex.Lock() was called but the overflow error path
// returned without Unlock(), permanently deadlocking the connection.
func TestP0_BodyOverflowNoDeadlock(t *testing.T) {
	srv := &Server{
		Log:              zap.NewNop(),
		Config:           config.DefaultConfig(),
		MetricsCollector: &NoOpMetricsCollector{},
	}
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	// WS-B: the body-overrun path now sends connection.close 505; drain the
	// client end so the write cannot block.
	go drainAckConn(clientConn)
	defer clientConn.Close()
	conn := protocol.NewConnection(serverConn)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	// Set up a pending message with a small expected body size
	pendingMsg := &protocol.PendingMessage{
		Method: &protocol.BasicPublishMethod{Exchange: "", RoutingKey: "test"},
		Header: &protocol.ContentHeader{ClassID: 60, BodySize: 5}, // expect 5 bytes
		Body:   make([]byte, 0, 5),
	}
	conn.PendingMessages[1] = pendingMsg

	// Send a body frame with MORE data than expected (10 bytes > 5 expected)
	bodyFrame := &protocol.Frame{
		Type:    protocol.FrameBody,
		Channel: 1,
		Size:    10,
		Payload: []byte("0123456789"),
	}

	// This must return an error, not hang. With the old mutex leak,
	// this would deadlock forever.
	done := make(chan struct{})
	var err error
	go func() {
		err = srv.processBodyFrame(conn, bodyFrame)
		close(done)
	}()

	select {
	case <-done:
		// Good — function returned, no deadlock
		if err == nil {
			t.Fatal("expected error for body overflow, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("processBodyFrame deadlocked on body overflow — conn.Mutex leak (PUB-16)")
	}

	// Verify the connection is not permanently locked by doing another operation
	// (with the old bug, conn.Mutex would still be held, blocking everything)
	broker2 := newAckTestBroker()
	close(broker2.pubBlock) // unblock PublishMessage immediately
	srv.Broker = broker2
	pendingMsg2 := &protocol.PendingMessage{
		Method: &protocol.BasicPublishMethod{Exchange: "", RoutingKey: "test2"},
		Header: &protocol.ContentHeader{ClassID: 60, BodySize: 0, PropertyFlags: 0},
	}
	conn.PendingMessages[1] = pendingMsg2

	headerFrame := &protocol.Frame{
		Type:    protocol.FrameHeader,
		Channel: 1,
		Size:    0,
		Payload: serializeEmptyContentHeader(t),
	}

	done2 := make(chan struct{})
	go func() {
		_ = srv.processHeaderFrame(conn, headerFrame)
		close(done2)
	}()

	select {
	case <-done2:
		// Good — connection not deadlocked
	case <-time.After(2 * time.Second):
		t.Fatal("connection permanently deadlocked after body overflow — conn.Mutex was never released (PUB-16)")
	}
}

// --- ACK-9: Ack error closes connection on consumer unregister race ---

// TestP0_AckUnknownTagIs406ChannelError verifies that handleBasicAck raises a
// channel-level 406 (*ChannelError) for an unknown delivery tag (RabbitMQ
// collect_acks parity), and the connection stays open. Before the WS-B error
// refactor this returned nil (tolerant); the ackProcessor closing gate keeps
// the connection alive while the channel closes.
func TestP0_AckUnknownTagIs406ChannelError(t *testing.T) {
	broker := newAckTestBroker()
	// Deliberately do NOT set a delivery tag — the wire-tag table stays empty.
	srv := newAckTestServer(broker)
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	go drainAckConn(clientConn)
	conn := protocol.NewConnection(serverConn)
	// Acks on a channel that was never opened are ignored (reorder hazard);
	// register the channel so the unknown tag produces the 406.
	conn.Channels.Store(uint16(1), protocol.NewChannel(1, conn))

	ackFrame := encodeAckFrame(t, 1, 999, false)

	// Call handleBasicAck directly (not processFrame which just enqueues to AckQueue)
	err := srv.processAckFrame(conn, ackFrame)
	var ce *ChannelError
	if !errors.As(err, &ce) {
		t.Fatalf("handleBasicAck should return *ChannelError for unknown delivery tag, got: %v (%T)", err, err)
	}
	if ce.Code != 406 {
		t.Fatalf("expected 406 PreconditionFailed, got %d", ce.Code)
	}

	// Verify connection was NOT closed
	if conn.Closed.Load() {
		t.Fatal("connection should NOT be closed when ack targets unknown delivery tag (ACK-9)")
	}
}

// TestP0_RejectUnknownTagIs406ChannelError verifies the same fix for basic.reject.
func TestP0_RejectUnknownTagIs406ChannelError(t *testing.T) {
	broker := newAckTestBroker()
	srv := newAckTestServer(broker)
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	go drainAckConn(clientConn)
	conn := protocol.NewConnection(serverConn)
	conn.Channels.Store(uint16(1), protocol.NewChannel(1, conn))

	rejectFrame := encodeRejectFrame(t, 1, 999, true)

	// Call processAckFrame directly to exercise handleBasicReject
	err := srv.processAckFrame(conn, rejectFrame)
	var ce *ChannelError
	if !errors.As(err, &ce) {
		t.Fatalf("handleBasicReject should return *ChannelError for unknown delivery tag, got: %v (%T)", err, err)
	}
	if ce.Code != 406 {
		t.Fatalf("expected 406 PreconditionFailed, got %d", ce.Code)
	}

	if conn.Closed.Load() {
		t.Fatal("connection should NOT be closed when reject targets unknown delivery tag (ACK-9)")
	}
}

// TestP0_NackUnknownTagIs406ChannelError verifies the same fix for basic.nack.
func TestP0_NackUnknownTagIs406ChannelError(t *testing.T) {
	broker := newAckTestBroker()
	srv := newAckTestServer(broker)
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	go drainAckConn(clientConn)
	conn := protocol.NewConnection(serverConn)
	conn.Channels.Store(uint16(1), protocol.NewChannel(1, conn))

	nackFrame := encodeNackFrame(t, 1, 999, false, true)

	// Call processAckFrame directly to exercise handleBasicNack
	err := srv.processAckFrame(conn, nackFrame)
	var ce *ChannelError
	if !errors.As(err, &ce) {
		t.Fatalf("handleBasicNack should return *ChannelError for unknown delivery tag, got: %v (%T)", err, err)
	}
	if ce.Code != 406 {
		t.Fatalf("expected 406 PreconditionFailed, got %d", ce.Code)
	}

	if conn.Closed.Load() {
		t.Fatal("connection should NOT be closed when nack targets unknown delivery tag (ACK-9)")
	}
}

// TestP0_AckProcessorSurvivesConsumerNotFound verifies that the ackProcessor
// goroutine continues running after processing an ack for an unregistered
// consumer. Before the fix, the ackProcessor would set conn.Closed=true and exit.
// WS-B: the unknown ack closes channel 1 (406); the valid ack goes out on
// channel 2 because channel 1's later acks are discarded by the closing gate.
func TestP0_AckProcessorSurvivesConsumerNotFound(t *testing.T) {
	broker := newAckTestBroker()
	srv := newAckTestServer(broker)
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	go drainAckConn(clientConn)
	conn := protocol.NewConnection(serverConn)
	conn.Channels.Store(uint16(1), protocol.NewChannel(1, conn))
	conn.Channels.Store(uint16(2), protocol.NewChannel(2, conn))

	// First: send an ack for an unknown delivery tag on channel 1.
	// No wire-tag tracking for 999: it is deliberately an unknown delivery.
	unknownAckFrame := encodeAckFrame(t, 1, 999, false)
	conn.AckQueue <- unknownAckFrame

	// Then: set up a real delivery tag on channel 2 and send a valid ack.
	broker.setDeliveryTag(100, "test-consumer")
	validWireTag := registerAckTestDelivery(conn, 2, 100, "test-consumer")
	validAckFrame := encodeAckFrame(t, 2, validWireTag, false)
	conn.AckQueue <- validAckFrame

	done := make(chan struct{})
	go srv.ackProcessor(srv.runtimeFor(conn), done)

	// Wait for the valid ack to be processed
	select {
	case <-broker.ackEntered:
		// Good — ackProcessor survived the unknown-delivery ack and processed the valid one
	case <-time.After(2 * time.Second):
		t.Fatal("ackProcessor did not process valid ack after surviving unknown-delivery ack (ACK-9)")
	}

	// Connection should still be alive
	if conn.Closed.Load() {
		t.Fatal("connection should NOT be closed — ackProcessor must survive unknown-delivery race (ACK-9)")
	}

	// Clean shutdown
	close(conn.AckQueue)
	<-done
}

// --- CON-14: Messages dropped on TCP write error without requeue ---

// TestP0_WriteErrorRequeuesDeliveries verifies that when sendBatchedDeliveries
// fails (TCP write error), the deliveries are requeued via RejectMessage and
// conn.Closed is set. Before the fix, deliveries were silently abandoned.
func TestP0_WriteErrorRequeuesDeliveries(t *testing.T) {
	broker := newAckTestBroker()
	broker.setDeliveryTag(1, "consumer-a")

	srv := &Server{
		Log:              zap.NewNop(),
		Broker:           broker,
		Config:           config.DefaultConfig(),
		MetricsCollector: &NoOpMetricsCollector{},
	}

	// Create a pipe and immediately close the client side to cause write errors
	clientConn, serverConn := net.Pipe()
	clientConn.Close() // Close client side — writes to serverConn will fail
	defer serverConn.Close()

	conn := protocol.NewConnection(serverConn)

	// Set up a consumer
	ch := protocol.NewChannel(1, conn)
	ch.Mutex.Lock()
	ch.Consumers = make(map[string]*protocol.Consumer)
	ch.Consumers["consumer-a"] = &protocol.Consumer{
		ID:       "consumer-a",
		Tag:      "consumer-a",
		Queue:    "queue-a",
		Messages: make(chan *protocol.Delivery, 10),
		Cancel:   make(chan struct{}),
	}
	ch.Mutex.Unlock()
	conn.Channels.Store(uint16(1), ch)

	// Start the delivery loop
	done := make(chan struct{})
	conn.ConsumersDirty.Store(true)
	go func() { srv.consumerDeliveryLoop(srv.runtimeFor(conn)); close(done) }()

	// Wait for consumer discovery
	time.Sleep(100 * time.Millisecond)

	// Send a delivery to the consumer's Messages channel
	ch.Consumers["consumer-a"].Messages <- &protocol.Delivery{
		Message: &protocol.Message{
			DeliveryTag: 1,
			Body:        []byte("msg-a"),
			Exchange:    "",
			RoutingKey:  "queue-a",
		},
		DeliveryTag: 1,
		ConsumerID:  "consumer-a",
		Exchange:    "",
		RoutingKey:  "queue-a",
	}

	// Wait for the delivery loop to exit (should exit after write error)
	select {
	case <-done:
		// Good — loop exited after write error
	case <-time.After(3 * time.Second):
		t.Fatal("consumerDeliveryLoop did not exit after write error (CON-14)")
	}

	// Verify conn.Closed was set
	if !conn.Closed.Load() {
		t.Fatal("conn.Closed should be set after write error (CON-14)")
	}

	// Verify RejectMessage was called with requeue=true to requeue the delivery
	rejects := broker.getRejects()
	if len(rejects) == 0 {
		t.Fatal("RejectMessage should be called to requeue deliveries on write error (CON-14)")
	}

	// At least one reject should have requeue=true
	foundRequeue := false
	for _, r := range rejects {
		if r.requeue {
			foundRequeue = true
			break
		}
	}
	if !foundRequeue {
		t.Error("RejectMessage should be called with requeue=true on write error (CON-14)")
	}
}

// --- Helper ---

func serializeEmptyContentHeader(t *testing.T) []byte {
	t.Helper()
	h := &protocol.ContentHeader{ClassID: 60, BodySize: 0, PropertyFlags: 0}
	data, err := h.Serialize()
	if err != nil {
		t.Fatalf("serialize content header: %v", err)
	}
	return data
}
