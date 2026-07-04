package server

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// ---------------------------------------------------------------------------
// Test infrastructure helpers
// ---------------------------------------------------------------------------

func newGetPurgeTestServer(t *testing.T) *Server {
	t.Helper()
	logger := zap.NewNop()
	cfg := config.DefaultConfig()
	cfg.Storage.Path = t.TempDir()

	storageImpl, err := storage.NewDisruptorStorageWithDataDir(cfg.Storage.Path)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	storageBroker := broker.NewStorageBroker(storageImpl, cfg.GetEngine())
	unifiedBroker := NewStorageBrokerAdapter(storageBroker)

	return &Server{
		Addr:             ":0",
		Connections:      make(map[string]*protocol.Connection),
		Log:              logger,
		Broker:           unifiedBroker,
		Config:           cfg,
		MetricsCollector: &NoOpMetricsCollector{},
	}
}

// newPipeConnWithFrames creates a protocol.Connection backed by a net.Pipe.
// A background goroutine reads frames from the client side into a channel so
// that server-side writes never block (net.Pipe is synchronous).
// Returns the connection and a frame channel. The frame channel is closed
// when the client side is closed or a read error occurs.
func newPipeConnWithFrames(t *testing.T) (*protocol.Connection, chan *protocol.Frame) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	conn := protocol.NewConnection(serverConn)

	frameCh := make(chan *protocol.Frame, 256)
	go func() {
		defer close(frameCh)
		for {
			if err := clientConn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
				return
			}
			frame, err := protocol.ReadFrame(clientConn)
			if err != nil {
				return
			}
			select {
			case frameCh <- frame:
			default:
				// Channel full, drop frame to avoid blocking the reader
			}
		}
	}()

	t.Cleanup(func() { clientConn.Close() })
	t.Cleanup(func() { serverConn.Close() })

	return conn, frameCh
}

// nextFrame reads the next frame from the channel, failing the test on timeout.
func nextFrame(t *testing.T, frameCh chan *protocol.Frame) *protocol.Frame {
	t.Helper()
	select {
	case frame, ok := <-frameCh:
		if !ok {
			t.Fatal("frame channel closed before expected frame was received")
		}
		return frame
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for frame")
		return nil
	}
}

// nextMethodFrame reads the next method frame and returns (classID, methodID, payload).
func nextMethodFrame(t *testing.T, frameCh chan *protocol.Frame) (uint16, uint16, []byte) {
	t.Helper()
	frame := nextFrame(t, frameCh)
	if frame.Type != protocol.FrameMethod {
		t.Fatalf("expected method frame, got type %d", frame.Type)
	}
	if len(frame.Payload) < 4 {
		t.Fatalf("method frame payload too short: %d", len(frame.Payload))
	}
	classID := binary.BigEndian.Uint16(frame.Payload[0:2])
	methodID := binary.BigEndian.Uint16(frame.Payload[2:4])
	return classID, methodID, frame.Payload[4:]
}

// drainFrame reads and discards one frame from the channel.
func drainFrame(t *testing.T, frameCh chan *protocol.Frame) {
	t.Helper()
	nextFrame(t, frameCh)
}

// setupQueueAndChannel declares a queue on channel 1, draining the declare-ok
// response frame from the frame channel.
func setupQueueAndChannel(t *testing.T, srv *Server, conn *protocol.Connection, frameCh chan *protocol.Frame, queueName string) {
	t.Helper()
	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	declareMethod := &protocol.QueueDeclareMethod{Queue: queueName}
	payload, err := declareMethod.Serialize()
	require.NoError(t, err)
	err = srv.handleQueueDeclare(conn, 1, payload)
	require.NoError(t, err)

	drainFrame(t, frameCh) // queue.declare-ok
}

// publishMessageToQueue publishes a message to the default exchange with the
// given routing key (queue name) and body.
func publishMessageToQueue(t *testing.T, srv *Server, queueName, body string) {
	t.Helper()
	msg := &protocol.Message{
		Body:       []byte(body),
		Exchange:   "",
		RoutingKey: queueName,
	}
	err := srv.Broker.PublishMessage("", queueName, msg)
	require.NoError(t, err)
}

// encodeBasicGet serializes a basic.get method and returns its payload.
func encodeBasicGet(t *testing.T, queueName string, noAck bool) []byte {
	t.Helper()
	m := &protocol.BasicGetMethod{Queue: queueName, NoAck: noAck}
	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("serialize basic.get: %v", err)
	}
	return data
}

// encodeQueuePurge serializes a queue.purge method and returns its payload.
func encodeQueuePurge(t *testing.T, queueName string, noWait bool) []byte {
	t.Helper()
	m := &protocol.QueuePurgeMethod{Queue: queueName, NoWait: noWait}
	data, err := m.Serialize()
	require.NoError(t, err)
	return data
}

// ===========================================================================
// Protocol-level tests: BasicGetOKMethod Deserialize
// ===========================================================================

func TestBasicGetOKMethodDeserializeRoundtrip(t *testing.T) {
	original := &protocol.BasicGetOKMethod{
		DeliveryTag:  12345,
		Redelivered:  true,
		Exchange:     "amq.direct",
		RoutingKey:   "routing.key",
		MessageCount: 42,
	}

	data, err := original.Serialize()
	require.NoError(t, err)

	decoded := &protocol.BasicGetOKMethod{}
	err = decoded.Deserialize(data)
	require.NoError(t, err)

	assert.Equal(t, original.DeliveryTag, decoded.DeliveryTag)
	assert.Equal(t, original.Redelivered, decoded.Redelivered)
	assert.Equal(t, original.Exchange, decoded.Exchange)
	assert.Equal(t, original.RoutingKey, decoded.RoutingKey)
	assert.Equal(t, original.MessageCount, decoded.MessageCount)
}

func TestBasicGetOKMethodDeserializeEmptyStrings(t *testing.T) {
	original := &protocol.BasicGetOKMethod{
		DeliveryTag:  1,
		Redelivered:  false,
		Exchange:     "",
		RoutingKey:   "",
		MessageCount: 0,
	}

	data, err := original.Serialize()
	require.NoError(t, err)

	decoded := &protocol.BasicGetOKMethod{}
	err = decoded.Deserialize(data)
	require.NoError(t, err)

	assert.Equal(t, uint64(1), decoded.DeliveryTag)
	assert.False(t, decoded.Redelivered)
	assert.Equal(t, "", decoded.Exchange)
	assert.Equal(t, "", decoded.RoutingKey)
	assert.Equal(t, uint32(0), decoded.MessageCount)
}

func TestBasicGetOKMethodDeserializeTooShort(t *testing.T) {
	decoded := &protocol.BasicGetOKMethod{}
	err := decoded.Deserialize([]byte{0, 0, 0, 0, 0, 0, 0, 1})
	if err == nil {
		t.Error("expected error for too-short payload (missing redelivered byte)")
	}
}

// ===========================================================================
// Protocol-level tests: QueuePurgeMethod
// ===========================================================================

func TestQueuePurgeMethodSerializeDeserializeRoundtrip(t *testing.T) {
	original := &protocol.QueuePurgeMethod{
		Reserved1: 0,
		Queue:     "my-queue",
		NoWait:    true,
	}

	data, err := original.Serialize()
	require.NoError(t, err)

	decoded := &protocol.QueuePurgeMethod{}
	err = decoded.Deserialize(data)
	require.NoError(t, err)

	assert.Equal(t, original.Reserved1, decoded.Reserved1)
	assert.Equal(t, original.Queue, decoded.Queue)
	assert.Equal(t, original.NoWait, decoded.NoWait)
}

func TestQueuePurgeMethodDeserializeNoWaitFalse(t *testing.T) {
	original := &protocol.QueuePurgeMethod{
		Queue:  "test",
		NoWait: false,
	}

	data, err := original.Serialize()
	require.NoError(t, err)

	decoded := &protocol.QueuePurgeMethod{}
	err = decoded.Deserialize(data)
	require.NoError(t, err)

	assert.False(t, decoded.NoWait)
}

func TestQueuePurgeMethodDeserializeEmptyQueue(t *testing.T) {
	original := &protocol.QueuePurgeMethod{
		Queue:  "",
		NoWait: false,
	}

	data, err := original.Serialize()
	require.NoError(t, err)

	decoded := &protocol.QueuePurgeMethod{}
	err = decoded.Deserialize(data)
	require.NoError(t, err)

	assert.Equal(t, "", decoded.Queue)
}

func TestQueuePurgeOKMethodSerializeRoundtrip(t *testing.T) {
	original := &protocol.QueuePurgeOKMethod{
		MessageCount: 99,
	}

	data, err := original.Serialize()
	require.NoError(t, err)
	require.Len(t, data, 4)

	assert.Equal(t, uint32(99), binary.BigEndian.Uint32(data))
}

// ===========================================================================
// Handler-level tests: basic.get
// ===========================================================================

func TestBasicGetEmptyQueueReturnsGetEmpty(t *testing.T) {
	srv := newGetPurgeTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	setupQueueAndChannel(t, srv, conn, frameCh, "test-get-empty")

	err := srv.handleBasicGet(conn, 1, encodeBasicGet(t, "test-get-empty", false))
	require.NoError(t, err)

	classID, methodID, _ := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(60), classID)
	assert.Equal(t, uint16(72), methodID) // basic.get-empty
}

func TestBasicGetWithMessageReturnsGetOK(t *testing.T) {
	srv := newGetPurgeTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	setupQueueAndChannel(t, srv, conn, frameCh, "test-get-ok")
	publishMessageToQueue(t, srv, "test-get-ok", "hello world")

	err := srv.handleBasicGet(conn, 1, encodeBasicGet(t, "test-get-ok", false))
	require.NoError(t, err)

	// Read basic.get-ok method frame
	classID, methodID, methodPayload := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(60), classID)
	assert.Equal(t, uint16(71), methodID) // basic.get-ok

	// Deserialize get-ok
	getOK := &protocol.BasicGetOKMethod{}
	err = getOK.Deserialize(methodPayload)
	require.NoError(t, err)

	assert.True(t, getOK.DeliveryTag > 0)
	assert.False(t, getOK.Redelivered)
	assert.Equal(t, "", getOK.Exchange)
	assert.Equal(t, "test-get-ok", getOK.RoutingKey)
	assert.Equal(t, uint32(0), getOK.MessageCount) // 0 remaining after consuming the only message

	// Read content header frame
	headerFrame := nextFrame(t, frameCh)
	assert.Equal(t, byte(protocol.FrameHeader), headerFrame.Type)

	// Read content body frame
	bodyFrame := nextFrame(t, frameCh)
	assert.Equal(t, byte(protocol.FrameBody), bodyFrame.Type)
	assert.Equal(t, "hello world", string(bodyFrame.Payload))
}

func TestBasicGetNoAckDoesNotTrackDelivery(t *testing.T) {
	srv := newGetPurgeTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	setupQueueAndChannel(t, srv, conn, frameCh, "test-get-noack")
	publishMessageToQueue(t, srv, "test-get-noack", "auto-ack")

	err := srv.handleBasicGet(conn, 1, encodeBasicGet(t, "test-get-noack", true))
	require.NoError(t, err)

	// Read get-ok + header + body
	_, _, methodPayload := nextMethodFrame(t, frameCh)

	getOK := &protocol.BasicGetOKMethod{}
	require.NoError(t, getOK.Deserialize(methodPayload))

	_, ok := srv.Broker.GetConsumerForDelivery(getOK.DeliveryTag)
	assert.False(t, ok, "no_ack delivery should not be tracked in deliveryIndex")

	drainFrame(t, frameCh) // header
	drainFrame(t, frameCh) // body
}

func TestBasicGetWithAckTrackingThenAck(t *testing.T) {
	srv := newGetPurgeTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	setupQueueAndChannel(t, srv, conn, frameCh, "test-get-ack")
	publishMessageToQueue(t, srv, "test-get-ack", "track-me")

	err := srv.handleBasicGet(conn, 1, encodeBasicGet(t, "test-get-ack", false))
	require.NoError(t, err)

	// Read get-ok
	_, _, methodPayload := nextMethodFrame(t, frameCh)

	getOK := &protocol.BasicGetOKMethod{}
	require.NoError(t, getOK.Deserialize(methodPayload))
	deliveryTag := getOK.DeliveryTag

	drainFrame(t, frameCh) // header
	drainFrame(t, frameCh) // body

	// Verify delivery is tracked with empty consumer tag
	consumerTag, ok := srv.Broker.GetConsumerForDelivery(deliveryTag)
	assert.True(t, ok)
	assert.Equal(t, "", consumerTag, "basic.get delivery should have empty consumer tag")

	// Now ack the delivery via handleBasicAck
	ackPayload := make([]byte, 10)
	binary.BigEndian.PutUint64(ackPayload[:8], deliveryTag)

	err = srv.handleBasicAck(conn, 1, ackPayload)
	require.NoError(t, err)

	_, ok = srv.Broker.GetConsumerForDelivery(deliveryTag)
	assert.False(t, ok, "delivery should be removed after ack")
}

func TestBasicGetWithAckTrackingThenReject(t *testing.T) {
	srv := newGetPurgeTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	setupQueueAndChannel(t, srv, conn, frameCh, "test-get-reject")
	publishMessageToQueue(t, srv, "test-get-reject", "reject-me")

	err := srv.handleBasicGet(conn, 1, encodeBasicGet(t, "test-get-reject", false))
	require.NoError(t, err)

	_, _, methodPayload := nextMethodFrame(t, frameCh)

	getOK := &protocol.BasicGetOKMethod{}
	require.NoError(t, getOK.Deserialize(methodPayload))
	deliveryTag := getOK.DeliveryTag

	drainFrame(t, frameCh) // header
	drainFrame(t, frameCh) // body

	// Reject with requeue=false (discard)
	rejectPayload := make([]byte, 10)
	binary.BigEndian.PutUint64(rejectPayload[:8], deliveryTag)

	err = srv.handleBasicReject(conn, 1, rejectPayload)
	require.NoError(t, err)

	_, ok := srv.Broker.GetConsumerForDelivery(deliveryTag)
	assert.False(t, ok, "delivery should be removed after reject")

	// Queue should be empty (message discarded, not requeued)
	err = srv.handleBasicGet(conn, 1, encodeBasicGet(t, "test-get-reject", true))
	require.NoError(t, err)
	_, methodID2, _ := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(72), methodID2, "should get-empty after reject without requeue")
}

func TestBasicGetWithAckTrackingThenRejectRequeue(t *testing.T) {
	srv := newGetPurgeTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	setupQueueAndChannel(t, srv, conn, frameCh, "test-get-reject-rq")
	publishMessageToQueue(t, srv, "test-get-reject-rq", "requeue-me")

	err := srv.handleBasicGet(conn, 1, encodeBasicGet(t, "test-get-reject-rq", false))
	require.NoError(t, err)

	_, _, methodPayload := nextMethodFrame(t, frameCh)

	getOK := &protocol.BasicGetOKMethod{}
	require.NoError(t, getOK.Deserialize(methodPayload))
	deliveryTag := getOK.DeliveryTag

	drainFrame(t, frameCh) // header
	drainFrame(t, frameCh) // body

	// Reject with requeue=true
	rejectPayload := make([]byte, 10)
	binary.BigEndian.PutUint64(rejectPayload[:8], deliveryTag)
	binary.BigEndian.PutUint16(rejectPayload[8:10], 1) // requeue bit 0 = 1

	err = srv.handleBasicReject(conn, 1, rejectPayload)
	require.NoError(t, err)

	_, ok := srv.Broker.GetConsumerForDelivery(deliveryTag)
	assert.False(t, ok, "delivery should be removed from index after reject")

	// Message should be requeued — another basic.get should get it back
	err = srv.handleBasicGet(conn, 1, encodeBasicGet(t, "test-get-reject-rq", true))
	require.NoError(t, err)
	_, methodID2, _ := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(71), methodID2, "should get-ok after reject with requeue")
}

func TestBasicGetWithAckTrackingThenNack(t *testing.T) {
	srv := newGetPurgeTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	setupQueueAndChannel(t, srv, conn, frameCh, "test-get-nack")
	publishMessageToQueue(t, srv, "test-get-nack", "nack-me")

	err := srv.handleBasicGet(conn, 1, encodeBasicGet(t, "test-get-nack", false))
	require.NoError(t, err)

	_, _, methodPayload := nextMethodFrame(t, frameCh)

	getOK := &protocol.BasicGetOKMethod{}
	require.NoError(t, getOK.Deserialize(methodPayload))
	deliveryTag := getOK.DeliveryTag

	drainFrame(t, frameCh) // header
	drainFrame(t, frameCh) // body

	// Nack with requeue=true
	nackPayload := make([]byte, 10)
	binary.BigEndian.PutUint64(nackPayload[:8], deliveryTag)
	binary.BigEndian.PutUint16(nackPayload[8:10], 2) // bit 1 = requeue

	err = srv.handleBasicNack(conn, 1, nackPayload)
	require.NoError(t, err)

	_, ok := srv.Broker.GetConsumerForDelivery(deliveryTag)
	assert.False(t, ok, "delivery should be removed from index after nack")

	// Message should be requeued
	err = srv.handleBasicGet(conn, 1, encodeBasicGet(t, "test-get-nack", true))
	require.NoError(t, err)
	_, methodID2, _ := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(71), methodID2, "should get-ok after nack with requeue")
}

func TestBasicGetMessageCount(t *testing.T) {
	srv := newGetPurgeTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	setupQueueAndChannel(t, srv, conn, frameCh, "test-get-count")
	publishMessageToQueue(t, srv, "test-get-count", "msg1")
	publishMessageToQueue(t, srv, "test-get-count", "msg2")
	publishMessageToQueue(t, srv, "test-get-count", "msg3")

	// Get first message with no_ack=true — message_count should be 2
	err := srv.handleBasicGet(conn, 1, encodeBasicGet(t, "test-get-count", true))
	require.NoError(t, err)

	_, _, methodPayload := nextMethodFrame(t, frameCh)

	getOK := &protocol.BasicGetOKMethod{}
	require.NoError(t, getOK.Deserialize(methodPayload))
	assert.Equal(t, uint32(2), getOK.MessageCount, "should report 2 remaining messages")

	drainFrame(t, frameCh) // header
	drainFrame(t, frameCh) // body
}

func TestBasicGetConsumesInFILOrder(t *testing.T) {
	srv := newGetPurgeTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	setupQueueAndChannel(t, srv, conn, frameCh, "test-get-fifo")
	publishMessageToQueue(t, srv, "test-get-fifo", "first")
	publishMessageToQueue(t, srv, "test-get-fifo", "second")
	publishMessageToQueue(t, srv, "test-get-fifo", "third")

	for _, expected := range []string{"first", "second", "third"} {
		err := srv.handleBasicGet(conn, 1, encodeBasicGet(t, "test-get-fifo", true))
		require.NoError(t, err)

		nextMethodFrame(t, frameCh) // get-ok
		drainFrame(t, frameCh)      // header

		bodyFrame := nextFrame(t, frameCh)
		assert.Equal(t, expected, string(bodyFrame.Payload), "messages should be consumed in FIFO order")
	}

	// Queue should now be empty
	err := srv.handleBasicGet(conn, 1, encodeBasicGet(t, "test-get-fifo", true))
	require.NoError(t, err)
	_, methodID, _ := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(72), methodID, "should get-empty after consuming all messages")
}

// ===========================================================================
// Handler-level tests: queue.purge
// ===========================================================================

func TestQueuePurgeRemovesAllMessages(t *testing.T) {
	srv := newGetPurgeTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	setupQueueAndChannel(t, srv, conn, frameCh, "test-purge")
	publishMessageToQueue(t, srv, "test-purge", "msg1")
	publishMessageToQueue(t, srv, "test-purge", "msg2")
	publishMessageToQueue(t, srv, "test-purge", "msg3")

	err := srv.handleQueuePurge(conn, 1, encodeQueuePurge(t, "test-purge", false))
	require.NoError(t, err)

	// Read queue.purge-ok
	classID, methodID, methodPayload := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(50), classID)
	assert.Equal(t, uint16(31), methodID) // queue.purge-ok

	purgeOK := &protocol.QueuePurgeOKMethod{}
	err = purgeOK.Deserialize(methodPayload)
	require.NoError(t, err)
	assert.Equal(t, uint32(3), purgeOK.MessageCount, "should purge 3 messages")

	// Queue should be empty — basic.get should return get-empty
	err = srv.handleBasicGet(conn, 1, encodeBasicGet(t, "test-purge", true))
	require.NoError(t, err)
	_, methodID2, _ := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(72), methodID2, "should get-empty after purge")
}

func TestQueuePurgeEmptyQueueReturnsZero(t *testing.T) {
	srv := newGetPurgeTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	setupQueueAndChannel(t, srv, conn, frameCh, "test-purge-empty")

	err := srv.handleQueuePurge(conn, 1, encodeQueuePurge(t, "test-purge-empty", false))
	require.NoError(t, err)

	_, methodID, methodPayload := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(31), methodID)

	purgeOK := &protocol.QueuePurgeOKMethod{}
	require.NoError(t, purgeOK.Deserialize(methodPayload))
	assert.Equal(t, uint32(0), purgeOK.MessageCount)
}

func TestQueuePurgeKeepsQueueAndBindings(t *testing.T) {
	srv := newGetPurgeTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	setupQueueAndChannel(t, srv, conn, frameCh, "test-purge-keep")

	// Bind to amq.direct with a routing key
	bindMethod := &protocol.QueueBindMethod{
		Queue:      "test-purge-keep",
		Exchange:   "amq.direct",
		RoutingKey: "rk",
		Arguments:  map[string]interface{}{},
	}
	bindPayload, err := bindMethod.Serialize()
	require.NoError(t, err)
	err = srv.handleQueueBind(conn, 1, bindPayload)
	require.NoError(t, err)
	drainFrame(t, frameCh) // bind-ok

	publishMessageToQueue(t, srv, "test-purge-keep", "msg1")

	// Purge
	err = srv.handleQueuePurge(conn, 1, encodeQueuePurge(t, "test-purge-keep", false))
	require.NoError(t, err)
	drainFrame(t, frameCh) // purge-ok

	// Queue should still exist
	queues := srv.Broker.GetQueues()
	_, exists := queues["test-purge-keep"]
	assert.True(t, exists, "queue should still exist after purge")

	// Binding should still work — publish to amq.direct with routing key
	msg := &protocol.Message{
		Body:       []byte("after-purge"),
		Exchange:   "amq.direct",
		RoutingKey: "rk",
	}
	err = srv.Broker.PublishMessage("amq.direct", "rk", msg)
	require.NoError(t, err)

	// Should be able to get the message
	err = srv.handleBasicGet(conn, 1, encodeBasicGet(t, "test-purge-keep", true))
	require.NoError(t, err)
	_, methodID, _ := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(71), methodID, "should get-ok after publish to bound exchange post-purge")
	drainFrame(t, frameCh) // header
	bodyFrame := nextFrame(t, frameCh)
	assert.Equal(t, "after-purge", string(bodyFrame.Payload))
}

func TestQueuePurgeEmptyNameResolvesCurrentQueue(t *testing.T) {
	srv := newGetPurgeTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	// Declare a queue to set CurrentQueue
	declareMethod := &protocol.QueueDeclareMethod{Queue: "purge-current"}
	payload, err := declareMethod.Serialize()
	require.NoError(t, err)
	err = srv.handleQueueDeclare(conn, 1, payload)
	require.NoError(t, err)
	drainFrame(t, frameCh) // declare-ok

	publishMessageToQueue(t, srv, "purge-current", "msg1")

	// Purge with empty queue name — should resolve to "purge-current"
	err = srv.handleQueuePurge(conn, 1, encodeQueuePurge(t, "", false))
	require.NoError(t, err)

	_, methodID, methodPayload := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(31), methodID)

	purgeOK := &protocol.QueuePurgeOKMethod{}
	require.NoError(t, purgeOK.Deserialize(methodPayload))
	assert.Equal(t, uint32(1), purgeOK.MessageCount, "should purge 1 message from current queue")
}

func TestQueuePurgeEmptyNameNoCurrentQueueReturnsError(t *testing.T) {
	srv := newGetPurgeTestServer(t)
	conn, _ := newPipeConnWithFrames(t)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	// No queue declared — CurrentQueue is empty
	err := srv.handleQueuePurge(conn, 1, encodeQueuePurge(t, "", false))
	if err == nil {
		t.Error("handleQueuePurge with empty queue name and no current queue should return an error")
	}
}

func TestQueuePurgeAfterGetAllowsNewPublish(t *testing.T) {
	srv := newGetPurgeTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	setupQueueAndChannel(t, srv, conn, frameCh, "test-purge-repub")
	publishMessageToQueue(t, srv, "test-purge-repub", "msg1")
	publishMessageToQueue(t, srv, "test-purge-repub", "msg2")

	// Purge
	err := srv.handleQueuePurge(conn, 1, encodeQueuePurge(t, "test-purge-repub", false))
	require.NoError(t, err)
	drainFrame(t, frameCh) // purge-ok

	// Publish new message after purge
	publishMessageToQueue(t, srv, "test-purge-repub", "new-msg")

	// Should be able to get the new message
	err = srv.handleBasicGet(conn, 1, encodeBasicGet(t, "test-purge-repub", true))
	require.NoError(t, err)
	_, methodID, _ := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(71), methodID, "should get-ok for new message after purge")
	drainFrame(t, frameCh) // header
	bodyFrame := nextFrame(t, frameCh)
	assert.Equal(t, "new-msg", string(bodyFrame.Payload))
}

func TestQueuePurgeViaProcessQueueMethod(t *testing.T) {
	srv := newGetPurgeTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	setupQueueAndChannel(t, srv, conn, frameCh, "test-purge-via-process")
	publishMessageToQueue(t, srv, "test-purge-via-process", "msg1")

	// Call processQueueMethod directly — should route to handleQueuePurge
	err := srv.processQueueMethod(conn, 1, 30, encodeQueuePurge(t, "test-purge-via-process", false))
	require.NoError(t, err)

	_, methodID, methodPayload := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(31), methodID)

	purgeOK := &protocol.QueuePurgeOKMethod{}
	require.NoError(t, purgeOK.Deserialize(methodPayload))
	assert.Equal(t, uint32(1), purgeOK.MessageCount)
}

func TestBasicGetViaProcessBasicMethod(t *testing.T) {
	srv := newGetPurgeTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)

	setupQueueAndChannel(t, srv, conn, frameCh, "test-get-via-process")
	publishMessageToQueue(t, srv, "test-get-via-process", "via-process")

	// Call processBasicMethod directly — should route to handleBasicGet
	err := srv.processBasicMethod(conn, 1, 70, encodeBasicGet(t, "test-get-via-process", true))
	require.NoError(t, err)

	_, methodID, _ := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(71), methodID, "should get-ok via processBasicMethod")
	drainFrame(t, frameCh) // header
	drainFrame(t, frameCh) // body
}

// ===========================================================================
// Broker-level tests: GetMessageForGet
// ===========================================================================

func TestGetMessageForGetReturnsNilOnEmptyQueue(t *testing.T) {
	srv := newGetPurgeTestServer(t)

	_, err := srv.Broker.DeclareQueue("test-getfor-empty", false, false, false, nil)
	require.NoError(t, err)

	msg, tag, count, err := srv.Broker.GetMessageForGet("test-getfor-empty", false)
	require.NoError(t, err)
	assert.Nil(t, msg)
	assert.Equal(t, uint64(0), tag)
	assert.Equal(t, uint32(0), count)
}

func TestGetMessageForGetReturnsMessage(t *testing.T) {
	srv := newGetPurgeTestServer(t)

	_, err := srv.Broker.DeclareQueue("test-getfor-msg", false, false, false, nil)
	require.NoError(t, err)

	publishMessageToQueue(t, srv, "test-getfor-msg", "broker-get")

	msg, tag, count, err := srv.Broker.GetMessageForGet("test-getfor-msg", false)
	require.NoError(t, err)
	require.NotNil(t, msg)
	assert.Equal(t, "broker-get", string(msg.Body))
	assert.True(t, tag > 0)
	assert.Equal(t, uint32(0), count) // 0 remaining
}

func TestGetMessageForGetNoAckDoesNotTrack(t *testing.T) {
	srv := newGetPurgeTestServer(t)

	_, err := srv.Broker.DeclareQueue("test-getfor-noack", false, false, false, nil)
	require.NoError(t, err)

	publishMessageToQueue(t, srv, "test-getfor-noack", "no-track")

	msg, tag, _, err := srv.Broker.GetMessageForGet("test-getfor-noack", true)
	require.NoError(t, err)
	require.NotNil(t, msg)

	_, ok := srv.Broker.GetConsumerForDelivery(tag)
	assert.False(t, ok, "no_ack get should not be tracked")
}

func TestGetMessageForGetWithAckTracksDelivery(t *testing.T) {
	srv := newGetPurgeTestServer(t)

	_, err := srv.Broker.DeclareQueue("test-getfor-track", false, false, false, nil)
	require.NoError(t, err)

	publishMessageToQueue(t, srv, "test-getfor-track", "track")

	msg, tag, _, err := srv.Broker.GetMessageForGet("test-getfor-track", false)
	require.NoError(t, err)
	require.NotNil(t, msg)

	consumerTag, ok := srv.Broker.GetConsumerForDelivery(tag)
	assert.True(t, ok)
	assert.Equal(t, "", consumerTag, "basic.get delivery should have empty consumer tag")
}

// ===========================================================================
// Broker-level tests: PurgeQueue
// ===========================================================================

func TestBrokerPurgeQueueRemovesMessages(t *testing.T) {
	srv := newGetPurgeTestServer(t)

	_, err := srv.Broker.DeclareQueue("test-broker-purge", false, false, false, nil)
	require.NoError(t, err)

	publishMessageToQueue(t, srv, "test-broker-purge", "msg1")
	publishMessageToQueue(t, srv, "test-broker-purge", "msg2")

	count, err := srv.Broker.PurgeQueue("test-broker-purge")
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	// Queue should be empty
	msg, _, _, err := srv.Broker.GetMessageForGet("test-broker-purge", true)
	require.NoError(t, err)
	assert.Nil(t, msg)
}

func TestBrokerPurgeQueueEmptyReturnsZero(t *testing.T) {
	srv := newGetPurgeTestServer(t)

	_, err := srv.Broker.DeclareQueue("test-broker-purge-empty", false, false, false, nil)
	require.NoError(t, err)

	count, err := srv.Broker.PurgeQueue("test-broker-purge-empty")
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestBrokerPurgeQueueAllowsNewPublish(t *testing.T) {
	srv := newGetPurgeTestServer(t)

	_, err := srv.Broker.DeclareQueue("test-broker-purge-repub", false, false, false, nil)
	require.NoError(t, err)

	publishMessageToQueue(t, srv, "test-broker-purge-repub", "old")
	_, err = srv.Broker.PurgeQueue("test-broker-purge-repub")
	require.NoError(t, err)

	publishMessageToQueue(t, srv, "test-broker-purge-repub", "new")

	msg, _, _, err := srv.Broker.GetMessageForGet("test-broker-purge-repub", true)
	require.NoError(t, err)
	require.NotNil(t, msg)
	assert.Equal(t, "new", string(msg.Body))
}
