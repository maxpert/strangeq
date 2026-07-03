package server

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestStorageBroker creates a StorageBroker with a temp-dir storage for
// broker-level unit tests that need direct access to *broker.StorageBroker
// (bypassing the UnifiedBroker adapter).
func createTestStorageBroker(t testing.TB) (*broker.StorageBroker, func()) {
	t.Helper()
	tmpDir := t.TempDir()
	store := storage.NewDisruptorStorageWithDataDir(tmpDir)
	engineConfig := interfaces.EngineConfig{
		RingBufferSize:          65536,
		SpillThresholdPercent:   80,
		WALBatchSize:            1000,
		WALBatchTimeoutMS:       10,
		ConsumerSelectTimeoutMS: 1,
		ConsumerMaxBatchSize:    100,
	}
	br := broker.NewStorageBroker(store, engineConfig)
	return br, func() {}
}

// parseMethodFrame extracts classID, methodID, and method payload from a
// method frame's payload.
func parseMethodFrame(t *testing.T, frame *protocol.Frame) (classID, methodID uint16, payload []byte) {
	t.Helper()
	require.Equal(t, byte(protocol.FrameMethod), frame.Type, "expected method frame")
	require.GreaterOrEqual(t, len(frame.Payload), 4, "method frame payload too short")
	classID = binary.BigEndian.Uint16(frame.Payload[0:2])
	methodID = binary.BigEndian.Uint16(frame.Payload[2:4])
	payload = frame.Payload[4:]
	return
}

// ============================================================================
// basic.return — protocol-level tests
// ============================================================================

func TestBasicReturnMethodSerialize(t *testing.T) {
	method := &protocol.BasicReturnMethod{
		ReplyCode:  312,
		ReplyText:  "NO_ROUTE",
		Exchange:   "amq.direct",
		RoutingKey: "routing.key",
	}
	data, err := method.Serialize()
	require.NoError(t, err)

	var expected []byte
	expected = binary.BigEndian.AppendUint16(expected, 312)
	expected = append(expected, byte(len("NO_ROUTE")))
	expected = append(expected, []byte("NO_ROUTE")...)
	expected = append(expected, byte(len("amq.direct")))
	expected = append(expected, []byte("amq.direct")...)
	expected = append(expected, byte(len("routing.key")))
	expected = append(expected, []byte("routing.key")...)

	assert.Equal(t, expected, data)
}

func TestBasicReturnMethodDeserialize(t *testing.T) {
	var data []byte
	data = binary.BigEndian.AppendUint16(data, 312)
	data = append(data, byte(len("NO_ROUTE")))
	data = append(data, []byte("NO_ROUTE")...)
	data = append(data, byte(len("amq.direct")))
	data = append(data, []byte("amq.direct")...)
	data = append(data, byte(len("routing.key")))
	data = append(data, []byte("routing.key")...)

	method := &protocol.BasicReturnMethod{}
	err := method.Deserialize(data)
	require.NoError(t, err)

	assert.Equal(t, uint16(312), method.ReplyCode)
	assert.Equal(t, "NO_ROUTE", method.ReplyText)
	assert.Equal(t, "amq.direct", method.Exchange)
	assert.Equal(t, "routing.key", method.RoutingKey)
}

func TestBasicReturnMethodRoundTrip(t *testing.T) {
	original := &protocol.BasicReturnMethod{
		ReplyCode:  404,
		ReplyText:  "NOT_FOUND",
		Exchange:   "test.exchange",
		RoutingKey: "test.key",
	}
	data, err := original.Serialize()
	require.NoError(t, err)

	decoded := &protocol.BasicReturnMethod{}
	err = decoded.Deserialize(data)
	require.NoError(t, err)

	assert.Equal(t, original.ReplyCode, decoded.ReplyCode)
	assert.Equal(t, original.ReplyText, decoded.ReplyText)
	assert.Equal(t, original.Exchange, decoded.Exchange)
	assert.Equal(t, original.RoutingKey, decoded.RoutingKey)
}

func TestBasicReturnMethodDeserializeTooShort(t *testing.T) {
	method := &protocol.BasicReturnMethod{}
	err := method.Deserialize([]byte{0x01})
	assert.Error(t, err)
}

// ============================================================================
// basic.recover / basic.recover-ok — protocol-level tests
// ============================================================================

func TestBasicRecoverMethodSerialize(t *testing.T) {
	t.Run("requeue=false", func(t *testing.T) {
		method := &protocol.BasicRecoverMethod{Requeue: false}
		data, err := method.Serialize()
		require.NoError(t, err)
		assert.Equal(t, []byte{0x00}, data)
	})

	t.Run("requeue=true", func(t *testing.T) {
		method := &protocol.BasicRecoverMethod{Requeue: true}
		data, err := method.Serialize()
		require.NoError(t, err)
		assert.Equal(t, []byte{0x01}, data)
	})
}

func TestBasicRecoverMethodDeserialize(t *testing.T) {
	t.Run("requeue=false", func(t *testing.T) {
		method := &protocol.BasicRecoverMethod{}
		err := method.Deserialize([]byte{0x00})
		require.NoError(t, err)
		assert.False(t, method.Requeue)
	})

	t.Run("requeue=true", func(t *testing.T) {
		method := &protocol.BasicRecoverMethod{}
		err := method.Deserialize([]byte{0x01})
		require.NoError(t, err)
		assert.True(t, method.Requeue)
	})

	t.Run("empty payload defaults to false", func(t *testing.T) {
		method := &protocol.BasicRecoverMethod{}
		err := method.Deserialize([]byte{})
		require.NoError(t, err)
		assert.False(t, method.Requeue)
	})
}

func TestBasicRecoverMethodRoundTrip(t *testing.T) {
	original := &protocol.BasicRecoverMethod{Requeue: true}
	data, err := original.Serialize()
	require.NoError(t, err)

	decoded := &protocol.BasicRecoverMethod{}
	err = decoded.Deserialize(data)
	require.NoError(t, err)
	assert.Equal(t, original.Requeue, decoded.Requeue)
}

func TestBasicRecoverOKMethodSerialize(t *testing.T) {
	method := &protocol.BasicRecoverOKMethod{}
	data, err := method.Serialize()
	require.NoError(t, err)
	assert.Empty(t, data)
}

// ============================================================================
// PublishMessage — mandatory / ErrNoRoute broker tests
// ============================================================================

func TestPublishMessageMandatoryNoRoute(t *testing.T) {
	br, cleanup := createTestStorageBroker(t)
	defer cleanup()

	msg := &protocol.Message{
		Body:       []byte("test"),
		Exchange:   "",
		RoutingKey: "nonexistent-queue",
		Mandatory:  true,
	}
	err := br.PublishMessage("", "nonexistent-queue", msg)
	assert.ErrorIs(t, err, broker.ErrNoRoute)
}

func TestPublishMessageNonMandatoryNoRoute(t *testing.T) {
	br, cleanup := createTestStorageBroker(t)
	defer cleanup()

	msg := &protocol.Message{
		Body:       []byte("test"),
		Exchange:   "",
		RoutingKey: "nonexistent-queue",
		Mandatory:  false,
	}
	err := br.PublishMessage("", "nonexistent-queue", msg)
	assert.NoError(t, err)
}

func TestPublishMessageMandatoryRouted(t *testing.T) {
	br, cleanup := createTestStorageBroker(t)
	defer cleanup()

	_, err := br.DeclareQueue("routed-queue", false, false, false, nil)
	require.NoError(t, err)

	msg := &protocol.Message{
		Body:       []byte("test"),
		Exchange:   "",
		RoutingKey: "routed-queue",
		Mandatory:  true,
	}
	err = br.PublishMessage("", "routed-queue", msg)
	assert.NoError(t, err)
}

func TestPublishMessageMandatoryNoRouteOnNamedExchange(t *testing.T) {
	br, cleanup := createTestStorageBroker(t)
	defer cleanup()

	err := br.DeclareExchange("test.direct", "direct", false, false, false, nil)
	require.NoError(t, err)

	msg := &protocol.Message{
		Body:       []byte("test"),
		Exchange:   "test.direct",
		RoutingKey: "unmatched-key",
		Mandatory:  true,
	}
	err = br.PublishMessage("test.direct", "unmatched-key", msg)
	assert.ErrorIs(t, err, broker.ErrNoRoute)
}

// ============================================================================
// RequeueAllForConsumer — broker tests
// ============================================================================

func TestRequeueAllForConsumer(t *testing.T) {
	br, cleanup := createTestStorageBroker(t)
	defer cleanup()

	_, err := br.DeclareQueue("test-requeue-q", false, false, false, nil)
	require.NoError(t, err)

	consumer := &protocol.Consumer{
		Tag:           "test-requeue-tag",
		Queue:         "test-requeue-q",
		NoAck:         false,
		PrefetchCount: 10,
		Messages:      make(chan *protocol.Delivery, 100),
		Cancel:        make(chan struct{}),
	}

	err = br.RegisterConsumer("test-requeue-q", "test-requeue-tag", consumer)
	require.NoError(t, err)
	defer br.UnregisterConsumer("test-requeue-tag")

	msg := &protocol.Message{
		Body:       []byte("requeue-me"),
		Exchange:   "",
		RoutingKey: "test-requeue-q",
	}
	err = br.PublishMessage("", "test-requeue-q", msg)
	require.NoError(t, err)

	select {
	case delivery := <-consumer.Messages:
		assert.Equal(t, "requeue-me", string(delivery.Message.Body))
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for initial delivery")
	}

	err = br.RequeueAllForConsumer("test-requeue-tag")
	require.NoError(t, err)

	select {
	case delivery := <-consumer.Messages:
		assert.Equal(t, "requeue-me", string(delivery.Message.Body))
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for redelivery after RequeueAllForConsumer")
	}
}

func TestRequeueAllForConsumerNotFound(t *testing.T) {
	br, cleanup := createTestStorageBroker(t)
	defer cleanup()

	err := br.RequeueAllForConsumer("nonexistent-consumer")
	assert.NoError(t, err)
}

func TestRequeueAllForConsumerMultipleMessages(t *testing.T) {
	br, cleanup := createTestStorageBroker(t)
	defer cleanup()

	_, err := br.DeclareQueue("test-multi-requeue-q", false, false, false, nil)
	require.NoError(t, err)

	consumer := &protocol.Consumer{
		Tag:           "test-multi-requeue-tag",
		Queue:         "test-multi-requeue-q",
		NoAck:         false,
		PrefetchCount: 10,
		Messages:      make(chan *protocol.Delivery, 100),
		Cancel:        make(chan struct{}),
	}

	err = br.RegisterConsumer("test-multi-requeue-q", "test-multi-requeue-tag", consumer)
	require.NoError(t, err)
	defer br.UnregisterConsumer("test-multi-requeue-tag")

	for i := 0; i < 3; i++ {
		msg := &protocol.Message{
			Body:       []byte("msg"),
			Exchange:   "",
			RoutingKey: "test-multi-requeue-q",
		}
		err = br.PublishMessage("", "test-multi-requeue-q", msg)
		require.NoError(t, err)
	}

	receivedCount := 0
	for i := 0; i < 3; i++ {
		select {
		case <-consumer.Messages:
			receivedCount++
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for delivery %d", i+1)
		}
	}
	assert.Equal(t, 3, receivedCount)

	err = br.RequeueAllForConsumer("test-multi-requeue-tag")
	require.NoError(t, err)

	redeliveredCount := 0
	for i := 0; i < 3; i++ {
		select {
		case <-consumer.Messages:
			redeliveredCount++
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for redelivery %d", i+1)
		}
	}
	assert.Equal(t, 3, redeliveredCount)
}

// ============================================================================
// sendBasicReturn — server frame tests
// ============================================================================

func TestSendBasicReturn(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	conn := protocol.NewConnection(serverConn)
	srv := makeTestServer()

	var received bytes.Buffer
	drainDone := make(chan struct{})
	drainConn(clientConn, &received, drainDone)

	msg := &protocol.Message{
		Body:         []byte("returned-body"),
		Exchange:     "amq.direct",
		RoutingKey:   "test.key",
		ContentType:  "text/plain",
		DeliveryMode: 2,
	}

	err := srv.sendBasicReturn(conn, 1, 312, "NO_ROUTE", "amq.direct", "test.key", msg)
	require.NoError(t, err)

	clientConn.Close()
	<-drainDone

	frames := parseFrames(t, received.Bytes())
	require.Len(t, frames, 3, "expected method + header + body frames")

	classID, methodID, payload := parseMethodFrame(t, frames[0])
	assert.Equal(t, uint16(60), classID)
	assert.Equal(t, uint16(50), methodID)

	returnMethod := &protocol.BasicReturnMethod{}
	err = returnMethod.Deserialize(payload)
	require.NoError(t, err)
	assert.Equal(t, uint16(312), returnMethod.ReplyCode)
	assert.Equal(t, "NO_ROUTE", returnMethod.ReplyText)
	assert.Equal(t, "amq.direct", returnMethod.Exchange)
	assert.Equal(t, "test.key", returnMethod.RoutingKey)

	assert.Equal(t, byte(protocol.FrameHeader), frames[1].Type)
	assert.Equal(t, uint16(1), frames[1].Channel)

	assert.Equal(t, byte(protocol.FrameBody), frames[2].Type)
	assert.Equal(t, uint16(1), frames[2].Channel)
	assert.Equal(t, []byte("returned-body"), frames[2].Payload)
}

func TestSendBasicReturnEmptyBody(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	conn := protocol.NewConnection(serverConn)
	srv := makeTestServer()

	var received bytes.Buffer
	drainDone := make(chan struct{})
	drainConn(clientConn, &received, drainDone)

	msg := &protocol.Message{
		Body:       nil,
		Exchange:   "",
		RoutingKey: "test",
	}

	err := srv.sendBasicReturn(conn, 1, 312, "NO_ROUTE", "", "test", msg)
	require.NoError(t, err)

	clientConn.Close()
	<-drainDone

	frames := parseFrames(t, received.Bytes())
	require.Len(t, frames, 2, "expected method + header frames for empty body")

	classID, methodID, _ := parseMethodFrame(t, frames[0])
	assert.Equal(t, uint16(60), classID)
	assert.Equal(t, uint16(50), methodID)

	assert.Equal(t, byte(protocol.FrameHeader), frames[1].Type)
}

// ============================================================================
// sendBasicRecoverOK — server frame tests
// ============================================================================

func TestSendBasicRecoverOK(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	conn := protocol.NewConnection(serverConn)
	srv := makeTestServer()

	var received bytes.Buffer
	drainDone := make(chan struct{})
	drainConn(clientConn, &received, drainDone)

	err := srv.sendBasicRecoverOK(conn, 1)
	require.NoError(t, err)

	clientConn.Close()
	<-drainDone

	frames := parseFrames(t, received.Bytes())
	require.Len(t, frames, 1, "expected exactly 1 frame")

	classID, methodID, payload := parseMethodFrame(t, frames[0])
	assert.Equal(t, uint16(60), classID)
	assert.Equal(t, uint16(111), methodID)
	assert.Empty(t, payload, "basic.recover-ok has no fields")
}

// ============================================================================
// processCompleteMessage — mandatory return integration tests
// ============================================================================

func TestProcessCompleteMessageMandatoryReturn(t *testing.T) {
	srv := newTestServer(t)

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	conn := protocol.NewConnection(serverConn)

	var received bytes.Buffer
	drainDone := make(chan struct{})
	drainConn(clientConn, &received, drainDone)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	body := []byte("test-body")
	pendingMsg := &protocol.PendingMessage{
		Method: &protocol.BasicPublishMethod{
			Exchange:   "",
			RoutingKey: "nonexistent-queue-mandatory",
			Mandatory:  true,
		},
		Header: &protocol.ContentHeader{
			ClassID:     60,
			BodySize:    uint64(len(body)),
			ContentType: "text/plain",
		},
		Body:     body,
		Received: uint64(len(body)),
	}

	err := srv.processCompleteMessage(conn, 1, pendingMsg)
	require.NoError(t, err)

	clientConn.Close()
	<-drainDone

	frames := parseFrames(t, received.Bytes())
	require.Len(t, frames, 3, "expected method + header + body frames for basic.return")

	classID, methodID, payload := parseMethodFrame(t, frames[0])
	assert.Equal(t, uint16(60), classID)
	assert.Equal(t, uint16(50), methodID)

	returnMethod := &protocol.BasicReturnMethod{}
	err = returnMethod.Deserialize(payload)
	require.NoError(t, err)
	assert.Equal(t, uint16(312), returnMethod.ReplyCode)
	assert.Equal(t, "NO_ROUTE", returnMethod.ReplyText)
	assert.Equal(t, "", returnMethod.Exchange)
	assert.Equal(t, "nonexistent-queue-mandatory", returnMethod.RoutingKey)

	assert.Equal(t, byte(protocol.FrameHeader), frames[1].Type)

	assert.Equal(t, byte(protocol.FrameBody), frames[2].Type)
	assert.Equal(t, body, frames[2].Payload)
}

func TestProcessCompleteMessageNonMandatoryNoReturn(t *testing.T) {
	srv := newTestServer(t)

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	conn := protocol.NewConnection(serverConn)

	var received bytes.Buffer
	drainDone := make(chan struct{})
	drainConn(clientConn, &received, drainDone)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	body := []byte("test-body")
	pendingMsg := &protocol.PendingMessage{
		Method: &protocol.BasicPublishMethod{
			Exchange:   "",
			RoutingKey: "nonexistent-queue-nonmandatory",
			Mandatory:  false,
		},
		Header: &protocol.ContentHeader{
			ClassID:  60,
			BodySize: uint64(len(body)),
		},
		Body:     body,
		Received: uint64(len(body)),
	}

	err := srv.processCompleteMessage(conn, 1, pendingMsg)
	require.NoError(t, err)

	clientConn.Close()
	<-drainDone

	frames := parseFrames(t, received.Bytes())
	assert.Empty(t, frames, "no frames should be sent for non-mandatory unroutable message")
}

// ============================================================================
// handleBasicRecover — server handler tests
// ============================================================================

func TestHandleBasicRecoverSendsRecoverOK(t *testing.T) {
	srv := newTestServer(t)

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	conn := protocol.NewConnection(serverConn)

	var received bytes.Buffer
	drainDone := make(chan struct{})
	drainConn(clientConn, &received, drainDone)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	recoverMethod := &protocol.BasicRecoverMethod{Requeue: true}
	payload, err := recoverMethod.Serialize()
	require.NoError(t, err)

	err = srv.handleBasicRecover(conn, 1, payload)
	require.NoError(t, err)

	clientConn.Close()
	<-drainDone

	frames := parseFrames(t, received.Bytes())
	require.Len(t, frames, 1, "expected exactly 1 frame (basic.recover-ok)")

	classID, methodID, methodPayload := parseMethodFrame(t, frames[0])
	assert.Equal(t, uint16(60), classID)
	assert.Equal(t, uint16(111), methodID)
	assert.Empty(t, methodPayload)
}

func TestHandleBasicRecoverRequeuesMessages(t *testing.T) {
	srv := newTestServer(t)

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	conn := protocol.NewConnection(serverConn)

	var received bytes.Buffer
	drainDone := make(chan struct{})
	drainConn(clientConn, &received, drainDone)

	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)

	_, err := srv.Broker.DeclareQueue("recover-test-q", false, false, false, nil)
	require.NoError(t, err)

	consumer := &protocol.Consumer{
		Tag:           "recover-test-tag",
		Queue:         "recover-test-q",
		NoAck:         false,
		PrefetchCount: 10,
		Messages:      make(chan *protocol.Delivery, 100),
		Cancel:        make(chan struct{}),
	}
	ch.Consumers["recover-test-tag"] = consumer
	conn.ConsumersDirty.Store(true)

	err = srv.Broker.RegisterConsumer("recover-test-q", "recover-test-tag", consumer)
	require.NoError(t, err)
	defer srv.Broker.UnregisterConsumer("recover-test-tag")

	msg := &protocol.Message{
		Body:       []byte("recover-me"),
		Exchange:   "",
		RoutingKey: "recover-test-q",
	}
	err = srv.Broker.PublishMessage("", "recover-test-q", msg)
	require.NoError(t, err)

	select {
	case delivery := <-consumer.Messages:
		assert.Equal(t, "recover-me", string(delivery.Message.Body))
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for initial delivery")
	}

	recoverMethod := &protocol.BasicRecoverMethod{Requeue: true}
	payload, err := recoverMethod.Serialize()
	require.NoError(t, err)

	err = srv.handleBasicRecover(conn, 1, payload)
	require.NoError(t, err)

	select {
	case delivery := <-consumer.Messages:
		assert.Equal(t, "recover-me", string(delivery.Message.Body))
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for redelivery after basic.recover")
	}

	clientConn.Close()
	<-drainDone

	frames := parseFrames(t, received.Bytes())
	require.Len(t, frames, 1, "expected exactly 1 frame (basic.recover-ok)")

	_, methodID, _ := parseMethodFrame(t, frames[0])
	assert.Equal(t, uint16(111), methodID)
}

func TestHandleBasicRecoverNoChannel(t *testing.T) {
	srv := newTestServer(t)

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	conn := protocol.NewConnection(serverConn)

	recoverMethod := &protocol.BasicRecoverMethod{Requeue: false}
	payload, err := recoverMethod.Serialize()
	require.NoError(t, err)

	err = srv.handleBasicRecover(conn, 99, payload)
	assert.Error(t, err)
}
