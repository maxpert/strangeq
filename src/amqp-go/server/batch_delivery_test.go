package server

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type countingConn struct {
	net.Conn
	mu           sync.Mutex
	writeCount   int
	bytesWritten int
	writeErr     error
}

func (c *countingConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	c.writeCount++
	c.bytesWritten += len(p)
	err := c.writeErr
	c.mu.Unlock()
	if err != nil {
		return 0, err
	}
	return c.Conn.Write(p)
}

func (c *countingConn) getWriteCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writeCount
}

type errorConn struct {
	net.Conn
	writeErr error
}

func (c *errorConn) Write(p []byte) (int, error) {
	return 0, c.writeErr
}

func makeTestServer() *Server {
	return &Server{
		Log:              zap.NewNop(),
		Config:           config.DefaultConfig(),
		MetricsCollector: &NoOpMetricsCollector{},
	}
}

func makeDelivery(tag uint64, body []byte) *protocol.Delivery {
	return &protocol.Delivery{
		Message: &protocol.Message{
			DeliveryTag:  tag,
			Body:         body,
			Exchange:     "test-exchange",
			RoutingKey:   "test.key",
			ContentType:  "text/plain",
			DeliveryMode: 2,
		},
		DeliveryTag: tag,
		Exchange:    "test-exchange",
		RoutingKey:  "test.key",
		ConsumerTag: "test-consumer",
	}
}

func drainConn(conn net.Conn, buf *bytes.Buffer, done chan struct{}) {
	go func() {
		defer close(done)
		io.Copy(buf, conn)
	}()
}

func parseFrames(t *testing.T, data []byte) []*protocol.Frame {
	t.Helper()
	var frames []*protocol.Frame
	offset := 0
	for offset < len(data) {
		if offset+7 > len(data) {
			t.Fatalf("incomplete frame header at offset %d", offset)
		}
		frameType := data[offset]
		channel := binary.BigEndian.Uint16(data[offset+1 : offset+3])
		size := binary.BigEndian.Uint32(data[offset+3 : offset+7])
		if offset+7+int(size)+1 > len(data) {
			t.Fatalf("incomplete frame at offset %d: need %d bytes, have %d",
				offset, 7+int(size)+1, len(data)-offset)
		}
		payload := data[offset+7 : offset+7+int(size)]
		endByte := data[offset+7+int(size)]
		if endByte != protocol.FrameEnd {
			t.Fatalf("invalid frame end byte at offset %d: got 0x%02X, expected 0x%02X",
				offset, endByte, protocol.FrameEnd)
		}
		frames = append(frames, &protocol.Frame{
			Type:    frameType,
			Channel: channel,
			Size:    size,
			Payload: payload,
		})
		offset += 7 + int(size) + 1
	}
	return frames
}

func TestSendBatchedDeliveries_SingleWriteCall(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	counter := &countingConn{Conn: serverConn}
	conn := protocol.NewConnection(counter)
	srv := makeTestServer()

	var received bytes.Buffer
	drainDone := make(chan struct{})
	drainConn(clientConn, &received, drainDone)

	deliveries := make([]*protocol.Delivery, 5)
	for i := 0; i < 5; i++ {
		deliveries[i] = makeDelivery(uint64(i+1), []byte("message-body"))
	}

	err := srv.sendBatchedDeliveries(conn, 1, "test-consumer", deliveries)
	assert.NoError(t, err)

	clientConn.Close()
	<-drainDone

	assert.Equal(t, 1, counter.getWriteCount(), "expected exactly 1 Write call for 5 deliveries")
}

func TestSendBatchedDeliveries_FramesInOrder(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	conn := protocol.NewConnection(serverConn)
	srv := makeTestServer()

	var received bytes.Buffer
	drainDone := make(chan struct{})
	drainConn(clientConn, &received, drainDone)

	n := 3
	deliveries := make([]*protocol.Delivery, n)
	for i := 0; i < n; i++ {
		deliveries[i] = makeDelivery(uint64(i+1), []byte("body"))
	}

	err := srv.sendBatchedDeliveries(conn, 1, "test-consumer", deliveries)
	require.NoError(t, err)

	clientConn.Close()
	<-drainDone

	frames := parseFrames(t, received.Bytes())

	framesPerDelivery := 3
	require.Equal(t, n*framesPerDelivery, len(frames), "expected %d frames for %d deliveries",
		n*framesPerDelivery, n)

	for i := 0; i < n; i++ {
		base := i * framesPerDelivery
		assert.Equal(t, byte(protocol.FrameMethod), frames[base].Type, "delivery %d: expected method frame", i)
		assert.Equal(t, byte(protocol.FrameHeader), frames[base+1].Type, "delivery %d: expected header frame", i)
		assert.Equal(t, byte(protocol.FrameBody), frames[base+2].Type, "delivery %d: expected body frame", i)
		assert.Equal(t, uint16(1), frames[base].Channel, "delivery %d: expected channel 1", i)
	}
}

func TestSendBatchedDeliveries_100DeliveriesSingleWriteMutex(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	counter := &countingConn{Conn: serverConn}
	conn := protocol.NewConnection(counter)
	srv := makeTestServer()

	var received bytes.Buffer
	drainDone := make(chan struct{})
	drainConn(clientConn, &received, drainDone)

	deliveries := make([]*protocol.Delivery, 100)
	for i := 0; i < 100; i++ {
		deliveries[i] = makeDelivery(uint64(i+1), []byte("x"))
	}

	err := srv.sendBatchedDeliveries(conn, 1, "test-consumer", deliveries)
	assert.NoError(t, err)

	clientConn.Close()
	<-drainDone

	assert.Equal(t, 1, counter.getWriteCount(),
		"expected exactly 1 Write call for 100 deliveries (single mutex acquisition)")
}

func TestSendBatchedDeliveries_WriteError(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	testErr := errors.New("simulated write failure")
	errConn := &errorConn{Conn: serverConn, writeErr: testErr}
	conn := protocol.NewConnection(errConn)
	srv := makeTestServer()

	deliveries := []*protocol.Delivery{makeDelivery(1, []byte("body"))}

	err := srv.sendBatchedDeliveries(conn, 1, "test-consumer", deliveries)
	assert.Error(t, err)
}

func TestSendBatchedDeliveries_SingleDelivery(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	counter := &countingConn{Conn: serverConn}
	conn := protocol.NewConnection(counter)
	srv := makeTestServer()

	var received bytes.Buffer
	drainDone := make(chan struct{})
	drainConn(clientConn, &received, drainDone)

	deliveries := []*protocol.Delivery{makeDelivery(1, []byte("single"))}

	err := srv.sendBatchedDeliveries(conn, 1, "test-consumer", deliveries)
	require.NoError(t, err)

	clientConn.Close()
	<-drainDone

	assert.Equal(t, 1, counter.getWriteCount(), "expected exactly 1 Write call for single delivery")

	frames := parseFrames(t, received.Bytes())
	require.Equal(t, 3, len(frames), "expected 3 frames for 1 delivery (method + header + body)")
	assert.Equal(t, byte(protocol.FrameMethod), frames[0].Type)
	assert.Equal(t, byte(protocol.FrameHeader), frames[1].Type)
	assert.Equal(t, byte(protocol.FrameBody), frames[2].Type)
}

func TestSendBatchedDeliveries_EmptyBatch(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	counter := &countingConn{Conn: serverConn}
	conn := protocol.NewConnection(counter)
	srv := makeTestServer()

	err := srv.sendBatchedDeliveries(conn, 1, "test-consumer", nil)
	assert.NoError(t, err)
	assert.Equal(t, 0, counter.getWriteCount(), "expected 0 Write calls for empty batch")
}

func TestSerializeDelivery_ProducesValidFrames(t *testing.T) {
	srv := makeTestServer()

	msg := &protocol.Message{
		Body:         []byte("hello world"),
		Exchange:     "ex",
		RoutingKey:   "rk",
		ContentType:  "text/plain",
		DeliveryMode: 2,
	}

	data, err := srv.serializeDelivery(1, "ct", 42, false, "ex", "rk", msg)
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	frames := parseFrames(t, data)
	require.Equal(t, 3, len(frames), "expected method + header + body frames")
	assert.Equal(t, byte(protocol.FrameMethod), frames[0].Type)
	assert.Equal(t, byte(protocol.FrameHeader), frames[1].Type)
	assert.Equal(t, byte(protocol.FrameBody), frames[2].Type)
	assert.Equal(t, uint16(1), frames[0].Channel)
}

func TestSerializeDelivery_EmptyBody(t *testing.T) {
	srv := makeTestServer()

	msg := &protocol.Message{
		Body:       nil,
		Exchange:   "ex",
		RoutingKey: "rk",
	}

	data, err := srv.serializeDelivery(1, "ct", 1, false, "ex", "rk", msg)
	require.NoError(t, err)

	frames := parseFrames(t, data)
	require.Equal(t, 2, len(frames), "expected method + header frames for empty body")
	assert.Equal(t, byte(protocol.FrameMethod), frames[0].Type)
	assert.Equal(t, byte(protocol.FrameHeader), frames[1].Type)
}
