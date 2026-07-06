package server

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// channel.flow / channel.flow-ok (AMQP 0.9.1 class 20, methods 20 & 21)
//
// Spec:
//   - channel.flow{active} asks the peer to start/stop sending content frames
//   - The receiver MUST respond with channel.flow-ok{active} (current state)
//   - channel.flow-ok{active} acknowledges a flow request (no further response)
// ============================================================================

// --- Protocol layer: Deserialize ---

func TestChannelFlowMethod_Deserialize(t *testing.T) {
	t.Run("active true", func(t *testing.T) {
		m := &protocol.ChannelFlowMethod{}
		require.NoError(t, m.Deserialize([]byte{1}))
		assert.True(t, m.Active)
	})

	t.Run("active false", func(t *testing.T) {
		m := &protocol.ChannelFlowMethod{}
		require.NoError(t, m.Deserialize([]byte{0}))
		assert.False(t, m.Active)
	})

	t.Run("empty payload errors", func(t *testing.T) {
		m := &protocol.ChannelFlowMethod{}
		assert.Error(t, m.Deserialize([]byte{}))
	})
}

func TestChannelFlowOKMethod_Deserialize(t *testing.T) {
	t.Run("active true", func(t *testing.T) {
		m := &protocol.ChannelFlowOKMethod{}
		require.NoError(t, m.Deserialize([]byte{1}))
		assert.True(t, m.Active)
	})

	t.Run("active false", func(t *testing.T) {
		m := &protocol.ChannelFlowOKMethod{}
		require.NoError(t, m.Deserialize([]byte{0}))
		assert.False(t, m.Active)
	})

	t.Run("empty payload errors", func(t *testing.T) {
		m := &protocol.ChannelFlowOKMethod{}
		assert.Error(t, m.Deserialize([]byte{}))
	})
}

func TestChannelFlowMethod_SerializeRoundTrip(t *testing.T) {
	for _, active := range []bool{true, false} {
		orig := &protocol.ChannelFlowMethod{Active: active}
		data, err := orig.Serialize()
		require.NoError(t, err)

		parsed := &protocol.ChannelFlowMethod{}
		require.NoError(t, parsed.Deserialize(data))
		assert.Equal(t, active, parsed.Active)
	}
}

func TestChannelFlowOKMethod_SerializeRoundTrip(t *testing.T) {
	for _, active := range []bool{true, false} {
		orig := &protocol.ChannelFlowOKMethod{Active: active}
		data, err := orig.Serialize()
		require.NoError(t, err)

		parsed := &protocol.ChannelFlowOKMethod{}
		require.NoError(t, parsed.Deserialize(data))
		assert.Equal(t, active, parsed.Active)
	}
}

// --- Channel state defaults ---

func TestNewChannel_FlowDefaultsActive(t *testing.T) {
	ch := protocol.NewChannel(1, nil)
	assert.True(t, ch.FlowActive.Load(), "FlowActive must default to true (flow active by default)")
	assert.NotNil(t, ch.FlowWake, "FlowWake channel must be initialized")
}

// --- Handler: channel.flow ---

// setupFlowTest creates a server, pipe connection, and a registered channel.
func setupFlowTest(t *testing.T) (*Server, *protocol.Connection, net.Conn, *protocol.Channel) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	conn := protocol.NewConnection(serverConn)
	srv := makeTestServer()
	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), ch)
	return srv, conn, clientConn, ch
}

// readFlowOK reads a method frame from the client side and verifies it is
// channel.flow-ok (class 20, method 21), returning the parsed method.
func readFlowOK(t *testing.T, clientConn net.Conn) *protocol.ChannelFlowOKMethod {
	t.Helper()
	frame, err := protocol.ReadFrame(clientConn)
	require.NoError(t, err, "expected channel.flow-ok response frame")
	require.Equal(t, byte(protocol.FrameMethod), frame.Type, "expected method frame")
	require.Equal(t, uint16(1), frame.Channel, "expected channel 1")
	require.GreaterOrEqual(t, len(frame.Payload), 5, "flow-ok payload too short")
	classID := binary.BigEndian.Uint16(frame.Payload[0:2])
	methodID := binary.BigEndian.Uint16(frame.Payload[2:4])
	require.Equal(t, uint16(20), classID, "expected channel class (20)")
	require.Equal(t, uint16(21), methodID, "expected channel.flow-ok method (21)")
	m := &protocol.ChannelFlowOKMethod{}
	require.NoError(t, m.Deserialize(frame.Payload[4:]))
	return m
}

func TestChannelFlowHandler_DeactivatesAndResponds(t *testing.T) {
	srv, conn, clientConn, ch := setupFlowTest(t)
	defer clientConn.Close()
	defer conn.Conn.Close()

	payload, err := (&protocol.ChannelFlowMethod{Active: false}).Serialize()
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- srv.processChannelSpecificMethod(conn, 1, protocol.ChannelFlow, payload) }()

	flowOK := readFlowOK(t, clientConn)
	require.NoError(t, <-done)

	assert.False(t, flowOK.Active, "flow-ok must echo the requested active state (false)")
	assert.False(t, ch.FlowActive.Load(), "channel FlowActive must be false after channel.flow{false}")

	select {
	case <-ch.FlowWake:
		t.Fatal("FlowWake must not be signaled when deactivating flow")
	default:
	}
}

func TestChannelFlowHandler_ActivatesAndSignalsWake(t *testing.T) {
	srv, conn, clientConn, ch := setupFlowTest(t)
	defer clientConn.Close()
	defer conn.Conn.Close()

	ch.FlowActive.Store(false)

	payload, err := (&protocol.ChannelFlowMethod{Active: true}).Serialize()
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- srv.processChannelSpecificMethod(conn, 1, protocol.ChannelFlow, payload) }()

	flowOK := readFlowOK(t, clientConn)
	require.NoError(t, <-done)

	assert.True(t, flowOK.Active, "flow-ok must echo the requested active state (true)")
	assert.True(t, ch.FlowActive.Load(), "channel FlowActive must be true after channel.flow{true}")

	select {
	case <-ch.FlowWake:
	default:
		t.Fatal("FlowWake must be signaled when activating flow to wake parked forwarders")
	}
}

// --- Handler: channel.flow-ok ---

// A client's channel.flow-ok acknowledges a server-initiated channel.flow about
// the CLIENT's publish direction. It must NOT be written into FlowActive, which
// gates the SERVER -> client delivery direction and is controlled only by
// client-initiated channel.flow. (Storing the echo used to wedge the broker:
// the reader-overflow flow(false) came back as flow-ok(false) and parked the
// delivery forwarders, halting the very acks that drain the overflow.)
func TestChannelFlowOKHandler_DoesNotTouchFlowActiveNoResponse(t *testing.T) {
	srv, conn, clientConn, ch := setupFlowTest(t)
	defer clientConn.Close()
	defer conn.Conn.Close()

	require.True(t, ch.FlowActive.Load(), "precondition: flow active")

	payload, err := (&protocol.ChannelFlowOKMethod{Active: false}).Serialize()
	require.NoError(t, err)

	require.NoError(t, srv.processChannelSpecificMethod(conn, 1, protocol.ChannelFlowOK, payload))
	assert.True(t, ch.FlowActive.Load(),
		"FlowActive (server->client delivery gate) must not change on the client's flow-ok echo")

	require.NoError(t, clientConn.SetReadDeadline(time.Now().Add(100*time.Millisecond)))
	buf := make([]byte, 128)
	if _, err := clientConn.Read(buf); err == nil {
		t.Fatal("channel.flow-ok must not elicit a response")
	}
	require.NoError(t, clientConn.SetReadDeadline(time.Time{}))
}

// --- Handler: channel.close signals FlowWake ---

func TestChannelClose_SignalsFlowWake(t *testing.T) {
	srv, conn, clientConn, ch := setupFlowTest(t)
	defer clientConn.Close()
	defer conn.Conn.Close()

	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		buf := make([]byte, 4096)
		for {
			if _, err := clientConn.Read(buf); err != nil {
				return
			}
		}
	}()

	payload, err := (&protocol.ChannelCloseMethod{}).Serialize()
	require.NoError(t, err)

	require.NoError(t, srv.processChannelSpecificMethod(conn, 1, protocol.ChannelClose, payload))

	select {
	case <-ch.FlowWake:
	default:
		t.Fatal("channel close must signal FlowWake to unblock parked forwarders")
	}

	clientConn.Close()
	<-readerDone
}

// --- Delivery path: forwardConsumerMessages respects FlowActive ---

func TestForwardConsumerMessages_BlocksWhenFlowInactive(t *testing.T) {
	ch := protocol.NewChannel(1, nil)
	ch.FlowActive.Store(false)

	msgChan := make(chan *protocol.Delivery, 1)
	fanIn := make(chan *protocol.Delivery, 1)
	stop := make(chan struct{})

	msgChan <- &protocol.Delivery{DeliveryTag: 1, ConsumerTag: "c1"}
	go forwardConsumerMessages(ch, msgChan, fanIn, stop)

	select {
	case <-fanIn:
		t.Fatal("forwarder must not deliver when FlowActive is false")
	case <-time.After(100 * time.Millisecond):
	}

	close(stop)
}

func TestForwardConsumerMessages_ResumesOnFlowWake(t *testing.T) {
	ch := protocol.NewChannel(1, nil)
	ch.FlowActive.Store(false)

	msgChan := make(chan *protocol.Delivery, 1)
	fanIn := make(chan *protocol.Delivery, 1)
	stop := make(chan struct{})

	msgChan <- &protocol.Delivery{DeliveryTag: 1, ConsumerTag: "c1"}
	go forwardConsumerMessages(ch, msgChan, fanIn, stop)

	select {
	case <-fanIn:
		t.Fatal("forwarder must not deliver when FlowActive is false")
	case <-time.After(100 * time.Millisecond):
	}

	ch.FlowActive.Store(true)
	select {
	case ch.FlowWake <- struct{}{}:
	default:
	}

	select {
	case d := <-fanIn:
		assert.Equal(t, uint64(1), d.DeliveryTag)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("forwarder must deliver after flow resumes and FlowWake is signaled")
	}

	close(stop)
}

func TestForwardConsumerMessages_UnblocksOnStop(t *testing.T) {
	ch := protocol.NewChannel(1, nil)
	ch.FlowActive.Store(false)

	msgChan := make(chan *protocol.Delivery, 1)
	fanIn := make(chan *protocol.Delivery, 1)
	stop := make(chan struct{})

	msgChan <- &protocol.Delivery{DeliveryTag: 1, ConsumerTag: "c1"}

	done := make(chan struct{})
	go func() {
		defer close(done)
		forwardConsumerMessages(ch, msgChan, fanIn, stop)
	}()

	select {
	case <-fanIn:
		t.Fatal("forwarder must not deliver when FlowActive is false")
	case <-time.After(100 * time.Millisecond):
	}

	close(stop)

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("parked forwarder must exit when stop is closed (channel close path)")
	}
}
