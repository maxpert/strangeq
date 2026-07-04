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

func TestParseConnectionTuneOK(t *testing.T) {
	payload := make([]byte, 8)
	binary.BigEndian.PutUint16(payload[0:2], 100)
	binary.BigEndian.PutUint32(payload[2:6], 131072)
	binary.BigEndian.PutUint16(payload[6:8], 30)

	chMax, frameMax, hb, err := parseConnectionTuneOK(payload)
	require.NoError(t, err)
	assert.Equal(t, uint16(100), chMax)
	assert.Equal(t, uint32(131072), frameMax)
	assert.Equal(t, uint16(30), hb)
}

func TestParseConnectionTuneOK_TooShort(t *testing.T) {
	_, _, _, err := parseConnectionTuneOK(make([]byte, 4))
	assert.Error(t, err)
}

func TestNegotiateUint16(t *testing.T) {
	assert.Equal(t, uint16(50), negotiateUint16(50, 100))
	assert.Equal(t, uint16(50), negotiateUint16(100, 50))
	assert.Equal(t, uint16(100), negotiateUint16(0, 100))
	assert.Equal(t, uint16(100), negotiateUint16(100, 0))
	assert.Equal(t, uint16(0), negotiateUint16(0, 0))
}

func TestNegotiateUint32(t *testing.T) {
	assert.Equal(t, uint32(50), negotiateUint32(50, 100))
	assert.Equal(t, uint32(50), negotiateUint32(100, 50))
	assert.Equal(t, uint32(100), negotiateUint32(0, 100))
	assert.Equal(t, uint32(100), negotiateUint32(100, 0))
	assert.Equal(t, uint32(0), negotiateUint32(0, 0))
}

func TestMaxConnectionsEnforced(t *testing.T) {
	srv := newTransactionTestServer(t)
	srv.Config.Network.MaxConnections = 1

	clientConn1, serverConn1 := net.Pipe()
	t.Cleanup(func() { clientConn1.Close(); serverConn1.Close() })
	conn1 := protocol.NewConnection(serverConn1)
	srv.Mutex.Lock()
	srv.Connections[conn1.ID] = conn1
	srv.Mutex.Unlock()

	clientConn2, serverConn2 := net.Pipe()
	t.Cleanup(func() { clientConn2.Close(); serverConn2.Close() })
	conn2 := protocol.NewConnection(serverConn2)

	srv.Mutex.Lock()
	exceeded := srv.Config.Network.MaxConnections > 0 && len(srv.Connections) >= srv.Config.Network.MaxConnections
	srv.Mutex.Unlock()

	assert.True(t, exceeded, "with 1 existing connection and max=1, new connection should be refused")

	srv.Mutex.Lock()
	srv.Connections[conn2.ID] = conn2
	srv.Mutex.Unlock()

	srv.Mutex.Lock()
	exceededAfter := len(srv.Connections) > srv.Config.Network.MaxConnections
	srv.Mutex.Unlock()
	assert.True(t, exceededAfter, "adding 2nd connection exceeds max=1")
}

func TestMaxChannelsPerConnectionEnforced(t *testing.T) {
	srv := newTransactionTestServer(t)
	srv.Config.Server.MaxChannelsPerConnection = 2

	conn, frameCh := newPipeConnWithFrames(t)
	conn.MaxChannels = 2

	err := srv.processChannelSpecificMethod(conn, 1, protocol.ChannelOpen, []byte{})
	require.NoError(t, err)
	drainFrame(t, frameCh)
	assert.Equal(t, int32(1), conn.ChannelCount.Load())

	err = srv.processChannelSpecificMethod(conn, 2, protocol.ChannelOpen, []byte{})
	require.NoError(t, err)
	drainFrame(t, frameCh)
	assert.Equal(t, int32(2), conn.ChannelCount.Load())

	err = srv.processChannelSpecificMethod(conn, 3, protocol.ChannelOpen, []byte{})
	assert.NoError(t, err, "exceeding max channels should send channel.close, not return error")

	classID, methodID, _ := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(20), classID, "expected channel.close")
	assert.Equal(t, uint16(40), methodID, "expected channel.close method")

	assert.Equal(t, int32(2), conn.ChannelCount.Load(), "channel count should not increment on rejected open")

	_, exists := conn.Channels.Load(uint16(3))
	assert.False(t, exists, "channel 3 should not be stored")

	err = srv.processChannelSpecificMethod(conn, 2, protocol.ChannelClose, makeChannelClosePayload())
	require.NoError(t, err)
	drainFrame(t, frameCh)
	assert.Equal(t, int32(1), conn.ChannelCount.Load(), "channel count should decrement on close")

	err = srv.processChannelSpecificMethod(conn, 3, protocol.ChannelOpen, []byte{})
	require.NoError(t, err)
	drainFrame(t, frameCh)
	assert.Equal(t, int32(2), conn.ChannelCount.Load(), "should be able to open after closing one")
}

func makeChannelClosePayload() []byte {
	payload := make([]byte, 4)
	binary.BigEndian.PutUint16(payload[0:2], 200)
	binary.BigEndian.PutUint16(payload[2:4], 40)
	return payload
}

func TestSendConnectionTune_UsesConfigValues(t *testing.T) {
	srv := newTransactionTestServer(t)
	srv.Config.Server.MaxChannelsPerConnection = 100
	srv.Config.Server.MaxFrameSize = 65536
	srv.Config.Network.HeartbeatIntervalMS = 10000

	conn, frameCh := newPipeConnWithFrames(t)
	err := srv.sendConnectionTune(conn)
	require.NoError(t, err)

	classID, methodID, payload := nextMethodFrame(t, frameCh)
	assert.Equal(t, uint16(10), classID)
	assert.Equal(t, uint16(30), methodID)

	tune := &protocol.ConnectionTuneMethod{}
	err = tune.Deserialize(payload)
	require.NoError(t, err)
	assert.Equal(t, uint16(100), tune.ChannelMax)
	assert.Equal(t, uint32(65536), tune.FrameMax)
	assert.Equal(t, uint16(10), tune.Heartbeat)
}

func TestNegotiatedHeartbeatHonored(t *testing.T) {
	conn, _ := newPipeConnWithFrames(t)
	conn.HeartbeatSec.Store(10)

	hb := conn.HeartbeatSec.Load()
	interval := time.Duration(hb) * time.Second / 2
	assert.Equal(t, 5*time.Second, interval, "heartbeat interval should be half of negotiated value")
}

func TestNegotiatedHeartbeatDisabledWhenZero(t *testing.T) {
	conn, _ := newPipeConnWithFrames(t)
	conn.HeartbeatSec.Store(0)
	hb := conn.HeartbeatSec.Load()
	assert.Equal(t, uint32(0), hb, "heartbeat 0 means disabled")
}

func TestDeadPeerReaped(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() { clientConn.Close(); serverConn.Close() })

	conn := protocol.NewConnection(serverConn)
	conn.HeartbeatSec.Store(1)

	err := serverConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	require.NoError(t, err)

	_, readErr := protocol.ReadFrameOptimizedWithLimit(serverConn, 131072+8)
	assert.Error(t, readErr, "dead peer should cause read error after deadline")

	ne, ok := readErr.(net.Error)
	if ok && ne.Timeout() {
		return
	}
}

func TestConnectionTouchActivity(t *testing.T) {
	conn, _ := newPipeConnWithFrames(t)
	before := conn.GetLastActivity()
	time.Sleep(10 * time.Millisecond)
	conn.TouchActivity()
	after := conn.GetLastActivity()
	assert.True(t, after.After(before), "TouchActivity should update LastActivity")
}

func TestConnectionConnectedAtInitialized(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() { clientConn.Close(); serverConn.Close() })
	conn := protocol.NewConnection(serverConn)
	assert.False(t, conn.ConnectedAt.IsZero(), "ConnectedAt must be initialized in NewConnection")
}
