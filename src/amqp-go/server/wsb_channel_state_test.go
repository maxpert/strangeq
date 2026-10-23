package server

import (
	"errors"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWSB_ChannelOpenOnChannel0_Gets503 (raw): channel.open is a channel-class
// method and is invalid on channel 0 → 503.
func TestWSB_ChannelOpenOnChannel0_Gets503(t *testing.T) {
	_, addr := startWSBServer(t, nil, nil)

	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 0, 0)

	r.send(0, 20, 10, []byte{})
	closeM := r.expectConnectionClose(503)
	assert.Equal(t, uint16(20), closeM.ClassID)
	assert.Equal(t, uint16(10), closeM.MethodID)
	r.send(0, 10, 51, []byte{})
	r.expectEOF()
}

// TestWSB_DuplicateChannelOpen_Gets504 (raw): a second channel.open on a live
// channel is a hard 504 (AMQP 0-9-1: "second 'channel.open' seen").
func TestWSB_DuplicateChannelOpen_Gets504(t *testing.T) {
	_, addr := startWSBServer(t, nil, nil)

	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 0, 0)
	r.openChannel(1)

	r.send(1, 20, 10, []byte{}) // duplicate open
	closeM := r.expectConnectionClose(504)
	assert.Equal(t, uint16(20), closeM.ClassID)
	assert.Equal(t, uint16(10), closeM.MethodID)
	r.send(0, 10, 51, []byte{})
	r.expectEOF()
}

// TestWSB_DuplicateChannelOpen_NoOverwrite (unit): a duplicate open must NOT
// overwrite the live channel — its consumers would leak in the broker.
func TestWSB_DuplicateChannelOpen_NoOverwrite(t *testing.T) {
	srv := newAckTestServer(newAckTestBroker())
	conn, _ := newPipeConnWithFrames(t)
	conn.MaxChannels = 16

	require.NoError(t, srv.processChannelSpecificMethod(conn, 1, protocol.ChannelOpen, []byte{}))
	first, ok := conn.Channels.Load(uint16(1))
	require.True(t, ok)

	// Install a consumer on the live channel.
	ch := first.(*protocol.Channel)
	ch.Mutex.Lock()
	ch.Consumers["c1"] = &protocol.Consumer{ID: "cid-1", Tag: "c1", Queue: "q"}
	ch.Mutex.Unlock()

	err := srv.processChannelSpecificMethod(conn, 1, protocol.ChannelOpen, []byte{})
	require.Error(t, err)
	var cne *ConnectionError
	require.True(t, errors.As(err, &cne), "want *ConnectionError, got %T", err)
	assert.Equal(t, uint16(504), cne.Code)

	assert.Equal(t, int32(1), conn.ChannelCount.Load(), "count must not be inflated")
	after, ok := conn.Channels.Load(uint16(1))
	require.True(t, ok)
	assert.Same(t, first, after, "the live channel must not be overwritten")
	ch.Mutex.RLock()
	_, present := ch.Consumers["c1"]
	ch.Mutex.RUnlock()
	assert.True(t, present, "the consumer must still be installed")
}

// TestWSB_ChannelIDAboveChannelMax_Gets530 (raw): a channel ID above the
// negotiated channel_max is a hard 530.
func TestWSB_ChannelIDAboveChannelMax_Gets530(t *testing.T) {
	_, addr := startWSBServer(t, nil, nil)

	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 2, 0) // channel_max = 2

	r.send(3, 20, 10, []byte{}) // open channel 3 > 2
	r.expectConnectionClose(530)
	r.send(0, 10, 51, []byte{})
	r.expectEOF()
}

// TestWSB_MethodOnUnopenedChannel_Gets504 (raw): any non-channel.open method
// on a never-opened channel is a hard 504.
func TestWSB_MethodOnUnopenedChannel_Gets504(t *testing.T) {
	_, addr := startWSBServer(t, nil, nil)

	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 0, 0)

	decl := &protocol.QueueDeclareMethod{Queue: "q7"}
	data, err := decl.Serialize()
	require.NoError(t, err)
	r.send(7, 50, 10, data)

	closeM := r.expectConnectionClose(504)
	assert.Equal(t, uint16(50), closeM.ClassID)
	assert.Equal(t, uint16(10), closeM.MethodID)
	r.send(0, 10, 51, []byte{})
	r.expectEOF()
}

// TestWSB_BasicPublishOnUnopenedChannel_Gets504 (raw).
func TestWSB_BasicPublishOnUnopenedChannel_Gets504(t *testing.T) {
	_, addr := startWSBServer(t, nil, nil)

	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 0, 0)

	pub := &protocol.BasicPublishMethod{Exchange: "", RoutingKey: "q"}
	data, err := pub.Serialize()
	require.NoError(t, err)
	r.send(7, 60, 40, data)

	r.expectConnectionClose(504)
	r.send(0, 10, 51, []byte{})
	r.expectEOF()
}
