package server

import (
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readDeliveryTag reads one full delivery (basic.deliver + content header +
// body) and returns its wire delivery tag.
func readDeliveryTag(t *testing.T, r *rawAMQP) uint64 {
	t.Helper()
	f := r_readMethod(t, r, 60, 60) // basic.deliver
	tagLen := int(f[0])
	tag := mustUint64(f[1+tagLen : 1+tagLen+8])
	hdr := r.readFrame() // content header
	require.Equal(t, byte(protocol.FrameHeader), hdr.Type)
	bodySize := int(uint64(hdr.Payload[4])<<56 | uint64(hdr.Payload[5])<<48 | uint64(hdr.Payload[6])<<40 |
		uint64(hdr.Payload[7])<<32 | uint64(hdr.Payload[8])<<24 | uint64(hdr.Payload[9])<<16 |
		uint64(hdr.Payload[10])<<8 | uint64(hdr.Payload[11]))
	for bodySize > 0 {
		b := r.readFrame()
		require.Equal(t, byte(protocol.FrameBody), b.Type)
		bodySize -= len(b.Payload)
	}
	return tag
}

// SL-22 (Sol HIGH, closure): a TRANSIENT publish to a queue at its high-water
// mark is synchronously STORED and confirmed PROMPTLY (bounded one-publish
// overshoot, never WaitForCapacity, no drain needed); the returned gate paces
// SUBSEQUENT producer-only reads. The lease is retained through broker+server
// completion (two-party join), then used == 0.
func TestLease_TransientHWMStoredWithGate(t *testing.T) {
	srv, addr, created, released, _ := leaseServer(t, 1<<20, func(cfg *config.AMQPConfig) {
		cfg.Engine.DepthHighWMOverride = 1
	}, nil)

	p := dialRaw(t, addr)
	p.handshake("guest", "guest", 4, 0)
	p.openChannel(1)
	p.declareQueue(1, "sl22-q")
	// Fill to HWM=1.
	leaseSendPublish(t, p, 1, "", "sl22-q", leaseBody(64), false)
	require.Eventually(t, func() bool {
		return srv.Broker.(*StorageBrokerAdapter).broker.GetQueueReadyCount("sl22-q") == 1
	}, 30*time.Second, time.Millisecond)

	// Producer-only confirm-mode publish AT HWM: stored + confirmed PROMPTLY
	// (no consumer drains anything).
	pub := dialRaw(t, addr)
	pub.handshake("guest", "guest", 4, 0)
	pub.openChannel(1)
	leaseConfirmSelect(t, pub, 1)
	leaseSendPublish(t, pub, 1, "", "sl22-q", leaseBody(64), false)
	leaseWaitAck(t, pub, 1, 1) // confirmed without any drain

	// The lease was retained through broker+server completion, then released.
	require.Eventually(t, func() bool {
		return created.Load() == released.Load() && created.Load() > 0 && leaseUsed(srv) == 0
	}, 30*time.Second, time.Millisecond)

	// The returned gate paces the producer-only reader for SUBSEQUENT reads.
	require.Eventually(t, func() bool {
		srv.Mutex.RLock()
		defer srv.Mutex.RUnlock()
		for _, c := range srv.Connections {
			if c.DepthBackpressure.Load() {
				return true
			}
		}
		return false
	}, 30*time.Second, time.Millisecond, "the producer-only reader must be depth-backpressured")

	// A MIXED connection (consumer + publisher) at HWM: its publish stores
	// and confirms, its ACKs flow, it is never 506-closed nor depth-parked.
	m := dialRaw(t, addr)
	m.handshake("guest", "guest", 4, 0)
	m.openChannel(1)
	leaseConfirmSelect(t, m, 1)
	m.consume(1, "sl22-q", "c1")
	leaseSendPublish(t, m, 1, "", "sl22-q", leaseBody(64), false)
	leaseWaitAck(t, m, 1, 1) // the publish confirms without waiting
	// Its consumer acks flow on the same connection.
	tag := readDeliveryTag(t, m)
	ack := &protocol.BasicAckMethod{DeliveryTag: tag}
	data, err := ack.Serialize()
	require.NoError(t, err)
	m.send(1, 60, 80, data)

	srv.Mutex.RLock()
	var mconn *protocol.Connection
	for _, c := range srv.Connections {
		if c.HasActiveConsumer() {
			mconn = c
		}
	}
	srv.Mutex.RUnlock()
	require.NotNil(t, mconn)
	assert.False(t, mconn.Closed.Load(), "the mixed connection is never closed")
	assert.False(t, mconn.DepthBackpressure.Load(), "the mixed connection's reader is never depth-parked")
}

// Sol's exact mixed-connection ACK schedule: a mixed consumer+producer
// connection whose transient publish at HWM stores immediately: the confirm
// for the publish and the consumer's own acks both flow, in order, with no
// wait and no close.
func TestLease_TransientHWMMixedConnAckSchedule(t *testing.T) {
	srv, addr, _, _, _ := leaseServer(t, 1<<20, func(cfg *config.AMQPConfig) {
		cfg.Engine.DepthHighWMOverride = 2
	}, nil)

	p := dialRaw(t, addr)
	p.handshake("guest", "guest", 4, 0)
	p.openChannel(1)
	p.declareQueue(1, "sl22m-q")
	for i := 0; i < 2; i++ {
		leaseSendPublish(t, p, 1, "", "sl22m-q", leaseBody(64), false)
	}
	require.Eventually(t, func() bool {
		return srv.Broker.(*StorageBrokerAdapter).broker.GetQueueReadyCount("sl22m-q") == 2
	}, 30*time.Second, time.Millisecond)

	// Mixed connection: consumer + confirm-mode publisher on channel 1.
	m := dialRaw(t, addr)
	m.handshake("guest", "guest", 4, 0)
	m.openChannel(1)
	leaseConfirmSelect(t, m, 1)
	m.consume(1, "sl22m-q", "c1")

	// At HWM: the publish STORES + confirms promptly (overshoot), and the
	// two queued deliveries arrive on the same connection afterwards.
	leaseSendPublish(t, m, 1, "", "sl22m-q", leaseBody(64), false)
	leaseWaitAck(t, m, 1, 1) // confirm first (the publish was stored at once)
	for i := 0; i < 2; i++ {
		tag := readDeliveryTag(t, m)
		ack := &protocol.BasicAckMethod{DeliveryTag: tag}
		data, err := ack.Serialize()
		require.NoError(t, err)
		m.send(1, 60, 80, data)
	}

	srv.Mutex.RLock()
	var mconn *protocol.Connection
	for _, c := range srv.Connections {
		if c.HasActiveConsumer() {
			mconn = c
		}
	}
	srv.Mutex.RUnlock()
	require.NotNil(t, mconn)
	assert.False(t, mconn.DepthBackpressure.Load(), "a mixed connection's reader is never depth-parked")
	assert.Equal(t, int64(0), leaseUsed(srv))
}

// LOW1 (Opus): the lease-wait metric records exactly one wait when one header
// waits; immediate grants are never counted.
func TestLease_WaitMetricsRecorded(t *testing.T) {
	release := gateWALFsync(t)
	srv, addr, _, _, probe := leaseServer(t, 1<<20, nil, nil)

	r := dialRaw(t, addr)
	r.handshake("guest", "guest", 4, 0)
	r.openChannel(1)
	r.declareQueue(1, "slpw-q")
	leaseConfirmSelect(t, r, 1)

	// msg1 holds the whole budget behind the gated fsync (immediate grant).
	leaseSendPublish(t, r, 1, "", "slpw-q", leaseBody(700<<10), true)
	require.Eventually(t, func() bool { return leaseUsed(srv) >= 717744 }, 30*time.Second, time.Millisecond)

	// msg2's header waits (no immediate grant possible).
	leaseSendMethod(t, r, 1, "", "slpw-q", false)
	leaseSendHeader(t, r, 1, 700<<10, true, "")
	bodyDone := make(chan struct{})
	go func() {
		defer close(bodyDone)
		leaseSendBody(t, r, 1, leaseBody(700<<10))
	}()
	require.Eventually(t, func() bool { return leaseWaiters(srv) == 1 }, 30*time.Second, time.Millisecond)

	release()
	leaseWaitAck(t, r, 1, 2)
	<-bodyDone
	assert.Equal(t, int64(1), probe.waits.Load(),
		"exactly one wait recorded (immediate grants are never counted)")
}
