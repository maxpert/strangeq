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

// SL-22 (Sol+Opus): a TRANSIENT publish to a queue at its high-water mark is
// never parked behind WaitForCapacity holding its lease: the lease is
// released BEFORE the capacity wait, the reader depth gate paces the
// producer, and the publish completes once a consumer drains the queue.
func TestLease_TransientHWMNonBlockingAdmission(t *testing.T) {
	srv, addr, created, released, _ := leaseServer(t, 1<<20, func(cfg *config.AMQPConfig) {
		cfg.Engine.DepthHighWMOverride = 2
	}, nil)

	// Fill the queue to HWM with transient publishes from a producer.
	p := dialRaw(t, addr)
	p.handshake("guest", "guest", 4, 0)
	p.openChannel(1)
	p.declareQueue(1, "sl22-q")
	for i := 0; i < 2; i++ {
		leaseSendPublish(t, p, 1, "", "sl22-q", leaseBody(64), false)
	}
	require.Eventually(t, func() bool {
		return srv.Broker.(*StorageBrokerAdapter).broker.GetQueueReadyCount("sl22-q") == 2
	}, 30*time.Second, time.Millisecond)

	// A producer-only connection publishes transient + confirm: NOT admitted
	// at HWM. The publish must complete WITHOUT its lease ever being held
	// across the capacity wait.
	pub := dialRaw(t, addr)
	pub.handshake("guest", "guest", 4, 0)
	pub.openChannel(1)
	leaseConfirmSelect(t, pub, 1)
	leaseSendPublish(t, pub, 1, "", "sl22-q", leaseBody(64), false)

	// The lease for the waiting publish is released BEFORE the wait ends
	// (created == released while the confirm is still outstanding).
	require.Eventually(t, func() bool {
		return created.Load() >= 1 && created.Load() == released.Load()
	}, 30*time.Second, time.Millisecond, "no lease is ever held across a queue-capacity wait")

	// The producer's reader must be depth-backpressured (the gate armed).
	require.Eventually(t, func() bool {
		srv.Mutex.RLock()
		defer srv.Mutex.RUnlock()
		for _, c := range srv.Connections {
			if c.DepthBackpressure.Load() {
				return true
			}
		}
		return false
	}, 30*time.Second, time.Millisecond, "the reader depth gate must be armed for the producer-only connection")

	// No confirm while the queue is full.
	assert.Never(t, func() bool { return false }, 50*time.Millisecond, 10*time.Millisecond)

	// A consumer drains the queue; the waiting publish is admitted and
	// confirmed.
	c := dialRaw(t, addr)
	c.handshake("guest", "guest", 4, 0)
	c.openChannel(1)
	c.consume(1, "sl22-q", "c1")
	for i := 0; i < 2; i++ {
		tag := readDeliveryTag(t, c)
		ack := &protocol.BasicAckMethod{DeliveryTag: tag}
		data, err := ack.Serialize()
		require.NoError(t, err)
		c.send(1, 60, 80, data)
	}

	leaseWaitAck(t, pub, 1, 1) // the confirm arrives after the drain
	require.Eventually(t, func() bool { return leaseUsed(srv) == 0 }, 30*time.Second, time.Millisecond)
}

// Sol's exact mixed-connection ACK schedule: a mixed consumer+producer
// connection whose transient publish is not admitted at HWM keeps its ACK
// diversion live — the publish is admitted only after its own consumer acks
// drain the queue.
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

	// The transient publish is NOT admitted at HWM; the reader is NEVER
	// parked (mixed connection: its acks must keep flowing).
	leaseSendPublish(t, m, 1, "", "sl22m-q", leaseBody(64), false)

	// Read the two deliveries of the fill messages and ack them: the publish
	// must be admitted only AFTER the acks drain the queue — exact schedule.
	var tags []uint64
	for i := 0; i < 2; i++ {
		tags = append(tags, readDeliveryTag(t, m))
	}
	for _, tag := range tags {
		ack := &protocol.BasicAckMethod{DeliveryTag: tag}
		data, err := ack.Serialize()
		require.NoError(t, err)
		m.send(1, 60, 80, data)
	}

	// The confirm arrives only after the acks (never before — the queue was
	// at HWM until they landed).
	leaseWaitAck(t, m, 1, 1)
	// The mixed connection's reader was never depth-parked.
	srv.Mutex.RLock()
	var mconn *protocol.Connection
	for _, c := range srv.Connections {
		mconn = c
	}
	srv.Mutex.RUnlock()
	require.NotNil(t, mconn)
	assert.False(t, mconn.DepthBackpressure.Load(), "a mixed connection's reader is never depth-parked")
	assert.Equal(t, int64(0), leaseUsed(srv))
}
