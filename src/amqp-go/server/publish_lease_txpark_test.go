package server

import (
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// LOW2 (Opus): with a tx COMMIT parked at a queue's high-water mark
// (PublishMessageTx's WaitForCapacity) and further publishes queued behind
// it, a connection close drains the QUEUED header leases at teardown step 3 —
// used drops to 0 while the commit is STILL parked. A second connection
// progresses normally throughout (the group-commit WAL is untouched).
func TestLease_TxCommitParkDrainsQueuedLeases(t *testing.T) {
	srv, addr, created, released, _ := leaseServer(t, 1<<20, func(cfg *config.AMQPConfig) {
		cfg.Engine.DepthHighWMOverride = 1
	}, nil)

	// Fill the tx target queue to HWM=1 with a transient publish.
	p := dialRaw(t, addr)
	p.handshake("guest", "guest", 4, 0)
	p.openChannel(1)
	p.declareQueue(1, "sltx-q")
	leaseSendPublish(t, p, 1, "", "sltx-q", leaseBody(64), false)
	require.Eventually(t, func() bool {
		return srv.Broker.(*StorageBrokerAdapter).broker.GetQueueReadyCount("sltx-q") == 1
	}, 30*time.Second, time.Millisecond)

	a := dialRaw(t, addr)
	a.handshake("guest", "guest", 4, 0)
	a.openChannel(1)
	a.send(1, 90, 10, []byte{}) // tx.select
	a.expectMethodCh(1, 90, 11) // tx.select-ok
	leaseSendPublish(t, a, 1, "", "sltx-q", leaseBody(64), true)
	require.Eventually(t, func() bool { return created.Load() == 1 && released.Load() == 1 },
		30*time.Second, time.Millisecond, "the tx publish's lease releases at buffer time")

	// The commit parks the processor in the target queue's capacity wait.
	a.send(1, 90, 20, []byte{}) // tx.commit

	// Queue two more publishes behind the parked commit: their header leases
	// are granted and sit in the LeaseFIFO (the processor never reaches them).
	// (The fill publish + the tx publish account for the first two created.)
	leaseSendPublish(t, a, 1, "", "sltx-q", leaseBody(64), true)
	leaseSendPublish(t, a, 1, "", "sltx-q", leaseBody(64), true)
	require.Eventually(t, func() bool { return created.Load() == 4 }, 30*time.Second, time.Millisecond,
		"both queued publishes must be leased")
	assert.Equal(t, 2*leaseBytes(64, 131072), leaseUsed(srv))

	// The client closes while the commit is STILL parked: teardown step 3
	// drains the queued leases immediately.
	_ = a.c.Close()
	require.Eventually(t, func() bool {
		return released.Load() == 4 && leaseUsed(srv) == 0
	}, 30*time.Second, time.Millisecond,
		"queued leases release at teardown step 3 while the commit is still parked")

	// Another connection progresses normally throughout (the group-commit WAL
	// is not touched by the capacity-parked commit).
	b := dialRaw(t, addr)
	b.handshake("guest", "guest", 4, 0)
	b.openChannel(1)
	b.declareQueue(1, "sltx-q2")
	leaseConfirmSelect(t, b, 1)
	leaseSendPublish(t, b, 1, "", "sltx-q2", leaseBody(64), true)
	leaseWaitAck(t, b, 1, 1)

	// Unpark the commit by draining the queue with a consumer.
	c := dialRaw(t, addr)
	c.handshake("guest", "guest", 4, 0)
	c.openChannel(1)
	c.consume(1, "sltx-q", "c1")
	tag := readDeliveryTag(t, c)
	ack := &protocol.BasicAckMethod{DeliveryTag: tag}
	data, err := ack.Serialize()
	require.NoError(t, err)
	c.send(1, 60, 80, data)

	// Teardown completes (the server shuts down cleanly at test end).
	assert.Equal(t, int64(0), leaseUsed(srv))
}
