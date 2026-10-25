package server

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Item 5 (Sol): two-party retention — for a mandatory no-route publish the
// lease releases only when the broker part (synchronous here) AND the server
// part (the basic.return + confirm wire writes) are both done. With the
// client not reading, the blocked basic.return write must HOLD the lease.
func TestLease_MandatoryReturnBlockedHoldsLease(t *testing.T) {
	srv := newTransactionTestServer(t)
	r, done := startPipeServerConnection(t, srv)
	// NOTE: close the client BEFORE waiting for the server side to finish.
	defer func() {
		_ = r.c.Close()
		<-done
	}()

	r.handshake("guest", "guest", 4, 0)
	r.openChannel(1)
	// A declared-but-unbound direct exchange: every mandatory publish to it is
	// a no-route → basic.return.
	r.send(1, 40, 10, mustSerialize(&protocol.ExchangeDeclareMethod{Exchange: "sl5-ex", Type: "direct"}))
	r.expectMethodCh(1, 40, 11) // exchange.declare-ok
	leaseConfirmSelect(t, r, 1)

	created := &atomic.Int64{}
	released := &atomic.Int64{}
	srv.leaseCreated = created
	srv.leaseReleased = released

	leaseSendMethod(t, r, 1, "sl5-ex", "nowhere", true)
	leaseSendHeader(t, r, 1, 128, true, "")
	leaseSendBody(t, r, 1, leaseBody(128))

	// The lease was created at the header. The broker part completes
	// synchronously (no route), but the basic.return write blocks on the
	// synchronous net.Pipe — the client is not reading. The lease must be
	// held until the return is written.
	require.Eventually(t, func() bool { return created.Load() == 1 }, 30*time.Second, time.Millisecond)
	assert.Never(t, func() bool { return released.Load() != 0 }, 300*time.Millisecond, 20*time.Millisecond,
		"the lease must be held while the basic.return write is blocked")
	assert.Equal(t, leaseBytes(128, 131072), leaseUsed(srv))

	// Read the basic.return, then the confirm: once the server's writes
	// complete, the lease releases.
	f := r.readFrame()
	require.Equal(t, byte(protocol.FrameMethod), f.Type)
	require.Equal(t, uint16(60), uint16(f.Payload[0])<<8|uint16(f.Payload[1]))
	require.Equal(t, uint16(50), uint16(f.Payload[2])<<8|uint16(f.Payload[3]), "basic.return")
	leaseWaitAck(t, r, 1, 1)
	require.Eventually(t, func() bool { return released.Load() == 1 }, 30*time.Second, time.Millisecond,
		"the lease releases once the return+confirm writes complete")
	assert.Equal(t, int64(0), leaseUsed(srv))
}
