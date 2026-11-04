package server

// WS-COLDREAD-PATH server tests (spec §5.20–§5.24).
//
// The release points on the server are where the body's last read completes:
// the batched contiguous path (W1), the vectored zero-copy path (W2), basic.get
// (W3), the dropped-consumer path (W4), and the write-error requeue (W8). Each
// must release a cold-read lease exactly once, never before the write.

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
)

// eventConn records each Write as "write" in a shared, mutex-guarded log.
type eventConn struct {
	net.Conn
	mu     *sync.Mutex
	events *[]string
}

func (c *eventConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	*c.events = append(*c.events, "write")
	c.mu.Unlock()
	return c.Conn.Write(p)
}

// leasedServerMessage returns a delivery whose message carries a release token
// that appends "release" to the ordered event log.
func leasedServerMessage(tag uint64, body []byte, mu *sync.Mutex, events *[]string) *protocol.Delivery {
	d := makeDelivery(tag, body)
	d.Message.AttachBodyRelease(func() {
		mu.Lock()
		*events = append(*events, "release")
		mu.Unlock()
	})
	return d
}

func drainServerPipe(t *testing.T, client net.Conn) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 1<<20)
		for {
			if _, err := client.Read(buf); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() { _ = client.Close(); <-done })
}

// --- 20. the contiguous batch path releases after the write ------------------

func TestSendBatchedDeliveries_ReleasesAfterWrite(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() { _ = serverConn.Close() })
	drainServerPipe(t, clientConn)

	var mu sync.Mutex
	var events []string
	conn := protocol.NewConnection(&eventConn{Conn: serverConn, mu: &mu, events: &events})
	srv := makeTestServer()

	d := leasedServerMessage(1, []byte("small-body"), &mu, &events)
	require.NoError(t, srv.sendBatchedDeliveries(conn, 1, "tag", []*protocol.Delivery{d}))

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"write", "release"}, events,
		"the lease must be released exactly once, AFTER the write")
}

// --- 21. the vectored batch path releases after the write --------------------

func TestSendBatchedDeliveriesVectored_ReleasesAfterWrite(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() { _ = serverConn.Close() })
	drainServerPipe(t, clientConn)

	var mu sync.Mutex
	var events []string
	conn := protocol.NewConnection(&eventConn{Conn: serverConn, mu: &mu, events: &events})
	srv := makeTestServer()

	body := make([]byte, writevBodyThreshold+1024)
	d := leasedServerMessage(2, body, &mu, &events)
	require.NoError(t, srv.sendBatchedDeliveries(conn, 1, "tag", []*protocol.Delivery{d}))

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, events)
	require.Equal(t, "release", events[len(events)-1],
		"the lease must be released AFTER the last write")
	releases := 0
	for i, e := range events {
		if e == "release" {
			releases++
			require.Equal(t, len(events)-1, i, "the release must be the last event")
		}
	}
	require.Equal(t, 1, releases, "the vectored lease must be released exactly once")
}

// --- 22. the dropped-consumer path releases ----------------------------------

func TestConsumerDeliveryLoop_DroppedConsumerReleases(t *testing.T) {
	var mu sync.Mutex
	var events []string
	d := leasedServerMessage(3, []byte("dropped"), &mu, &events)

	// The drop arm releases the delivery whose consumer was removed after
	// forwarding.
	releaseDeliveryBody(d)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"release"}, events,
		"a dropped-consumer delivery must have its lease released")
}

// --- 23. basic.get releases on success and on a write error ------------------

// leasedGetBroker is a UnifiedBroker whose GetMessageForGetOwned returns a
// leased message.
type leasedGetBroker struct {
	UnifiedBroker
	msg *protocol.Message
	tag uint64
}

func (b *leasedGetBroker) GetMessageForGetOwned(queueName string, noAck bool, connID string) (*protocol.Message, uint64, uint32, error) {
	return b.msg, b.tag, 1, nil
}

func TestBasicGet_ReleasesLeasedBodyAfterWrite(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		srv, conn, frameCh := newBasicGetLeaseServer(t, false)
		var released atomic.Int32
		srv.Broker = &leasedGetBroker{
			msg: leasedProtocolMessage([]byte("get-body"), &released),
			tag: 9001,
		}

		require.NoError(t, srv.handleBasicGet(conn, 1, encodeBasicGet(t, "get-lease-q", false)))
		drainFrame(t, frameCh) // basic.get-ok
		require.Equal(t, int32(1), released.Load(),
			"basic.get must release the lease exactly once after the write")
	})

	t.Run("write-error", func(t *testing.T) {
		srv, conn, _ := newBasicGetLeaseServer(t, true)
		var released atomic.Int32
		srv.Broker = &leasedGetBroker{
			msg: leasedProtocolMessage([]byte("get-body"), &released),
			tag: 9002,
		}

		err := srv.handleBasicGet(conn, 1, encodeBasicGet(t, "get-lease-err-q", false))
		require.Error(t, err, "premise: the write fails")
		require.Equal(t, int32(1), released.Load(),
			"basic.get must release the lease once even when the write fails")
	})
}

// --- 24. the write-error requeue releases ------------------------------------

func TestConsumerDeliveryLoop_WriteErrorReleasesAndRequeues(t *testing.T) {
	srv := newGetPurgeTestServer(t)
	var released atomic.Int32
	d := &protocol.Delivery{
		Message:     leasedProtocolMessage([]byte("requeue-body"), &released),
		DeliveryTag: 7777,
	}

	srv.requeueFailedDeliveries(nil, []*protocol.Delivery{d})

	require.Equal(t, int32(1), released.Load(),
		"the write-error requeue path must release the lease")
	assert.True(t, !d.Message.HasBodyRelease(), "the token must be cleared after release")
}

// --- helpers -----------------------------------------------------------------

// newBasicGetLeaseServer builds a minimal Server with a working (or failing)
// connection, for a direct handleBasicGet call. Authorization is disabled.
func newBasicGetLeaseServer(t *testing.T, writeErr bool) (*Server, *protocol.Connection, chan *protocol.Frame) {
	t.Helper()
	srv := &Server{
		Log:              zap.NewNop(),
		Config:           config.DefaultConfig(),
		MetricsCollector: &NoOpMetricsCollector{},
	}
	if writeErr {
		clientConn, serverConn := net.Pipe()
		t.Cleanup(func() { _ = clientConn.Close(); _ = serverConn.Close() })
		conn := protocol.NewConnection(&errorConn{Conn: serverConn, writeErr: errors.New("write boom")})
		return srv, conn, nil
	}
	conn, frameCh := newPipeConnWithFrames(t)
	return srv, conn, frameCh
}

// leasedProtocolMessage builds a message carrying a counting release token.
func leasedProtocolMessage(body []byte, released *atomic.Int32) *protocol.Message {
	m := &protocol.Message{
		Exchange:     "test-exchange",
		RoutingKey:   "test.key",
		Body:         append([]byte(nil), body...),
		DeliveryMode: 2,
	}
	m.AttachBodyRelease(func() { released.Add(1) })
	return m
}
