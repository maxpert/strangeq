package server

// FINDINGS 3 and 8 — connection teardown must keep the entity gauges/counters
// honest.
//
//  * FINDING 3: cleanupConnection tore down still-open channels and moved the
//    entity delta, but never called RecordChannelClosed, so
//    amqp_channels_total stayed permanently high after any connection dropped
//    with channels open.
//  * FINDING 8: cleanupConnection deleted exclusive queues owned by the
//    connection but never called RecordQueueDeleted, so
//    amqp_queues_deleted_total under-reported.
//
// Both tests use a REAL listener and a raw AMQP client, and bound the wait with
// require.Eventually (the teardown runs on the connection goroutine).

import (
	"testing"
	"time"

	"github.com/maxpert/amqp-go/metrics"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// declareExclusiveQueue issues queue.declare with exclusive=true and awaits
// declare-ok. The queue is then owned by the connection for teardown.
func (r *rawAMQP) declareExclusiveQueue(ch uint16, name string) {
	r.t.Helper()
	m := &protocol.QueueDeclareMethod{Queue: name, Exclusive: true}
	data, err := m.Serialize()
	require.NoError(r.t, err)
	r.send(ch, 50, 10, data)
	r.expectMethodCh(ch, 50, 11) // queue.declare-ok
}

func TestConnectionTeardown_ReclaimsChannelsGauge(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := metrics.NewCollectorWithRegistry(reg, "amqp")

	_, addr := startWSBServer(t, nil, func(s *Server) { s.MetricsCollector = c })

	cli := dialRaw(t, addr)
	cli.handshake("guest", "guest", 2047, 0)
	cli.openChannel(1)
	cli.openChannel(2)

	require.Equal(t, 2.0, testutil.ToFloat64(c.ChannelsCreated))
	require.Equal(t, 2.0, testutil.ToFloat64(c.ChannelsTotal))

	// Drop the socket without a clean connection.close.
	_ = cli.c.Close()

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(c.ChannelsTotal) == 0
	}, 5*time.Second, 10*time.Millisecond,
		"amqp_channels_total must return to 0 after connection teardown (leaked channel gauge)")

	created := testutil.ToFloat64(c.ChannelsCreated)
	closed := testutil.ToFloat64(c.ChannelsClosed)
	require.Equal(t, created, closed,
		"created - closed delta must be 0 after the connection's channels are torn down")
}

func TestConnectionTeardown_CountsExclusiveQueueDelete(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := metrics.NewCollectorWithRegistry(reg, "amqp")

	_, addr := startWSBServer(t, nil, func(s *Server) { s.MetricsCollector = c })

	cli := dialRaw(t, addr)
	cli.handshake("guest", "guest", 2047, 0)
	cli.openChannel(1)
	cli.declareExclusiveQueue(1, "excl-reclaim-q")

	require.Equal(t, 0.0, testutil.ToFloat64(c.QueuesDeleted))

	_ = cli.c.Close()

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(c.QueuesDeleted) == 1
	}, 5*time.Second, 10*time.Millisecond,
		"tearing down an exclusive queue at connection close must count one queue deletion")
}
