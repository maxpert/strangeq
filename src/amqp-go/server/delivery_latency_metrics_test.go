package server

// FINDING 2 — amqp_message_delivery_duration_seconds had no production
// producer: RecordDeliveryLatency was called only from sendBasicDeliver, which
// has no production caller. Every production delivery goes through
// sendBatchedDeliveries (contiguous) or sendBatchedDeliveriesVectored
// (zero-copy). Both must observe the batch write duration once per delivered
// message.

import (
	"bytes"
	"net"
	"testing"

	"github.com/maxpert/amqp-go/metrics"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// histogramSampleCount returns the summed sample count of a histogram family.
func histogramSampleCount(t *testing.T, g prometheus.Gatherer, name string) uint64 {
	t.Helper()
	fams, err := g.Gather()
	require.NoError(t, err)
	for _, f := range fams {
		if f.GetName() == name {
			var total uint64
			for _, m := range f.GetMetric() {
				total += m.GetHistogram().GetSampleCount()
			}
			return total
		}
	}
	return 0
}

func TestBatchedDeliveries_RecordsDeliveryLatency(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := metrics.NewCollectorWithRegistry(reg, "amqp")

	srv := makeTestServer()
	srv.MetricsCollector = c

	clientConn, serverConn := net.Pipe()
	conn := protocol.NewConnection(serverConn)

	var received bytes.Buffer
	drainDone := make(chan struct{})
	drainConn(clientConn, &received, drainDone)

	const fam = "amqp_message_delivery_duration_seconds"
	require.Equal(t, uint64(0), histogramSampleCount(t, reg, fam), "no observation before the batch")

	deliveries := []*protocol.Delivery{makeDelivery(1, []byte("body"))}
	require.NoError(t, srv.sendBatchedDeliveries(conn, 1, "test-consumer", deliveries))

	require.Equal(t, uint64(1), histogramSampleCount(t, reg, fam),
		"one delivered message must produce one delivery-latency observation")

	clientConn.Close()
	serverConn.Close()
	<-drainDone
}

func TestBatchedDeliveriesVectored_RecordsDeliveryLatency(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := metrics.NewCollectorWithRegistry(reg, "amqp")

	srv := makeTestServer()
	srv.MetricsCollector = c

	clientConn, serverConn := net.Pipe()
	conn := protocol.NewConnection(serverConn)

	var received bytes.Buffer
	drainDone := make(chan struct{})
	drainConn(clientConn, &received, drainDone)

	const fam = "amqp_message_delivery_duration_seconds"
	require.Equal(t, uint64(0), histogramSampleCount(t, reg, fam), "no observation before the batch")

	// A body at/above the threshold takes the vectored path.
	body := bytes.Repeat([]byte("x"), writevBodyThreshold)
	deliveries := []*protocol.Delivery{makeDelivery(1, body)}
	require.NoError(t, srv.sendBatchedDeliveries(conn, 1, "test-consumer", deliveries))

	require.Equal(t, uint64(1), histogramSampleCount(t, reg, fam),
		"one delivered message through the vectored writer must produce one observation")

	clientConn.Close()
	serverConn.Close()
	<-drainDone
}
