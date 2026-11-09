package server

import (
	"context"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBuilder_WiresMetricsCollectorToConcreteBroker is the F9/D1
// production-wiring regression test.
//
// It builds a server through the REAL ServerBuilder path — WithMetrics, then
// Build, with the storage-backed broker branch taken — and drives a genuine
// redelivery through the delivered-message path. Before the fix the builder
// resolved the metrics collector only AFTER wrapping the concrete
// *broker.StorageBroker in NewStorageBrokerAdapter, so
// StorageBroker.SetMetricsCollector was never called in production and
// amqp_messages_redelivered_total stayed permanently zero.
//
// The sibling spy test (TestMetricsRejectedAndRedeliveredThroughRealPath) builds
// a bare &Server{} and calls adapter.broker.SetMetricsCollector by hand, so it
// passes even while production is dark — it masks this defect. This test does
// NOT call the setter: the only wiring is the builder's.
func TestBuilder_WiresMetricsCollectorToConcreteBroker(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Storage.Path = t.TempDir()
	cfg.Network.Address = "127.0.0.1:0"
	cfg.Server.LogLevel = "silent"

	spy := &spyMetricsCollector{}
	srv, err := NewServerBuilder().
		WithConfig(cfg).
		WithMetrics(spy).
		Build()
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := srv.Shutdown(context.Background()); err != nil {
			t.Logf("shutdown: %v", err)
		}
	})

	conn, frameCh := newPipeConnWithFrames(t)
	setupQueueAndChannel(t, srv, conn, frameCh, "builder-redeliver-q")

	require.NoError(t, srv.handleBasicConsume(conn, 1,
		encodeBasicConsume(t, "builder-redeliver-q", "builder-redeliver", false)))
	drainFrame(t, frameCh)

	ch, ok := conn.Channels.Load(uint16(1))
	require.True(t, ok)
	channel := ch.(*protocol.Channel)
	channel.Mutex.RLock()
	consumer := channel.Consumers["builder-redeliver"]
	channel.Mutex.RUnlock()
	require.NotNil(t, consumer)

	publishMessageToQueue(t, srv, "builder-redeliver-q", "redeliver-me")

	select {
	case delivery := <-consumer.Messages:
		assert.False(t, delivery.Redelivered, "first delivery must not be redelivered")
		// SQ-18: this test reads directly from consumer.Messages, bypassing the
		// real delivery path that assigns and tracks the per-channel wire tag.
		// Register that mapping so handleBasicReject can translate the tag back
		// to the broker msgID.
		channel.TrackDelivery(delivery.DeliveryTag, delivery.DeliveryTag, delivery.ConsumerID, false)
		require.NoError(t, srv.handleBasicReject(conn, 1, makeRejectPayload(delivery.DeliveryTag, true)))
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first delivery")
	}

	select {
	case delivery := <-consumer.Messages:
		assert.True(t, delivery.Redelivered, "second delivery must be redelivered")
		assert.Equal(t, int64(1), spy.redeliver.Load(),
			"RecordMessageRedelivered must reach the concrete broker through the builder's wiring (F9)")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for redelivered message")
	}

	_ = srv.Broker.UnregisterConsumer("builder-redeliver")
}
