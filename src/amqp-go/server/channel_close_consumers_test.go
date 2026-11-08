package server

// FINDING 6 — a channel-only close (channel.close with the connection still
// open) unregisters the channel's consumers from the broker but never refreshed
// amqp_consumers_total, so the gauge kept counting a consumer that no longer
// existed. teardownChannel must refresh it after unregistering.

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChannelCloseOnly_ResetsConsumersGauge(t *testing.T) {
	spy := &spyMetricsCollector{}
	srv := newServerWithSpy(t, spy)
	conn, frameCh := newPipeConnWithFrames(t)
	setupQueueAndChannel(t, srv, conn, frameCh, "chan-close-q")

	require.NoError(t, srv.handleBasicConsume(conn, 1,
		encodeBasicConsume(t, "chan-close-q", "chan-close-tag", false)))
	drainFrame(t, frameCh) // basic.consume-ok
	require.Equal(t, int64(1), spy.consumersTotal.Load(),
		"the consumer gauge must be 1 after basic.consume")

	// Channel-only close: the connection stays registered and open.
	srv.teardownChannel(conn, 1)

	require.Equal(t, int64(0), spy.consumersTotal.Load(),
		"closing only the channel must drop amqp_consumers_total to 0")

	// The channel is gone but the connection is still present.
	_, ok := conn.Channels.Load(uint16(1))
	require.False(t, ok, "the channel must be removed from the connection")
}
