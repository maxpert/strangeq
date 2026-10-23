package server

// S7 seam test: WS-E's StorageBrokerAdapter.QueueReadyCount (queue_stats.go)
// must keep working against WS-D's broker — ready depth reflects publishes,
// excludes delivered-but-unacked, and unknown queues report 0 without
// materializing state.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueueReadyCount_AgainstWSDStorageBroker(t *testing.T) {
	srv := newTransactionTestServer(t)
	conn, frameCh := newPipeConnWithFrames(t)
	setupTxChannelAndQueue(t, srv, conn, frameCh, "s7-q")

	adapter, ok := srv.Broker.(*StorageBrokerAdapter)
	require.True(t, ok)

	assert.Equal(t, uint32(0), adapter.QueueReadyCount("s7-q"), "empty queue")
	assert.Equal(t, uint32(0), adapter.QueueReadyCount("never-declared"),
		"unknown queue reports 0 without materializing queue state")

	publishMessageToQueue(t, srv, "s7-q", "m1")
	publishMessageToQueue(t, srv, "s7-q", "m2")
	assert.Equal(t, uint32(2), adapter.QueueReadyCount("s7-q"))

	// A delivered-but-unacked message leaves READY depth.
	_ = getDeliveryTag(t, srv, conn, frameCh, "s7-q")
	assert.Equal(t, uint32(1), adapter.QueueReadyCount("s7-q"),
		"delivered-but-unacked is not ready depth")

	// Nil-safety: a zero-value adapter reports 0 instead of panicking.
	var nilAdapter *StorageBrokerAdapter
	assert.Equal(t, uint32(0), nilAdapter.QueueReadyCount("s7-q"))
}
