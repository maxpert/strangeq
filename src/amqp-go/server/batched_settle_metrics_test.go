package server

// FINDING 5 — a cumulative (multiple=true) basic.ack / basic.nack settles every
// outstanding delivery up to the tag, but the metrics were incremented exactly
// once per METHOD. Five settled deliveries must move the counter by five.
//
// The test drives the real handler path: five manual-ack basic.get deliveries
// are tracked on the channel's wire-tag table, then one multiple ack/nack at
// tag 5 is processed.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCumulativeAck_CountsEverySettledDelivery(t *testing.T) {
	spy := &spyMetricsCollector{}
	srv := newServerWithSpy(t, spy)
	conn, frameCh := newPipeConnWithFrames(t)
	setupQueueAndChannel(t, srv, conn, frameCh, "cum-ack-q")

	const n = 5
	for i := 0; i < n; i++ {
		publishMessageToQueue(t, srv, "cum-ack-q", "msg")
	}
	var lastTag uint64
	for i := 0; i < n; i++ {
		lastTag = getDeliveryTag(t, srv, conn, frameCh, "cum-ack-q")
	}
	require.Equal(t, uint64(n), lastTag, "wire tags are 1-based and strictly increasing")

	require.Equal(t, int64(0), spy.msgsAcked.Load(), "no ack counted before the cumulative ack")

	err := srv.handleBasicAck(conn, 1, makeAckPayload(lastTag, true))
	require.NoError(t, err)

	assert.Equal(t, int64(n), spy.msgsAcked.Load(),
		"one cumulative ack of %d deliveries must count %d acknowledgements", n, n)
	assert.Equal(t, int64(0), spy.acks.Load(),
		"the singular counter must not fire for the cumulative path")
}

func TestCumulativeNack_CountsEverySettledDelivery(t *testing.T) {
	spy := &spyMetricsCollector{}
	srv := newServerWithSpy(t, spy)
	conn, frameCh := newPipeConnWithFrames(t)
	setupQueueAndChannel(t, srv, conn, frameCh, "cum-nack-q")

	const n = 5
	for i := 0; i < n; i++ {
		publishMessageToQueue(t, srv, "cum-nack-q", "msg")
	}
	var lastTag uint64
	for i := 0; i < n; i++ {
		lastTag = getDeliveryTag(t, srv, conn, frameCh, "cum-nack-q")
	}

	err := srv.handleBasicNack(conn, 1, makeNackPayload(lastTag, true, false))
	require.NoError(t, err)

	assert.Equal(t, int64(n), spy.msgsRejected.Load(),
		"one cumulative nack of %d deliveries must count %d rejections", n, n)
}
