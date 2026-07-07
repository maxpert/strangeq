//go:build conformance

package main

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ----------------------------------------------------------------------------
// queue.declare-ok / queue.declare-passive-ok message-count semantics.
//
// RabbitMQ's declare-ok message_count is the READY count: messages available for
// delivery, EXCLUDING delivered-but-unacknowledged (inflight) messages. This is
// PARITY on both brokers after the W7 fix (StrangeQ previously always reported 0;
// a naive fix sourcing the storage ring count would over-report by including
// unacked messages, since the ring retains them until ack).
// ----------------------------------------------------------------------------

func TestConformance_DeclareReadyCountExcludesUnacked(t *testing.T) {
	b := newConfBroker(t)
	defer b.cleanup()
	_, ch := b.dial(t)

	q := uniqueName("declcount")
	declareDurable(t, ch, q, nil)

	for i := 0; i < 3; i++ {
		require.NoError(t, ch.PublishWithContext(t.Context(), "", q, false, false,
			amqp.Publishing{Body: []byte{byte('a' + i)}}))
	}
	// All 3 ready before any delivery.
	require.Eventually(t, func() bool { return queueDepth(t, ch, q) == 3 }, 5*time.Second, 100*time.Millisecond,
		"all published messages must be ready before delivery")

	// Take ONE delivery and hold it unacked (autoAck=false, no ack). It becomes
	// inflight/unacked and must be EXCLUDED from the declared ready count.
	d, ok, err := ch.Get(q, false)
	require.NoError(t, err)
	require.True(t, ok, "expected a delivery")
	defer func() { _ = d.Nack(false, true) }() // requeue on cleanup

	// declare-ok ready count must be 2 (3 published − 1 unacked) on BOTH brokers.
	time.Sleep(200 * time.Millisecond) // let the inflight claim settle
	got := queueDepth(t, ch, q)
	lockLog(t, "declare ready-count with 1 unacked", got)
	want := expectByTarget(t, "declare-ready-count-excludes-unacked", 2, 2) // parity
	assert.Equal(t, want, got, "target=%s: declare-ok message_count must be READY only (excludes unacked)", confTarget())
}
