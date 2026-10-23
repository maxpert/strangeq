package main

// S3 (WS-D D-NEW-1) regression test: channel numbers are per-CONNECTION in
// AMQP 0-9-1. Two connections that both run tx on their channel 1 must have
// fully independent transaction state — a rollback on one must not affect the
// other's commit. Pre-seam, the server keyed transaction state by channelID
// alone, so the second connection's tx.select collided with the first's
// (541'd), and any shared key would let one connection's rollback discard the
// other's buffered publishes.

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTxPerConnectionIsolation_RollbackDoesNotTouchOtherConnection(t *testing.T) {
	b := newRestartableBroker(t, nil)

	connA, err := amqp.Dial(b.uri)
	require.NoError(t, err)
	defer connA.Close()
	connB, err := amqp.Dial(b.uri)
	require.NoError(t, err)
	defer connB.Close()

	// The FIRST channel each connection opens is channel number 1 on the wire,
	// so both connections operate on (their own) channel 1.
	chA, err := connA.Channel()
	require.NoError(t, err)
	chB, err := connB.Channel()
	require.NoError(t, err)

	_, err = chA.QueueDeclare("txiso-a", false, false, false, false, nil)
	require.NoError(t, err)
	_, err = chB.QueueDeclare("txiso-b", false, false, false, false, nil)
	require.NoError(t, err)

	// Both connections enter tx mode on their channel 1.
	require.NoError(t, chA.Tx(), "connection A tx.select")
	require.NoError(t, chB.Tx(), "connection B tx.select must not collide with A's channel-1 tx")

	// Both buffer one publish.
	require.NoError(t, chA.Publish("", "txiso-a", false, false,
		amqp.Publishing{Body: []byte("a1")}))
	require.NoError(t, chB.Publish("", "txiso-b", false, false,
		amqp.Publishing{Body: []byte("b1")}))

	// A rolls back; B commits. A's rollback must not discard B's buffer.
	require.NoError(t, chA.TxRollback())
	require.NoError(t, chB.TxCommit(), "B's commit must succeed after A's rollback")

	// B's publish is visible; A's was discarded. Read through a fresh
	// non-tx connection so basic.get is unaffected by either tx channel.
	connC, err := amqp.Dial(b.uri)
	require.NoError(t, err)
	defer connC.Close()
	chC, err := connC.Channel()
	require.NoError(t, err)

	msgB, ok, err := chC.Get("txiso-b", true)
	require.NoError(t, err)
	require.True(t, ok, "B's committed publish must be visible")
	assert.Equal(t, "b1", string(msgB.Body))

	deadline := time.Now().Add(2 * time.Second)
	for {
		_, ok, err = chC.Get("txiso-a", true)
		require.NoError(t, err)
		if !ok {
			break
		}
		require.True(t, time.Now().Before(deadline), "A's rolled-back publish must never appear")
	}

	// A's channel is still transactional and usable after the rollback:
	// publish again and commit — it must land.
	require.NoError(t, chA.Publish("", "txiso-a", false, false,
		amqp.Publishing{Body: []byte("a2")}))
	require.NoError(t, chA.TxCommit())
	msgA, ok, err := chC.Get("txiso-a", true)
	require.NoError(t, err)
	require.True(t, ok, "A's post-rollback commit must be visible")
	assert.Equal(t, "a2", string(msgA.Body))
}
