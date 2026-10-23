package server

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWSB_GetRequeueScopedToClosingConnection: connection close must requeue
// ONLY its own unacked basic.get deliveries — not every connection's (P1:
// RequeueAllGetDeliveries requeued globally, so another client's later ack
// became a no-op and the message was delivered twice).
func TestWSB_GetRequeueScopedToClosingConnection(t *testing.T) {
	_, addr := startWSBServer(t, nil, nil)

	setup, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer setup.Close()
	sch, err := setup.Channel()
	require.NoError(t, err)
	_, err = sch.QueueDeclare("gq", false, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, sch.Publish("", "gq", false, false, amqp.Publishing{Body: []byte("A")}))
	require.NoError(t, sch.Publish("", "gq", false, false, amqp.Publishing{Body: []byte("B")}))

	conn1, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	c1, err := conn1.Channel()
	require.NoError(t, err)
	a, ok, err := c1.Get("gq", false) // manual ack
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "A", string(a.Body))

	conn2, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn2.Close()
	c2, err := conn2.Channel()
	require.NoError(t, err)
	b, ok, err := c2.Get("gq", false) // manual ack
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "B", string(b.Body))

	// Close conn1: ONLY its delivery (A) is requeued.
	require.NoError(t, conn1.Close())

	conn3, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn3.Close()
	c3, err := conn3.Channel()
	require.NoError(t, err)

	var gotA amqp.Delivery
	require.Eventually(t, func() bool {
		d, ok, err := c3.Get("gq", false)
		if err != nil || !ok {
			return false
		}
		gotA = d
		return true
	}, 2*time.Second, 20*time.Millisecond, "A must be requeued after conn1 closes")
	assert.Equal(t, "A", string(gotA.Body))
	assert.True(t, gotA.Redelivered)

	// The NEXT get is empty: B belongs to conn2 and must NOT have been
	// requeued (today's global requeue makes this return B — a duplicate).
	d2, ok, err := c3.Get("gq", true)
	require.NoError(t, err)
	assert.False(t, ok, "B must still be owned by conn2, got %v", d2)

	// conn2's ack of B still works (its delivery was never requeued).
	require.NoError(t, b.Ack(false))

	// After conn3 acks A, the queue is empty.
	require.NoError(t, gotA.Ack(false))
	require.Eventually(t, func() bool {
		q, err := c3.QueueDeclarePassive("gq", false, false, false, false, nil)
		return err == nil && q.Messages == 0
	}, 2*time.Second, 20*time.Millisecond)
}

// TestWSB_GetRequeueOwnDeliveriesOnClose: a connection's own unacked get
// deliveries come back after it closes.
func TestWSB_GetRequeueOwnDeliveriesOnClose(t *testing.T) {
	_, addr := startWSBServer(t, nil, nil)

	setup, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer setup.Close()
	sch, err := setup.Channel()
	require.NoError(t, err)
	_, err = sch.QueueDeclare("gq2", false, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, sch.Publish("", "gq2", false, false, amqp.Publishing{Body: []byte("A")}))

	conn1, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	c1, err := conn1.Channel()
	require.NoError(t, err)
	a, ok, err := c1.Get("gq2", false)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "A", string(a.Body))
	require.NoError(t, conn1.Close())

	require.Eventually(t, func() bool {
		d, ok, err := sch.Get("gq2", true)
		return err == nil && ok && string(d.Body) == "A"
	}, 2*time.Second, 20*time.Millisecond, "A must be available again after conn1 closes")
}
