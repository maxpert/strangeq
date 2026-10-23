package server

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWSB_PanicInProcessor_ConnectionClosedProcessAlive: a panic in the frame
// processor kills only that connection (§8); the broker stays alive.
func TestWSB_PanicInProcessor_ConnectionClosedProcessAlive(t *testing.T) {
	pb := &panicBroker{declarePanic: "boom-q"}
	srv, addr := startWSBPanicBrokerServer(t, pb, nil)

	conn, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	_, _ = ch.QueueDeclare("boom-q", false, true, false, false, nil)

	require.Eventually(t, func() bool {
		return srv.PanicCount() == 1
	}, 2*time.Second, 10*time.Millisecond, "processor panic must be recovered")
	require.Eventually(t, func() bool {
		return wsbConnCount(srv) == 0
	}, 2*time.Second, 10*time.Millisecond, "panicked connection must be cleaned up")

	// The broker is alive: a fresh dial and round-trip works.
	conn2, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn2.Close()
	ch2, err := conn2.Channel()
	require.NoError(t, err)
	_, err = ch2.QueueDeclare("alive-q", false, true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch2.Publish("", "alive-q", false, false, amqp.Publishing{Body: []byte("m")}))
	msg, ok, err := ch2.Get("alive-q", true)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "m", string(msg.Body))
}

// TestWSB_PanicInAckProcessor: a panic in the ack processor kills only that
// connection.
func TestWSB_PanicInAckProcessor(t *testing.T) {
	pb := &panicBroker{}
	pb.ackPanic.Store(true)
	srv, addr := startWSBPanicBrokerServer(t, pb, nil)

	conn, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	_, err = ch.QueueDeclare("ack-q", false, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.Publish("", "ack-q", false, false, amqp.Publishing{Body: []byte("m")}))

	// Push consumer with manual acks: the ack flows through
	// Broker.AcknowledgeMessage, where the panic is armed.
	deliveries, err := ch.Consume("ack-q", "c1", false, false, false, false, nil)
	require.NoError(t, err)
	select {
	case d := <-deliveries:
		_ = d.Ack(false) // AcknowledgeMessage panics on the ack processor
	case <-time.After(2 * time.Second):
		t.Fatal("no delivery within 2s")
	}

	require.Eventually(t, func() bool {
		return srv.PanicCount() >= 1
	}, 2*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return wsbConnCount(srv) == 0
	}, 2*time.Second, 10*time.Millisecond)

	conn2, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err, "broker must stay alive")
	defer conn2.Close()
}

// TestWSB_PanicInDeliveryLoop: a panic in the consumer delivery loop kills
// only that connection.
func TestWSB_PanicInDeliveryLoop(t *testing.T) {
	pm := &panicMetrics{}
	pm.onDelivered.Store(true)
	srv, addr := startWSBServer(t, nil, func(s *Server) {
		s.MetricsCollector = pm
	})

	conn, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	_, err = ch.QueueDeclare("dl-q", false, true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.Publish("", "dl-q", false, false, amqp.Publishing{Body: []byte("m")}))

	deliveries, err := ch.Consume("dl-q", "c1", true, false, false, false, nil)
	require.NoError(t, err)
	select {
	case <-deliveries:
	case <-time.After(2 * time.Second):
		t.Fatal("no delivery within 2s")
	}

	require.Eventually(t, func() bool {
		return srv.PanicCount() >= 1
	}, 2*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		return wsbConnCount(srv) == 0
	}, 2*time.Second, 10*time.Millisecond)

	conn2, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err, "broker must stay alive")
	defer conn2.Close()
}

// TestWSB_PanicInHandleConnection: a panic in handleConnection itself (here,
// the metrics hook) is caught by the outer recoverConnection guard.
func TestWSB_PanicInHandleConnection(t *testing.T) {
	pm := &panicMetrics{}
	pm.onCreated.Store(true)
	srv, addr := startWSBServer(t, nil, func(s *Server) {
		s.MetricsCollector = pm
	})

	// The first dial trips the armed panic: it fails or its socket closes.
	first, firstErr := amqp.Dial("amqp://guest:guest@" + addr + "/")
	if firstErr == nil {
		closeCh := first.NotifyClose(make(chan *amqp.Error, 1))
		select {
		case <-closeCh:
		case <-time.After(2 * time.Second):
			first.Close()
			t.Fatal("first connection should have closed after the panic")
		}
		first.Close()
	}

	require.Eventually(t, func() bool {
		return srv.PanicCount() == 1
	}, 2*time.Second, 10*time.Millisecond)

	// The panic fired only once (armed once); the next dial succeeds.
	conn2, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn2.Close()
	ch2, err := conn2.Channel()
	require.NoError(t, err)
	require.NoError(t, ch2.Close())
}
