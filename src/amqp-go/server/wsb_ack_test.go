package server

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// expectChannelErrorCode waits for a channel NotifyClose and asserts the code
// and (optionally) a substring of the reason.
func expectChannelErrorCode(t *testing.T, closeCh chan *amqp.Error, code int, contains string) {
	t.Helper()
	select {
	case e := <-closeCh:
		require.NotNil(t, e)
		assert.Equal(t, code, e.Code)
		if contains != "" {
			assert.Contains(t, e.Reason, contains)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("channel NotifyClose did not fire within 2s (want %d)", code)
	}
}

// TestWSB_AckUnknownTag_Gets406: an ack for a tag that was never delivered (or
// already settled) is a channel-level 406 (RabbitMQ collect_acks parity).
func TestWSB_AckUnknownTag_Gets406(t *testing.T) {
	_, addr := startWSBServer(t, nil, nil)

	conn, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	closeCh := ch.NotifyClose(make(chan *amqp.Error, 1))

	require.NoError(t, ch.Ack(999, false))
	expectChannelErrorCode(t, closeCh, 406, "unknown delivery tag 999")

	require.False(t, conn.IsClosed(), "connection must survive a 406")

	ch2, err := conn.Channel()
	require.NoError(t, err)
	require.NoError(t, ch2.Close())
}

// TestWSB_DoubleAck_Gets406: the second ack of the same tag is unknown.
func TestWSB_DoubleAck_Gets406(t *testing.T) {
	_, addr := startWSBServer(t, nil, nil)

	conn, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn.Close()

	setup, err := conn.Channel()
	require.NoError(t, err)
	_, err = setup.QueueDeclare("da-q", false, true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, setup.Publish("", "da-q", false, false, amqp.Publishing{Body: []byte("m")}))

	ch, err := conn.Channel()
	require.NoError(t, err)
	closeCh := ch.NotifyClose(make(chan *amqp.Error, 1))

	msg, ok, err := ch.Get("da-q", false)
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, msg.Ack(false))
	require.NoError(t, msg.Ack(false)) // double ack → 406

	expectChannelErrorCode(t, closeCh, 406, "unknown delivery tag")
	require.False(t, conn.IsClosed())
}

// TestWSB_AckMultipleUnknown_Gets406_SettlesNothing: a multiple=true ack whose
// top tag is not outstanding raises 406 BEFORE anything is settled.
func TestWSB_AckMultipleUnknown_Gets406_SettlesNothing(t *testing.T) {
	_, addr := startWSBServer(t, nil, nil)

	setup, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer setup.Close()
	sch, err := setup.Channel()
	require.NoError(t, err)
	_, err = sch.QueueDeclare("ma-q", false, true, false, false, nil)
	require.NoError(t, err)
	for _, b := range []string{"m1", "m2"} {
		require.NoError(t, sch.Publish("", "ma-q", false, false, amqp.Publishing{Body: []byte(b)}))
	}

	conn1, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	ch1, err := conn1.Channel()
	require.NoError(t, err)
	closeCh := ch1.NotifyClose(make(chan *amqp.Error, 1))

	// Two manual-ack gets (wire tags 1, 2).
	for i := 0; i < 2; i++ {
		_, ok, err := ch1.Get("ma-q", false)
		require.NoError(t, err)
		require.True(t, ok)
	}

	// multiple=true with a top tag that is not outstanding: error, NOTHING settled.
	require.NoError(t, ch1.Ack(5, true))
	expectChannelErrorCode(t, closeCh, 406, "unknown delivery tag 5")

	// Closing conn1 requeues its get deliveries (§10).
	require.NoError(t, conn1.Close())

	conn2, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn2.Close()
	ch2, err := conn2.Channel()
	require.NoError(t, err)

	// Both messages come back: had the bad multiple-ack settled tags 1–2, the
	// queue would be empty.
	bodies := map[string]bool{}
	require.Eventually(t, func() bool {
		d, ok, err := ch2.Get("ma-q", true)
		if err != nil || !ok {
			return false
		}
		bodies[string(d.Body)] = true
		return len(bodies) == 2
	}, 2*time.Second, 20*time.Millisecond)
	assert.True(t, bodies["m1"] && bodies["m2"])
}

// TestWSB_AckMultipleZero_SettlesAll: multiple=true, tag=0 settles every
// outstanding delivery and never errors.
func TestWSB_AckMultipleZero_SettlesAll(t *testing.T) {
	_, addr := startWSBServer(t, nil, nil)

	conn, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)

	ch, err := conn.Channel()
	require.NoError(t, err)
	_, err = ch.QueueDeclare("mz-q", false, true, false, false, nil)
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		require.NoError(t, ch.Publish("", "mz-q", false, false, amqp.Publishing{Body: []byte("m")}))
	}
	for i := 0; i < 3; i++ {
		_, ok, err := ch.Get("mz-q", false)
		require.NoError(t, err)
		require.True(t, ok)
	}

	closeCh := ch.NotifyClose(make(chan *amqp.Error, 1))
	require.NoError(t, ch.Ack(0, true)) // settle ALL — must not error

	select {
	case e := <-closeCh:
		t.Fatalf("channel closed unexpectedly: %v", e)
	case <-time.After(300 * time.Millisecond):
	}
	require.NoError(t, conn.Close())

	// Redeclare-passive on a fresh connection: count must be 0.
	conn2, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn2.Close()
	ch2, err := conn2.Channel()
	require.NoError(t, err)
	q, err := ch2.QueueDeclarePassive("mz-q", false, true, false, false, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, q.Messages)
}

// TestWSB_NackUnknownTag_Gets406: as ack, for basic.nack.
func TestWSB_NackUnknownTag_Gets406(t *testing.T) {
	_, addr := startWSBServer(t, nil, nil)

	conn, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	closeCh := ch.NotifyClose(make(chan *amqp.Error, 1))

	require.NoError(t, ch.Nack(999, false, false))
	expectChannelErrorCode(t, closeCh, 406, "unknown delivery tag 999")
	require.False(t, conn.IsClosed())
}

// TestWSB_RejectUnknownTag_Gets406: as ack, for basic.reject.
func TestWSB_RejectUnknownTag_Gets406(t *testing.T) {
	_, addr := startWSBServer(t, nil, nil)

	conn, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	closeCh := ch.NotifyClose(make(chan *amqp.Error, 1))

	require.NoError(t, ch.Reject(999, false))
	expectChannelErrorCode(t, closeCh, 406, "unknown delivery tag 999")
	require.False(t, conn.IsClosed())
}

// TestWSB_AckOnTornDownChannel_Ignored: an ack on a channel absent from
// conn.Channels is a legitimate reordering (the reader diverts acks to
// AckQueue while channel.close goes through FrameQueue) — it must be ignored,
// not fatal.
func TestWSB_AckOnTornDownChannel_Ignored(t *testing.T) {
	srv := newAckTestServer(newAckTestBroker())
	conn, frameCh := newPipeConnWithFrames(t)

	// Channel 7 is deliberately NOT registered.
	err := srv.processAckFrame(conn, encodeAckFrame(t, 7, 1, false))
	require.NoError(t, err, "ack on a missing channel is ignored")
	require.False(t, conn.Closed.Load())

	select {
	case f := <-frameCh:
		t.Fatalf("no frame expected, got %v", f)
	default:
	}
}

// TestWSB_CancelUnknownConsumer_GetsCancelOK: basic.cancel for an unknown
// consumer tag replies with cancel-ok (RabbitMQ parity) instead of hanging.
func TestWSB_CancelUnknownConsumer_GetsCancelOK(t *testing.T) {
	_, addr := startWSBServer(t, nil, nil)

	conn, err := amqp.Dial("amqp://guest:guest@" + addr + "/")
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)

	errCh := make(chan error, 1)
	go func() { errCh <- ch.Cancel("nope", false) }()
	select {
	case err := <-errCh:
		require.NoError(t, err, "cancel of an unknown consumer must return cancel-ok")
	case <-time.After(2 * time.Second):
		t.Fatal("basic.cancel hung (today it never replies)")
	}
}
