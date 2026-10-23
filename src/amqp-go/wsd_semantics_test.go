package main

// WS-D client-level broker-semantics tests (spec §17). Channel-error tests
// assert errors.As(*amqp.Error) Code plus connection survival: WS-D's
// closeChannelWithBrokerError sends channel.close and keeps the connection
// (pre-WS-B body), and a follow-up declare on a new channel proves it.

import (
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// amqpError extracts the *amqp.Error from a client call error.
func amqpError(t *testing.T, err error) *amqp.Error {
	t.Helper()
	var aerr *amqp.Error
	require.True(t, errors.As(err, &aerr), "expected *amqp.Error, got %T: %v", err, err)
	return aerr
}

// T1.1: passive declare of a missing queue is a 404 channel error, creates
// nothing, and the connection survives. Pre-fix mechanism: the handler
// consulted Passive only for the reserved-name check and then CREATED the
// queue via DeclareQueue.
func TestPassiveDeclare_MissingQueue_404ChannelClose(t *testing.T) {
	b := newRestartableBroker(t, nil)

	conn, err := amqp.Dial(b.uri)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	_, err = ch.QueueDeclarePassive("nope", false, false, false, false, nil)
	aerr := amqpError(t, err)
	assert.Equal(t, 404, aerr.Code)
	assert.Contains(t, aerr.Reason, "no queue 'nope'")
	assert.False(t, conn.IsClosed(), "channel error must not kill the connection")

	// A follow-up passive declare on a NEW channel must STILL 404 — proving
	// the first attempt created nothing.
	ch2, err := conn.Channel()
	require.NoError(t, err)
	_, err = ch2.QueueDeclarePassive("nope", false, false, false, false, nil)
	aerr = amqpError(t, err)
	assert.Equal(t, 404, aerr.Code)

	// And the connection is still usable.
	ch3, err := conn.Channel()
	require.NoError(t, err)
	_, err = ch3.QueueDeclare("survives", false, false, false, false, nil)
	require.NoError(t, err)
}

// T1.2: passive declare of a missing exchange is a 404 channel error.
func TestPassiveDeclare_MissingExchange_404ChannelClose(t *testing.T) {
	b := newRestartableBroker(t, nil)

	conn, err := amqp.Dial(b.uri)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	err = ch.ExchangeDeclarePassive("noex", "direct", false, false, false, false, nil)
	aerr := amqpError(t, err)
	assert.Equal(t, 404, aerr.Code)
	assert.Contains(t, aerr.Reason, "no exchange 'noex'")
	assert.False(t, conn.IsClosed())

	ch2, err := conn.Channel()
	require.NoError(t, err)
	err = ch2.ExchangeDeclarePassive("noex", "direct", false, false, false, false, nil)
	aerr = amqpError(t, err)
	assert.Equal(t, 404, aerr.Code, "second passive declare must still 404 (nothing was created)")
}

// T1.3 (D1): an EMPTY passive queue name is NOT resolved to the last-declared
// queue (queue.declare is not in RabbitMQ's expand_shortcuts, R1/R2): it is a
// plain 404 for queue ”. No amq.gen-* queue may be created.
func TestPassiveDeclare_EmptyName_404_NoGeneratedQueue(t *testing.T) {
	b := newRestartableBroker(t, nil)

	conn, err := amqp.Dial(b.uri)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	// Declare a real queue first, so a "last-declared" resolution would have
	// something to resolve to if the bug were present.
	_, err = ch.QueueDeclare("real-q", false, false, false, false, nil)
	require.NoError(t, err)

	before := b.srv.Broker.GetQueues()
	_, err = ch.QueueDeclarePassive("", false, false, false, false, nil)
	aerr := amqpError(t, err)
	assert.Equal(t, 404, aerr.Code, "empty passive name must 404, not resolve or generate (D1)")
	after := b.srv.Broker.GetQueues()
	assert.Equal(t, len(before), len(after), "no amq.gen-* queue may be created by a passive declare")
	for name := range after {
		assert.Contains(t, before, name)
	}
}

// T1.4: passive declare on an EXISTING queue with different flags/args is a
// declare-ok carrying the REAL counts (R1: the passive clause never calls
// assert_equivalence).
func TestPassiveDeclare_Existing_DifferentArgs_DeclareOk(t *testing.T) {
	b := newRestartableBroker(t, nil)

	conn, err := amqp.Dial(b.uri)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	_, err = ch.QueueDeclare("q", true, false, false, false, amqp.Table{"x-message-ttl": int32(1000)})
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		require.NoError(t, ch.Publish("", "q", false, false, amqp.Publishing{Body: []byte("m")}))
	}

	// Passive declare with DIFFERENT flags and args: declare-ok, real counts.
	ch2, err := conn.Channel()
	require.NoError(t, err)
	q, err := ch2.QueueDeclarePassive("q", false, true, true, false, amqp.Table{"x-message-ttl": int32(5)})
	require.NoError(t, err, "passive declare ignores flags/args entirely (R1)")
	assert.Equal(t, "q", q.Name)
	assert.Equal(t, 2, q.Messages, "real ready count")
	assert.Equal(t, 0, q.Consumers)
}

// T1.5: passive declare of an exclusive queue owned by ANOTHER connection is
// 405 RESOURCE_LOCKED (R1 check_exclusive_access, lax).
func TestPassiveDeclare_ExclusiveOtherConnection_405(t *testing.T) {
	b := newRestartableBroker(t, nil)

	connA, err := amqp.Dial(b.uri)
	require.NoError(t, err)
	defer connA.Close()
	chA, err := connA.Channel()
	require.NoError(t, err)
	_, err = chA.QueueDeclare("excl-q", false, false, true, false, nil)
	require.NoError(t, err)

	connB, err := amqp.Dial(b.uri)
	require.NoError(t, err)
	defer connB.Close()
	chB, err := connB.Channel()
	require.NoError(t, err)
	_, err = chB.QueueDeclarePassive("excl-q", false, false, false, false, nil)
	aerr := amqpError(t, err)
	assert.Equal(t, 405, aerr.Code)
	assert.Contains(t, aerr.Reason, "exclusive access")
	assert.False(t, connB.IsClosed())

	// Same connection CAN passive-declare its own exclusive queue.
	_, err = chA.QueueDeclarePassive("excl-q", false, false, true, false, nil)
	require.NoError(t, err)
}

// T1.6: declaring the default exchange "" is a 403 (R4
// check_not_default_exchange), and deleting an amq.* exchange is a 403 that
// leaves it routing (R4 check_exchange_deletion).
func TestExchangeDeclare_DefaultExchange_403(t *testing.T) {
	b := newRestartableBroker(t, nil)

	conn, err := amqp.Dial(b.uri)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	err = ch.ExchangeDeclare("", "direct", false, false, false, false, nil)
	aerr := amqpError(t, err)
	assert.Equal(t, 403, aerr.Code)
	assert.Contains(t, aerr.Reason, "operation not permitted on the default exchange")
	assert.False(t, conn.IsClosed())
}

func TestExchangeDelete_AmqPrefix_403(t *testing.T) {
	b := newRestartableBroker(t, nil)

	conn, err := amqp.Dial(b.uri)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	err = ch.ExchangeDelete("amq.direct", false, false)
	aerr := amqpError(t, err)
	assert.Equal(t, 403, aerr.Code)
	assert.Contains(t, aerr.Reason, "deletion of system exchange 'amq.direct' in vhost '/' not allowed")
	assert.False(t, conn.IsClosed())

	// amq.direct still routes afterwards.
	ch2, err := conn.Channel()
	require.NoError(t, err)
	q, err := ch2.QueueDeclare("route-check", false, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch2.QueueBind(q.Name, "rk", "amq.direct", false, nil))
	require.NoError(t, ch2.Publish("amq.direct", "rk", false, false, amqp.Publishing{Body: []byte("x")}))
	getCh, err := conn.Channel()
	require.NoError(t, err)
	msg, ok, err := getCh.Get(q.Name, true)
	require.NoError(t, err)
	require.True(t, ok, "amq.direct must still route after the refused delete")
	assert.Equal(t, "x", string(msg.Body))
}

// T2.1 client-level: redeclare inequivalence is a 406 channel error, and the
// connection survives (the old generic error killed the connection).
func TestRedeclare_InequivalentArgs_406(t *testing.T) {
	b := newRestartableBroker(t, nil)

	conn, err := amqp.Dial(b.uri)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	_, err = ch.QueueDeclare("q", true, false, false, false, amqp.Table{"x-message-ttl": int32(1000)})
	require.NoError(t, err)

	cases := []struct {
		name     string
		declare  func(ch *amqp.Channel) error
		contains string
	}{
		{"durable diff", func(ch *amqp.Channel) error {
			_, err := ch.QueueDeclare("q", false, false, false, false, amqp.Table{"x-message-ttl": int32(1000)})
			return err
		}, "inequivalent arg 'durable' for queue 'q' in vhost '/': received 'false' but current is 'true'"},
		{"auto_delete diff", func(ch *amqp.Channel) error {
			_, err := ch.QueueDeclare("q", true, true, false, false, amqp.Table{"x-message-ttl": int32(1000)})
			return err
		}, "inequivalent arg 'auto_delete'"},
		{"ttl diff", func(ch *amqp.Channel) error {
			_, err := ch.QueueDeclare("q", true, false, false, false, amqp.Table{"x-message-ttl": int32(2000)})
			return err
		}, "inequivalent arg 'x-message-ttl' for queue 'q' in vhost '/': received the value '2000' of type 'signedint'"},
		{"ttl removed", func(ch *amqp.Channel) error {
			_, err := ch.QueueDeclare("q", true, false, false, false, nil)
			return err
		}, "received none"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			chX, err := conn.Channel()
			require.NoError(t, err)
			err = tc.declare(chX)
			aerr := amqpError(t, err)
			assert.Equal(t, 406, aerr.Code)
			assert.Contains(t, aerr.Reason, tc.contains)
			assert.False(t, conn.IsClosed(), "406 is a channel error; the connection survives")
		})
	}

	// Exchange redeclare: type diff.
	chE, err := conn.Channel()
	require.NoError(t, err)
	require.NoError(t, chE.ExchangeDeclare("tex", "direct", true, false, false, false, nil))
	chE2, err := conn.Channel()
	require.NoError(t, err)
	err = chE2.ExchangeDeclare("tex", "fanout", true, false, false, false, nil)
	aerr := amqpError(t, err)
	assert.Equal(t, 406, aerr.Code)
	assert.Contains(t, aerr.Reason, "inequivalent arg 'type' for exchange 'tex'")
}

// T2.4 client-level: exclusive-property mismatch on redeclare is 405 (D4).
func TestRedeclare_ExclusiveMismatch_405(t *testing.T) {
	b := newRestartableBroker(t, nil)

	conn, err := amqp.Dial(b.uri)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	_, err = ch.QueueDeclare("qx", false, false, true, false, nil)
	require.NoError(t, err)

	ch2, err := conn.Channel()
	require.NoError(t, err)
	_, err = ch2.QueueDeclare("qx", false, false, false, false, nil)
	aerr := amqpError(t, err)
	assert.Equal(t, 405, aerr.Code)
	assert.Contains(t, aerr.Reason, "exclusive access")
	assert.False(t, conn.IsClosed(), "405 is a channel error; the connection survives")
}
