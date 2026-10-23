//go:build conformance

package main

// WS-A end-to-end conformance: the client paths the protocol-codec fixes
// (WI-1/WI-2/WI-3) unlock, exercised with a real amqp091-go client against
// the embedded StrangeQ server (or RabbitMQ under AMQP_TARGET=rabbitmq).

import (
	"errors"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

// queue.bind with a non-empty arguments table used to fail decoding outright;
// now a headers binding routes.
func TestConformance_WSA_QueueBindWithArgumentsRoutesHeaders(t *testing.T) {
	b := newConfBroker(t)
	defer b.cleanup()
	_, ch := b.dial(t)

	ex := uniqueName("wsa.headers")
	require.NoError(t, ch.ExchangeDeclare(ex, "headers", true, false, false, false, nil))
	t.Cleanup(func() { _ = ch.ExchangeDelete(ex, false, false) })
	q := uniqueName("wsa.bindargs")
	declareDurable(t, ch, q, nil)

	err := ch.QueueBind(q, "", ex, false, amqp.Table{"x-match": "all", "k": "v"})
	require.NoError(t, err, "queue.bind with arguments must decode and succeed")

	require.NoError(t, ch.PublishWithContext(t.Context(), ex, "", false, false,
		amqp.Publishing{Headers: amqp.Table{"k": "v"}, Body: []byte("match")}))
	require.NoError(t, ch.PublishWithContext(t.Context(), ex, "", false, false,
		amqp.Publishing{Headers: amqp.Table{"k": "x"}, Body: []byte("nomatch")}))

	d := consumeOne(t, ch, q, 5*time.Second)
	require.Equal(t, []byte("match"), d.Body)
	require.NoError(t, d.Ack(false))

	if _, ok := getOne(t, ch, q, 500*time.Millisecond); ok {
		t.Fatal("the k=x message must not route through an x-match=all k=v binding")
	}
}

// queue.unbind carries an arguments table (and NO no-wait octet): an
// argument-qualified unbind removes only the binding whose arguments match.
func TestConformance_WSA_QueueUnbindArgumentQualified(t *testing.T) {
	b := newConfBroker(t)
	defer b.cleanup()
	_, ch := b.dial(t)

	ex := uniqueName("wsa.headers")
	require.NoError(t, ch.ExchangeDeclare(ex, "headers", true, false, false, false, nil))
	t.Cleanup(func() { _ = ch.ExchangeDelete(ex, false, false) })
	q := uniqueName("wsa.unbindargs")
	declareDurable(t, ch, q, nil)

	bindingA := amqp.Table{"x-match": "all", "k": "v"}
	require.NoError(t, ch.QueueBind(q, "", ex, false, bindingA))

	// Unbind with DIFFERENT arguments: must succeed (unbind of a nonexistent
	// binding replies unbind-ok) and must NOT remove the real binding.
	err := ch.QueueUnbind(q, "", ex, amqp.Table{"x-match": "all", "k": "w"})
	require.NoError(t, err, "unbind with non-matching arguments is a no-op, not an error")
	lockLog(t, "unbind-nonmatching-args-ok", expectByTarget(t, "unbind-nonmatching-args-ok", true, true))

	require.NoError(t, ch.PublishWithContext(t.Context(), ex, "", false, false,
		amqp.Publishing{Headers: amqp.Table{"k": "v"}, Body: []byte("still-bound")}))
	d := consumeOne(t, ch, q, 5*time.Second)
	require.Equal(t, []byte("still-bound"), d.Body, "binding with different arguments must survive")
	require.NoError(t, d.Ack(false))

	// Unbind with the MATCHING arguments: now the binding goes away.
	require.NoError(t, ch.QueueUnbind(q, "", ex, bindingA))
	require.NoError(t, ch.PublishWithContext(t.Context(), ex, "", false, false,
		amqp.Publishing{Headers: amqp.Table{"k": "v"}, Body: []byte("unbound")}))
	if _, ok := getOne(t, ch, q, 500*time.Millisecond); ok {
		t.Fatal("after unbind with matching arguments nothing may be delivered")
	}
}

// exchange.bind/unbind never even decoded before WI-1; and until WI-3 the
// unbind-ok reply carried the wrong method id (40.41), which amqp091-go
// rejects with 501. This exercises the full round trip including routing.
func TestConformance_WSA_ExchangeBindUnbindRouting(t *testing.T) {
	b := newConfBroker(t)
	defer b.cleanup()
	_, ch := b.dial(t)

	src := uniqueName("wsa.src")
	require.NoError(t, ch.ExchangeDeclare(src, "direct", true, false, false, false, nil))
	t.Cleanup(func() { _ = ch.ExchangeDelete(src, false, false) })
	dst := uniqueName("wsa.dst")
	require.NoError(t, ch.ExchangeDeclare(dst, "direct", true, false, false, false, nil))
	t.Cleanup(func() { _ = ch.ExchangeDelete(dst, false, false) })
	q := uniqueName("wsa.e2e")
	declareDurable(t, ch, q, nil)
	require.NoError(t, ch.QueueBind(q, "r", dst, false, nil))

	require.NoError(t, ch.ExchangeBind(dst, "r", src, false, nil), "exchange.bind must decode and succeed")
	require.NoError(t, ch.PublishWithContext(t.Context(), src, "r", false, false,
		amqp.Publishing{Body: []byte("via-e2e")}))
	d := consumeOne(t, ch, q, 5*time.Second)
	require.Equal(t, []byte("via-e2e"), d.Body, "message published to src must reach q via dst")
	require.NoError(t, d.Ack(false))

	require.NoError(t, ch.ExchangeUnbind(dst, "r", src, false, nil), "exchange.unbind must decode; unbind-ok must be 40.51")
	require.NoError(t, ch.PublishWithContext(t.Context(), src, "r", false, false,
		amqp.Publishing{Body: []byte("no-route")}))
	if _, ok := getOne(t, ch, q, 500*time.Millisecond); ok {
		t.Fatal("after exchange.unbind nothing may route src -> dst")
	}
}

// queue.delete if-empty used to decode as false (silent data loss). Now the
// refusal fires. Until WS-B's typed channel errors land, StrangeQ reports the
// refusal by tearing down the connection; RabbitMQ replies 406.
func TestConformance_WSA_QueueDeleteIfEmptyRefusesNonEmpty(t *testing.T) {
	b := newConfBroker(t)
	defer b.cleanup()
	_, ch := b.dial(t)

	q := uniqueName("wsa.ifempty")
	_, err := ch.QueueDeclare(q, true, false, false, false, nil)
	require.NoError(t, err)

	require.NoError(t, ch.PublishWithContext(t.Context(), "", q, false, false,
		amqp.Publishing{Body: []byte("x")}))
	require.Eventually(t, func() bool { return queueDepth(t, ch, q) == 1 }, 5*time.Second, 100*time.Millisecond)

	_, err = ch.QueueDelete(q, false, true, false)
	require.Error(t, err, "queue.delete if-empty on a non-empty queue must be refused")
	if targetIsRabbit() {
		var ae *amqp.Error
		require.True(t, errors.As(err, &ae), "want *amqp.Error, got %T (%v)", err, err)
		require.Equal(t, uint16(amqp.PreconditionFailed), ae.Code)
	} else {
		t.Logf("DIVERGENCE[if-empty-refusal-code]: strangeq reports the refusal with a connection error until WS-B lands: %v", err)
	}

	// The refusal tore down the old connection (see above); prove on a FRESH
	// connection that the queue AND its message survived (a recreated queue
	// would report depth 0).
	_, ch2 := b.dial(t)
	t.Cleanup(func() { _, _ = ch2.QueueDelete(q, false, false, false) })
	require.Equal(t, 1, queueDepth(t, ch2, q), "refused if-empty delete must leave the queue and its message intact")
}

// exchange.delete if-unused used to decode as false. Same refusal shape as
// the queue case.
func TestConformance_WSA_ExchangeDeleteIfUnusedRefusesBound(t *testing.T) {
	b := newConfBroker(t)
	defer b.cleanup()
	_, ch := b.dial(t)

	ex := uniqueName("wsa.ifunused")
	require.NoError(t, ch.ExchangeDeclare(ex, "direct", true, false, false, false, nil))
	q := uniqueName("wsa.ifunusedq")
	declareDurable(t, ch, q, nil)
	require.NoError(t, ch.QueueBind(q, "r", ex, false, nil))

	err := ch.ExchangeDelete(ex, true, false)
	require.Error(t, err, "exchange.delete if-unused on a bound exchange must be refused")
	if targetIsRabbit() {
		var ae *amqp.Error
		require.True(t, errors.As(err, &ae), "want *amqp.Error, got %T (%v)", err, err)
		require.Equal(t, uint16(amqp.PreconditionFailed), ae.Code)
	} else {
		t.Logf("DIVERGENCE[if-unused-refusal-code]: strangeq reports the refusal with a connection error until WS-B lands: %v", err)
	}

	// Fresh connection: the exchange and its binding must still route.
	_, ch2 := b.dial(t)
	t.Cleanup(func() { _ = ch2.ExchangeDelete(ex, false, false) })
	t.Cleanup(func() { _, _ = ch2.QueueDelete(q, false, false, false) })
	require.NoError(t, ch2.PublishWithContext(t.Context(), ex, "r", false, false,
		amqp.Publishing{Body: []byte("survived")}))
	d := consumeOne(t, ch2, q, 5*time.Second)
	require.Equal(t, []byte("survived"), d.Body, "refused if-unused delete must leave exchange and binding intact")
	require.NoError(t, d.Ack(false))
}

// With no-wait now decoding (WI-1), the server must NOT answer no-wait
// methods. A stray reply desynchronises amqp091-go's RPC state machine: the
// NEXT RPC then fails with ErrCommandInvalid (amqp091-go channel.go).
func TestConformance_WSA_NoWaitMethodsSendNoReply(t *testing.T) {
	b := newConfBroker(t)
	defer b.cleanup()
	_, ch := b.dial(t)

	q := uniqueName("wsa.nowait")
	declareDurable(t, ch, q, nil)

	probe := func(step string) {
		t.Helper()
		_, err := ch.QueueDeclarePassive(q, true, false, false, false, nil)
		require.NoError(t, err, "probe after %s: a stray reply would desync the channel RPC state", step)
	}

	_, err := ch.QueuePurge(q, true) // no-wait
	require.NoError(t, err)
	probe("queue.purge no-wait")

	deliveries, err := ch.Consume(q, "wsa-nw", true, false, false, false, nil)
	require.NoError(t, err)
	require.NotNil(t, deliveries)
	require.NoError(t, ch.Cancel("wsa-nw", true)) // no-wait
	probe("basic.cancel no-wait")

	q2 := uniqueName("wsa.nowait.del")
	_, err = ch.QueueDeclare(q2, false, false, false, false, nil)
	require.NoError(t, err)
	_, err = ch.QueueDelete(q2, false, false, true) // no-wait
	require.NoError(t, err)
	probe("queue.delete no-wait")

	src := uniqueName("wsa.nw.src")
	require.NoError(t, ch.ExchangeDeclare(src, "direct", false, false, false, false, nil))
	t.Cleanup(func() { _ = ch.ExchangeDelete(src, false, false) })
	dst := uniqueName("wsa.nw.dst")
	require.NoError(t, ch.ExchangeDeclare(dst, "direct", false, false, false, false, nil))
	t.Cleanup(func() { _ = ch.ExchangeDelete(dst, false, false) })
	require.NoError(t, ch.ExchangeBind(dst, "k", src, true, nil)) // no-wait
	probe("exchange.bind no-wait")
	require.NoError(t, ch.ExchangeUnbind(dst, "k", src, true, nil)) // no-wait
	probe("exchange.unbind no-wait")
}
