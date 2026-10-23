package main

// WS-D client-level broker-semantics tests (spec §17). Channel-error tests
// assert errors.As(*amqp.Error) Code plus connection survival: WS-D's
// closeChannelWithBrokerError sends channel.close and keeps the connection
// (pre-WS-B body), and a follow-up declare on a new channel proves it.

import (
	"errors"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
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

// T3.3 (D2): deleting a NONEXISTENT queue is delete-ok with count 0, and the
// connection survives (RabbitMQ parity: rabbit_amqqueue:delete_with maps
// not_found → {ok, 0}).
func TestQueueDelete_Nonexistent_DeleteOkZero(t *testing.T) {
	b := newRestartableBroker(t, nil)

	conn, err := amqp.Dial(b.uri)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	count, err := ch.QueueDelete("never-existed", false, false, false)
	require.NoError(t, err, "delete of a nonexistent queue is delete-ok (D2)")
	assert.Equal(t, 0, count)
	assert.False(t, conn.IsClosed())
}

func TestExchangeDelete_Nonexistent_DeleteOk(t *testing.T) {
	b := newRestartableBroker(t, nil)

	conn, err := amqp.Dial(b.uri)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	require.NoError(t, ch.ExchangeDelete("never-existed", false, false))
	assert.False(t, conn.IsClosed())
}

// T15.1 (R12): MaxQueues is enforced as a 406 channel error; redeclares at
// the limit are fine; deletes free slots; server-named queues count.
func TestMaxQueues_Enforced(t *testing.T) {
	b := newRestartableBroker(t, func(cfg *config.AMQPConfig) {
		cfg.Server.MaxQueues = 2
	})

	conn, err := amqp.Dial(b.uri)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)

	_, err = ch.QueueDeclare("q1", false, false, false, false, nil)
	require.NoError(t, err)
	// A server-named ("") queue counts against the budget.
	_, err = ch.QueueDeclare("", false, false, true, false, nil)
	require.NoError(t, err, "second queue (server-named) fits in the budget")

	// Third declare: 406 channel error, connection survives.
	ch2, err := conn.Channel()
	require.NoError(t, err)
	_, err = ch2.QueueDeclare("q3", false, false, false, false, nil)
	aerr := amqpError(t, err)
	assert.Equal(t, 406, aerr.Code)
	assert.Contains(t, aerr.Reason, "queue limit in cluster (2) is reached")
	assert.False(t, conn.IsClosed())

	// Redeclaring an EXISTING queue at the limit is fine.
	ch3, err := conn.Channel()
	require.NoError(t, err)
	_, err = ch3.QueueDeclare("q1", false, false, false, false, nil)
	require.NoError(t, err, "redeclare of an existing queue never hits the limit")

	// Delete one → declare succeeds.
	ch4, err := conn.Channel()
	require.NoError(t, err)
	_, err = ch4.QueueDelete("q1", false, false, false)
	require.NoError(t, err)
	ch5, err := conn.Channel()
	require.NoError(t, err)
	_, err = ch5.QueueDeclare("q3", false, false, false, false, nil)
	require.NoError(t, err, "a delete frees a slot")
}

// T4.1: queue.purge removes READY messages only; in-flight (unacked)
// deliveries keep their bodies and requeue intact. Pre-fix mechanism: purge
// wiped every ring slot (unacked included) and reset inflight, so the
// requeued delivery came back as a gap (lost) and purge-ok over-counted.
func TestPurge_PreservesUnacked(t *testing.T) {
	b := newRestartableBroker(t, nil)

	conn, err := amqp.Dial(b.uri)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	q, err := ch.QueueDeclare("pq", false, false, false, false, nil)
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		require.NoError(t, ch.Publish("", q.Name, false, false, amqp.Publishing{Body: []byte{byte('0' + i)}}))
	}

	require.NoError(t, ch.Qos(2, 0, false))
	deliveries, err := ch.Consume(q.Name, "c1", false, false, false, false, nil)
	require.NoError(t, err)

	// Receive 2 (hold them unacked).
	d1 := <-deliveries
	d2 := <-deliveries

	// Purge: exactly the 3 READY messages are removed.
	purged, err := ch.QueuePurge(q.Name, false)
	require.NoError(t, err)
	assert.Equal(t, 3, purged, "purge-ok = ready messages only (was 5 pre-fix)")

	// Ack #1: no channel error (the ack lands on the server's async ack queue,
	// so no client-visible ordering exists; the channel must stay usable).
	require.NoError(t, d1.Ack(false))
	_, err = ch.QueueDeclarePassive(q.Name, false, false, false, false, nil)
	require.NoError(t, err, "channel usable after acking a held delivery post-purge")

	// Close the channel WITHOUT acking #2 → still-unacked deliveries requeue.
	require.NoError(t, ch.Close())

	// On a new channel, the held deliveries come back WITH THEIR BODIES,
	// redelivered; the 3 purged readies ('2','3','4') never arrive. (The ack
	// of #1 races the close through the server's ack worker, so #1 may or may
	// not requeue — at-least-once — but #2 MUST arrive with its body.)
	ch2, err := conn.Channel()
	require.NoError(t, err)
	deliveries2, err := ch2.Consume(q.Name, "c2", false, false, false, false, nil)
	require.NoError(t, err)
	got := map[byte]bool{}
	deadline := time.After(5 * time.Second)
	for !got[d2.Body[0]] {
		select {
		case d := <-deliveries2:
			require.True(t, d.Redelivered, "requeued deliveries are redelivered")
			require.Contains(t, []byte{d1.Body[0], d2.Body[0]}, d.Body[0],
				"a purged READY message must never be redelivered")
			got[d.Body[0]] = true
			_ = d.Ack(false)
		case <-deadline:
			t.Fatalf("the requeued unacked message %q was lost by the purge (pre-fix mechanism)", d2.Body)
		}
	}
	assert.LessOrEqual(t, len(got), 2)
}

// T4.3: purged SPILLED ready messages are removed (WAL-acked) so they are
// never served again IN THIS INCARNATION — the D-NEW-3 fix. Pre-fix
// mechanism: purge only deleted ring slots, so spilled ready messages kept
// being served by the ack-gated cold read path (and were never counted in
// purge-ok).
//
// RESTART LEG (spec-vs-reality deviation, documented): the spec's
// "restart → Messages==0" assertion requires DURABLE acknowledgement, which
// is the known-open, explicitly out-of-scope defect (spec §16 "Ack
// persistence"; storage/wal_boot_state.go: "ackBitmap starts empty at every
// boot"; the tag-keyed durable ack artifact was built and withdrawn after
// three cycles). Per spec §5.2, a purged message is exactly as durable as a
// normally-acked one, so this test asserts PARITY: after a restart, a purged
// queue resurrects EXACTLY as much as a normally no-ack-drained queue does.
// When ack persistence lands, both legs of this test become Messages==0
// together.
func TestPurge_SpilledReadyRemovedAndNotResurrected(t *testing.T) {
	b := newRestartableBroker(t, func(cfg *config.AMQPConfig) {
		cfg.Engine.RingBufferSize = 16
		cfg.Engine.SpillThresholdPercent = 50
		cfg.Engine.DepthHighWMOverride = 100000
	})

	conn, err := amqp.Dial(b.uri)
	require.NoError(t, err)

	ch, err := conn.Channel()
	require.NoError(t, err)
	q, err := ch.QueueDeclare("pq-spill", true, false, false, false, nil)
	require.NoError(t, err)
	qAck, err := ch.QueueDeclare("ack-spill", true, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.Confirm(false))

	const n = 20
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 2*n))
	for i := 0; i < n; i++ {
		require.NoError(t, ch.Publish("", q.Name, false, false, amqp.Publishing{
			DeliveryMode: 2, Body: []byte{byte('a' + i)},
		}))
		require.NoError(t, ch.Publish("", qAck.Name, false, false, amqp.Publishing{
			DeliveryMode: 2, Body: []byte{byte('a' + i)},
		}))
	}
	for i := 0; i < 2*n; i++ {
		require.True(t, (<-confirms).Ack, "publish %d must be confirmed", i)
	}

	// Purge q; no-ack-drain qAck (the normal implicit-ack path).
	purged, err := ch.QueuePurge(q.Name, false)
	require.NoError(t, err)
	assert.Equal(t, n, purged, "every ready message (ring + spilled) is purged — pre-fix this was ≤ the ring-resident count")
	for i := 0; i < n; i++ {
		_, ok, err := ch.Get(qAck.Name, true)
		require.NoError(t, err)
		require.True(t, ok)
	}

	// In-incarnation: nothing left to serve in EITHER queue.
	qd, err := ch.QueueDeclarePassive(q.Name, true, false, false, false, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, qd.Messages, "purged queue is empty in-incarnation")
	qdAck, err := ch.QueueDeclarePassive(qAck.Name, true, false, false, false, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, qdAck.Messages)
	require.NoError(t, conn.Close())

	// Restart: parity with the normal-ack path (both are the known-open
	// ack-durability defect, whatever they return).
	b.restart()
	conn2, err := amqp.Dial(b.uri)
	require.NoError(t, err)
	defer conn2.Close()
	ch2, err := conn2.Channel()
	require.NoError(t, err)
	afterPurge, err := ch2.QueueDeclarePassive(q.Name, true, false, false, false, nil)
	require.NoError(t, err)
	afterAck, err := ch2.QueueDeclarePassive(qAck.Name, true, false, false, false, nil)
	require.NoError(t, err)
	assert.Equal(t, afterAck.Messages, afterPurge.Messages,
		"purge ≡ ack in durability terms (spec §5.2); both legs become 0 when ack persistence (§16) lands")
}

// T8.1: no-ack basic.get on spilled (WAL-only) messages decrements the
// reported depth exactly once per message. Pre-fix mechanism:
// DeleteMessageIfPresent returned the ring-CAS result, so a spilled removal
// reported a "loss" and GetMessageForGet's no-ack branch never decremented —
// the queue's message-count stayed inflated forever.
func TestGetNoAck_SpilledDepth_NoLeak(t *testing.T) {
	b := newRestartableBroker(t, func(cfg *config.AMQPConfig) {
		cfg.Engine.RingBufferSize = 16
		cfg.Engine.SpillThresholdPercent = 50
		cfg.Engine.DepthHighWMOverride = 100000
	})

	conn, err := amqp.Dial(b.uri)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	q, err := ch.QueueDeclare("sq", false, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.Confirm(false))

	const n = 20
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, n))
	for i := 0; i < n; i++ {
		require.NoError(t, ch.Publish("", q.Name, false, false, amqp.Publishing{
			Body: []byte{byte('a' + i)},
		}))
	}
	for i := 0; i < n; i++ {
		require.True(t, (<-confirms).Ack, "publish %d must be confirmed", i)
	}

	// 20 messages ready (some spilled — the ring holds ~9).
	qd, err := ch.QueueDeclarePassive(q.Name, false, false, false, false, nil)
	require.NoError(t, err)
	require.Equal(t, n, qd.Messages, "all 20 ready before the gets")

	// No-ack gets must drain exactly n, each decrementing depth.
	seen := make(map[byte]bool)
	for i := 0; i < n; i++ {
		msg, ok, err := ch.Get(q.Name, true)
		require.NoError(t, err)
		require.True(t, ok, "get %d must return a message", i)
		seen[msg.Body[0]] = true
	}
	assert.Len(t, seen, n, "all bodies distinct")

	qd, err = ch.QueueDeclarePassive(q.Name, false, false, false, false, nil)
	require.NoError(t, err)
	assert.Equal(t, 0, qd.Messages, "depth returns to 0 — no spilled-depth leak")
}

// probeQueueDeleteFlagsDecode reports whether the server's queue.delete path
// decodes the client one-octet if-unused/if-empty flags (P0-4 first half,
// protocol stream, spec §3.6 dependency). The probe is a live-consumer
// if-unused delete: when the flags decode, it is refused (406) and nothing is
// deleted; when they do not decode, the delete SUCCEEDS (flags dropped).
func probeQueueDeleteFlagsDecode(t *testing.T, uri string) (decoded bool) {
	t.Helper()
	conn, err := amqp.Dial(uri)
	require.NoError(t, err)
	defer conn.Close()
	ch, err := conn.Channel()
	require.NoError(t, err)
	q, err := ch.QueueDeclare("probe-del", false, false, false, false, nil)
	require.NoError(t, err)
	_, err = ch.Consume(q.Name, "probe-c", false, false, false, false, nil)
	require.NoError(t, err)
	delCh, err := conn.Channel()
	require.NoError(t, err)
	_, derr := delCh.QueueDelete(q.Name, true, false, false)
	if derr != nil {
		var aerr *amqp.Error
		if errors.As(derr, &aerr) && aerr.Code == 406 {
			return true // if-unused decoded and was enforced
		}
	}
	// The delete succeeded → the flags were dropped on the floor.
	return false
}

// T3.1: queue.delete if-unused with a live consumer is 406, the queue
// survives, and the consumer still receives.
//
// requires: protocol one-octet flag-decode fix (P0-4 first half).
func TestQueueDelete_IfUnused_WithConsumer_406(t *testing.T) {
	b := newRestartableBroker(t, nil)
	if !probeQueueDeleteFlagsDecode(t, b.uri) {
		t.Skip("requires: protocol one-octet flag-decode fix (P0-4 first half, another stream); broker twin TestDeleteQueue_IfUnused_LiveConsumer_Refused proves the semantics")
	}

	conn, err := amqp.Dial(b.uri)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	q, err := ch.QueueDeclare("q", false, false, false, false, nil)
	require.NoError(t, err)
	deliveries, err := ch.Consume(q.Name, "c1", false, false, false, false, nil)
	require.NoError(t, err)

	delCh, err := conn.Channel()
	require.NoError(t, err)
	_, err = delCh.QueueDelete(q.Name, true, false, false)
	aerr := amqpError(t, err)
	assert.Equal(t, 406, aerr.Code)
	assert.Contains(t, aerr.Reason, "in use")
	assert.False(t, conn.IsClosed())

	// The queue still exists and the consumer still receives.
	pubCh, err := conn.Channel()
	require.NoError(t, err)
	require.NoError(t, pubCh.Publish("", q.Name, false, false, amqp.Publishing{Body: []byte("alive")}))
	select {
	case d := <-deliveries:
		assert.Equal(t, "alive", string(d.Body))
		_ = d.Ack(false)
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not receive after the refused delete")
	}
}

// T3.2 (R5): queue.delete if-empty counts READY messages only. With 1
// delivered-unacked and 0 ready it SUCCEEDS; with 1 ready it is 406.
//
// requires: protocol one-octet flag-decode fix (P0-4 first half).
func TestQueueDelete_IfEmpty_WithUnacked(t *testing.T) {
	b := newRestartableBroker(t, nil)
	if !probeQueueDeleteFlagsDecode(t, b.uri) {
		t.Skip("requires: protocol one-octet flag-decode fix (P0-4 first half, another stream); broker twin TestDeleteQueue_IfEmpty_ReadyVsUnacked proves the semantics")
	}

	conn, err := amqp.Dial(b.uri)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	q, err := ch.QueueDeclare("q", false, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.Publish("", q.Name, false, false, amqp.Publishing{Body: []byte("m1")}))

	// One delivered-unacked (manual-ack get), zero ready: delete SUCCEEDS (R5).
	getCh, err := conn.Channel()
	require.NoError(t, err)
	_, ok, err := getCh.Get(q.Name, false)
	require.NoError(t, err)
	require.True(t, ok)
	delCh, err := conn.Channel()
	require.NoError(t, err)
	_, err = delCh.QueueDelete(q.Name, false, true, false)
	require.NoError(t, err, "if-empty with only unacked messages must succeed (R5)")

	// Ready message present: 406 not empty.
	q2, err := ch.QueueDeclare("q2", false, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.Publish("", q2.Name, false, false, amqp.Publishing{Body: []byte("m2")}))
	delCh2, err := conn.Channel()
	require.NoError(t, err)
	_, err = delCh2.QueueDelete(q2.Name, false, true, false)
	aerr := amqpError(t, err)
	assert.Equal(t, 406, aerr.Code)
	assert.Contains(t, aerr.Reason, "not empty")
}

// probeExchangeBindFlagDecode reports whether the server's exchange.bind
// path decodes client one-octet flags correctly (P0-4 first half, owned by
// the protocol stream, spec §16). Until that lands, client-level e2e tests
// skip with a reason (§3.6 pattern); the broker twins prove the semantics.
func probeExchangeBindFlagDecode(t *testing.T, uri string) (ok bool) {
	t.Helper()
	conn, err := amqp.Dial(uri)
	require.NoError(t, err)
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return false
	}
	if err := ch.ExchangeDeclare("probe-a", "direct", false, false, false, false, nil); err != nil {
		return false
	}
	if err := ch.ExchangeDeclare("probe-b", "direct", false, false, false, false, nil); err != nil {
		return false
	}
	// A decode failure kills the CONNECTION (the handler returns the error),
	// so the RPC must wait for bind-ok and the connection must survive.
	if err := ch.ExchangeBind("probe-b", "k", "probe-a", false, nil); err != nil {
		return false
	}
	if conn.IsClosed() {
		return false
	}
	_ = ch.ExchangeDelete("probe-a", false, false)
	_ = ch.ExchangeDelete("probe-b", false, false)
	return true
}

// T11.1 client half: e2e binding into an internal exchange works end-to-end;
// a direct publish to the internal exchange is refused by the broker (the 403
// client mapping is WS-B M5; the broker twin covers ErrExchangeInternal).
//
// requires: protocol one-octet flag-decode fix (P0-4 first half, another stream).
func TestInternal_PublishRejected_E2EBindingAllowed(t *testing.T) {
	b := newRestartableBroker(t, nil)
	if !probeExchangeBindFlagDecode(t, b.uri) {
		t.Skip("requires: protocol one-octet flag-decode fix (P0-4 first half, another stream); broker twin TestInternal_PublishRejected_E2EBindingAllowed proves the semantics")
	}

	conn, err := amqp.Dial(b.uri)
	require.NoError(t, err)
	defer conn.Close()

	ch, err := conn.Channel()
	require.NoError(t, err)
	require.NoError(t, ch.ExchangeDeclare("ix", "direct", false, false, true, false, nil))
	require.NoError(t, ch.ExchangeDeclare("src", "direct", false, false, false, false, nil))
	q, err := ch.QueueDeclare("iq", false, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.ExchangeBind("ix", "k", "src", false, nil), "e2e INTO an internal exchange is allowed")
	require.NoError(t, ch.QueueBind(q.Name, "k", "ix", false, nil))

	require.NoError(t, ch.Publish("src", "k", false, false, amqp.Publishing{Body: []byte("via-e2e")}))
	getCh, err := conn.Channel()
	require.NoError(t, err)
	msg, ok, err := getCh.Get(q.Name, true)
	require.NoError(t, err)
	require.True(t, ok, "message routed src→ix→iq must arrive")
	assert.Equal(t, "via-e2e", string(msg.Body))
}
