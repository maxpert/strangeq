package main

// WS-D restart-dependent tests (spec §17): recovery self-redeclare reflexivity
// (T2.6) and headers numeric-binding survival (T10.2).

import (
	"strings"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/interfaces"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recoveryFaultsFor returns the unsafe-recovery faults whose artifact names
// the given entity.
func recoveryFaultsFor(s interface {
	UnsafeRecoveryFaults() []*interfaces.RecoveryFault
}, name string) []*interfaces.RecoveryFault {
	var out []*interfaces.RecoveryFault
	for _, f := range s.UnsafeRecoveryFaults() {
		if strings.Contains(f.Artifact, name) {
			out = append(out, f)
		}
	}
	return out
}

// TestRecovery_RedeclareWithArgs_NoDegradedFault (T2.6): a durable queue with
// typed x-args ({x-message-ttl:int32, x-dead-letter-exchange:string,
// x-max-length:int64}) and a durable exchange with alternate-exchange survive
// a restart: recovery's self-redeclare through CBOR-decoded arguments
// (uint64/int64 widths) is reflexive (R7 type-class equivalence), so NO
// recovery fault names them, and a client redeclare with the original int32
// widths succeeds. Pre-fix mechanism: recovery's redeclare never compared
// arguments (known gap), so this pinned nothing; with naive full-table
// comparison (the brief's original ask), the CBOR uint64 vs client int32
// widths would 406 every post-restart redeclare.
func TestRecovery_RedeclareWithArgs_NoDegradedFault(t *testing.T) {
	b := newRestartableBroker(t, nil)

	conn, err := amqp.Dial(b.uri)
	require.NoError(t, err)

	ch, err := conn.Channel()
	require.NoError(t, err)
	require.NoError(t, ch.ExchangeDeclare("ae", "fanout", true, false, false, false, nil))
	require.NoError(t, ch.ExchangeDeclare("dx", "direct", true, false, false, false,
		amqp.Table{"alternate-exchange": "ae"}))
	_, err = ch.QueueDeclare("dq", true, false, false, false, amqp.Table{
		"x-message-ttl":          int32(60000),
		"x-dead-letter-exchange": "ae",
		"x-max-length":           int64(100),
	})
	require.NoError(t, err)
	require.NoError(t, ch.QueueBind("dq", "rk", "dx", false, nil))
	require.NoError(t, ch.Publish("dx", "rk", false, false, amqp.Publishing{
		DeliveryMode: 2, Body: []byte("durable-1"),
	}))
	require.NoError(t, conn.Close())

	b.restart()

	// No recovery fault names either entity (recovery's self-redeclare was
	// reflexive through the CBOR-decoded argument widths).
	require.Empty(t, recoveryFaultsFor(b.srv, "dq"), "no degraded fault for the queue")
	require.Empty(t, recoveryFaultsFor(b.srv, "dx"), "no degraded fault for the exchange")

	conn2, err := amqp.Dial(b.uri)
	require.NoError(t, err)
	defer conn2.Close()
	ch2, err := conn2.Channel()
	require.NoError(t, err)

	// The queue is present with its message.
	qd, err := ch2.QueueDeclarePassive("dq", true, false, false, false, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, qd.Messages, "the durable message survived")

	// Redeclare from the client with the ORIGINAL int32 width → ok (R7:
	// integer widths are one class against the CBOR-decoded uint64).
	_, err = ch2.QueueDeclare("dq", true, false, false, false, amqp.Table{
		"x-message-ttl":          int32(60000),
		"x-dead-letter-exchange": "ae",
		"x-max-length":           int64(100),
	})
	require.NoError(t, err, "client redeclare with original widths succeeds after restart")
	// And with a DIFFERENT width of the same value.
	_, err = ch2.QueueDeclare("dq", true, false, false, false, amqp.Table{
		"x-message-ttl":          int64(60000),
		"x-dead-letter-exchange": "ae",
		"x-max-length":           int32(100),
	})
	require.NoError(t, err, "same values, other widths")

	// A genuinely different value still 406s.
	ch3, err := conn2.Channel()
	require.NoError(t, err)
	_, err = ch3.QueueDeclare("dq", true, false, false, false, amqp.Table{
		"x-message-ttl":          int32(60001),
		"x-dead-letter-exchange": "ae",
		"x-max-length":           int64(100),
	})
	aerr := amqpError(t, err)
	assert.Equal(t, 406, aerr.Code)

	// The exchange redeclares with its alternate-exchange.
	require.NoError(t, ch2.ExchangeDeclare("dx", "direct", true, false, false, false,
		amqp.Table{"alternate-exchange": "ae"}))
}

// probeQueueBindArgsDecode reports whether queue.bind with an arguments table
// decodes on the server (P0-4 first half: one-octet flags are misread as
// uint16, so a bind carrying a table kills the connection — protocol stream,
// spec §16).
func probeQueueBindArgsDecode(t *testing.T, uri string) bool {
	t.Helper()
	conn, err := amqp.Dial(uri)
	require.NoError(t, err)
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return false
	}
	if err := ch.ExchangeDeclare("probe-hx", "headers", false, false, false, false, nil); err != nil {
		return false
	}
	q, err := ch.QueueDeclare("probe-hq", false, false, false, false, nil)
	if err != nil {
		return false
	}
	if err := ch.QueueBind(q.Name, "", "probe-hx", false, amqp.Table{"x-match": "all"}); err != nil {
		return false
	}
	if conn.IsClosed() {
		return false
	}
	_, _ = ch.QueueDelete(q.Name, false, false, false)
	_ = ch.ExchangeDelete("probe-hx", false, false)
	return true
}

// TestHeaders_BindingSurvivesRestart (T10.2): a headers binding carrying an
// integer value ({n: int32(5)}) keeps routing after a restart — CBOR decodes
// it as uint64/int64, and headerValueMatches compares integer widths by value
// (R9). Pre-fix mechanism: reflect.DeepEqual made the recovered uint64(5)
// binding never match a client's int32(5) header — NOTHING routed after a
// restart.
//
// requires: protocol one-octet flag-decode fix (P0-4 first half, another
// stream) — queue.bind carrying an arguments table.
func TestHeaders_BindingSurvivesRestart(t *testing.T) {
	b := newRestartableBroker(t, nil)
	if !probeQueueBindArgsDecode(t, b.uri) {
		t.Skip("requires: protocol one-octet flag-decode fix (P0-4 first half, another stream); broker-level numeric parity is covered by TestHeaders_NumericWidth_Parity")
	}

	conn, err := amqp.Dial(b.uri)
	require.NoError(t, err)

	ch, err := conn.Channel()
	require.NoError(t, err)
	require.NoError(t, ch.ExchangeDeclare("hx", "headers", true, false, false, false, nil))
	_, err = ch.QueueDeclare("hq", true, false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, ch.QueueBind("hq", "", "hx", false, amqp.Table{
		"x-match": "all",
		"n":       int32(5),
	}))
	// Sanity before restart: it routes.
	require.NoError(t, ch.Publish("hx", "", false, false, amqp.Publishing{
		Headers:      amqp.Table{"n": int32(5)},
		DeliveryMode: 2,
		Body:         []byte("before"),
	}))
	require.NoError(t, conn.Close())
	// Drain it so only the after-restart message remains.
	// (Consume+ack now; the point of the test is the POST-restart publish.)

	b.restart()
	require.Empty(t, recoveryFaultsFor(b.srv, "hq"), "no degraded fault for hq")
	require.Empty(t, recoveryFaultsFor(b.srv, "hx"), "no degraded fault for hx")

	conn2, err := amqp.Dial(b.uri)
	require.NoError(t, err)
	defer conn2.Close()
	ch2, err := conn2.Channel()
	require.NoError(t, err)

	// Drain the pre-restart message (recovered).
	for {
		_, ok, err := ch2.Get("hq", true)
		require.NoError(t, err)
		if !ok {
			break
		}
	}

	require.NoError(t, ch2.Publish("hx", "", false, false, amqp.Publishing{
		Headers:      amqp.Table{"n": int32(5)},
		DeliveryMode: 2,
		Body:         []byte("after"),
	}))

	// The recovered binding (CBOR uint64) matches the int32 header. Durable
	// publishes become visible on the group-commit fsync completion, so a
	// synchronous basic.get must poll (same discipline as getOne in the
	// conformance harness; this test's immediate get raced the fsync).
	msg, ok := getWithin(ch2, "hq", 3*time.Second)
	require.True(t, ok, "the binding survived the restart and routes (CBOR-width parity)")
	assert.Equal(t, "after", string(msg.Body))

	// And a mismatching header does NOT route.
	require.NoError(t, ch2.Publish("hx", "", false, false, amqp.Publishing{
		Headers:      amqp.Table{"n": int32(6)},
		DeliveryMode: 2,
		Body:         []byte("never"),
	}))
	_, ok = getWithin(ch2, "hq", 500*time.Millisecond)
	assert.False(t, ok, "a mismatching header never routes")
}

// getWithin polls basic.get (autoAck=true) until a delivery arrives or the
// timeout elapses. Local twin of the conformance harness's getOne, which sits
// behind the conformance build tag.
func getWithin(ch *amqp.Channel, queue string, timeout time.Duration) (amqp.Delivery, bool) {
	deadline := time.Now().Add(timeout)
	for {
		d, ok, err := ch.Get(queue, true)
		if err == nil && ok {
			return d, true
		}
		if time.Now().After(deadline) {
			return amqp.Delivery{}, false
		}
		time.Sleep(50 * time.Millisecond)
	}
}
