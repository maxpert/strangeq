package broker

import (
	"errors"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mustDeclareQueue declares a non-durable queue and fails the test on error.
func mustDeclareQueue(t *testing.T, b *StorageBroker, name string) {
	t.Helper()
	_, err := b.DeclareQueue(name, false, false, false, nil)
	require.NoError(t, err)
}

// These tests pin the observable routing behavior of PublishMessage and
// findTargetQueues so the SQ-6 allocation fast path (default exchange and
// direct exchanges without exchange-to-exchange bindings) provably preserves
// semantics: ErrNoRoute for mandatory unroutable messages, dedup across
// multiple matching bindings, and cycle protection for exchange-to-exchange
// bindings.

func TestPublishMessage_Mandatory_DefaultExchange_NoQueue_ErrNoRoute(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	msg := &protocol.Message{
		Body:       []byte("payload"),
		RoutingKey: "no-such-queue",
		Mandatory:  true,
	}

	err := broker.PublishMessage("", "no-such-queue", msg)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNoRoute), "mandatory publish to missing queue must return ErrNoRoute, got: %v", err)
}

func TestPublishMessage_NonMandatory_DefaultExchange_NoQueue_Silent(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	msg := &protocol.Message{
		Body:       []byte("payload"),
		RoutingKey: "no-such-queue",
		Mandatory:  false,
	}

	err := broker.PublishMessage("", "no-such-queue", msg)
	assert.NoError(t, err, "non-mandatory unroutable publish must be silently dropped")
}

func TestPublishMessage_Mandatory_DirectExchange_UnmatchedKey_ErrNoRoute(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, broker.DeclareExchange("mand.direct", "direct", false, false, false, nil))
	mustDeclareQueue(t, broker, "mand.q")
	require.NoError(t, broker.BindQueue("mand.q", "mand.direct", "bound-key", nil))

	msg := &protocol.Message{
		Body:       []byte("payload"),
		RoutingKey: "other-key",
		Mandatory:  true,
	}

	err := broker.PublishMessage("mand.direct", "other-key", msg)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNoRoute), "mandatory publish with unmatched key must return ErrNoRoute, got: %v", err)
}

func TestPublishMessage_DefaultExchange_ActiveQueue_Routes(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	mustDeclareQueue(t, broker, "active.q")
	// Force the queue into the activeQueues fast lookup used by routing.
	broker.getOrCreateQueueState("active.q")

	msg := &protocol.Message{Body: []byte("payload"), RoutingKey: "active.q"}
	require.NoError(t, broker.PublishMessage("", "active.q", msg))

	stored, err := broker.storage.GetMessage("active.q", msg.DeliveryTag)
	require.NoError(t, err, "message must be stored in the target queue")
	assert.Equal(t, []byte("payload"), stored.Body)
}

func TestFindTargetQueues_DefaultExchange_DeclaredButInactiveQueue(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	// Store the queue directly so it exists in storage but has no live
	// QueueState — routing must fall back to the storage lookup.
	require.NoError(t, broker.storage.StoreQueue(&protocol.Queue{Name: "cold.q"}))

	exchange := &protocol.Exchange{Name: "", Kind: "direct"}
	msg := &protocol.Message{Body: []byte("x"), RoutingKey: "cold.q"}

	queues, err := broker.findTargetQueues(exchange, "cold.q", msg)
	require.NoError(t, err)
	assert.Equal(t, []string{"cold.q"}, queues)
}

func TestFindTargetQueues_DirectExchange_MultipleQueuesSameKey(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, broker.DeclareExchange("multi.direct", "direct", false, false, false, nil))
	for _, q := range []string{"mq1", "mq2", "mq3"} {
		mustDeclareQueue(t, broker, q)
	}
	require.NoError(t, broker.BindQueue("mq1", "multi.direct", "k", nil))
	require.NoError(t, broker.BindQueue("mq2", "multi.direct", "k", nil))
	require.NoError(t, broker.BindQueue("mq3", "multi.direct", "other", nil))

	exchange, err := broker.storage.GetExchange("multi.direct")
	require.NoError(t, err)

	msg := &protocol.Message{Body: []byte("x"), RoutingKey: "k"}
	queues, err := broker.findTargetQueues(exchange, "k", msg)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"mq1", "mq2"}, queues)
}

// TestFindTargetQueues_E2ECycle_Terminates creates an exchange-to-exchange
// binding cycle directly in storage (bypassing broker-level cycle rejection)
// and verifies routing terminates and deduplicates rather than looping.
func TestFindTargetQueues_E2ECycle_Terminates(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, broker.DeclareExchange("cyc.a", "direct", false, false, false, nil))
	require.NoError(t, broker.DeclareExchange("cyc.b", "direct", false, false, false, nil))
	mustDeclareQueue(t, broker, "cyc.q")
	require.NoError(t, broker.BindQueue("cyc.q", "cyc.b", "k", nil))

	// A -> B -> A cycle, written straight to storage.
	require.NoError(t, broker.storage.StoreExchangeBinding("cyc.a", "cyc.b", "k", nil))
	require.NoError(t, broker.storage.StoreExchangeBinding("cyc.b", "cyc.a", "k", nil))

	exchange, err := broker.storage.GetExchange("cyc.a")
	require.NoError(t, err)

	msg := &protocol.Message{Body: []byte("x"), RoutingKey: "k"}
	queues, err := broker.findTargetQueues(exchange, "k", msg)
	require.NoError(t, err)
	assert.Equal(t, []string{"cyc.q"}, queues, "cycle must terminate and deliver exactly once")
}

// TestFindTargetQueues_DirectWithE2EBinding_UsesRecursivePath verifies that a
// direct exchange that has exchange-to-exchange bindings still routes through
// both its own queue bindings and the downstream exchange.
func TestFindTargetQueues_DirectWithE2EBinding_RoutesBothLevels(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, broker.DeclareExchange("lvl.src", "direct", false, false, false, nil))
	require.NoError(t, broker.DeclareExchange("lvl.dst", "direct", false, false, false, nil))
	mustDeclareQueue(t, broker, "lvl.q1")
	mustDeclareQueue(t, broker, "lvl.q2")
	require.NoError(t, broker.BindQueue("lvl.q1", "lvl.src", "k", nil))
	require.NoError(t, broker.BindQueue("lvl.q2", "lvl.dst", "k", nil))
	require.NoError(t, broker.BindExchange("lvl.dst", "lvl.src", "k", nil))

	exchange, err := broker.storage.GetExchange("lvl.src")
	require.NoError(t, err)

	msg := &protocol.Message{Body: []byte("x"), RoutingKey: "k"}
	queues, err := broker.findTargetQueues(exchange, "k", msg)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"lvl.q1", "lvl.q2"}, queues)
}

// TestFindTargetQueues_SharedQueueAcrossExchanges_Dedup verifies queue-level
// dedup when the same queue is reachable both directly and via an
// exchange-to-exchange binding.
func TestFindTargetQueues_SharedQueueAcrossExchanges_Dedup(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, broker.DeclareExchange("dup.src", "direct", false, false, false, nil))
	require.NoError(t, broker.DeclareExchange("dup.dst", "direct", false, false, false, nil))
	mustDeclareQueue(t, broker, "dup.q")
	require.NoError(t, broker.BindQueue("dup.q", "dup.src", "k", nil))
	require.NoError(t, broker.BindQueue("dup.q", "dup.dst", "k", nil))
	require.NoError(t, broker.BindExchange("dup.dst", "dup.src", "k", nil))

	exchange, err := broker.storage.GetExchange("dup.src")
	require.NoError(t, err)

	msg := &protocol.Message{Body: []byte("x"), RoutingKey: "k"}
	queues, err := broker.findTargetQueues(exchange, "k", msg)
	require.NoError(t, err)
	assert.Equal(t, []string{"dup.q"}, queues, "queue reachable via two paths must be delivered exactly once")
}
