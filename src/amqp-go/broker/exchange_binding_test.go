package broker

import (
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createExchanges(t *testing.T, b *StorageBroker, namesAndTypes ...string) {
	t.Helper()
	for i := 0; i < len(namesAndTypes); i += 2 {
		exch := &protocol.Exchange{
			Name:      namesAndTypes[i],
			Kind:      namesAndTypes[i+1],
			Durable:   false,
			Arguments: make(map[string]interface{}),
		}
		err := b.storage.StoreExchange(exch)
		require.NoError(t, err)
	}
}

func createQueues(t *testing.T, b *StorageBroker, names ...string) {
	t.Helper()
	for _, name := range names {
		q := &protocol.Queue{Name: name, Durable: false}
		err := b.storage.StoreQueue(q)
		require.NoError(t, err)
	}
}

func getExchange(t *testing.T, b *StorageBroker, name string) *protocol.Exchange {
	t.Helper()
	exch, err := b.storage.GetExchange(name)
	require.NoError(t, err)
	return exch
}

func TestBindExchange_BasicBind(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	createExchanges(t, broker, "src", "direct", "dst", "fanout")
	createQueues(t, broker, "q1")

	err := broker.storage.StoreBinding("q1", "dst", "", nil)
	require.NoError(t, err)

	err = broker.BindExchange("dst", "src", "key1", nil)
	require.NoError(t, err)

	bindings, err := broker.storage.GetExchangeBindingsFrom("src")
	require.NoError(t, err)
	require.Len(t, bindings, 1)
	assert.Equal(t, "src", bindings[0].Source)
	assert.Equal(t, "dst", bindings[0].Destination)
	assert.Equal(t, "key1", bindings[0].RoutingKey)
}

func TestBindExchange_SourceNotFound(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	createExchanges(t, broker, "dst", "direct")

	err := broker.BindExchange("dst", "nonexistent", "key", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestBindExchange_DestinationNotFound(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	createExchanges(t, broker, "src", "direct")

	err := broker.BindExchange("nonexistent", "src", "key", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestBindExchange_CycleDetection_Direct(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	createExchanges(t, broker, "A", "direct", "B", "direct")

	err := broker.BindExchange("B", "A", "key", nil)
	require.NoError(t, err)

	err = broker.BindExchange("A", "B", "key", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cycle")
}

func TestBindExchange_CycleDetection_ThreeHop(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	createExchanges(t, broker, "A", "direct", "B", "direct", "C", "direct")

	require.NoError(t, broker.BindExchange("B", "A", "key", nil))
	require.NoError(t, broker.BindExchange("C", "B", "key", nil))

	err := broker.BindExchange("A", "C", "key", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cycle")
}

func TestBindExchange_SelfBinding(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	createExchanges(t, broker, "A", "direct")

	err := broker.BindExchange("A", "A", "key", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cycle")
}

func TestUnbindExchange_BasicUnbind(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	createExchanges(t, broker, "src", "direct", "dst", "direct")
	createQueues(t, broker, "q1")

	require.NoError(t, broker.storage.StoreBinding("q1", "dst", "key1", nil))
	require.NoError(t, broker.BindExchange("dst", "src", "key1", nil))

	bindings, err := broker.storage.GetExchangeBindingsFrom("src")
	require.NoError(t, err)
	require.Len(t, bindings, 1)

	err = broker.UnbindExchange("dst", "src", "key1")
	require.NoError(t, err)

	bindings, err = broker.storage.GetExchangeBindingsFrom("src")
	require.NoError(t, err)
	assert.Empty(t, bindings)
}

func TestExchangeBindingRouting_DirectChain(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	createExchanges(t, broker, "A", "direct", "B", "direct")
	createQueues(t, broker, "q1")

	require.NoError(t, broker.storage.StoreBinding("q1", "B", "foo", nil))
	require.NoError(t, broker.BindExchange("B", "A", "foo", nil))

	exchA := getExchange(t, broker, "A")
	msg := &protocol.Message{Body: []byte("hello"), RoutingKey: "foo"}

	queues, err := broker.findTargetQueues(exchA, "foo", msg)
	require.NoError(t, err)
	assert.Contains(t, queues, "q1")
	assert.Len(t, queues, 1)
}

func TestExchangeBindingRouting_KeyMismatchAtSource(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	createExchanges(t, broker, "A", "direct", "B", "direct")
	createQueues(t, broker, "q1")

	require.NoError(t, broker.storage.StoreBinding("q1", "B", "foo", nil))
	require.NoError(t, broker.BindExchange("B", "A", "bar", nil))

	exchA := getExchange(t, broker, "A")
	msg := &protocol.Message{Body: []byte("hello"), RoutingKey: "foo"}

	queues, err := broker.findTargetQueues(exchA, "foo", msg)
	require.NoError(t, err)
	assert.Empty(t, queues)
}

func TestExchangeBindingRouting_FanoutSource(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	createExchanges(t, broker, "A", "fanout", "B", "direct")
	createQueues(t, broker, "q1")

	require.NoError(t, broker.storage.StoreBinding("q1", "B", "anything", nil))
	require.NoError(t, broker.BindExchange("B", "A", "", nil))

	exchA := getExchange(t, broker, "A")
	msg := &protocol.Message{Body: []byte("hello"), RoutingKey: "anything"}

	queues, err := broker.findTargetQueues(exchA, "anything", msg)
	require.NoError(t, err)
	assert.Contains(t, queues, "q1")
}

func TestExchangeBindingRouting_ThreeHopChain(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	createExchanges(t, broker, "A", "direct", "B", "direct", "C", "direct")
	createQueues(t, broker, "q1")

	require.NoError(t, broker.storage.StoreBinding("q1", "C", "foo", nil))
	require.NoError(t, broker.BindExchange("B", "A", "foo", nil))
	require.NoError(t, broker.BindExchange("C", "B", "foo", nil))

	exchA := getExchange(t, broker, "A")
	msg := &protocol.Message{Body: []byte("hello"), RoutingKey: "foo"}

	queues, err := broker.findTargetQueues(exchA, "foo", msg)
	require.NoError(t, err)
	assert.Contains(t, queues, "q1")
	assert.Len(t, queues, 1)
}

func TestExchangeBindingRouting_UnbindBreaksChain(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	createExchanges(t, broker, "A", "direct", "B", "direct")
	createQueues(t, broker, "q1")

	require.NoError(t, broker.storage.StoreBinding("q1", "B", "foo", nil))
	require.NoError(t, broker.BindExchange("B", "A", "foo", nil))

	exchA := getExchange(t, broker, "A")
	msg := &protocol.Message{Body: []byte("hello"), RoutingKey: "foo"}

	queues, err := broker.findTargetQueues(exchA, "foo", msg)
	require.NoError(t, err)
	assert.Len(t, queues, 1)

	require.NoError(t, broker.UnbindExchange("B", "A", "foo"))

	queues, err = broker.findTargetQueues(exchA, "foo", msg)
	require.NoError(t, err)
	assert.Empty(t, queues)
}

func TestExchangeBindingRouting_Deduplication(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	createExchanges(t, broker, "A", "direct", "B", "direct")
	createQueues(t, broker, "q1")

	require.NoError(t, broker.storage.StoreBinding("q1", "A", "foo", nil))
	require.NoError(t, broker.storage.StoreBinding("q1", "B", "foo", nil))
	require.NoError(t, broker.BindExchange("B", "A", "foo", nil))

	exchA := getExchange(t, broker, "A")
	msg := &protocol.Message{Body: []byte("hello"), RoutingKey: "foo"}

	queues, err := broker.findTargetQueues(exchA, "foo", msg)
	require.NoError(t, err)
	assert.Len(t, queues, 1, "q1 should appear only once despite two paths")
	assert.Contains(t, queues, "q1")
}

func TestExchangeBindingRouting_DAGMultiplePaths(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	createExchanges(t, broker, "A", "direct", "B", "direct", "C", "direct", "D", "direct")
	createQueues(t, broker, "qB", "qC", "qD")

	require.NoError(t, broker.storage.StoreBinding("qB", "B", "foo", nil))
	require.NoError(t, broker.storage.StoreBinding("qC", "C", "foo", nil))
	require.NoError(t, broker.storage.StoreBinding("qD", "D", "foo", nil))

	require.NoError(t, broker.BindExchange("B", "A", "foo", nil))
	require.NoError(t, broker.BindExchange("C", "A", "foo", nil))
	require.NoError(t, broker.BindExchange("D", "B", "foo", nil))
	require.NoError(t, broker.BindExchange("D", "C", "foo", nil))

	exchA := getExchange(t, broker, "A")
	msg := &protocol.Message{Body: []byte("hello"), RoutingKey: "foo"}

	queues, err := broker.findTargetQueues(exchA, "foo", msg)
	require.NoError(t, err)
	assert.Len(t, queues, 3)
	assert.Contains(t, queues, "qB")
	assert.Contains(t, queues, "qC")
	assert.Contains(t, queues, "qD")
}

func TestExchangeBindingRouting_TopicSource(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	createExchanges(t, broker, "A", "topic", "B", "direct")
	createQueues(t, broker, "q1")

	require.NoError(t, broker.storage.StoreBinding("q1", "B", "logs.error", nil))
	require.NoError(t, broker.BindExchange("B", "A", "logs.#", nil))

	exchA := getExchange(t, broker, "A")
	msg := &protocol.Message{Body: []byte("err"), RoutingKey: "logs.error"}

	queues, err := broker.findTargetQueues(exchA, "logs.error", msg)
	require.NoError(t, err)
	assert.Contains(t, queues, "q1")

	msg2 := &protocol.Message{Body: []byte("info"), RoutingKey: "logs.info"}
	queues2, err := broker.findTargetQueues(exchA, "logs.info", msg2)
	require.NoError(t, err)
	assert.NotContains(t, queues2, "q1")
}

func TestExchangeBindingRouting_PublishE2E(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	createExchanges(t, broker, "A", "direct", "B", "fanout")
	createQueues(t, broker, "q1", "q2")

	require.NoError(t, broker.storage.StoreBinding("q1", "B", "", nil))
	require.NoError(t, broker.storage.StoreBinding("q2", "B", "", nil))
	require.NoError(t, broker.BindExchange("B", "A", "foo", nil))

	msg := &protocol.Message{
		Body:       []byte("e2e test"),
		RoutingKey: "foo",
		Exchange:   "A",
	}

	err := broker.PublishMessage("A", "foo", msg)
	require.NoError(t, err)

	msgs1, err := broker.storage.GetQueueMessages("q1")
	require.NoError(t, err)
	assert.Len(t, msgs1, 1)
	assert.Equal(t, "e2e test", string(msgs1[0].Body))

	msgs2, err := broker.storage.GetQueueMessages("q2")
	require.NoError(t, err)
	assert.Len(t, msgs2, 1)
	assert.Equal(t, "e2e test", string(msgs2[0].Body))
}

func TestExchangeBindingRouting_NoExchangeBindings_PreservesExistingBehavior(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	exch := &protocol.Exchange{Name: "test.direct", Kind: "direct", Durable: false, Arguments: make(map[string]interface{})}
	require.NoError(t, broker.storage.StoreExchange(exch))
	createQueues(t, broker, "q1", "q2")

	require.NoError(t, broker.storage.StoreBinding("q1", "test.direct", "key1", nil))
	require.NoError(t, broker.storage.StoreBinding("q2", "test.direct", "key1", nil))

	msg := &protocol.Message{Body: []byte("test"), RoutingKey: "key1"}
	queues, err := broker.findTargetQueues(exch, "key1", msg)
	require.NoError(t, err)
	assert.Len(t, queues, 2)
	assert.Contains(t, queues, "q1")
	assert.Contains(t, queues, "q2")
}
