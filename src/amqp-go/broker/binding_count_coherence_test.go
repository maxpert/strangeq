package broker

import (
	"errors"
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/stretchr/testify/require"
)

// failDeleteBindingStorage fails queue/exchange binding deletes while armed,
// leaving the binding rows in place. It pins WS4 review finding 3: the entity
// counter must only be decremented for bindings that were ACTUALLY removed, so
// object_totals.bindings can never fall below len(ListBindings()).
type failDeleteBindingStorage struct {
	interfaces.Storage
	armed bool
}

func (s *failDeleteBindingStorage) DeleteBinding(queueName, exchangeName, routingKey string) error {
	if s.armed {
		return errors.New("injected DeleteBinding failure")
	}
	return s.Storage.DeleteBinding(queueName, exchangeName, routingKey)
}

func (s *failDeleteBindingStorage) DeleteExchangeBinding(source, destination, routingKey string) error {
	if s.armed {
		return errors.New("injected DeleteExchangeBinding failure")
	}
	return s.Storage.DeleteExchangeBinding(source, destination, routingKey)
}

func bindingDelta(events []entityEvent) int64 {
	var n int64
	for _, e := range events {
		if e.kind == "binding" {
			n += e.delta
		}
	}
	return n
}

// TestBindingCountCoherentOnQueueDeleteStorageError drives a queue delete whose
// binding deletes all fail: the rows survive, so the binding count must not
// change.
func TestBindingCountCoherentOnQueueDeleteStorageError(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, b.DeclareExchange("x", "direct", false, false, false, nil))
	_, err := b.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, b.BindQueue("q", "x", "a", nil))
	require.NoError(t, b.BindQueue("q", "x", "b", nil))

	fail := &failDeleteBindingStorage{Storage: b.storage}
	b.storage = fail
	obs := &recordingObserver{}
	b.SetEntityObserver(obs)
	fail.armed = true

	_, err = b.DeleteQueue("q", false, false)
	require.NoError(t, err)

	require.Zero(t, bindingDelta(obs.snapshot()),
		"a queue whose DeleteBinding calls all failed must not decrement the binding count")

	views, err := b.ListBindings()
	require.NoError(t, err)
	require.Len(t, views, 2, "the failed deletes left both binding rows in place")
}

// TestBindingCountCoherentOnExchangeDeleteStorageError is the exchange-teardown
// counterpart: the queue binding and the reverse e2e binding both fail to
// delete and must both remain counted.
func TestBindingCountCoherentOnExchangeDeleteStorageError(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, b.DeclareExchange("x", "direct", false, false, false, nil))
	require.NoError(t, b.DeclareExchange("up", "direct", false, false, false, nil))
	_, err := b.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, b.BindQueue("q", "x", "a", nil))     // x is the source
	require.NoError(t, b.BindExchange("x", "up", "d", nil)) // x is the destination

	fail := &failDeleteBindingStorage{Storage: b.storage}
	b.storage = fail
	obs := &recordingObserver{}
	b.SetEntityObserver(obs)
	fail.armed = true

	require.NoError(t, b.DeleteExchange("x", false))

	require.Zero(t, bindingDelta(obs.snapshot()),
		"failed binding deletes must not decrement the binding count")

	views, err := b.ListBindings()
	require.NoError(t, err)
	require.Len(t, views, 2, "both binding rows survived the failed deletes")
}
