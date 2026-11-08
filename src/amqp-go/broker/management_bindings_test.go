package broker

import (
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/stretchr/testify/require"
)

// --- BOB-4: aggregate listing ------------------------------------------------

// TestListBindings_MapsTypes declares a queue binding and an exchange-to-exchange
// binding and proves ListBindings returns one of each with the right
// destination_type (spec mgmt-04 §5.1 test 4).
func TestListBindings_MapsTypes(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, b.DeclareExchange("x", "direct", false, false, false, nil))
	require.NoError(t, b.DeclareExchange("y", "direct", false, false, false, nil))
	_, err := b.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)

	require.NoError(t, b.BindQueue("q", "x", "rk-q", nil))
	require.NoError(t, b.BindExchange("y", "x", "rk-e", nil))

	views, err := b.ListBindings()
	require.NoError(t, err)
	require.Len(t, views, 2)

	var queueView, exchangeView *BindingView
	for i := range views {
		switch views[i].DestinationType {
		case "queue":
			queueView = &views[i]
		case "exchange":
			exchangeView = &views[i]
		}
	}
	require.NotNil(t, queueView, "a queue binding is listed")
	require.Equal(t, "x", queueView.Source)
	require.Equal(t, "q", queueView.Destination)
	require.Equal(t, "rk-q", queueView.RoutingKey)

	require.NotNil(t, exchangeView, "an e2e binding is listed")
	require.Equal(t, "x", exchangeView.Source)
	require.Equal(t, "y", exchangeView.Destination)
	require.Equal(t, "rk-e", exchangeView.RoutingKey)
}

// TestListBindings_Empty proves an empty vhost lists as an empty slice, no error.
func TestListBindings_Empty(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	views, err := b.ListBindings()
	require.NoError(t, err)
	require.Empty(t, views)
}

// TestBindQueue_MissingExchangeIsSentinel pins the sentinel the management layer
// maps to 404 (spec mgmt-04 §5.1 test 5).
func TestBindQueue_MissingExchangeIsSentinel(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()
	_, err := b.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)

	err = b.BindQueue("q", "no-such-exchange", "rk", nil)
	require.Error(t, err)
	require.ErrorIs(t, err, interfaces.ErrExchangeNotFound)
}

// TestBindQueue_MissingQueueIsSentinel pins the queue-side sentinel.
func TestBindQueue_MissingQueueIsSentinel(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()
	require.NoError(t, b.DeclareExchange("x", "direct", false, false, false, nil))

	err := b.BindQueue("no-such-queue", "x", "rk", nil)
	require.Error(t, err)
	require.ErrorIs(t, err, interfaces.ErrQueueNotFound)
}

// TestBindExchange_CycleError pins the broker-side cycle detection (spec §5.1
// test 6): a self-cycle and a two-node cycle both fail, so the management layer
// maps them to 400 without re-implementing detection.
func TestBindExchange_CycleError(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()
	require.NoError(t, b.DeclareExchange("a", "direct", false, false, false, nil))
	require.NoError(t, b.DeclareExchange("bb", "direct", false, false, false, nil))

	// self-cycle
	err := b.BindExchange("a", "a", "rk", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cycle")

	// two-node cycle: a -> bb then bb -> a
	require.NoError(t, b.BindExchange("bb", "a", "rk", nil))
	err = b.BindExchange("a", "bb", "rk", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cycle")
}

// --- BOB-4: binding entity deltas -------------------------------------------

// TestEntityObserver_BindingDeltas pins the exact (kind="binding", delta)
// sequence for create/rebind/unbind (spec §5.1 test 10).
func TestEntityObserver_BindingDeltas(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, b.DeclareExchange("x", "direct", false, false, false, nil))
	require.NoError(t, b.DeclareExchange("y", "direct", false, false, false, nil))
	_, err := b.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)

	obs := &recordingObserver{}
	b.SetEntityObserver(obs)

	require.NoError(t, b.BindQueue("q", "x", "rk", nil))                // +1
	require.NoError(t, b.BindQueue("q", "x", "rk", nil))                // rebind same triple: 0
	require.NoError(t, b.UnbindQueueWithArguments("q", "x", "rk", nil)) // -1
	require.NoError(t, b.BindExchange("y", "x", "erk", nil))            // +1
	require.NoError(t, b.UnbindExchange("y", "x", "erk"))               // -1

	want := []entityEvent{
		{kind: "binding", delta: 1},
		{kind: "binding", delta: -1},
		{kind: "binding", delta: 1},
		{kind: "binding", delta: -1},
	}
	require.Equal(t, want, obs.snapshot())
}

// TestEntityObserver_BulkSweepDeltas proves a queue delete fires exactly one
// binding delta for the whole sweep, and an exchange delete fires one per
// category (spec §5.1 test 11).
func TestEntityObserver_BulkSweepDeltas(t *testing.T) {
	t.Run("queue sweep is one delta", func(t *testing.T) {
		b, cleanup := createTestBroker(t)
		defer cleanup()
		require.NoError(t, b.DeclareExchange("x", "direct", false, false, false, nil))
		_, err := b.DeclareQueue("q", false, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, b.BindQueue("q", "x", "a", nil))
		require.NoError(t, b.BindQueue("q", "x", "b", nil))
		require.NoError(t, b.BindQueue("q", "x", "c", nil))

		obs := &recordingObserver{}
		b.SetEntityObserver(obs)

		_, err = b.DeleteQueue("q", false, false)
		require.NoError(t, err)

		require.Equal(t, []entityEvent{
			{kind: "binding", delta: -3},
			{kind: "queue", delta: -1},
		}, obs.snapshot())
	})

	t.Run("exchange sweep is one delta", func(t *testing.T) {
		b, cleanup := createTestBroker(t)
		defer cleanup()
		require.NoError(t, b.DeclareExchange("x", "direct", false, false, false, nil))
		require.NoError(t, b.DeclareExchange("up", "direct", false, false, false, nil))
		require.NoError(t, b.DeclareExchange("down", "direct", false, false, false, nil))
		_, err := b.DeclareQueue("q1", false, false, false, nil)
		require.NoError(t, err)
		_, err = b.DeclareQueue("q2", false, false, false, nil)
		require.NoError(t, err)

		// x is the source of two queue bindings and one e2e binding, and the
		// destination of one e2e binding: four binding rows total.
		require.NoError(t, b.BindQueue("q1", "x", "a", nil))
		require.NoError(t, b.BindQueue("q2", "x", "b", nil))
		require.NoError(t, b.BindExchange("down", "x", "c", nil))
		require.NoError(t, b.BindExchange("x", "up", "d", nil))

		obs := &recordingObserver{}
		b.SetEntityObserver(obs)

		require.NoError(t, b.DeleteExchange("x", false))

		require.Equal(t, []entityEvent{
			{kind: "binding", delta: -4},
			{kind: "exchange", delta: -1},
		}, obs.snapshot(), "the whole exchange teardown is exactly one binding delta")
	})
}

// TestEntityObserver_NotFiredOnMissingUnbind proves an unbind of a binding that
// does not exist emits no delta (spec §5.1 test 12), for both the
// arguments-qualified and the legacy triple unbind.
func TestEntityObserver_NotFiredOnMissingUnbind(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()
	require.NoError(t, b.DeclareExchange("x", "direct", false, false, false, nil))
	require.NoError(t, b.DeclareExchange("y", "direct", false, false, false, nil))
	_, err := b.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)

	obs := &recordingObserver{}
	b.SetEntityObserver(obs)

	require.NoError(t, b.UnbindQueueWithArguments("q", "x", "nope", nil))
	require.NoError(t, b.UnbindQueue("q", "x", "nope"))
	require.NoError(t, b.UnbindExchange("y", "x", "nope"))

	require.Empty(t, obs.snapshot(), "unbinding a nonexistent binding must not emit a delta")
}

// TestEntityObserver_NoDeltaWhenArgumentsDiffer proves the argument-qualified
// unbind refuses to remove a binding whose arguments differ, so the count is
// untouched (DATA-010/DIV-12 on the broker side).
func TestEntityObserver_NoDeltaWhenArgumentsDiffer(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()
	require.NoError(t, b.DeclareExchange("x", "direct", false, false, false, nil))
	_, err := b.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, b.BindQueue("q", "x", "rk", map[string]interface{}{"k": "v"}))

	obs := &recordingObserver{}
	b.SetEntityObserver(obs)

	require.NoError(t, b.UnbindQueueWithArguments("q", "x", "rk", map[string]interface{}{"k": "other"}))
	require.Empty(t, obs.snapshot(), "different arguments must not remove the binding")

	// The binding is still listed.
	views, err := b.ListBindings()
	require.NoError(t, err)
	require.Len(t, views, 1)
}
