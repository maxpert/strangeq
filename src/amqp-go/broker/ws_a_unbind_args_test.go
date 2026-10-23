package broker

import (
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// WS-A (WI-2): (*StorageBroker).UnbindQueueWithArguments removes a binding
// only when the FULL AMQP 0-9-1 binding key — (queue, exchange, routing-key,
// arguments) — matches. The legacy UnbindQueue (triple-only) is untouched and
// covered by the existing TestAutoDeleteExchangeDeletedWhenLastBindingRemoved.

func TestUnbindQueueWithArguments_MatchingArgsRemoves(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, broker.DeclareExchange("hx", "headers", false, false, false, nil))
	_, err := broker.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, broker.BindQueue("q", "hx", "", map[string]interface{}{"x-match": "all", "k": "v"}))

	err = broker.UnbindQueueWithArguments("q", "hx", "", map[string]interface{}{"x-match": "all", "k": "v"})
	require.NoError(t, err)

	_, err = broker.storage.GetBinding("q", "hx", "")
	assert.ErrorIs(t, err, interfaces.ErrBindingNotFound)
}

func TestUnbindQueueWithArguments_MismatchedArgsIsNoop(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, broker.DeclareExchange("hx", "headers", false, false, false, nil))
	_, err := broker.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, broker.BindQueue("q", "hx", "", map[string]interface{}{"x-match": "all", "k": "v"}))

	// queue.unbind of a binding whose arguments differ must NOT remove the
	// stored binding, and (per spec: unbind of a nonexistent binding replies
	// unbind-ok) must not error either.
	err = broker.UnbindQueueWithArguments("q", "hx", "", map[string]interface{}{"x-match": "all", "k": "w"})
	require.NoError(t, err)

	binding, err := broker.storage.GetBinding("q", "hx", "")
	require.NoError(t, err, "binding with different arguments must survive")
	assert.Equal(t, "v", binding.Arguments["k"])
}

func TestUnbindQueueWithArguments_NilBindEmptyUnbind(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, broker.DeclareExchange("hx", "headers", false, false, false, nil))
	_, err := broker.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, broker.BindQueue("q", "hx", "", nil))

	// nil (bind time) == empty table (unbind time)
	err = broker.UnbindQueueWithArguments("q", "hx", "", map[string]interface{}{})
	require.NoError(t, err)

	_, err = broker.storage.GetBinding("q", "hx", "")
	assert.ErrorIs(t, err, interfaces.ErrBindingNotFound)
}

func TestUnbindQueueWithArguments_TypeTolerant(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, broker.DeclareExchange("hx", "headers", false, false, false, nil))
	_, err := broker.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, broker.BindQueue("q", "hx", "", map[string]interface{}{"n": int32(1)}))

	// int widths must not matter (a persisted/reloaded table drifts types)
	err = broker.UnbindQueueWithArguments("q", "hx", "", map[string]interface{}{"n": int64(1)})
	require.NoError(t, err)

	_, err = broker.storage.GetBinding("q", "hx", "")
	assert.ErrorIs(t, err, interfaces.ErrBindingNotFound)
}

func TestUnbindQueueWithArguments_MissingBindingIsNoop(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, broker.DeclareExchange("hx", "headers", false, false, false, nil))
	_, err := broker.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)

	// Never bound: queue.unbind of a nonexistent binding is a successful no-op.
	err = broker.UnbindQueueWithArguments("q", "hx", "", map[string]interface{}{"k": "v"})
	require.NoError(t, err)
}

func TestUnbindQueueWithArguments_AutoDeleteExchange(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	// Mirrors TestAutoDeleteExchangeDeletedWhenLastBindingRemoved, through the
	// new argument-aware path.
	broker.DeclareExchange("auto-del-ex", "direct", false, true, false, nil)
	broker.DeclareQueue("adex-q", false, false, false, nil)
	broker.BindQueue("adex-q", "auto-del-ex", "rk1", nil)

	_, err := broker.storage.GetExchange("auto-del-ex")
	require.NoError(t, err)

	err = broker.UnbindQueueWithArguments("adex-q", "auto-del-ex", "rk1", nil)
	require.NoError(t, err)

	_, err = broker.storage.GetExchange("auto-del-ex")
	assert.Error(t, err, "auto-delete exchange should be deleted when last binding removed")
}
