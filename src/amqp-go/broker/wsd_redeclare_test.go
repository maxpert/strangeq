package broker

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRedeclare_InequivalentArgs_Broker covers the §2.2 queue rules at the
// broker level (the client-level 406 table is T2.1). Pre-fix mechanism:
// declareExistingQueueLocked compared only durable/auto-delete/exclusive with
// a generic "properties mismatch" error and never compared arguments.
func TestRedeclare_InequivalentArgs_Broker(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	mustDeclareQueueArgs(t, b, "q", true, false, false, map[string]interface{}{
		"x-message-ttl": int32(1000),
	})

	// durable mismatch → 405? No: exclusive is the 405; durable is 406 (R6 order).
	_, err := b.DeclareQueue("q", false, false, false, map[string]interface{}{"x-message-ttl": int32(1000)})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInequivalentArgs), "%v", err)
	assert.Contains(t, err.Error(), "inequivalent arg 'durable' for queue 'q' in vhost '/': received 'false' but current is 'true'")

	// auto_delete mismatch.
	_, err = b.DeclareQueue("q", true, true, false, map[string]interface{}{"x-message-ttl": int32(1000)})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "inequivalent arg 'auto_delete' for queue 'q' in vhost '/': received 'true' but current is 'false'")

	// x-message-ttl value mismatch: R7 val() formatting with AMQP type names.
	_, err = b.DeclareQueue("q", true, false, false, map[string]interface{}{"x-message-ttl": int32(2000)})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInequivalentArgs), "%v", err)
	assert.Contains(t, err.Error(), "inequivalent arg 'x-message-ttl' for queue 'q' in vhost '/': received the value '2000' of type 'signedint' but current is the value '1000' of type 'signedint'")

	// present → absent: received none.
	_, err = b.DeclareQueue("q", true, false, false, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "inequivalent arg 'x-message-ttl' for queue 'q' in vhost '/': received none but current is the value '1000' of type 'signedint'")

	// absent → present: current is none.
	mustDeclareQueueArgs(t, b, "q2", false, false, false, nil)
	_, err = b.DeclareQueue("q2", false, false, false, map[string]interface{}{"x-max-length": int64(3)})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "inequivalent arg 'x-max-length' for queue 'q2' in vhost '/': received the value '3' of type 'long' but current is none")

	// exclusive mismatch → 405 RESOURCE_LOCKED class (D4, R6 strict check first).
	mustDeclareQueueArgs(t, b, "qx", false, false, true, nil)
	_, err = b.DeclareQueue("qx", false, false, false, nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrQueueLocked), "%v", err)
	assert.Contains(t, err.Error(), "cannot obtain exclusive access to locked queue 'qx' in vhost '/'")
	// exclusive mismatch outranks a durable mismatch (R6 order).
	mustDeclareQueueArgs(t, b, "qx2", true, false, true, nil)
	_, err = b.DeclareQueue("qx2", false, false, false, nil)
	assert.True(t, errors.Is(err, ErrQueueLocked), "exclusive check runs first: %v", err)
}

// TestRedeclare_ExchangeInequivalent covers §2.3: type (keeps
// ErrExchangeTypeMismatch reachable), durable, auto_delete, internal, then
// ONLY alternate-exchange among the arguments (D3).
func TestRedeclare_ExchangeInequivalent(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, b.DeclareExchange("ex", "direct", true, false, false, nil))

	// type mismatch: both ErrInequivalentArgs AND ErrExchangeTypeMismatch.
	err := b.DeclareExchange("ex", "fanout", true, false, false, nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInequivalentArgs), "%v", err)
	assert.True(t, errors.Is(err, ErrExchangeTypeMismatch), "%v", err)
	assert.Contains(t, err.Error(), "inequivalent arg 'type' for exchange 'ex' in vhost '/': received 'fanout' but current is 'direct'")

	// durable / auto_delete / internal mismatches.
	err = b.DeclareExchange("ex", "direct", false, false, false, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "inequivalent arg 'durable'")
	err = b.DeclareExchange("ex", "direct", true, true, false, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "inequivalent arg 'auto_delete'")
	err = b.DeclareExchange("ex", "direct", true, false, true, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "inequivalent arg 'internal'")

	// alternate-exchange compared; other argument keys ignored (D3).
	require.NoError(t, b.DeclareExchange("ex2", "direct", false, false, false, map[string]interface{}{"alternate-exchange": "ae1"}))
	err = b.DeclareExchange("ex2", "direct", false, false, false, map[string]interface{}{"alternate-exchange": "ae2"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "inequivalent arg 'alternate-exchange'")
	assert.NoError(t, b.DeclareExchange("ex2", "direct", false, false, false,
		map[string]interface{}{"alternate-exchange": "ae1", "x-custom-plugin": "anything"}))
	assert.NoError(t, b.DeclareExchange("ex2", "direct", false, false, false,
		map[string]interface{}{"alternate-exchange": "ae1"}))
	// alternate-exchange present → absent is a mismatch.
	err = b.DeclareExchange("ex2", "direct", false, false, false, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "alternate-exchange")
}

// TestRedeclare_EquivalentAcrossIntWidths_Ok (T2.2): integer widths are one
// class (R7); int vs float is not.
func TestRedeclare_EquivalentAcrossIntWidths_Ok(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	mustDeclareQueueArgs(t, b, "qw", false, false, false, map[string]interface{}{"x-max-length": int8(5)})
	_, err := b.DeclareQueue("qw", false, false, false, map[string]interface{}{"x-max-length": int64(5)})
	assert.NoError(t, err, "int8 vs int64 of same value")
	_, err = b.DeclareQueue("qw", false, false, false, map[string]interface{}{"x-max-length": uint64(5)})
	assert.NoError(t, err, "int8 vs uint64 of same value")
	_, err = b.DeclareQueue("qw", false, false, false, map[string]interface{}{"x-max-length": float64(5)})
	require.Error(t, err, "int vs float of same value is a different class (R7)")
	assert.True(t, errors.Is(err, ErrInequivalentArgs), "%v", err)
}

// TestRedeclare_IgnoresUnlistedArgs (T2.3, D3): keys outside RabbitMQ's
// classic queue_arguments list are ignored; x-queue-type "classic" vs absent
// is special-cased equivalent (R7 assert_field_equivalence).
func TestRedeclare_IgnoresUnlistedArgs(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	mustDeclareQueueArgs(t, b, "qc", false, false, false, map[string]interface{}{"x-custom-plugin": "a"})
	_, err := b.DeclareQueue("qc", false, false, false, map[string]interface{}{"x-custom-plugin": "b"})
	assert.NoError(t, err, "unlisted argument keys are ignored")

	// x-queue-type special case: one side absent, other "classic" → ok.
	mustDeclareQueueArgs(t, b, "qq", false, false, false, map[string]interface{}{"x-queue-type": "classic"})
	_, err = b.DeclareQueue("qq", false, false, false, nil)
	assert.NoError(t, err, "x-queue-type classic vs absent")
	_, err = b.DeclareQueue("qq", false, false, false, map[string]interface{}{"x-queue-type": "classic"})
	assert.NoError(t, err, "x-queue-type classic vs classic")
	// x-queue-type quorum vs absent → mismatch.
	_, err = b.DeclareQueue("qq", false, false, false, map[string]interface{}{"x-queue-type": "quorum"})
	require.Error(t, err, "x-queue-type quorum vs classic")
	assert.True(t, errors.Is(err, ErrInequivalentArgs), "%v", err)
}

// TestRedeclare_Refused_NoSideEffects (T2.5): a refused redeclare must not
// touch activeQueues, the QueueState policy, or spawn a reaper — equivalence
// runs BEFORE any side effect.
func TestRedeclare_Refused_NoSideEffects(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	mustDeclareQueueArgs(t, b, "qs", false, false, false, map[string]interface{}{"x-message-ttl": int32(1000)})
	before, _ := b.activeQueues.Load("qs")
	qsBefore := b.getOrCreateQueueState("qs")
	polBefore := qsBefore.Policy()
	require.NotNil(t, polBefore)

	_, err := b.DeclareQueue("qs", false, false, false, map[string]interface{}{"x-message-ttl": int32(2000)})
	require.Error(t, err)

	after, _ := b.activeQueues.Load("qs")
	assert.Same(t, before, after, "activeQueues entry must be untouched by a refused redeclare")
	assert.Same(t, polBefore, qsBefore.Policy(), "policy must be unchanged")
}

// mustDeclareQueueArgs declares a queue with arguments, failing the test on error.
func mustDeclareQueueArgs(t *testing.T, b *StorageBroker, name string, durable, autoDelete, exclusive bool, args map[string]interface{}) {
	t.Helper()
	_, err := b.DeclareQueue(name, durable, autoDelete, exclusive, args)
	require.NoError(t, err)
}
