package broker

import (
	"errors"
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestQueueDeclarePassive (§1.2): read-only declare — never creates a record,
// QueueState, ordinal or reaper; missing → interfaces.ErrQueueNotFound with
// the RabbitMQ-shaped text (R1).
func TestQueueDeclarePassive(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	// Missing queue: typed 404 error.
	_, err := b.QueueDeclarePassive("nope")
	require.Error(t, err)
	assert.True(t, errors.Is(err, interfaces.ErrQueueNotFound), "%v", err)
	assert.Equal(t, "no queue 'nope' in vhost '/'", err.Error())

	// Nothing was created as a side effect.
	_, qerr := b.storage.GetQueue("nope")
	assert.True(t, errors.Is(qerr, interfaces.ErrQueueNotFound), "passive declare must not create a record")
	_, hasState := b.queueStates.Load("nope")
	assert.False(t, hasState, "passive declare must not create a QueueState")

	// Existing queue: returns the record, creates nothing new.
	mustDeclareQueue(t, b, "real")
	q, err := b.QueueDeclarePassive("real")
	require.NoError(t, err)
	assert.Equal(t, "real", q.Name)

	// A passive declare on an existing queue with an x-expires policy resets
	// the idle clock (a declare renews the lease; RabbitMQ's stat path
	// re-arms the expiry timer).
	mustDeclareQueueArgs(t, b, "expiring", false, false, false, map[string]interface{}{"x-expires": int32(60000)})
	qs := b.getOrCreateQueueState("expiring")
	qs.MarkActivity(1) // simulate a stale clock
	_, err = b.QueueDeclarePassive("expiring")
	require.NoError(t, err)
	assert.Greater(t, qs.LastActivityMilli(), int64(1), "passive declare renews the x-expires lease")
}

// TestQueueDeclarePassive_NoStateCreationOnExistingWithoutState: a queue whose
// record exists but has no QueueState yet must NOT gain one from a passive
// declare (§1.2: must not call getOrCreateQueueState).
func TestQueueDeclarePassive_NoStateCreation(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	mustDeclareQueue(t, b, "stateless")
	// Simulate a cold record: drop the QueueState.
	b.queueStates.Delete("stateless")

	_, err := b.QueueDeclarePassive("stateless")
	require.NoError(t, err)
	_, hasState := b.queueStates.Load("stateless")
	assert.False(t, hasState, "passive declare must not create a QueueState even for an existing queue")
}

// TestExchangeDeclarePassive (§1.2): missing → ErrExchangeNotFound; existing
// returns the record.
func TestExchangeDeclarePassive(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	_, err := b.ExchangeDeclarePassive("noex")
	require.Error(t, err)
	assert.True(t, errors.Is(err, interfaces.ErrExchangeNotFound), "%v", err)
	assert.Equal(t, "no exchange 'noex' in vhost '/'", err.Error())

	require.NoError(t, b.DeclareExchange("myex", "topic", false, false, false, nil))
	ex, err := b.ExchangeDeclarePassive("myex")
	require.NoError(t, err)
	assert.Equal(t, "myex", ex.Name)
	assert.Equal(t, "topic", ex.Kind)

	// amq.* predeclared exchanges resolve passively too.
	amq, err := b.ExchangeDeclarePassive("amq.direct")
	require.NoError(t, err)
	assert.Equal(t, "amq.direct", amq.Name)
}
