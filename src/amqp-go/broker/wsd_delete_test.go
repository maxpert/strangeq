package broker

import (
	"errors"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeleteQueue_IfUnused_LiveConsumer_Refused (T3.1b): if-unused consults
// the LIVE consumer registry (queueConsumers), not the persisted-consumer
// table (StoreConsumer is a documented no-op, so the old check always passed).
// Pre-fix mechanism: DeleteQueue(q, true, false) with a live consumer
// returned (0, nil) and deleted the queue.
func TestDeleteQueue_IfUnused_LiveConsumer_Refused(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	mustDeclareQueue(t, b, "q")
	registerConsumer(b, "q", "c1", &protocol.Consumer{})

	_, err := b.DeleteQueue("q", true, false)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrQueueInUse), "%v", err)
	assert.Contains(t, err.Error(), "queue 'q' in vhost '/' in use")

	// The queue record still exists.
	_, qerr := b.storage.GetQueue("q")
	assert.NoError(t, qerr, "queue must survive a refused delete")

	// With no consumers the delete proceeds.
	_, err = b.DeleteQueue("q", true, false)
	require.Error(t, err, "consumer still registered")

	// Unregister, then it succeeds with (0, nil) semantics for the record.
	require.NoError(t, b.UnregisterConsumer("c1"))
	_, err = b.DeleteQueue("q", true, false)
	assert.NoError(t, err)
}

// TestDeleteQueue_IfEmpty_ReadyVsUnacked (T3.2b, R5): if-empty counts READY
// messages only — delivered-unacked messages do NOT block the delete, a ready
// message does. Pre-fix mechanism: the check used the storage ring count,
// which includes delivered-unacked (and excludes spilled), so the unacked
// case was wrongly refused.
func TestDeleteQueue_IfEmpty_ReadyVsUnacked(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	mustDeclareQueue(t, b, "q")
	// One ready message → refused with ErrQueueNotEmpty.
	publishBody(b, "q", "ready-1")
	_, err := b.DeleteQueue("q", false, true)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrQueueNotEmpty), "%v", err)
	assert.Contains(t, err.Error(), "queue 'q' in vhost '/' not empty")

	// Consume it WITHOUT acking (manual-ack get): 1 delivered-unacked, 0 ready.
	msg, tag, _, gerr := b.GetMessageForGet("q", false)
	require.NoError(t, gerr)
	require.NotNil(t, msg)
	require.NotZero(t, tag)
	qs := b.getOrCreateQueueState("q")
	require.Equal(t, int64(0), qs.WaitingCount(), "0 ready")
	require.Equal(t, int64(1), qs.InflightCount(), "1 unacked")

	// R5: if-empty must SUCCEED with only unacked messages present.
	deleted, err := b.DeleteQueue("q", false, true)
	require.NoError(t, err)
	_ = deleted
	_, qerr := b.storage.GetQueue("q")
	assert.Error(t, qerr, "queue is deleted")
}

// TestDeleteQueue_Nonexistent_DeleteOkZero (T3.3 broker half, D2/R3): deleting
// a missing queue returns (0, nil) — idempotent delete-ok count 0.
func TestDeleteQueue_Nonexistent_DeleteOkZero(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	n, err := b.DeleteQueue("never-existed", false, false)
	assert.NoError(t, err)
	assert.Equal(t, 0, n)

	// Exchange twin (R4): deleting a missing exchange is ok.
	assert.NoError(t, b.DeleteExchange("never-existed", false))
}

// TestExchangeDelete_IfUnused_E2EBindingCounts (T3.4, R4): RabbitMQ's in_use
// means "has bindings where it is the SOURCE" — queue bindings OR
// exchange-to-exchange source bindings. Pre-fix mechanism: only queue
// bindings were checked, so an exchange with a lone e2e source binding was
// deleted.
func TestExchangeDelete_IfUnused_E2EBindingCounts(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, b.DeclareExchange("src", "direct", false, false, false, nil))
	require.NoError(t, b.DeclareExchange("dst", "direct", false, false, false, nil))
	require.NoError(t, b.BindExchange("dst", "src", "k", nil))

	err := b.DeleteExchange("src", true)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrExchangeInUse), "%v", err)
	assert.Contains(t, err.Error(), "exchange 'src' in vhost '/' in use")

	// Queue bindings also count.
	require.NoError(t, b.DeclareExchange("qx-ex", "direct", false, false, false, nil))
	mustDeclareQueue(t, b, "qq")
	require.NoError(t, b.BindQueue("qq", "qx-ex", "k", nil))
	err = b.DeleteExchange("qx-ex", true)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrExchangeInUse), "%v", err)

	// Unbound → deletes fine.
	require.NoError(t, b.UnbindExchange("dst", "src", "k"))
	assert.NoError(t, b.DeleteExchange("src", true))
}

// TestAutoDeleteExchange_QueueDelete_Cleanup (T12.1): deleting the last queue
// bound to an auto-delete exchange removes the exchange (centralized
// maybe-auto-delete on the source, RabbitMQ parity). Pre-fix mechanism: only
// UnbindQueue/UnbindExchange ran the auto-delete check, so queue.delete
// leaked the exchange.
func TestAutoDeleteExchange_QueueDelete_Cleanup(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, b.DeclareExchange("adx", "direct", false, true, false, nil))
	mustDeclareQueue(t, b, "only-q")
	require.NoError(t, b.BindQueue("only-q", "adx", "k", nil))

	_, err := b.DeleteQueue("only-q", false, false)
	require.NoError(t, err)

	_, xerr := b.storage.GetExchange("adx")
	assert.Error(t, xerr, "auto-delete exchange with no remaining source bindings must be removed")

	// An auto-delete exchange still bound elsewhere survives.
	require.NoError(t, b.DeclareExchange("adx2", "direct", false, true, false, nil))
	mustDeclareQueue(t, b, "qa")
	mustDeclareQueue(t, b, "qb")
	require.NoError(t, b.BindQueue("qa", "adx2", "k", nil))
	require.NoError(t, b.BindQueue("qb", "adx2", "k", nil))
	_, err = b.DeleteQueue("qa", false, false)
	require.NoError(t, err)
	_, xerr = b.storage.GetExchange("adx2")
	assert.NoError(t, xerr, "exchange still bound to qb must survive")

	// Non-auto-delete exchange is never collected.
	require.NoError(t, b.DeclareExchange("permx", "direct", false, false, false, nil))
	mustDeclareQueue(t, b, "pq")
	require.NoError(t, b.BindQueue("pq", "permx", "k", nil))
	_, err = b.DeleteQueue("pq", false, false)
	require.NoError(t, err)
	_, xerr = b.storage.GetExchange("permx")
	assert.NoError(t, xerr)
}

// TestAutoDeleteExchange_DestinationExchangeDelete_CascadesToSource (T12.2):
// deleting the DESTINATION of the last e2e binding removes the binding, which
// collects the auto-delete SOURCE exchange.
func TestAutoDeleteExchange_DestinationExchangeDelete_CascadesToSource(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, b.DeclareExchange("src", "direct", false, true, false, nil))
	require.NoError(t, b.DeclareExchange("dst", "direct", false, false, false, nil))
	require.NoError(t, b.BindExchange("dst", "src", "k", nil))

	require.NoError(t, b.DeleteExchange("dst", false))

	_, xerr := b.storage.GetExchange("src")
	assert.Error(t, xerr, "auto-delete source with no remaining bindings must be collected")
}
