package transaction

import (
	"fmt"
	"sync"
	"testing"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/maxpert/amqp-go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newRealTxStack builds a DisruptorStorage + StorageBroker + tx manager wired
// for atomic execution, mirroring the server's construction
// (NewTransactionManagerWithStorage + UnifiedBrokerExecutor).
func newRealTxStack(t *testing.T) (*storage.DisruptorStorage, *broker.StorageBroker, *DefaultTransactionManager) {
	t.Helper()
	ds, err := storage.NewDisruptorStorageWithDataDir(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = ds.Close() })
	b := broker.NewStorageBroker(ds, interfaces.EngineConfig{
		RingBufferSize:          65536,
		SpillThresholdPercent:   80,
		WALBatchSize:            1000,
		WALBatchTimeoutMS:       10,
		ConsumerSelectTimeoutMS: 1,
		ConsumerMaxBatchSize:    100,
	})
	t.Cleanup(b.Close)
	tm := NewTransactionManagerWithStorage(ds)
	tm.SetExecutor(NewUnifiedBrokerExecutor(b))
	return ds, b, tm
}

// TestTx_CommitFailure_SettlementNotApplied (T5.1): a tx buffering
// [ack(get-delivery m1), publish to a MISSING exchange] fails at commit when
// the publish errors inside ExecuteAtomic — and the settlement must NOT have
// touched the live broker. Pre-fix mechanism: executeStaged applied
// settlements LIVE inside the atomic scope, so the ack landed even though the
// commit aborted.
func TestTx_CommitFailure_SettlementNotApplied(t *testing.T) {
	_, b, tm := newRealTxStack(t)

	// Queue Q1 with a message fetched via manual-ack basic.get (a get-delivery,
	// tracked under consumer identity "").
	_, err := b.DeclareQueue("q1", false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, b.PublishMessage("", "q1", &protocol.Message{Exchange: "", RoutingKey: "q1", Body: []byte("m1")}))
	_, m1tag, _, err := b.GetMessageForGet("q1", false)
	require.NoError(t, err)
	require.NotZero(t, m1tag)

	const ch = uint16(1)
	require.NoError(t, tm.Select(ch))
	// Op 1: settle the get-delivery (ConsumerTag "" routes to *GetDelivery).
	require.NoError(t, tm.AddOperation(ch, &interfaces.TransactionOperation{
		Type:        interfaces.OpAck,
		ConsumerTag: "",
		DeliveryTag: m1tag,
	}))
	// Op 2: publish to a missing exchange — fails inside ExecuteAtomic.
	require.NoError(t, tm.AddOperation(ch, NewPublishOperation("missing-exchange", "k",
		&protocol.Message{Exchange: "missing-exchange", RoutingKey: "k", Body: []byte("x")})))

	err = tm.Commit(ch)
	require.Error(t, err, "the publish failure aborts the commit")

	_, stillTracked := b.GetConsumerForDelivery(m1tag)
	assert.True(t, stillTracked,
		"the settlement must NOT be applied on a failed commit (pre-fix: it was applied live inside ExecuteAtomic)")
}

// recordingExecutor wraps UnifiedBrokerExecutor and records the ORDER of
// publish-visibility callbacks vs settlement calls (R14: publishes first).
type recordingExecutor struct {
	inner *UnifiedBrokerExecutor
	mu    sync.Mutex
	log   []string
}

func (r *recordingExecutor) ExecutePublish(exchange, routingKey string, message *protocol.Message) error {
	r.mu.Lock()
	r.log = append(r.log, "publish")
	r.mu.Unlock()
	return r.inner.ExecutePublish(exchange, routingKey, message)
}

func (r *recordingExecutor) ExecutePublishStaged(txnStore interfaces.Storage, exchange, routingKey string, message *protocol.Message) ([]func(bool), error) {
	deferred, err := r.inner.ExecutePublishStaged(txnStore, exchange, routingKey, message)
	if err != nil {
		return nil, err
	}
	wrapped := make([]func(bool), 0, len(deferred))
	for _, d := range deferred {
		d := d
		wrapped = append(wrapped, func(commit bool) {
			r.mu.Lock()
			r.log = append(r.log, "visibility")
			r.mu.Unlock()
			d(commit)
		})
	}
	return wrapped, nil
}

func (r *recordingExecutor) ExecuteAck(consumerTag string, deliveryTag uint64, multiple bool) error {
	r.mu.Lock()
	r.log = append(r.log, fmt.Sprintf("ack:%d", deliveryTag))
	r.mu.Unlock()
	return r.inner.ExecuteAck(consumerTag, deliveryTag, multiple)
}
func (r *recordingExecutor) ExecuteNack(consumerTag string, deliveryTag uint64, multiple, requeue bool) error {
	r.mu.Lock()
	r.log = append(r.log, fmt.Sprintf("nack:%d", deliveryTag))
	r.mu.Unlock()
	return r.inner.ExecuteNack(consumerTag, deliveryTag, multiple, requeue)
}
func (r *recordingExecutor) ExecuteReject(consumerTag string, deliveryTag uint64, requeue bool) error {
	r.mu.Lock()
	r.log = append(r.log, fmt.Sprintf("reject:%d", deliveryTag))
	r.mu.Unlock()
	return r.inner.ExecuteReject(consumerTag, deliveryTag, requeue)
}

// TestTx_Commit_PublishesVisibleBeforeSettlements (T5.2, R14): commit delivers
// publishes FIRST, then applies acks/rejects in buffered order.
func TestTx_Commit_PublishesVisibleBeforeSettlements(t *testing.T) {
	_, b, tm := newRealTxStack(t)
	rec := &recordingExecutor{inner: NewUnifiedBrokerExecutor(b)}
	tm.SetExecutor(rec)

	_, err := b.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, b.PublishMessage("", "q", &protocol.Message{Exchange: "", RoutingKey: "q", Body: []byte("m1")}))
	_, m1tag, _, err := b.GetMessageForGet("q", false)
	require.NoError(t, err)

	const ch = uint16(1)
	require.NoError(t, tm.Select(ch))
	require.NoError(t, tm.AddOperation(ch, &interfaces.TransactionOperation{
		Type: interfaces.OpAck, ConsumerTag: "", DeliveryTag: m1tag,
	}))
	require.NoError(t, tm.AddOperation(ch, NewPublishOperation("", "q",
		&protocol.Message{Exchange: "", RoutingKey: "q", Body: []byte("m2")})))

	require.NoError(t, tm.Commit(ch))

	rec.mu.Lock()
	log := append([]string(nil), rec.log...)
	rec.mu.Unlock()
	require.Equal(t, []string{"visibility", fmt.Sprintf("ack:%d", m1tag)}, log,
		"R14 order: the publish's visibility callback runs BEFORE the settlement")

	// The settlement actually landed.
	_, stillTracked := b.GetConsumerForDelivery(m1tag)
	assert.False(t, stillTracked)
}

// TestTx_AckAppliedAfterCommitFailure_NeverDuplicates: commit applies each
// settlement exactly once, in buffered order, even with multiple settlement
// kinds mixed.
func TestTx_SettlementsAppliedInBufferedOrder(t *testing.T) {
	_, b, tm := newRealTxStack(t)
	rec := &recordingExecutor{inner: NewUnifiedBrokerExecutor(b)}
	tm.SetExecutor(rec)

	_, err := b.DeclareQueue("q", false, false, false, nil)
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		require.NoError(t, b.PublishMessage("", "q", &protocol.Message{Exchange: "", RoutingKey: "q", Body: []byte{byte('a' + i)}}))
	}
	_, t1, _, _ := b.GetMessageForGet("q", false)
	_, t2, _, _ := b.GetMessageForGet("q", false)
	_, t3, _, _ := b.GetMessageForGet("q", false)

	const ch = uint16(1)
	require.NoError(t, tm.Select(ch))
	require.NoError(t, tm.AddOperation(ch, &interfaces.TransactionOperation{Type: interfaces.OpAck, ConsumerTag: "", DeliveryTag: t1}))
	require.NoError(t, tm.AddOperation(ch, &interfaces.TransactionOperation{Type: interfaces.OpReject, ConsumerTag: "", DeliveryTag: t2, Requeue: false}))
	require.NoError(t, tm.AddOperation(ch, &interfaces.TransactionOperation{Type: interfaces.OpNack, ConsumerTag: "", DeliveryTag: t3, Requeue: true}))

	require.NoError(t, tm.Commit(ch))

	rec.mu.Lock()
	log := append([]string(nil), rec.log...)
	rec.mu.Unlock()
	assert.Equal(t, []string{
		fmt.Sprintf("ack:%d", t1),
		fmt.Sprintf("reject:%d", t2),
		fmt.Sprintf("nack:%d", t3),
	}, log, "settlements applied in buffered order")
}

// --- §5.4 undo support -------------------------------------------------------

// TestTx_AddOperationWithUndo_RollbackRestores: undos run in REVERSE order on
// rollback, then ops and undos are cleared.
func TestTx_AddOperationWithUndo_RollbackRestores(t *testing.T) {
	tm := NewTransactionManager()
	tm.SetExecutor(&MockExecutor{})

	const ch = uint16(1)
	require.NoError(t, tm.Select(ch))

	var ran []int
	for i := 1; i <= 3; i++ {
		i := i
		err := tm.AddOperationWithUndo(ch, NewPublishOperation("ex", "k", &protocol.Message{}), func() { ran = append(ran, i) })
		require.NoError(t, err)
	}
	require.NoError(t, tm.Rollback(ch))
	assert.Equal(t, []int{3, 2, 1}, ran, "undos run in reverse order on rollback")

	ops, err := tm.GetPendingOperations(ch)
	require.NoError(t, err)
	assert.Empty(t, ops)
}

// TestTx_AddOperationWithUndo_CommitSuccessSkipsUndos: a successful commit
// clears undos WITHOUT running them (the buffered state was genuinely consumed).
func TestTx_AddOperationWithUndo_CommitSuccessSkipsUndos(t *testing.T) {
	tm := NewTransactionManager()
	tm.SetExecutor(&MockExecutor{})

	const ch = uint16(1)
	require.NoError(t, tm.Select(ch))

	ran := false
	require.NoError(t, tm.AddOperationWithUndo(ch, NewPublishOperation("ex", "k", &protocol.Message{}), func() { ran = true }))
	require.NoError(t, tm.Commit(ch))
	assert.False(t, ran, "commit success must not run undos")
}

// TestTx_AddOperationWithUndo_CommitFailureRunsUndos: a FAILED commit runs
// undos in reverse (the channel-side state is restored; the server then closes
// the channel).
func TestTx_AddOperationWithUndo_CommitFailureRunsUndos(t *testing.T) {
	tm := NewTransactionManager()
	tm.SetExecutor(&MockExecutor{fail: true, failOn: "publish"})

	const ch = uint16(1)
	require.NoError(t, tm.Select(ch))

	var ran []int
	for i := 1; i <= 2; i++ {
		i := i
		require.NoError(t, tm.AddOperationWithUndo(ch, NewPublishOperation("ex", "k", &protocol.Message{}), func() { ran = append(ran, i) }))
	}
	require.Error(t, tm.Commit(ch))
	assert.Equal(t, []int{2, 1}, ran, "failed commit runs undos in reverse")

	// Undos are cleared: a second rollback does not re-run them.
	ran = nil
	_ = tm.Rollback(ch)
	assert.Empty(t, ran)
}

// TestTx_AddOperationWithUndo_ErrorRunsUndoImmediately: when buffering itself
// fails (channel not transactional), the undo runs at once and nothing is buffered.
func TestTx_AddOperationWithUndo_ErrorRunsUndoImmediately(t *testing.T) {
	tm := NewTransactionManager()
	tm.SetExecutor(&MockExecutor{})

	ran := false
	err := tm.AddOperationWithUndo(999, NewPublishOperation("ex", "k", &protocol.Message{}), func() { ran = true })
	require.Error(t, err)
	assert.True(t, ran, "undo runs immediately when buffering fails")
}

// TestTx_CloseDropsUndos: Close(channelID) drops undos without running them
// (the channel is gone; there is nothing to restore into).
func TestTx_CloseDropsUndos(t *testing.T) {
	tm := NewTransactionManager()
	tm.SetExecutor(&MockExecutor{})

	const ch = uint16(1)
	require.NoError(t, tm.Select(ch))
	ran := false
	require.NoError(t, tm.AddOperationWithUndo(ch, NewPublishOperation("ex", "k", &protocol.Message{}), func() { ran = true }))
	require.NoError(t, tm.Close(ch))
	assert.False(t, ran, "Close drops undos without running them")
}
