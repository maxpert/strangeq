package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestClusterAckAndClaimHeldReplyHonorsReducedCreditAndCancel(t *testing.T) {
	const queue = "held-ack-refill"
	fixture := &ackClaimFrameRuntime{supported: true,
		committed: make(chan ackClaimFrameObservation, 1), reply: make(chan struct{})}
	readiness := &ackClaimReadyRuntime{ackClaimFrameRuntime: fixture,
		entered: make(chan struct{}, 1), canceled: make(chan error, 1)}
	var replyOnce sync.Once
	allowReply := func() { replyOnce.Do(func() { close(fixture.reply) }) }
	t.Cleanup(allowReply)
	b := NewClusterBroker(readiness)
	_, err := b.DeclareQueue(queue, true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, b.PublishMessage("", queue, &protocol.Message{Body: []byte("first")}))

	srv := &Server{Broker: b, Log: zap.NewNop(), Config: config.DefaultConfig()}
	conn, output := newPipeConnWithFrames(t)
	ch := protocol.NewChannel(1, conn)
	ch.PrefetchCount.Store(2)
	conn.Channels.Store(ch.ID, ch)
	rt := srv.runtimeFor(conn)
	processed, delivered := make(chan struct{}), make(chan struct{})
	go srv.processFrames(rt, processed)
	go func() {
		srv.consumerDeliveryLoop(rt)
		rt.wg.Wait()
		close(delivered)
	}()
	t.Cleanup(func() {
		allowReply()
		close(conn.Done)
		for _, done := range []chan struct{}{processed, delivered} {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("frame or delivery loop did not exit")
			}
		}
		for id := range b.GetConsumers() {
			require.NoError(t, b.UnregisterConsumer(id))
		}
		if timer := rt.closeTimer.Load(); timer != nil {
			timer.Stop()
		}
	})
	push := func(frame *protocol.Frame) {
		conn.FrameQueueBytes.Add(frameCost(frame))
		conn.FrameQueue <- frame
	}
	consume, err := (&protocol.BasicConsumeMethod{Queue: queue, ConsumerTag: "reader"}).Serialize()
	require.NoError(t, err)
	push(protocol.EncodeMethodFrameForChannel(ch.ID, 60, protocol.BasicConsume, consume))
	class, method, _ := nextMethodFrame(t, output)
	require.EqualValues(t, 60, class)
	require.EqualValues(t, 21, method, "consume-ok precedes delivery")
	first := nextAckClaimDelivery(t, output, ch.ID, "first")
	ref, ok := ch.ResolveWireTag(first.DeliveryTag)
	require.True(t, ok)
	b.mu.Lock()
	entry := b.consumers[ref.ConsumerID]
	firstRetained := b.deliveries[ref.MsgID]
	b.mu.Unlock()
	require.NotNil(t, entry)
	require.Same(t, entry.session, firstRetained.Session)
	require.EqualValues(t, 2, entry.credit.Load())

	// The only ready message has been delivered, and the consumer has entered
	// the real advisory wait. Publishing cannot release this fixture's wait.
	select {
	case <-readiness.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not enter its empty-queue readiness wait")
	}
	for _, body := range []string{"second", "third"} {
		require.NoError(t, b.PublishMessage("", queue, &protocol.Message{Body: []byte(body)}))
	}
	push(encodeAckFrame(t, ch.ID, first.DeliveryTag, false))
	var committed ackClaimFrameObservation
	select {
	case committed = <-fixture.committed:
	case <-time.After(2 * time.Second):
		t.Fatal("ACK frame did not reach the held committed reply")
	}
	require.Equal(t, clusterqueue.AckAndClaim, committed.command.Kind)
	require.Equal(t, []clusterqueue.AttemptID{firstRetained.Attempt}, committed.command.Attempts)
	require.EqualValues(t, 2, committed.command.ClaimLimit)
	require.NoError(t, committed.result.Err())
	require.NoError(t, committed.result.ClaimErr())
	require.EqualValues(t, 1, committed.result.Settled)
	require.EqualValues(t, 2, committed.result.Count)
	require.Len(t, committed.result.Deliveries, 2)
	info, err := b.describe(context.Background(), queue)
	require.NoError(t, err)
	require.Zero(t, info.Ready, "both refill attempts are already committed")
	require.EqualValues(t, 2, info.Unacknowledged)
	require.EqualValues(t, 1, entry.session.holds.Load(), "local bookkeeping still awaits the held reply")

	// A QoS frame on this connection cannot overtake its ACK in the frame processor.
	// Exercise the same broker credit-update API while that reply is held.
	b.UpdateConsumerPrefetch(ref.ConsumerID, 1)
	require.EqualValues(t, 1, entry.credit.Load())
	b.mu.Lock()
	retainedWhileHeld := len(b.deliveries)
	b.mu.Unlock()
	require.Equal(t, 1, retainedWhileHeld, "committed refill has not reached the handoff ledger")
	allowReply()
	select {
	case err := <-readiness.canceled:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("installed refill did not interrupt the advisory wait")
	}
	second := nextAckClaimDelivery(t, output, ch.ID, "second")
	require.Greater(t, second.DeliveryTag, first.DeliveryTag)

	// Probe the production handoff gate with the actual remaining committed
	// attempt. This establishes refusal at credit one without a timed absence
	// assertion or depending on when the dispatch goroutine next runs.
	remaining := committed.result.Deliveries[1]
	message, err := decodeClusterMessage(remaining.Body)
	require.NoError(t, err)
	require.Equal(t, "third", string(message.Body))
	issued, delay := b.offerBatchDelivery(ref.ConsumerID, entry, remaining.Attempt, message, 1)
	require.False(t, issued, "the second committed refill must remain pending at credit one")
	require.Zero(t, delay, "credit exhaustion waits for a credit change")
	secondRef, secondTracked := ch.ResolveWireTag(second.DeliveryTag)
	b.mu.Lock()
	retainedAfterRefill := len(b.deliveries)
	secondRetained := b.deliveries[secondRef.MsgID]
	b.mu.Unlock()
	require.Equal(t, 1, retainedAfterRefill, "only one refill reached a consumer handoff")
	require.True(t, secondTracked)
	require.Equal(t, committed.result.Deliveries[0].Attempt, secondRetained.Attempt)
	require.EqualValues(t, 2, entry.session.holds.Load(), "sent and pending refill both remain unsettled")

	cancel, err := (&protocol.BasicCancelMethod{ConsumerTag: "reader"}).Serialize()
	require.NoError(t, err)
	push(protocol.EncodeMethodFrameForChannel(ch.ID, 60, protocol.BasicCancel, cancel))
	class, method, _ = nextMethodFrame(t, output)
	require.EqualValues(t, 60, class)
	require.EqualValues(t, protocol.BasicCancelOK, method, "no second refill delivery precedes cancel-ok")
	require.ErrorIs(t, entry.session.ctx.Err(), context.Canceled)
	_, firstTracked := ch.ResolveWireTag(first.DeliveryTag)
	require.False(t, firstTracked, "the preceding ACK completed before cancellation")
	info, err = b.describe(context.Background(), queue)
	require.NoError(t, err)
	require.EqualValues(t, 2, info.Ready, "cancel recovers the sent and pending refill")
	require.Zero(t, info.Unacknowledged)

	verifier := NewClusterBroker(&fixture.state)
	t.Cleanup(func() { verifier.RequeueGetDeliveries("held-refill-verification") })
	for _, want := range []string{"second", "third"} {
		message, _, _, err := verifier.GetMessageForGetOwned(queue, true, "held-refill-verification")
		require.NoError(t, err)
		require.NotNil(t, message)
		require.Equal(t, want, string(message.Body), "release preserves the committed attempt order")
		require.True(t, message.Redelivered)
	}
	info, err = verifier.describe(context.Background(), queue)
	require.NoError(t, err)
	require.Zero(t, info.Ready, "the acknowledged original message does not return")
	require.Zero(t, info.Unacknowledged)
}
