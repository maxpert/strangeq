package server

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

func TestClusterAckAndClaimDoesNotBlockEarlierReservedHandoff(t *testing.T) {
	const queue, consumerID = "ack-handoff", "reader"
	fixture := &ackClaimFrameRuntime{supported: true,
		committed: make(chan ackClaimFrameObservation, 1), reply: make(chan struct{})}
	var replyOnce sync.Once
	allowReply := func() { replyOnce.Do(func() { close(fixture.reply) }) }
	b := NewClusterBroker(fixture)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	var settlementDone, handoffDone chan struct{}
	t.Cleanup(func() {
		allowReply()
		for _, done := range []chan struct{}{settlementDone, handoffDone} {
			if done == nil {
				continue
			}
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Error("held settlement or handoff did not exit after releasing the reply")
			}
		}
		cancel()
		require.NoError(t, b.UnregisterConsumer(consumerID))
	})
	_, err := b.DeclareQueue(queue, true, false, false, nil)
	require.NoError(t, err)
	for _, body := range []string{"first", "second", "third"} {
		require.NoError(t, b.PublishMessage("", queue, &protocol.Message{Body: []byte(body)}))
	}
	consumer := &protocol.Consumer{ID: consumerID, Tag: consumerID, Queue: queue, PrefetchCount: 2,
		Messages: make(chan *protocol.Delivery, 2), Cancel: make(chan struct{})}
	// Drive the real claim and handoff entry points explicitly, so no automatic
	// dispatcher can hand off the second reservation before the ACK is held.
	require.NoError(t, b.registerConsumer(queue, consumerID, consumer))
	b.mu.Lock()
	entry := b.consumers[consumerID]
	b.mu.Unlock()
	require.NotNil(t, entry)
	claimed, err := entry.nextManualClaims(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 2, claimed.Count)
	require.Len(t, claimed.Deliveries, 2)
	firstMessage, err := decodeClusterMessage(claimed.Deliveries[0].Body)
	require.NoError(t, err)
	issued, delay := b.offerBatchDelivery(consumerID, entry, claimed.Deliveries[0].Attempt, firstMessage, 2)
	require.True(t, issued)
	require.Zero(t, delay)
	first := <-consumer.Messages
	require.Equal(t, "first", string(first.Message.Body))

	var settled int
	var settlementErr error
	settlementDone = make(chan struct{})
	go func() {
		defer close(settlementDone)
		settled, settlementErr = b.settleWireRun(ctx, clusterqueue.AckMessage,
			[]protocol.WireDeliveryRef{{MsgID: first.DeliveryTag, ConsumerID: consumerID}})
	}()
	var committed ackClaimFrameObservation
	select {
	case committed = <-fixture.committed:
	case <-time.After(2 * time.Second):
		t.Fatal("the first ACK did not reach the held committed combined reply")
	}
	require.Equal(t, clusterqueue.AckAndClaim, committed.command.Kind)
	require.Equal(t, []clusterqueue.AttemptID{claimed.Deliveries[0].Attempt}, committed.command.Attempts)
	require.EqualValues(t, 1, committed.command.ClaimLimit)
	require.NoError(t, committed.result.Err())
	require.NoError(t, committed.result.ClaimErr())
	require.EqualValues(t, 1, committed.result.Settled)
	require.EqualValues(t, 1, committed.result.Count)
	require.Len(t, committed.result.Deliveries, 1)
	require.Greater(t, committed.result.Deliveries[0].Attempt, claimed.Deliveries[1].Attempt,
		"the held reply owns a later refill, not the already reserved second message")
	info, err := b.describe(ctx, queue)
	require.NoError(t, err)
	require.Zero(t, info.Ready)
	require.EqualValues(t, 2, info.Unacknowledged, "the old reservation and new refill are committed")
	b.mu.Lock()
	holdsBefore := entry.session.holds.Load()
	creditBefore := entry.credit.Load()
	retainedBefore := len(b.deliveries)
	b.mu.Unlock()
	require.EqualValues(t, 2, holdsBefore, "ACK bookkeeping still awaits its reply")
	require.EqualValues(t, 2, creditBefore)
	require.Equal(t, 1, retainedBefore)
	require.Less(t, holdsBefore-1, int32(creditBefore), "the earlier pending reservation has handoff credit")

	secondMessage, err := decodeClusterMessage(claimed.Deliveries[1].Body)
	require.NoError(t, err)
	require.Equal(t, "second", string(secondMessage.Body))
	var handoffIssued bool
	var handoffDelay time.Duration
	handoffStarted := make(chan struct{})
	handoffDone = make(chan struct{})
	go func() {
		defer close(handoffDone)
		close(handoffStarted)
		handoffIssued, handoffDelay = b.offerBatchDelivery(consumerID, entry,
			claimed.Deliveries[1].Attempt, secondMessage, 1)
	}()
	<-handoffStarted
	select {
	case <-handoffDone:
	case <-time.After(2 * time.Second):
		require.NoError(t, ctx.Err(), "the settlement context must still be live at the handoff deadline")
		t.Fatal("an earlier committed reservation with available credit waited for the later ACK/refill reply")
	}
	require.NoError(t, ctx.Err())
	select {
	case <-settlementDone:
		t.Fatal("the settlement unexpectedly completed before its reply was released")
	default:
	}
	require.True(t, handoffIssued, "older reservations must keep flowing while an ACK RPC waits")
	require.Zero(t, handoffDelay)
	second := <-consumer.Messages
	require.Equal(t, "second", string(second.Message.Body))
	b.mu.Lock()
	retainedAfter := len(b.deliveries)
	secondRetained := b.deliveries[second.DeliveryTag]
	creditAfter := entry.credit.Load()
	b.mu.Unlock()
	require.Equal(t, claimed.Deliveries[1].Attempt, secondRetained.Attempt)
	require.Equal(t, 2, retainedAfter, "the held ACK and earlier handoff remain in the local ledger")
	require.LessOrEqual(t, retainedAfter, int(creditAfter), "handoff preserves the active prefetch bound")

	allowReply()
	select {
	case <-settlementDone:
	case <-time.After(2 * time.Second):
		t.Fatal("settlement did not complete after releasing its response")
	}
	require.NoError(t, settlementErr)
	require.Equal(t, 1, settled)
	refill, err := entry.nextManualClaims(ctx)
	require.NoError(t, err)
	require.Len(t, refill.Deliveries, 1)
	require.Equal(t, committed.result.Deliveries[0].Attempt, refill.Deliveries[0].Attempt)
	refillMessage, err := decodeClusterMessage(refill.Deliveries[0].Body)
	require.NoError(t, err)
	require.Equal(t, "third", string(refillMessage.Body), "later refill stays queued behind the earlier handoff")
}
