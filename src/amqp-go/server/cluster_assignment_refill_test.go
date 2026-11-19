package server

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type heldAssignmentPagePeer struct {
	assignmentAckGatherPeer
	pageStarted chan struct{}
	firstHold   sync.Once
}

func (p *heldAssignmentPagePeer) ReadAssignmentPage(ctx context.Context, request clusterqueue.AssignmentRequest) ([]clusterqueue.Delivery, clusterqueue.AttemptID, error) {
	if request.After != 0 {
		p.firstHold.Do(func() { close(p.pageStarted) })
		<-ctx.Done()
	}
	return p.assignmentAckGatherPeer.ReadAssignmentPage(ctx, request)
}

func TestBufferedAssignmentPageWaitsForEarlierAttempts(t *testing.T) {
	session := &clusterSession{
		assignmentCursor: 18,
		assignmentPage: bufferedAssignmentPage{
			after: 20, through: 23,
			deliveries: []clusterqueue.Delivery{{Attempt: 21}, {Attempt: 22}, {Attempt: 23}},
		},
	}
	require.False(t, session.assignmentPage.ready(session.assignmentCursor))
	require.Empty(t, session.takeBufferedAssignmentPage().deliveries)
	require.Len(t, session.assignmentPage.deliveries, 3)

	// The ordinary read fills the gap and also claims the first overlapping
	// item. Accepting the buffered page must only return its remaining suffix.
	session.assignmentCursor = 21
	require.True(t, session.assignmentPage.ready(session.assignmentCursor))
	page := session.takeBufferedAssignmentPage()
	require.Equal(t, []clusterqueue.Delivery{{Attempt: 22}, {Attempt: 23}}, page.deliveries)
	require.EqualValues(t, 23, page.through)
	require.Empty(t, session.takeBufferedAssignmentPage().deliveries)
}

func TestBufferedAssignmentPageCannotCancelForAnEmptySuffix(t *testing.T) {
	page := bufferedAssignmentPage{
		after: 20, through: 23, deliveries: []clusterqueue.Delivery{{Attempt: 21}},
	}
	require.False(t, page.ready(21), "a watermark without an undelivered record cannot replace an ordinary read")
}

func TestSettlementRequestsMoreAssignmentsAfterTheBufferedPage(t *testing.T) {
	session := &clusterSession{
		broker: &ClusterBroker{}, autoAssign: true, assignmentCursor: 18,
		assignmentPage: bufferedAssignmentPage{
			after: 20, through: 22, deliveries: []clusterqueue.Delivery{{Attempt: 21}, {Attempt: 22}},
		},
	}
	command := clusterqueue.QueueCommand{Kind: clusterqueue.AckMessage}
	session.requestAssignmentPage(&command)
	require.NotNil(t, command.AssignmentAfter, "a pending page with room should accept contiguous assignments from another ACK")
	require.EqualValues(t, 22, *command.AssignmentAfter)

	next := bufferedAssignmentPage{after: 22, through: 23, deliveries: []clusterqueue.Delivery{{Attempt: 23}}, wireBytes: 64}
	require.True(t, session.assignmentPage.extend(next))
	require.EqualValues(t, 23, session.assignmentPage.through)
	gap := bufferedAssignmentPage{after: 24, through: 25, deliveries: []clusterqueue.Delivery{{Attempt: 25}}, wireBytes: 64}
	require.False(t, session.assignmentPage.extend(gap))
	next.after, next.through, next.wireBytes = 23, 24, clusterqueue.MaxSettlementAssignmentPageBytes
	require.False(t, session.assignmentPage.extend(next))

	session.assignmentCursor = 22
	page := session.takeBufferedAssignmentPage()
	require.Equal(t, []clusterqueue.Delivery{{Attempt: 23}}, page.deliveries)
}

func TestClusterSettlementCanDeliverWhileOrdinaryPageReadIsHeld(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const queue = "assignment-refill"
		peer := &heldAssignmentPagePeer{
			assignmentAckGatherPeer: assignmentAckGatherPeer{
				localClusterMachine: localClusterMachine{machine: *NewClusterAssignmentMachine()},
				acks:                make(chan clusterqueue.QueueCommand, 1),
				commit:              make(chan struct{}),
			},
			pageStarted: make(chan struct{}),
		}
		b := NewClusterBroker(peer)
		_, err := b.DeclareQueue(queue, true, false, false, nil)
		require.NoError(t, err)
		for _, body := range []string{"first", "replacement", "still queued"} {
			require.NoError(t, b.PublishMessage("", queue, &protocol.Message{Body: []byte(body)}))
		}

		srv := &Server{Broker: b, Log: zap.NewNop(), Config: config.DefaultConfig()}
		conn, output := newPipeConnWithFrames(t)
		ch := protocol.NewChannel(1, conn)
		ch.PrefetchCount.Store(1)
		conn.Channels.Store(ch.ID, ch)
		rt := srv.runtimeFor(conn)
		processed, delivered := make(chan struct{}), make(chan struct{})
		go srv.processFrames(rt, processed)
		go func() {
			srv.consumerDeliveryLoop(rt)
			rt.wg.Wait()
			close(delivered)
		}()
		releaseCommit := sync.OnceFunc(func() { close(peer.commit) })
		defer func() {
			releaseCommit()
			close(conn.Done)
			<-processed
			<-delivered
			for id := range b.GetConsumers() {
				require.NoError(t, b.UnregisterConsumer(id))
			}
		}()
		push := func(frame *protocol.Frame) {
			conn.FrameQueueBytes.Add(frameCost(frame))
			conn.FrameQueue <- frame
		}
		consume, err := (&protocol.BasicConsumeMethod{Queue: queue, ConsumerTag: "reader"}).Serialize()
		require.NoError(t, err)
		push(protocol.EncodeMethodFrameForChannel(ch.ID, 60, protocol.BasicConsume, consume))
		class, method, _ := nextMethodFrame(t, output)
		require.EqualValues(t, 60, class)
		require.EqualValues(t, 21, method)
		first := nextAckClaimDelivery(t, output, ch.ID, "first")
		<-peer.pageStarted
		push(encodeAckFrame(t, ch.ID, first.DeliveryTag, false))
		require.Equal(t, clusterqueue.AckMessage, (<-peer.acks).Kind)
		synctest.Wait()
		require.Empty(t, output, "replacement must wait for the settlement")

		releaseCommit()
		synctest.Wait()
		require.NotEmpty(t, output, "the committed settlement should supply the replacement without the held page read")
		second := nextAckClaimDelivery(t, output, ch.ID, "replacement")
		require.Greater(t, second.DeliveryTag, first.DeliveryTag)
		synctest.Wait()
		require.Empty(t, output, "credit of one cannot send the replacement twice or send the next ready message yet")
	})
}
