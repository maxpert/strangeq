package server

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type assignmentAckGatherPeer struct {
	localClusterMachine
	acks   chan clusterqueue.QueueCommand
	commit chan struct{}
}

func (*assignmentAckGatherPeer) SupportsAssignments() bool { return true }

func (p *assignmentAckGatherPeer) Submit(ctx context.Context, queue string, request []byte) ([]byte, error) {
	command, err := clusterqueue.DecodeCommand(request)
	if err != nil {
		return nil, err
	}
	switch command.Kind {
	case clusterqueue.AckMessage, clusterqueue.SettleBatch:
		p.acks <- command
		select {
		case <-p.commit:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return p.localClusterMachine.Submit(ctx, queue, request)
}

func (p *assignmentAckGatherPeer) ReadAssignmentPage(_ context.Context, request clusterqueue.AssignmentRequest) ([]clusterqueue.Delivery, clusterqueue.AttemptID, error) {
	p.mu.Lock()
	encode, _, err := p.machine.CaptureAssignmentPage(request)
	p.mu.Unlock()
	if err != nil {
		return nil, request.After, err
	}
	data, err := encode()
	if err != nil {
		return nil, request.After, err
	}
	return clusterqueue.DecodeAssignmentPage(data, request)
}

func (*assignmentAckGatherPeer) WaitAssignmentReady(ctx context.Context, _ clusterqueue.AssignmentRequest) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestClusterAckGatherCombinesSparseWaves(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const queue = "ack-gather"
		const maxGather = 125 * time.Microsecond
		peer := &assignmentAckGatherPeer{
			localClusterMachine: localClusterMachine{machine: *NewClusterAssignmentMachine()},
			acks:                make(chan clusterqueue.QueueCommand, 3),
			commit:              make(chan struct{}),
		}
		b := NewClusterBroker(peer)
		_, err := b.DeclareQueue(queue, true, false, false, nil)
		require.NoError(t, err)
		for _, body := range []string{"first", "skipped", "last"} {
			require.NoError(t, b.PublishMessage("", queue, &protocol.Message{Body: []byte(body)}))
		}

		srv := &Server{Broker: b, Log: zap.NewNop(), Config: config.DefaultConfig()}
		conn, output := newPipeConnWithFrames(t)
		ch := protocol.NewChannel(1, conn)
		ch.PrefetchCount.Store(3)
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
		require.EqualValues(t, 21, method, "basic.consume-ok")

		var tags []uint64
		var attempts []clusterqueue.AttemptID
		var session *clusterSession
		for _, body := range []string{"first", "skipped", "last"} {
			delivery := nextAckClaimDelivery(t, output, ch.ID, body)
			tags = append(tags, delivery.DeliveryTag)
			ref, ok := ch.ResolveWireTag(delivery.DeliveryTag)
			require.True(t, ok)
			b.mu.Lock()
			entry := b.deliveries[ref.MsgID]
			b.mu.Unlock()
			require.NotNil(t, entry.Session)
			session = entry.Session
			attempts = append(attempts, entry.Attempt)
		}
		require.True(t, session.autoAssign)
		require.EqualValues(t, 3, session.holds.Load())

		push(encodeAckFrame(t, ch.ID, tags[0], false))
		synctest.Wait()
		require.Empty(t, conn.FrameQueue, "the first wave drained through a transient empty frame queue")
		time.Sleep(50 * time.Microsecond)
		push(encodeAckFrame(t, ch.ID, tags[2], false))
		barrier, err := (&protocol.QueueDeclareMethod{Queue: queue, Passive: true}).Serialize()
		require.NoError(t, err)
		push(protocol.EncodeMethodFrameForChannel(ch.ID, 50, protocol.QueueDeclare, barrier))

		synctest.Wait()
		require.Len(t, peer.acks, 1, "the earlier ACKs reach the held durable mutation")
		require.Empty(t, output, "the control reply cannot overtake the blocked settlement")
		require.EqualValues(t, 3, session.holds.Load(), "holds still include all uncommitted ACKs")
		info, err := b.describe(context.Background(), queue)
		require.NoError(t, err)
		require.EqualValues(t, 3, info.Unacknowledged, "holds still exist on the shard before apply")

		releaseCommit()
		class, method, _ = nextMethodFrame(t, output)
		require.EqualValues(t, 50, class)
		require.EqualValues(t, 11, method, "queue.declare-ok follows settlement")
		synctest.Wait()
		require.EqualValues(t, 1, session.holds.Load())
		info, err = b.describe(context.Background(), queue)
		require.NoError(t, err)
		require.EqualValues(t, 1, info.Unacknowledged)
		for i, tag := range tags {
			_, tracked := ch.ResolveWireTag(tag)
			require.Equal(t, i == 1, tracked, "the skipped wire tag stays outstanding")
		}
		require.Len(t, peer.acks, 1, "compatible sparse ACK waves must use one durable mutation")
		ack := <-peer.acks
		require.Equal(t, clusterqueue.SettleBatch, ack.Kind)
		require.Equal(t, clusterqueue.AckMessage, ack.Settlement)
		require.Equal(t, []clusterqueue.AttemptID{attempts[0], attempts[2]}, ack.Attempts)

		push(encodeAckFrame(t, ch.ID, tags[1], false))
		synctest.Wait()
		require.Empty(t, peer.acks, "the lone ACK starts a bounded gather")
		time.Sleep(maxGather)
		synctest.Wait()
		require.Len(t, peer.acks, 1, "the lone ACK progresses at the deadline without a new frame")
		ack = <-peer.acks
		require.Equal(t, clusterqueue.AckMessage, ack.Kind)
		require.Equal(t, attempts[1], ack.Attempt)
		require.Zero(t, session.holds.Load())
	})
}
