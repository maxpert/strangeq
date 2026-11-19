package server

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestClusterAutoAckConsumerExitPreservesBufferedDelivery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const queue = "jobs"
		state := &localClusterMachine{}
		releasing, allowRelease, cancelled := make(chan struct{}), make(chan struct{}), make(chan struct{})
		defer close(allowRelease)
		b := NewClusterBroker(clusterSubmitFunc(func(ctx context.Context, key string, request []byte) ([]byte, error) {
			command, err := clusterqueue.DecodeCommand(request)
			if err != nil {
				return nil, err
			}
			if command.Kind == clusterqueue.ReleaseQueueSession {
				close(releasing)
				<-allowRelease
			}
			return state.Submit(ctx, key, request)
		}))
		b.cancelConsumers = func([]string) { close(cancelled) }
		_, err := b.DeclareQueue(queue, true, false, false, nil)
		require.NoError(t, err)
		for _, body := range []string{"held by forwarder", "buffered for forwarder"} {
			require.NoError(t, b.PublishMessage("", queue, &protocol.Message{Body: []byte(body)}))
		}

		channel := protocol.NewChannel(1, nil)
		channel.FlowActive.Store(false)
		consumer := &protocol.Consumer{ID: "reader", NoAck: true, Messages: make(chan *protocol.Delivery, 1), Cancel: make(chan struct{})}
		require.NoError(t, b.registerConsumer(queue, consumer.ID, consumer))
		entry := b.consumers[consumer.ID]
		defer entry.session.cancel()
		fanIn := make(chan *protocol.Delivery, 2)
		stop := make(chan struct{})
		defer close(stop)
		go forwardConsumerMessages(channel, consumer.Messages, fanIn, stop)
		b.startConsumer(consumer.ID)
		synctest.Wait()
		require.Len(t, consumer.Messages, 1, "flow control must leave the second delivery buffered")
		require.Empty(t, fanIn)
		info, err := b.describe(context.Background(), queue)
		require.NoError(t, err)
		require.Zero(t, info.Ready)
		require.Zero(t, info.Unacknowledged, "both deliveries must already be settled")

		entry.session.cancel()
		synctest.Wait()
		select {
		case <-releasing:
		default:
			t.Fatal("consumer exit did not reach session release")
		}
		select {
		case <-cancelled:
			t.Fatal("consumer was cancelled before session release")
		default:
		}
		channel.FlowActive.Store(true)
		channel.FlowWake <- struct{}{}
		synctest.Wait()
		require.Len(t, fanIn, 2, "a live forwarder must receive the buffered auto-ack delivery before consumer cancellation")
		require.Equal(t, "held by forwarder", string((<-fanIn).Message.Body))
		require.Equal(t, "buffered for forwarder", string((<-fanIn).Message.Body))
	})
}

func TestRemovedConsumerCountsDiscardedAutoAckDeliveries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		spy := &wireSpy{}
		s := &Server{Log: zap.NewNop(), MetricsCollector: spy}
		conn := protocol.NewConnection(nil)
		channel := protocol.NewChannel(1, conn)
		channel.FlowActive.Store(false)
		consumer := &protocol.Consumer{ID: "reader", Tag: "reader", NoAck: true, Messages: make(chan *protocol.Delivery, 2)}
		channel.Consumers[consumer.Tag] = consumer
		conn.Channels.Store(channel.ID, channel)
		for range 2 {
			consumer.Messages <- &protocol.Delivery{ConsumerID: consumer.ID, NoAck: true}
		}

		rt := s.runtimeFor(conn)
		defer rt.leaseCancel()
		consumerInfos := make(map[string]*consumerInfo)
		forwarders := make(map[string]chan struct{})
		activeIDs := make(map[string]struct{})
		fanIn := make(chan *protocol.Delivery)
		defer func() {
			for _, stop := range forwarders {
				close(stop)
			}
			rt.deliveryForwarders.Wait()
		}()
		s.discoverConsumers(rt, consumerInfos, forwarders, activeIDs, fanIn)
		synctest.Wait()
		require.Len(t, consumer.Messages, 1, "flow control must park the forwarder holding the first delivery")

		delete(channel.Consumers, consumer.Tag)
		s.discoverConsumers(rt, consumerInfos, forwarders, activeIDs, fanIn)
		rt.deliveryForwarders.Wait()
		require.EqualValues(t, 2, spy.droppedCount(DeliveryDropConsumerRemoved), "one held and one buffered auto-ack delivery were discarded")
		require.Empty(t, consumer.Messages)
	})
}

func TestClusterManualConsumerBatchesClaimsAndFencesDeliveryByCredit(t *testing.T) {
	const queue = "jobs"
	type claimObservation struct {
		kind  clusterqueue.QueueCommandKind
		count uint64
	}
	state := &localClusterMachine{}
	committed := make(chan claimObservation, 1)
	releaseReply := make(chan struct{})
	var releaseOnce sync.Once
	allowReply := func() { releaseOnce.Do(func() { close(releaseReply) }) }
	t.Cleanup(allowReply)
	var claims atomic.Int32
	b := NewClusterBroker(clusterSubmitFunc(func(ctx context.Context, key string, request []byte) ([]byte, error) {
		reply, err := state.Submit(ctx, key, request)
		if err != nil {
			return nil, err
		}
		command, err := clusterqueue.DecodeCommand(request)
		if err != nil {
			return nil, err
		}
		if (command.Kind == clusterqueue.ClaimMessage || command.Kind == clusterqueue.ClaimBatch) && claims.Add(1) == 1 {
			result, err := clusterqueue.DecodeResponse(reply)
			if err != nil {
				return nil, err
			}
			count := result.Count
			if result.Result.Delivery.Attempt != 0 {
				count = 1
			}
			committed <- claimObservation{kind: command.Kind, count: count}
			select {
			case <-releaseReply:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return reply, nil
	}))
	_, err := b.DeclareQueue(queue, true, false, false, nil)
	require.NoError(t, err)
	bodies := []string{"first", "second", "third", "fourth"}
	for _, body := range bodies {
		require.NoError(t, b.PublishMessage("", queue, &protocol.Message{Body: []byte(body)}))
	}

	srv := &Server{Broker: b, Log: zap.NewNop(), Config: config.DefaultConfig()}
	conn, output := newPipeConnWithFrames(t)
	ch := protocol.NewChannel(1, conn)
	ch.PrefetchCount.Store(3)
	conn.Channels.Store(ch.ID, ch)
	runtime := srv.runtimeFor(conn)
	deliveryDone := make(chan struct{})
	go func() {
		srv.consumerDeliveryLoop(runtime)
		runtime.wg.Wait()
		close(deliveryDone)
	}()
	t.Cleanup(func() {
		allowReply()
		close(conn.Done)
		select {
		case <-deliveryDone:
		case <-time.After(5 * time.Second):
			t.Error("delivery loop did not exit")
		}
		for id := range b.GetConsumers() {
			require.NoError(t, b.UnregisterConsumer(id))
		}
	})

	consume, err := (&protocol.BasicConsumeMethod{Queue: queue, ConsumerTag: "reader"}).Serialize()
	require.NoError(t, err)
	require.NoError(t, srv.handleBasicConsume(conn, ch.ID, consume))
	class, method, _ := nextMethodFrame(t, output)
	require.EqualValues(t, 60, class)
	require.EqualValues(t, 21, method, "basic.consume-ok")

	var firstClaim claimObservation
	select {
	case firstClaim = <-committed:
	case <-time.After(2 * time.Second):
		t.Fatal("consumer did not commit a claim")
	}
	require.Positive(t, firstClaim.count)
	require.LessOrEqual(t, firstClaim.count, uint64(3), "initial prefetch is 3")
	info, err := b.describe(context.Background(), queue)
	require.NoError(t, err)
	require.Equal(t, firstClaim.count, info.Unacknowledged)
	expectNoFrame(t, output, "a claim reply is still needed before delivering")

	qos, err := (&protocol.BasicQosMethod{PrefetchCount: 1}).Serialize()
	require.NoError(t, err)
	require.NoError(t, srv.handleBasicQos(conn, ch.ID, qos))
	class, method, _ = nextMethodFrame(t, output)
	require.EqualValues(t, 60, class)
	require.EqualValues(t, 11, method, "basic.qos-ok")
	allowReply()

	class, method, payload := nextMethodFrame(t, output)
	require.EqualValues(t, 60, class)
	require.EqualValues(t, 60, method, "basic.deliver")
	delivery := &protocol.BasicDeliverMethod{}
	require.NoError(t, delivery.Deserialize(payload))
	require.Equal(t, "reader", delivery.ConsumerTag)
	require.EqualValues(t, protocol.FrameHeader, nextFrame(t, output).Type)
	body := nextFrame(t, output)
	require.EqualValues(t, protocol.FrameBody, body.Type)
	require.Equal(t, "first", string(body.Payload))
	_, tracked := ch.ResolveWireTag(delivery.DeliveryTag)
	require.True(t, tracked)

	cancel, err := (&protocol.BasicCancelMethod{ConsumerTag: "reader"}).Serialize()
	require.NoError(t, err)
	require.NoError(t, srv.handleBasicCancel(conn, ch.ID, cancel))
	class, method, _ = nextMethodFrame(t, output)
	require.EqualValues(t, 60, class)
	require.EqualValues(t, protocol.BasicCancelOK, method, "lowered credit permits only one delivery before cancel")
	info, err = b.describe(context.Background(), queue)
	require.NoError(t, err)
	require.EqualValues(t, len(bodies), info.Ready)
	require.Zero(t, info.Unacknowledged)

	// Cancellation appends the claimed prefix behind ready messages. The
	// observed prefix is one on the baseline, and three with claim batching.
	count := int(firstClaim.count)
	expected := append(append([]string(nil), bodies[count:]...), bodies[:count]...)
	t.Cleanup(func() { b.RequeueGetDeliveries("verification") })
	for i, want := range expected {
		message, _, _, err := b.GetMessageForGetOwned(queue, true, "verification")
		require.NoError(t, err)
		require.NotNil(t, message)
		require.Equal(t, want, string(message.Body))
		require.Equal(t, i >= len(bodies)-count, message.Redelivered)
	}
	require.Equal(t, claimObservation{kind: clusterqueue.ClaimBatch, count: 3}, firstClaim,
		"one replicated claim must reserve the initial credit for preloaded messages")
}
