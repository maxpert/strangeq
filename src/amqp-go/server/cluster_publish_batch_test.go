package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/internal/cluster"
	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestClusterPublishAmbiguousBurstNacksEveryTagAndContinues(t *testing.T) {
	const queue = "ambiguous-batch"
	state := &localClusterMachine{}
	var submits int
	b := NewClusterBroker(clusterSubmitFunc(func(ctx context.Context, key string, wire []byte) ([]byte, error) {
		command, err := clusterqueue.DecodeCommand(wire)
		if err == nil && (command.Kind == clusterqueue.PublishBatch || command.Kind == clusterqueue.PublishMessage) {
			submits++
			if submits == 1 {
				require.Equal(t, clusterqueue.PublishBatch, command.Kind, "fixture must reach the actual multi-publish submission")
				_, err := state.Submit(ctx, key, wire)
				require.NoError(t, err)
				return nil, fmt.Errorf("committed batch reply lost: %w", cluster.ErrUncertain)
			}
		}
		return state.Submit(ctx, key, wire)
	}))
	_, err := b.DeclareQueue(queue, true, false, false, nil)
	require.NoError(t, err)
	srv := &Server{Broker: b, Log: zap.NewNop(), Config: config.DefaultConfig()}
	conn, output := newPipeConnWithFrames(t)
	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(ch.ID, ch)
	ch.ConfirmMode.Store(true)
	burst := &clusterPublishBurst{server: srv, broker: b, conn: conn}
	for _, body := range []string{"one", "two", "three"} {
		require.NoError(t, srv.processCompleteMessageWithBurst(conn, ch.ID, makeDurablePendingMessage("", queue, body), burst))
	}
	require.Len(t, burst.items, 3)
	require.NoError(t, burst.flush(), "ambiguous cluster result must settle confirms without closing the healthy gateway")
	for tag := uint64(1); tag <= 3; tag++ {
		class, method, payload := nextMethodFrame(t, output)
		require.EqualValues(t, 60, class)
		require.EqualValues(t, protocol.BasicNack, method)
		nack := &protocol.BasicNackMethod{}
		require.NoError(t, nack.Deserialize(payload))
		require.Equal(t, tag, nack.DeliveryTag)
		require.False(t, nack.Multiple)
		require.False(t, nack.Requeue)
	}
	require.EqualValues(t, 3, ch.ConfirmContig(), "no ambiguous batch tag can strand the confirm frontier")
	require.NoError(t, srv.processCompleteMessageWithBurst(conn, ch.ID, makeDurablePendingMessage("", queue, "later"), burst))
	require.NoError(t, burst.flush())
	class, method, payload := nextMethodFrame(t, output)
	require.EqualValues(t, 60, class)
	require.EqualValues(t, protocol.BasicAck, method)
	ack := &protocol.BasicAckMethod{}
	require.NoError(t, ack.Deserialize(payload))
	require.EqualValues(t, 4, ack.DeliveryTag, "a later successful publish must confirm on the original channel")
	require.Equal(t, 2, submits, "an ambiguous committed batch must never be submitted again")
	require.False(t, conn.Closed.Load())
	for _, body := range []string{"one", "two", "three", "later"} {
		message, _, _, err := b.GetMessageForGetOwned(queue, true, "audit")
		require.NoError(t, err)
		require.NotNil(t, message)
		require.Equal(t, body, string(message.Body))
	}
	message, _, _, err := b.GetMessageForGetOwned(queue, true, "audit")
	require.NoError(t, err)
	require.Nil(t, message, "negative confirmation reports uncertainty without duplicating committed messages")
}

func TestClusterRecoverablePublishPreservesNoConfirmErrorsAndWireFailures(t *testing.T) {
	for _, outcome := range []error{cluster.ErrUnavailable, cluster.ErrUncertain} {
		t.Run(outcome.Error(), func(t *testing.T) {
			srv := &Server{Log: zap.NewNop()}
			message := &protocol.Message{Body: []byte("unresolved")}
			conn, _ := newPipeConnWithFrames(t)
			require.ErrorIs(t, srv.finishPublish(conn, 1, message, nil, 0, time.Time{}, false, outcome), outcome,
				"a publisher without confirms must retain its existing error behavior")
			client, server := net.Pipe()
			t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
			wireError := errors.New("injected nack write failure")
			broken := protocol.NewConnection(&errorConn{Conn: server, writeErr: wireError})
			ch := protocol.NewChannel(1, broken)
			ch.ConfirmMode.Store(true)
			require.ErrorIs(t, srv.finishPublish(broken, 1, message, ch, 1, time.Time{}, false, outcome), wireError,
				"recoverable shard failure must not hide an actual gateway wire failure")
		})
	}
}

func TestClusterPublishBurstsRespectTopologyAndConfirmOrder(t *testing.T) {
	const (
		queue    = "jobs"
		exchange = "jobs.direct"
		key      = "new"
	)
	state := &localClusterMachine{}
	firstSubmit := make(chan struct{})
	commitAllowed := make(chan struct{})
	var allowOnce sync.Once
	releaseCommit := func() { allowOnce.Do(func() { close(commitAllowed) }) }

	var submissions []clusterqueue.QueueCommandKind
	b := NewClusterBroker(clusterSubmitFunc(func(ctx context.Context, key string, wire []byte) ([]byte, error) {
		if command, err := clusterqueue.DecodeCommand(wire); err == nil &&
			(command.Kind == clusterqueue.PublishMessage || command.Kind == clusterqueue.PublishBatch) {
			submissions = append(submissions, command.Kind)
			if len(submissions) == 1 {
				close(firstSubmit)
				select {
				case <-commitAllowed:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
		}
		return state.Submit(ctx, key, wire)
	}))
	_, err := b.DeclareQueue(queue, true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, b.DeclareExchange(exchange, "direct", true, false, false, nil))
	require.NoError(t, b.BindQueue(queue, exchange, key, nil))

	srv := &Server{Broker: b, Log: zap.NewNop(), Config: config.DefaultConfig()}
	conn, output := newPipeConnWithFrames(t)
	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(ch.ID, ch)
	ch.ConfirmMode.Store(true)

	push := func(frame *protocol.Frame) {
		conn.FrameQueueBytes.Add(frameCost(frame))
		conn.FrameQueue <- frame
	}
	publish := func(body string) {
		method := &protocol.BasicPublishMethod{Exchange: exchange, RoutingKey: key, Mandatory: true}
		payload, err := method.Serialize()
		require.NoError(t, err)
		push(protocol.EncodeMethodFrameForChannel(ch.ID, 60, protocol.BasicPublish, payload))
		header := &protocol.ContentHeader{ClassID: 60, BodySize: uint64(len(body)),
			DeliveryMode: 2, PropertyFlags: protocol.FlagDeliveryMode}
		payload, err = header.Serialize()
		require.NoError(t, err)
		push(&protocol.Frame{Type: protocol.FrameHeader, Channel: ch.ID, Size: uint32(len(payload)), Payload: payload})
		push(&protocol.Frame{Type: protocol.FrameBody, Channel: ch.ID, Size: uint32(len(body)), Payload: []byte(body)})
	}
	publish("one")
	publish("two")
	unbind := &protocol.QueueUnbindMethod{Queue: queue, Exchange: exchange, RoutingKey: key}
	payload, err := unbind.Serialize()
	require.NoError(t, err)
	push(protocol.EncodeMethodFrameForChannel(ch.ID, 50, protocol.QueueUnbind, payload))
	publish("returned")
	push(protocol.EncodeMethodFrameForChannel(ch.ID, 20, protocol.ChannelClose, nil))
	close(conn.FrameQueue)

	processorDone := make(chan struct{})
	go srv.processFrames(srv.runtimeFor(conn), processorDone)
	t.Cleanup(func() {
		releaseCommit()
		select {
		case <-processorDone:
		case <-time.After(5 * time.Second):
			t.Error("frame processor did not exit")
		}
	})

	select {
	case <-firstSubmit:
	case <-time.After(2 * time.Second):
		t.Fatal("frame processor did not submit a publish")
	}
	require.Zero(t, ch.ConfirmContig(), "confirms must wait for the committed publish reply")
	expectNoFrame(t, output, "topology and confirm frames must wait for the preceding publish")
	releaseCommit()

	var sawUnbind, sawReturn, sawClose bool
	var confirmed uint64
	for !sawClose {
		class, method, payload := nextMethodFrame(t, output)
		switch {
		case class == 50 && method == protocol.QueueUnbindOK:
			sawUnbind = true
		case class == 60 && method == 50: // basic.return
			require.True(t, sawUnbind)
			ret := &protocol.BasicReturnMethod{}
			require.NoError(t, ret.Deserialize(payload))
			require.Equal(t, uint16(312), ret.ReplyCode)
			require.Equal(t, exchange, ret.Exchange)
			require.Equal(t, key, ret.RoutingKey)
			require.EqualValues(t, protocol.FrameHeader, nextFrame(t, output).Type)
			body := nextFrame(t, output)
			require.EqualValues(t, protocol.FrameBody, body.Type)
			require.Equal(t, "returned", string(body.Payload))
			sawReturn = true
		case class == 60 && method == protocol.BasicAck:
			ack := &protocol.BasicAckMethod{}
			require.NoError(t, ack.Deserialize(payload))
			if ack.DeliveryTag == 3 {
				require.True(t, sawReturn, "return content must precede any confirm covering tag 3")
			}
			require.Greater(t, ack.DeliveryTag, confirmed)
			confirmed = ack.DeliveryTag
		case class == 20 && method == protocol.ChannelCloseOK:
			require.EqualValues(t, 3, confirmed, "close-ok must follow the final confirm")
			sawClose = true
		default:
			t.Fatalf("unexpected method %d.%d", class, method)
		}
	}
	require.True(t, sawReturn)
	select {
	case <-processorDone:
	case <-time.After(5 * time.Second):
		t.Fatal("frame processor did not exit after draining the frame queue")
	}

	t.Cleanup(func() { b.RequeueGetDeliveries("verification") })
	for _, want := range []string{"one", "two"} {
		message, _, _, err := b.GetMessageForGetOwned(queue, true, "verification")
		require.NoError(t, err)
		require.NotNil(t, message)
		require.Equal(t, want, string(message.Body))
	}
	message, _, _, err := b.GetMessageForGetOwned(queue, true, "verification")
	require.NoError(t, err)
	require.Nil(t, message, "the mandatory publish after unbind must not reach the queue")
	require.Equal(t, []clusterqueue.QueueCommandKind{clusterqueue.PublishBatch}, submissions,
		"the two publishes before queue.unbind must reach the queue in a single batch")
}
