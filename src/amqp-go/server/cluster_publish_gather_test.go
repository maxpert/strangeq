package server

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
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

type gatherPublishPeer struct {
	machine *localClusterMachine
	writes  chan []byte
	commit  <-chan struct{}
}

func (*gatherPublishPeer) SupportsAssignments() bool { return true }

func (p *gatherPublishPeer) Query(_ context.Context, _ string, command []byte) ([]byte, error) {
	p.machine.mu.Lock()
	defer p.machine.mu.Unlock()
	return p.machine.machine.Query(command)
}

func (p *gatherPublishPeer) Submit(ctx context.Context, key string, command []byte) ([]byte, error) {
	if decoded, err := clusterqueue.DecodeCommand(command); err == nil &&
		(decoded.Kind == clusterqueue.PublishMessage || decoded.Kind == clusterqueue.PublishBatch) {
		p.writes <- command
		select {
		case <-p.commit:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return p.machine.Submit(ctx, key, command)
}

func TestClusterPublishGatherCombinesCompatibleWaves(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const queue, exchange, key = "gather-jobs", "gather-direct", "jobs"
		const wave = 32
		commit := make(chan struct{})
		peer := &gatherPublishPeer{machine: &localClusterMachine{}, writes: make(chan []byte, 64), commit: commit}
		broker := NewClusterBroker(peer)
		_, err := broker.DeclareQueue(queue, true, false, false, nil)
		require.NoError(t, err)
		require.NoError(t, broker.DeclareExchange(exchange, "direct", true, false, false, nil))
		require.NoError(t, broker.BindQueue(queue, exchange, key, nil))

		srv := &Server{Broker: broker, Log: zap.NewNop(), Config: config.DefaultConfig()}
		conn, output := newPipeConnWithFrames(t)
		ch := protocol.NewChannel(1, conn)
		conn.Channels.Store(ch.ID, ch)
		ch.ConfirmMode.Store(true)
		push := func(frame *protocol.Frame) {
			conn.FrameQueueBytes.Add(frameCost(frame))
			conn.FrameQueue <- frame
		}
		body := func(index int) string { return fmt.Sprintf("%04d", index) + strings.Repeat("x", 1020) }
		publish := func(from, to int) {
			t.Helper()
			for index := from; index < to; index++ {
				method := &protocol.BasicPublishMethod{Exchange: exchange, RoutingKey: key, Mandatory: true}
				wire, err := method.Serialize()
				require.NoError(t, err)
				push(protocol.EncodeMethodFrameForChannel(ch.ID, 60, protocol.BasicPublish, wire))
				payload := []byte(body(index))
				header := &protocol.ContentHeader{ClassID: 60, BodySize: uint64(len(payload)),
					DeliveryMode: 2, PropertyFlags: protocol.FlagDeliveryMode}
				wire, err = header.Serialize()
				require.NoError(t, err)
				push(&protocol.Frame{Type: protocol.FrameHeader, Channel: ch.ID, Size: uint32(len(wire)), Payload: wire})
				push(&protocol.Frame{Type: protocol.FrameBody, Channel: ch.ID, Size: uint32(len(payload)), Payload: payload})
			}
		}

		publish(0, wave)
		done := make(chan struct{})
		go srv.processFrames(srv.runtimeFor(conn), done)
		var releaseOnce, closeOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(commit) }) }
		closeFrames := func() { closeOnce.Do(func() { close(conn.FrameQueue) }) }
		defer func() {
			release()
			closeFrames()
			<-done
		}()

		synctest.Wait()
		require.Empty(t, conn.FrameQueue, "the first wave must drain through a transient empty frame queue")
		require.EqualValues(t, wave, ch.ConfirmSequence.Load(), "the first wave must reach normal publish staging")
		require.Empty(t, peer.writes, "a transient empty frame queue must keep a compatible small burst eligible to gather")

		time.Sleep(100 * time.Microsecond)
		publish(wave, 2*wave)
		synctest.Wait()
		require.Len(t, peer.writes, 1, "both waves must reach one submit before the gather deadline")
		command, err := clusterqueue.DecodeCommand(<-peer.writes)
		require.NoError(t, err)
		require.Equal(t, clusterqueue.PublishBatch, command.Kind)
		require.Equal(t, queue, command.Queue)
		records := command.Body
		for index := range 2 * wave {
			require.GreaterOrEqual(t, len(records), 4)
			size := int(binary.BigEndian.Uint32(records[:4]))
			require.LessOrEqual(t, size, len(records)-4)
			message, err := decodeClusterMessage(records[4 : 4+size])
			require.NoError(t, err)
			require.Equal(t, body(index), string(message.Body))
			records = records[4+size:]
		}
		require.Empty(t, records, "the command must contain exactly 64 ordered records")
		push(protocol.EncodeMethodFrameForChannel(ch.ID, 20, protocol.ChannelClose, nil))
		synctest.Wait()
		require.Zero(t, ch.ConfirmContig(), "the blocked submit has no authoritative completion")
		require.Empty(t, output, "the confirm and channel.close-ok must wait for the batch commit")

		release()
		closeFrames()
		synctest.Wait()
		require.Empty(t, peer.writes)
		require.Len(t, output, 2, "one cumulative confirm must precede channel.close-ok")
		frame := <-output
		class, method, ok := methodIDs(frame)
		require.True(t, ok)
		require.EqualValues(t, 60, class)
		require.EqualValues(t, protocol.BasicAck, method)
		var ack protocol.BasicAckMethod
		require.NoError(t, ack.Deserialize(frame.Payload[4:]))
		require.EqualValues(t, 2*wave, ack.DeliveryTag)
		require.True(t, ack.Multiple)
		class, method, ok = methodIDs(<-output)
		require.True(t, ok)
		require.EqualValues(t, 20, class)
		require.EqualValues(t, protocol.ChannelCloseOK, method)
	})
}
