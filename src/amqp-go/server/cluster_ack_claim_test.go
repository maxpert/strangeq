package server

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type ackClaimFrameObservation struct {
	command clusterqueue.QueueCommand
	result  clusterqueue.QueueResponse
}

type ackClaimFrameRuntime struct {
	state     localClusterMachine
	supported bool
	mu        sync.Mutex
	commands  []clusterqueue.QueueCommand
	firstAck  atomic.Bool
	committed chan ackClaimFrameObservation
	reply     chan struct{}
}

type ackClaimReadyRuntime struct {
	*ackClaimFrameRuntime
	entered  chan struct{}
	canceled chan error
}

func (f *ackClaimReadyRuntime) WaitQueueReady(ctx context.Context, _ string, _ clusterqueue.QueueID) error {
	select {
	case f.entered <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	<-ctx.Done()
	f.canceled <- ctx.Err()
	return ctx.Err()
}

// This optional runtime capability is deliberately absent from production
// runtimes until their replicated state machines can all accept AckAndClaim.
func (f *ackClaimFrameRuntime) SupportsAckAndClaim() bool { return f.supported }

func (f *ackClaimFrameRuntime) Submit(ctx context.Context, queue string, request []byte) ([]byte, error) {
	command, err := clusterqueue.DecodeCommand(request)
	if err != nil {
		return nil, err
	}
	switch command.Kind {
	case clusterqueue.ClaimMessage, clusterqueue.ClaimBatch, clusterqueue.AckMessage, clusterqueue.SettleBatch, clusterqueue.AckAndClaim:
		f.mu.Lock()
		f.commands = append(f.commands, command)
		f.mu.Unlock()
	}
	wire, err := f.state.Submit(ctx, queue, request)
	if err != nil {
		return nil, err
	}
	switch command.Kind {
	case clusterqueue.AckMessage, clusterqueue.SettleBatch, clusterqueue.AckAndClaim:
		if f.firstAck.CompareAndSwap(false, true) {
			result, err := clusterqueue.DecodeResponse(wire)
			if err != nil {
				return nil, err
			}
			select {
			case f.committed <- ackClaimFrameObservation{command: command, result: result}:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			select {
			case <-f.reply:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	return wire, nil
}

func nextAckClaimDelivery(t *testing.T, output chan *protocol.Frame, channel uint16, body string) *protocol.BasicDeliverMethod {
	t.Helper()
	frame := nextFrame(t, output)
	class, method, ok := methodIDs(frame)
	require.True(t, ok)
	require.Equal(t, channel, frame.Channel)
	require.EqualValues(t, 60, class)
	require.EqualValues(t, 60, method, "basic.deliver")
	delivery := &protocol.BasicDeliverMethod{}
	require.NoError(t, delivery.Deserialize(frame.Payload[4:]))
	require.Equal(t, "reader", delivery.ConsumerTag)
	require.False(t, delivery.Redelivered)
	header := nextFrame(t, output)
	require.EqualValues(t, protocol.FrameHeader, header.Type)
	require.Equal(t, channel, header.Channel)
	payload := nextFrame(t, output)
	require.EqualValues(t, protocol.FrameBody, payload.Type)
	require.Equal(t, channel, payload.Channel)
	require.Equal(t, body, string(payload.Payload))
	return delivery
}

func TestClusterManualAckFrameRefillsInOneCommandWhenRuntimeSupportsIt(t *testing.T) {
	for _, tc := range []struct {
		name       string
		advertised bool
		supported  bool
		parked     bool
	}{
		{name: "enabled", advertised: true, supported: true},
		{name: "disabled", advertised: true},
		{name: "older_runtime_without_capability"},
		{name: "installed_refill_interrupts_readiness", advertised: true, supported: true, parked: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const queue = "jobs"
			fixture := &ackClaimFrameRuntime{supported: tc.supported,
				committed: make(chan ackClaimFrameObservation, 1), reply: make(chan struct{})}
			var replyOnce sync.Once
			allowReply := func() { replyOnce.Do(func() { close(fixture.reply) }) }
			t.Cleanup(allowReply)
			var submitter ClusterSubmitter = fixture
			var readiness *ackClaimReadyRuntime
			if tc.parked {
				readiness = &ackClaimReadyRuntime{ackClaimFrameRuntime: fixture,
					entered: make(chan struct{}, 1), canceled: make(chan error, 1)}
				submitter = readiness
			}
			if !tc.advertised {
				// A Submit-only wrapper must keep the old path even when the
				// underlying test machine understands the new opcode.
				submitter = clusterSubmitFunc(fixture.Submit)
			}
			b := NewClusterBroker(submitter)
			_, err := b.DeclareQueue(queue, true, false, false, nil)
			require.NoError(t, err)
			bodies := []string{"first", "second"}
			if tc.parked {
				bodies = bodies[:1]
			}
			for _, body := range bodies {
				require.NoError(t, b.PublishMessage("", queue, &protocol.Message{Body: []byte(body)}))
			}

			srv := &Server{Broker: b, Log: zap.NewNop(), Config: config.DefaultConfig()}
			conn, output := newPipeConnWithFrames(t)
			ch := protocol.NewChannel(1, conn)
			ch.PrefetchCount.Store(1)
			if tc.parked {
				ch.PrefetchCount.Store(2)
			}
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
			require.EqualValues(t, 21, method, "consume-ok precedes the first delivery")
			first := nextAckClaimDelivery(t, output, ch.ID, "first")
			ref, ok := ch.ResolveWireTag(first.DeliveryTag)
			require.True(t, ok)
			require.False(t, ref.IsGet)
			b.mu.Lock()
			entry, retained := b.deliveries[ref.MsgID]
			b.mu.Unlock()
			require.True(t, retained)
			require.EqualValues(t, 1, entry.Session.holds.Load())
			if tc.parked {
				select {
				case <-readiness.entered:
				case <-time.After(2 * time.Second):
					t.Fatal("consumer did not park in the actual readiness entry point")
				}
				// The consumer already exhausted the ready queue. Reduce its
				// credit while parked, then publish without releasing the wait:
				// only installed refill may interrupt this request.
				qos, err := (&protocol.BasicQosMethod{PrefetchCount: 1}).Serialize()
				require.NoError(t, err)
				push(protocol.EncodeMethodFrameForChannel(ch.ID, 60, protocol.BasicQos, qos))
				class, method, _ = nextMethodFrame(t, output)
				require.EqualValues(t, 60, class)
				require.EqualValues(t, 11, method, "basic.qos-ok")
				require.NoError(t, b.PublishMessage("", queue, &protocol.Message{Body: []byte("second")}))
			}

			push(encodeAckFrame(t, ch.ID, first.DeliveryTag, false))
			var committed ackClaimFrameObservation
			select {
			case committed = <-fixture.committed:
			case <-time.After(2 * time.Second):
				t.Fatal("ACK frame did not reach a committed queue settlement")
			}
			require.NoError(t, committed.result.Err())
			expectNoFrame(t, output, "the committed settlement reply is still held")
			allowReply()
			if tc.parked {
				select {
				case err := <-readiness.canceled:
					require.ErrorIs(t, err, context.Canceled)
				case <-time.After(2 * time.Second):
					t.Fatal("installed refill did not cancel the parked readiness request")
				}
				require.NoError(t, entry.Session.ctx.Err(), "interrupt only the readiness child, not the consumer session")
			}
			second := nextAckClaimDelivery(t, output, ch.ID, "second")
			require.Greater(t, second.DeliveryTag, first.DeliveryTag)

			// A frame barrier after the second payload establishes completed
			// ACK bookkeeping before inspecting wire tags and command history.
			barrier, err := (&protocol.QueueDeclareMethod{Queue: queue, Passive: true}).Serialize()
			require.NoError(t, err)
			push(protocol.EncodeMethodFrameForChannel(ch.ID, 50, protocol.QueueDeclare, barrier))
			close(conn.FrameQueue)
			class, method, _ = nextMethodFrame(t, output)
			require.EqualValues(t, 50, class)
			require.EqualValues(t, 11, method, "queue.declare-ok follows the second delivery body")
			select {
			case <-processed:
			case <-time.After(2 * time.Second):
				t.Fatal("frame barrier did not complete")
			}
			_, tracked := ch.ResolveWireTag(first.DeliveryTag)
			require.False(t, tracked, "the settled wire tag is retired")
			_, tracked = ch.ResolveWireTag(second.DeliveryTag)
			require.True(t, tracked, "the refilled delivery requires its own ACK")
			require.EqualValues(t, 1, entry.Session.holds.Load())
			info, err := b.describe(context.Background(), queue)
			require.NoError(t, err)
			require.Zero(t, info.Ready)
			require.EqualValues(t, 1, info.Unacknowledged)

			fixture.mu.Lock()
			commands := append([]clusterqueue.QueueCommand(nil), fixture.commands...)
			fixture.mu.Unlock()
			kinds := make([]clusterqueue.QueueCommandKind, len(commands))
			for i, command := range commands {
				kinds[i] = command.Kind
			}
			want := []clusterqueue.QueueCommandKind{clusterqueue.ClaimBatch, clusterqueue.AckMessage, clusterqueue.ClaimBatch}
			if tc.supported {
				want = []clusterqueue.QueueCommandKind{clusterqueue.ClaimBatch, clusterqueue.AckAndClaim}
				if tc.parked {
					want = []clusterqueue.QueueCommandKind{clusterqueue.ClaimBatch, clusterqueue.ClaimBatch, clusterqueue.AckAndClaim}
				}
			}
			require.Equal(t, want, kinds, "an opted-in runtime must settle and refill in one replicated command; older runtimes retain the split path")
			if tc.supported {
				require.Equal(t, []clusterqueue.AttemptID{entry.Attempt}, committed.command.Attempts)
				require.EqualValues(t, 1, committed.command.ClaimLimit)
				require.EqualValues(t, clusterqueue.MaxClaimBatchBytes, committed.command.ClaimBytes)
				require.Equal(t, commands[len(commands)-2].Sequence+1, committed.command.Sequence)
				require.EqualValues(t, 1, committed.result.Settled)
				require.EqualValues(t, 1, committed.result.Count)
				require.NoError(t, committed.result.ClaimErr())
			}
		})
	}
}
