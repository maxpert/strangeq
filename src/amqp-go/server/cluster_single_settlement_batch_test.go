package server

import (
	"context"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	amqperrors "github.com/maxpert/amqp-go/errors"
	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestClusterSingleSettlementFramesBatchExactTagsBeforeBarrier(t *testing.T) {
	for _, method := range []struct {
		name  string
		id    uint16
		kind  clusterqueue.QueueCommandKind
		frame func(*testing.T, uint16, uint64) *protocol.Frame
	}{
		{"ack", protocol.BasicAck, clusterqueue.AckMessage, func(t *testing.T, ch uint16, tag uint64) *protocol.Frame {
			return encodeAckFrame(t, ch, tag, false)
		}},
		{"reject", protocol.BasicReject, clusterqueue.DropMessage, func(t *testing.T, ch uint16, tag uint64) *protocol.Frame {
			return encodeRejectFrame(t, ch, tag, false)
		}},
		{"nack", protocol.BasicNack, clusterqueue.DropMessage, func(t *testing.T, ch uint16, tag uint64) *protocol.Frame {
			return encodeNackFrame(t, ch, tag, false, false)
		}},
	} {
		t.Run(method.name, func(t *testing.T) {
			checkClusterSingleSettlementFrames(t, method.id, method.kind, method.frame)
		})
	}
}

func checkClusterSingleSettlementFrames(t *testing.T, methodID uint16, settlement clusterqueue.QueueCommandKind,
	frame func(*testing.T, uint16, uint64) *protocol.Frame) {
	t.Helper()
	for _, tc := range []struct {
		name      string
		partial   bool
		duplicate bool
	}{
		{name: "skip_unacknowledged_tag"},
		{name: "stop_after_failed_attempt", partial: true},
		{name: "duplicate_tag_keeps_channel_error", duplicate: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const queue = "jobs"
			state := &localClusterMachine{}
			var submissions []clusterqueue.QueueCommandKind
			var barrierUnacked []uint64
			observeBarrier := false
			b := NewClusterBroker(clusterSubmitFunc(func(ctx context.Context, key string, request []byte) ([]byte, error) {
				command, err := clusterqueue.DecodeCommand(request)
				if err != nil {
					return nil, err
				}
				wire, err := state.Submit(ctx, key, request)
				if err != nil {
					return nil, err
				}
				switch command.Kind {
				case clusterqueue.AckMessage, clusterqueue.DropMessage, clusterqueue.SettleBatch:
					if command.Kind == clusterqueue.SettleBatch {
						require.Equal(t, settlement, command.Settlement)
					}
					submissions = append(submissions, command.Kind)
				case clusterqueue.DescribeQueue:
					if observeBarrier {
						result, err := clusterqueue.DecodeResponse(wire)
						if err != nil {
							return nil, err
						}
						barrierUnacked = append(barrierUnacked, result.Queue.Unacknowledged)
					}
				}
				return wire, nil
			}))
			_, err := b.DeclareQueue(queue, true, false, false, nil)
			require.NoError(t, err)
			srv := &Server{Broker: b, Log: zap.NewNop(), Config: config.DefaultConfig()}
			conn, output := newPipeConnWithFrames(t)
			ch := protocol.NewChannel(1, conn)
			conn.Channels.Store(ch.ID, ch)
			t.Cleanup(func() { b.RequeueGetDeliveries(conn.ID) })

			for i, body := range []string{"first", "second", "third", "fourth"} {
				require.NoError(t, b.PublishMessage("", queue, &protocol.Message{Body: []byte(body)}))
				delivered, brokerTag, _, err := b.getMessageForChannel(queue, false, conn.ID, ch.ID)
				require.NoError(t, err)
				require.NotNil(t, delivered)
				require.Equal(t, body, string(delivered.Body))
				wireTag := uint64(i + 1)
				ch.TrackDelivery(wireTag, brokerTag, "", true)
				if tc.partial && wireTag == 3 {
					b.mu.Lock()
					entry := b.deliveries[brokerTag]
					entry.Attempt = ^clusterqueue.AttemptID(0)
					b.deliveries[brokerTag] = entry
					b.mu.Unlock()
				}
			}
			info, err := b.describe(context.Background(), queue)
			require.NoError(t, err)
			require.EqualValues(t, 4, info.Unacknowledged)

			push := func(frame *protocol.Frame) {
				conn.FrameQueueBytes.Add(frameCost(frame))
				conn.FrameQueue <- frame
			}
			tags := []uint64{1, 3, 4}
			if tc.duplicate {
				tags = []uint64{1, 3, 3, 4}
			}
			for _, tag := range tags {
				push(frame(t, ch.ID, tag))
			}
			barrier, err := (&protocol.QueueDeclareMethod{Queue: queue, Passive: true}).Serialize()
			require.NoError(t, err)
			push(protocol.EncodeMethodFrameForChannel(ch.ID, 50, protocol.QueueDeclare, barrier))
			if tc.partial {
				push(protocol.EncodeMethodFrameForChannel(0, 10, protocol.ConnectionCloseOK, nil))
			} else if tc.duplicate {
				push(protocol.EncodeMethodFrameForChannel(ch.ID, 20, protocol.ChannelCloseOK, nil))
			}
			close(conn.FrameQueue)

			rt := srv.runtimeFor(conn)
			observeBarrier = true
			done := make(chan struct{})
			go srv.processFrames(rt, done)
			t.Cleanup(func() {
				close(conn.Done)
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("cluster frame processor did not exit during cleanup")
				}
				if timer := rt.closeTimer.Load(); timer != nil {
					timer.Stop()
				}
			})
			class, method, payload := nextMethodFrame(t, output)
			switch {
			case tc.partial:
				require.EqualValues(t, 10, class)
				require.EqualValues(t, protocol.ConnectionClose, method)
				closeMethod := &protocol.ConnectionCloseMethod{}
				require.NoError(t, closeMethod.Deserialize(payload))
				require.EqualValues(t, amqperrors.InternalError, closeMethod.ReplyCode)
				require.EqualValues(t, 60, closeMethod.ClassID)
				require.EqualValues(t, methodID, closeMethod.MethodID)
			case tc.duplicate:
				require.EqualValues(t, 20, class)
				require.EqualValues(t, protocol.ChannelClose, method)
				// The method IDs distinguish the two close frames. Their
				// arguments share the same wire layout.
				closeMethod := &protocol.ConnectionCloseMethod{}
				require.NoError(t, closeMethod.Deserialize(payload))
				require.EqualValues(t, amqperrors.PreconditionFailed, closeMethod.ReplyCode)
				require.EqualValues(t, 60, closeMethod.ClassID)
				require.EqualValues(t, methodID, closeMethod.MethodID)
				require.Contains(t, closeMethod.ReplyText, "unknown delivery tag 3")
			default:
				require.EqualValues(t, 50, class)
				require.EqualValues(t, 11, method, "queue.declare-ok")
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("cluster frame processor did not exit")
			}
			observeBarrier = false
			if tc.partial || tc.duplicate {
				require.Empty(t, barrierUnacked, "the close discards the later control frame")
			} else {
				require.Equal(t, []uint64{1}, barrierUnacked, "the barrier observes all preceding settlements")
			}

			info, err = b.describe(context.Background(), queue)
			require.NoError(t, err)
			if tc.duplicate {
				require.EqualValues(t, 2, info.Ready, "the skipped and later deliveries were requeued")
				require.Zero(t, info.Unacknowledged)
			} else {
				wantUnacked := uint64(1)
				if tc.partial {
					wantUnacked = 3
				}
				require.Equal(t, wantUnacked, info.Unacknowledged)
				for _, tag := range []uint64{1, 2, 3, 4} {
					_, tracked := ch.ResolveWireTag(tag)
					wantTracked := tag == 2 || (tc.partial && tag == 4)
					require.Equal(t, wantTracked, tracked, "wire tag %d", tag)
				}
			}
			require.Len(t, submissions, 1, "consecutive individual settlement frames use one replicated settlement")
			require.Equal(t, clusterqueue.SettleBatch, submissions[0])
		})
	}
}
