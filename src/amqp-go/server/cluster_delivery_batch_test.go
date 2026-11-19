package server

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestClusterCumulativeSettlementBatchesOneSession(t *testing.T) {
	for _, tc := range []struct {
		name          string
		nack          bool
		requeue       bool
		loseReply     bool
		deliveryCount int
		batchWidths   []int
	}{
		{name: "ack"},
		{name: "nack_drop", nack: true},
		{name: "nack_requeue", nack: true, requeue: true},
		{name: "nack_requeue_lost_reply", nack: true, requeue: true, loseReply: true},
		{name: "at_limit", nack: true, requeue: true, deliveryCount: 64, batchWidths: []int{64}},
		{name: "above_limit", nack: true, requeue: true, deliveryCount: 65, batchWidths: []int{64, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := &localClusterMachine{}
			var count atomic.Bool
			var submissions atomic.Int32
			var lostRequest, lostResponse []byte
			var widths []int
			b := NewClusterBroker(clusterSubmitFunc(func(ctx context.Context, queue string, request []byte) ([]byte, error) {
				command, err := clusterqueue.DecodeCommand(request)
				if err != nil {
					return nil, err
				}
				counted := count.Load() && command.Sequence != 0
				if counted {
					submissions.Add(1)
					widths = append(widths, max(1, len(command.Attempts)))
				}
				response, err := state.Submit(ctx, queue, request)
				if err != nil || !counted || !tc.loseReply {
					return response, err
				}
				if lostRequest == nil {
					lostRequest = append([]byte(nil), request...)
					lostResponse = append([]byte(nil), response...)
					return nil, errors.New("test discarded a committed settlement response")
				}
				require.Equal(t, lostRequest, request)
				require.Equal(t, lostResponse, response)
				return response, nil
			}))
			_, err := b.DeclareQueue("jobs", true, false, false, nil)
			require.NoError(t, err)
			client, transport := net.Pipe()
			t.Cleanup(func() { _ = client.Close(); _ = transport.Close() })
			conn := protocol.NewConnection(transport)
			ch := protocol.NewChannel(1, conn)
			conn.Channels.Store(ch.ID, ch)
			t.Cleanup(func() { b.RequeueGetDeliveries(conn.ID) })
			bodies := []string{"first", "second", "third"}
			if tc.deliveryCount > 0 {
				bodies = make([]string, tc.deliveryCount)
				for i := range bodies {
					bodies[i] = strconv.Itoa(i + 1)
				}
			}
			for i, body := range bodies {
				require.NoError(t, b.PublishMessage("", "jobs", &protocol.Message{Body: []byte(body)}))
				message, tag, _, err := b.getMessageForChannel("jobs", false, conn.ID, ch.ID)
				require.NoError(t, err)
				require.Equal(t, body, string(message.Body))
				ch.TrackDelivery(uint64(i+1), tag, "", true)
			}
			info, err := b.describe(context.Background(), "jobs")
			require.NoError(t, err)
			require.Equal(t, uint64(len(bodies)), info.Unacknowledged)

			srv := &Server{Broker: b, Log: zap.NewNop()}
			count.Store(true)
			if tc.nack {
				err = srv.handleBasicNack(conn, ch.ID, makeNackPayload(uint64(len(bodies)), true, tc.requeue))
			} else {
				err = srv.handleBasicAck(conn, ch.ID, makeAckPayload(uint64(len(bodies)), true))
			}
			count.Store(false)
			require.NoError(t, err)
			for i := range bodies {
				_, tracked := ch.ResolveWireTag(uint64(i + 1))
				require.False(t, tracked)
			}
			info, err = b.describe(context.Background(), "jobs")
			require.NoError(t, err)
			require.Zero(t, info.Unacknowledged)
			if tc.requeue {
				require.Equal(t, uint64(len(bodies)), info.Ready)
				for _, body := range bodies {
					message, _, _, err := b.getMessageForChannel("jobs", true, conn.ID, ch.ID)
					require.NoError(t, err)
					require.Equal(t, body, string(message.Body))
					require.True(t, message.Redelivered)
				}
				info, err = b.describe(context.Background(), "jobs")
				require.NoError(t, err)
				require.Zero(t, info.Ready, "each message was requeued once")
			} else {
				require.Zero(t, info.Ready)
			}
			wantSubmissions := int32(1)
			if tc.loseReply {
				require.NotEmpty(t, lostResponse, "the first submitted settlement committed")
				wantSubmissions++
			}
			if tc.batchWidths != nil {
				require.Equal(t, tc.batchWidths, widths, "replicated settlements respect the attempt limit")
				wantSubmissions = int32(len(tc.batchWidths))
			}
			require.Equal(t, wantSubmissions, submissions.Load(), "one replicated settlement per run of consecutive tags")
		})
	}
}

func TestClusterCumulativeSettlementPreservesPrefixAcrossSessions(t *testing.T) {
	b := NewClusterBroker(&localClusterMachine{})
	for _, queue := range []string{"alpha", "beta"} {
		_, err := b.DeclareQueue(queue, true, false, false, nil)
		require.NoError(t, err)
	}
	client, transport := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = transport.Close() })
	conn := protocol.NewConnection(transport)
	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(ch.ID, ch)
	t.Cleanup(func() { b.RequeueGetDeliveries(conn.ID) })

	queues := []string{"alpha", "alpha", "beta", "alpha", "beta"}
	for i, queue := range queues {
		require.NoError(t, b.PublishMessage("", queue, &protocol.Message{Body: []byte(queue)}))
		message, tag, _, err := b.getMessageForChannel(queue, false, conn.ID, ch.ID)
		require.NoError(t, err)
		require.Equal(t, queue, string(message.Body))
		ch.TrackDelivery(uint64(i+1), tag, "", true)
		if i == 2 {
			b.mu.Lock()
			delivery := b.deliveries[tag]
			delivery.Attempt = 100
			b.deliveries[tag] = delivery
			b.mu.Unlock()
		}
	}
	srv := &Server{Broker: b, Log: zap.NewNop()}
	err := srv.handleBasicAck(conn, ch.ID, makeAckPayload(uint64(len(queues)), true))
	require.ErrorIs(t, err, clusterqueue.ErrUnknownAttempt)
	require.ErrorContains(t, err, "at delivery tag 3")
	for i := range queues {
		_, tracked := ch.ResolveWireTag(uint64(i + 1))
		require.Equal(t, i >= 2, tracked)
	}
	alpha, err := b.describe(context.Background(), "alpha")
	require.NoError(t, err)
	require.EqualValues(t, 1, alpha.Unacknowledged, "alpha's tag after the failed beta tag must survive")
	beta, err := b.describe(context.Background(), "beta")
	require.NoError(t, err)
	require.EqualValues(t, 2, beta.Unacknowledged)
}
