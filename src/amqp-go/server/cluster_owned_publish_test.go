package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/internal/cluster"
	"github.com/maxpert/amqp-go/internal/clusterproposal"
	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type ownedPublishPeer struct {
	state localClusterMachine
	kinds []clusterqueue.QueueCommandKind
	fail  bool
}

func (p *ownedPublishPeer) Submit(ctx context.Context, queue string, data []byte) ([]byte, error) {
	return p.state.Submit(ctx, queue, data)
}
func (p *ownedPublishPeer) SupportsOwnedPublishes() bool { return true }
func (p *ownedPublishPeer) SubmitOwned(ctx context.Context, queue string, command *clusterproposal.Owned) ([]byte, error) {
	owned, err := command.Begin()
	if err != nil {
		return nil, err
	}
	defer owned.Discard()
	data := owned.Command()
	decoded, err := clusterqueue.DecodeCommand(data)
	if err != nil {
		return nil, err
	}
	p.kinds = append(p.kinds, decoded.Kind)
	reply, err := p.state.Submit(ctx, queue, data)
	if err == nil && p.fail {
		return nil, fmt.Errorf("reply lost after commit: %w", cluster.ErrUncertain)
	}
	return reply, err
}

func TestOwnedPublishNormalBurstAndSinglePreserveConfirmsAndAmbiguity(t *testing.T) {
	const queue = "owned-publishes"
	peer := &ownedPublishPeer{}
	b := NewClusterBroker(peer)
	_, err := b.DeclareQueue(queue, true, false, false, nil)
	require.NoError(t, err)
	srv := &Server{Broker: b, Log: zap.NewNop(), Config: config.DefaultConfig()}
	conn, output := newPipeConnWithFrames(t)
	ch := protocol.NewChannel(1, conn)
	conn.Channels.Store(ch.ID, ch)
	ch.ConfirmMode.Store(true)
	burst := &clusterPublishBurst{server: srv, broker: b, conn: conn}
	// The singleton flush takes publishMessage's singular encoder/submit path.
	require.NoError(t, srv.processCompleteMessageWithBurst(conn, ch.ID, makeDurablePendingMessage("", queue, "single"), burst))
	require.NoError(t, burst.flush())
	_, method, _ := nextMethodFrame(t, output)
	require.EqualValues(t, protocol.BasicAck, method)
	require.Equal(t, []clusterqueue.QueueCommandKind{clusterqueue.PublishMessage}, peer.kinds)
	peer.fail = true
	for _, body := range []string{"one", "two", "three"} {
		require.NoError(t, srv.processCompleteMessageWithBurst(conn, ch.ID, makeDurablePendingMessage("", queue, body), burst))
	}
	require.Len(t, burst.items, 3)
	require.NoError(t, burst.flush())
	for tag := uint64(2); tag <= 4; tag++ {
		_, method, payload := nextMethodFrame(t, output)
		require.EqualValues(t, protocol.BasicNack, method)
		nack := &protocol.BasicNackMethod{}
		require.NoError(t, nack.Deserialize(payload))
		require.Equal(t, tag, nack.DeliveryTag)
	}
	require.Equal(t, []clusterqueue.QueueCommandKind{clusterqueue.PublishMessage, clusterqueue.PublishBatch}, peer.kinds,
		"normal burst must call owned submission once, including a committed ambiguous result")
	require.EqualValues(t, 4, ch.ConfirmContig())
	require.False(t, conn.Closed.Load())
	for _, body := range []string{"single", "one", "two", "three"} {
		message, _, _, err := b.GetMessageForGetOwned(queue, true, "audit")
		require.NoError(t, err)
		require.NotNil(t, message)
		require.Equal(t, body, string(message.Body))
	}
	message, _, _, err := b.GetMessageForGetOwned(queue, true, "audit")
	require.NoError(t, err)
	require.Nil(t, message)
}
