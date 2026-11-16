package server

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

type localClusterMachine struct {
	mu      sync.Mutex
	machine ClusterMachine
}

func (c *localClusterMachine) Submit(_ context.Context, _ string, p []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.machine.Apply(p), nil
}

func TestClusterBrokersShareQueueAndFenceSettlement(t *testing.T) {
	state := &localClusterMachine{}
	publisher := NewClusterBroker(state)
	first := NewClusterBroker(state)
	second := NewClusterBroker(state)
	ctx := context.Background()
	_, err := publisher.DeclareQueue("jobs", true, false, false, nil)
	require.NoError(t, err)
	for _, body := range []string{"first", "second"} {
		err = publisher.PublishMessage("", "jobs", &protocol.Message{Body: []byte(body), MessageID: "id-" + body})
		require.NoError(t, err)
	}
	message, tag, remaining, err := first.GetMessageForGetOwned("jobs", false, "first")
	require.NoError(t, err)
	require.Equal(t, "first", string(message.Body))
	require.Equal(t, "id-first", message.MessageID)
	require.Equal(t, uint32(1), remaining)
	require.NoError(t, first.NackGetDelivery(tag, true))
	message, tag2, remaining, err := second.GetMessageForGetOwned("jobs", false, "second")
	require.NoError(t, err)
	require.Equal(t, "second", string(message.Body))
	require.Equal(t, uint32(1), remaining)
	require.NoError(t, second.AcknowledgeGetDelivery(tag2))
	message, tag3, remaining, err := first.GetMessageForGetOwned("jobs", false, "first")
	require.NoError(t, err)
	require.Equal(t, "first", string(message.Body))
	require.True(t, message.Redelivered)
	require.Equal(t, uint32(0), remaining)
	require.NotEqual(t, tag, tag3)
	require.Error(t, first.AcknowledgeGetDelivery(tag))
	info, err := first.describe(ctx, "jobs")
	require.NoError(t, err)
	require.Equal(t, uint64(1), info.Unacknowledged)
	require.NoError(t, first.AcknowledgeGetDelivery(tag3))
	first.RequeueGetDeliveries("first")
	second.RequeueGetDeliveries("second")
}

func TestClusterNamedExchangeFencesDeletedQueueAfterRestore(t *testing.T) {
	state := &localClusterMachine{}
	b := NewClusterBroker(state)
	_, err := b.DeclareQueue("jobs", true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, b.DeclareExchange("events", "topic", true, false, false, nil))
	require.NoError(t, b.BindQueue("jobs", "events", "jobs.*", nil))

	snapshot, err := state.machine.Snapshot()
	require.NoError(t, err)
	restored := &localClusterMachine{}
	require.NoError(t, restored.machine.Restore(snapshot))
	remote := NewClusterBroker(restored)
	require.NoError(t, remote.ExchangeDeclarePassive("events"))

	message := &protocol.Message{Body: []byte("first"), Exchange: "events", RoutingKey: "jobs.green", Mandatory: true}
	require.NoError(t, remote.PublishMessage(message.Exchange, message.RoutingKey, message))
	delivered, _, _, err := remote.GetMessageForGetOwned("jobs", true, "reader")
	require.NoError(t, err)
	require.Equal(t, message.Exchange, delivered.Exchange)
	require.Equal(t, message.RoutingKey, delivered.RoutingKey)
	require.Equal(t, message.Body, delivered.Body)
	remote.RequeueGetDeliveries("reader")

	_, err = remote.DeleteQueue("jobs", false, false)
	require.NoError(t, err)
	_, err = remote.DeclareQueue("jobs", true, false, false, nil)
	require.NoError(t, err)
	require.ErrorIs(t, remote.PublishMessage(message.Exchange, message.RoutingKey, message), broker.ErrNoRoute)
	require.NoError(t, remote.DeleteExchange("events", true))
}

func TestClusterCreditAndCancellationReleaseOutstandingDelivery(t *testing.T) {
	state := &localClusterMachine{}
	source, other := NewClusterBroker(state), NewClusterBroker(state)
	_, err := source.DeclareQueue("jobs", true, false, false, nil)
	require.NoError(t, err)
	for _, body := range []string{"first", "second"} {
		require.NoError(t, source.PublishMessage("", "jobs", &protocol.Message{Body: []byte(body)}))
	}
	consumer := &protocol.Consumer{ID: "c", PrefetchCount: 1,
		Messages: make(chan *protocol.Delivery, 2), Cancel: make(chan struct{})}
	require.NoError(t, source.RegisterConsumer("jobs", consumer.ID, consumer))
	t.Cleanup(func() { _ = source.UnregisterConsumer(consumer.ID) })
	select {
	case message := <-consumer.Messages:
		require.Equal(t, "first", string(message.Message.Body))
	case <-time.After(time.Second):
		t.Fatal("no delivery")
	}
	info, err := other.describe(context.Background(), "jobs")
	require.NoError(t, err)
	require.Equal(t, uint64(1), info.Ready)
	require.Equal(t, uint64(1), info.Unacknowledged)
	require.NoError(t, source.UnregisterConsumer(consumer.ID))
	for _, want := range []string{"second", "first"} {
		msg, _, _, err := other.GetMessageForGetOwned("jobs", true, "r")
		require.NoError(t, err)
		require.Equal(t, want, string(msg.Body))
		if want == "first" {
			require.True(t, msg.Redelivered)
		}
	}
	other.RequeueGetDeliveries("r")
}

func TestClusterGetChannelCloseRequeuesOnlyItsOwnDelivery(t *testing.T) {
	state := &localClusterMachine{}
	ingress, other := NewClusterBroker(state), NewClusterBroker(state)
	_, err := ingress.DeclareQueue("jobs", true, false, false, nil)
	require.NoError(t, err)
	for _, value := range []string{"one", "two"} {
		require.NoError(t, ingress.PublishMessage("", "jobs", &protocol.Message{Body: []byte(value)}))
	}
	_, first, _, err := ingress.getMessageForChannel("jobs", false, "conn", 1)
	require.NoError(t, err)
	_, second, _, err := ingress.getMessageForChannel("jobs", false, "conn", 2)
	require.NoError(t, err)
	t.Cleanup(func() {
		ingress.RequeueGetDeliveries("conn")
		other.RequeueGetDeliveries("other")
	})
	ingress.requeueGetChannelDeliveries("conn", 1)
	require.Error(t, ingress.AcknowledgeGetDelivery(first))
	message, _, _, err := other.GetMessageForGetOwned("jobs", true, "other")
	require.NoError(t, err)
	require.NotNil(t, message)
	require.Equal(t, "one", string(message.Body))
	require.True(t, message.Redelivered)
	require.NoError(t, ingress.AcknowledgeGetDelivery(second))
	info, err := other.describe(context.Background(), "jobs")
	require.NoError(t, err)
	require.Zero(t, info.Ready)
	require.Zero(t, info.Unacknowledged)
}

func TestClusterConnectionRefusesUnsupportedVHost(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Network.Address = "127.0.0.1:0"
	cfg.Storage.Path = t.TempDir()
	server, err := NewServerBuilder().WithConfig(cfg).WithUnifiedBroker(NewClusterBroker(&localClusterMachine{})).Build()
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- server.Start() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		<-done
	})
	require.Eventually(t, server.IsListening, time.Second, 10*time.Millisecond)
	server.Mutex.RLock()
	endpoint := "amqp://guest:guest@" + server.Listener.Addr().String()
	server.Mutex.RUnlock()
	connection, err := amqp.Dial(endpoint + "/tenant")
	if connection != nil {
		_ = connection.Close()
	}
	require.Error(t, err)
	connection, err = amqp.Dial(endpoint + "/")
	require.NoError(t, err)
	require.NoError(t, connection.Close())
}

func TestClusterExchangeDeleteIfUnusedIgnoresObsoleteQueueIncarnations(t *testing.T) {
	b := NewClusterBroker(&localClusterMachine{})
	_, err := b.DeclareQueue("jobs", true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, b.DeclareExchange("source", "direct", true, false, false, nil))
	require.NoError(t, b.BindQueue("jobs", "source", "events", nil))
	require.ErrorIs(t, b.DeleteExchange("source", true), broker.ErrExchangeInUse)
	_, err = b.DeleteQueue("jobs", false, false)
	require.NoError(t, err)
	_, err = b.DeclareQueue("jobs", true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, b.DeleteExchange("source", true))
}

type lostClusterReplies struct {
	state  localClusterMachine
	first  map[clusterqueue.QueueCommandKind][]byte
	failed map[clusterqueue.QueueCommandKind]int
}

func (p *lostClusterReplies) Submit(ctx context.Context, queue string, command []byte) ([]byte, error) {
	reply, err := p.state.Submit(ctx, queue, command)
	if err != nil {
		return nil, err
	}
	envelope, err := clusterqueue.DecodeCommand(command)
	if err != nil {
		return nil, err
	}
	if envelope.Kind != clusterqueue.OpenQueueSession && envelope.Kind != clusterqueue.ClaimMessage {
		return reply, nil
	}
	if p.first[envelope.Kind] == nil {
		p.first[envelope.Kind] = append([]byte(nil), command...)
	}
	if string(p.first[envelope.Kind]) != string(command) && p.failed[envelope.Kind] < 3 {
		return nil, errors.New("retry changed command")
	}
	if p.failed[envelope.Kind] < 3 {
		p.failed[envelope.Kind]++
		return nil, errors.New("committed command response lost")
	}
	return reply, nil
}

func TestClusterSessionRetriesCommittedOpenAndClaim(t *testing.T) {
	cluster := &lostClusterReplies{first: make(map[clusterqueue.QueueCommandKind][]byte), failed: make(map[clusterqueue.QueueCommandKind]int)}
	b := NewClusterBroker(cluster)
	_, err := b.DeclareQueue("retry", true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, b.PublishMessage("", "retry", &protocol.Message{Body: []byte("hello")}))
	t.Cleanup(func() { b.RequeueGetDeliveries("connection") })
	message, tag, remaining, err := b.GetMessageForGetOwned("retry", false, "connection")
	require.NoError(t, err)
	require.NotNil(t, message)
	require.Equal(t, "hello", string(message.Body))
	require.Zero(t, remaining)
	require.NoError(t, b.AcknowledgeGetDelivery(tag))
	info, err := b.describe(context.Background(), "retry")
	require.NoError(t, err)
	require.Equal(t, clusterqueue.SessionID(2), info.NextSession)
	require.Zero(t, info.Unacknowledged)
}

func TestClusterBuilderRejectsStandaloneFallback(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Storage.Path = t.TempDir()
	cfg.Cluster = config.ClusterConfig{
		ID: "embedded-guard", NodeID: 1, PeerAddress: "127.0.0.1:7662",
		Seeds: []string{"1@127.0.0.1:7662"}, TokenFile: "peer-token", AllowLoopbackPlaintext: true,
	}
	srv, err := NewServerBuilder().WithConfig(cfg).Build()
	if srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}
	require.ErrorContains(t, err, "requires a distributed broker")
}
