package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	"github.com/maxpert/amqp-go/internal/cluster"
	"github.com/maxpert/amqp-go/server"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func TestClusterAMQPScaleOneToSevenAndRestart(t *testing.T) {
	const brokers = 7
	nodes := newTestCluster(t, brokers, 1, 3)
	_, err := openCluster(nodes[0].config, true)
	require.NoError(t, err)
	nodes[0].start(t, false)
	const queue = "integration-cross-node"
	publisher := waitQueue(t, &nodes[0], queue, false)
	write := openClusterChannel(t, publisher)
	require.NoError(t, write.Confirm(false))
	publishClusterConfirm(t, write, queue, "before-join")
	single := getClusterMessage(t, write, queue, "before-join", false)
	require.NoError(t, single.Ack(false))
	_, err = write.QueueInspect(queue)
	require.NoError(t, err)
	for _, body := range []string{"one", "two", "restart"} {
		publishClusterConfirm(t, write, queue, body)
	}

	for i := 1; i < brokers; i++ {
		nodes[i].start(t, false)
		waitClusterBroker(t, &nodes[i])
		conn := waitQueue(t, &nodes[i], queue, true)
		ch := openClusterChannel(t, conn)
		require.NoError(t, ch.Confirm(false))
		publishClusterConfirm(t, ch, queue, fmt.Sprintf("node-%d", i+1))
	}
	read := openClusterChannel(t, nodes[brokers-1].dial(t))
	delivery := getClusterMessage(t, read, queue, "one", false)
	require.Equal(t, "text/plain", delivery.ContentType)
	require.Equal(t, "message-one", delivery.MessageId)
	require.NoError(t, delivery.Nack(false, true))

	other := openClusterChannel(t, publisher)
	delivery = getClusterMessage(t, other, queue, "two", false)
	require.NoError(t, delivery.Ack(false))
	// The synchronous Get following Ack on the same channel fences settlement.
	delivery = getClusterMessage(t, other, queue, "restart", false)
	require.NoError(t, delivery.Nack(false, true))
	_, err = other.QueueInspect(queue)
	require.NoError(t, err)

	for i := range nodes {
		nodes[i].close(t)
	}
	for i := range nodes {
		nodes[i].start(t, false)
	}
	for i := 1; i < brokers; i++ {
		waitClusterBroker(t, &nodes[i])
		recovered := waitQueue(t, &nodes[i], queue, true)
		read = openClusterChannel(t, recovered)
		msg := getClusterMessage(t, read, queue, fmt.Sprintf("node-%d", i+1), false)
		require.NoError(t, msg.Ack(false))
		_, err := read.QueueInspect(queue)
		require.NoError(t, err)
	}
	for _, body := range []string{"one", "restart"} {
		msg := getClusterMessage(t, read, queue, body, true)
		require.NoError(t, msg.Ack(false))
	}
	_, ok, err := read.Get(queue, false)
	require.NoError(t, err)
	require.False(t, ok)

	// Only the final joined broker remains. It could replace one replica
	// of a shard, but it cannot have been the shard's original sole voter.
	survivor := nodes[brokers-1].dial(t)
	minority := openClusterChannel(t, survivor)
	require.NoError(t, minority.Confirm(false))
	_, err = minority.QueueInspect(queue)
	require.NoError(t, err)
	for i := range brokers - 1 {
		nodes[i].close(t)
	}
	assertClusterPublishRejected(t, survivor, minority, queue)
}

func newTestCluster(t *testing.T, brokers, originalVoters int, shards uint64) []testClusterBroker {
	t.Helper()
	sockets := make([]net.Listener, 2*brokers)
	addresses := make([]string, len(sockets))
	for i := range sockets {
		s, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		sockets[i] = s
		t.Cleanup(func() { _ = s.Close() })
		addresses[i] = s.Addr().String()
	}
	for _, s := range sockets {
		require.NoError(t, s.Close())
	}

	token := make([]byte, 32)
	_, err := rand.Read(token)
	require.NoError(t, err)
	tokenFile := filepath.Join(t.TempDir(), "peer-token")
	require.NoError(t, os.WriteFile(tokenFile, []byte(base64.RawURLEncoding.EncodeToString(token)), 0600))
	seeds := make([]string, originalVoters)
	for i := range seeds {
		seeds[i] = fmt.Sprintf("%d@%s", i+1, addresses[i])
	}
	nodes := make([]testClusterBroker, brokers)
	for i := range nodes {
		cfg := config.DefaultConfig()
		cfg.Storage.Path = t.TempDir()
		cfg.Network.Address = addresses[i+brokers]
		cfg.Cluster = config.ClusterConfig{
			ID: "amqp-network", NodeID: uint64(i + 1), PeerAddress: addresses[i], Seeds: seeds, Shards: shards,
			TokenFile: tokenFile, AllowLoopbackPlaintext: true,
		}
		if originalVoters == 1 && i > 1 {
			cfg.Cluster.Discovery = []string{addresses[1]}
		}
		nodes[i].config = cfg
		t.Cleanup(func() { nodes[i].close(t) })
	}
	return nodes
}

func waitClusterBroker(t *testing.T, broker *testClusterBroker) {
	t.Helper()
	require.Eventually(t, func() bool { return broker.node.Ready() == nil }, 15*time.Second, 30*time.Millisecond)
}

type testClusterBroker struct {
	config *config.AMQPConfig
	node   *cluster.Node
	broker *server.Server
}

func (f *testClusterBroker) start(t *testing.T, initialize bool) {
	t.Helper()
	require.NoError(t, f.config.Validate())
	n, err := openCluster(f.config, initialize)
	require.NoError(t, err)
	f.node = n
	b, err := server.NewServerBuilder().WithConfig(f.config).WithUnifiedBroker(server.NewClusterBroker(n)).Build()
	require.NoError(t, err)
	f.broker = b
	exited := make(chan error, 1)
	go func() { exited <- b.Start() }()
	require.Eventually(t, func() bool {
		select {
		case err := <-exited:
			t.Errorf("AMQP listener stopped early: %v", err)
		default:
		}
		return b.IsListening()
	}, 4*time.Second, 10*time.Millisecond)
}

func (f *testClusterBroker) close(t *testing.T) {
	t.Helper()
	if f.broker != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		err := f.broker.Shutdown(ctx)
		cancel()
		require.NoError(t, err)
		f.broker = nil
	}
	if f.node != nil {
		require.NoError(t, f.node.Close())
		f.node = nil
	}
}

func (f *testClusterBroker) dial(t *testing.T) *amqp.Connection {
	t.Helper()
	c, err := amqp.Dial(f.amqpURL())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func (f *testClusterBroker) amqpURL() string {
	return "amqp://guest:guest@" + f.config.Network.Address + "/"
}

func openClusterChannel(t *testing.T, c *amqp.Connection) *amqp.Channel {
	t.Helper()
	ch, err := c.Channel()
	require.NoError(t, err)
	t.Cleanup(func() { _ = ch.Close() })
	return ch
}

func waitQueue(t *testing.T, broker *testClusterBroker, queue string, passive bool) *amqp.Connection {
	t.Helper()
	var connection *amqp.Connection
	require.Eventually(t, func() bool {
		c, err := amqp.Dial(broker.amqpURL())
		if err != nil {
			return false
		}
		ch, err := c.Channel()
		if err != nil {
			_ = c.Close()
			return false
		}
		defer ch.Close()
		if passive {
			_, err = ch.QueueDeclarePassive(queue, true, false, false, false, nil)
		} else {
			_, err = ch.QueueDeclare(queue, true, false, false, false, nil)
		}
		if err != nil {
			_ = c.Close()
			return false
		}
		connection = c
		return true
	}, 15*time.Second, 60*time.Millisecond)
	t.Cleanup(func() { _ = connection.Close() })
	return connection
}

func publishClusterConfirm(t *testing.T, c *amqp.Channel, queue, body string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	confirm, err := c.PublishWithDeferredConfirmWithContext(ctx, "", queue, true, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent, ContentType: "text/plain", MessageId: "message-" + body, Body: []byte(body),
	})
	require.NoError(t, err)
	require.NotNil(t, confirm)
	ok, err := confirm.WaitContext(ctx)
	require.NoError(t, err)
	require.True(t, ok)
}

func assertClusterPublishRejected(t *testing.T, conn *amqp.Connection, ch *amqp.Channel, queue string) {
	t.Helper()
	wireConfirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	wireClose := conn.NotifyClose(make(chan *amqp.Error, 1))
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	confirm, err := ch.PublishWithDeferredConfirmWithContext(ctx, "", queue, true, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent, Body: []byte("minority must never confirm"),
	})
	require.NoError(t, err)
	require.NotNil(t, confirm)
	acked, err := confirm.WaitContext(ctx)
	require.NoError(t, err)
	require.False(t, acked)
	select {
	case reply, open := <-wireConfirms:
		if open {
			require.Equal(t, confirm.DeliveryTag, reply.DeliveryTag)
			require.False(t, reply.Ack)
			return
		}
	case <-ctx.Done():
		t.Fatal("no broker response to minority publish")
	}
	select {
	case brokerErr := <-wireClose:
		require.NotNil(t, brokerErr)
		require.True(t, brokerErr.Server)
		require.Contains(t, brokerErr.Reason, "shard")
	case <-ctx.Done():
		t.Fatal("publisher closed without a broker rejection")
	}
}

func getClusterMessage(t *testing.T, c *amqp.Channel, queue, body string, redelivered bool) amqp.Delivery {
	t.Helper()
	msg, ok, err := c.Get(queue, false)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, body, string(msg.Body))
	require.Equal(t, redelivered, msg.Redelivered)
	return msg
}
