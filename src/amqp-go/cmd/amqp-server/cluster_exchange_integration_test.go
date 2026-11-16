package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func TestClusterNamedExchangeFromAnyBroker(t *testing.T) {
	var addresses [6]string
	var sockets []net.Listener
	for i := range addresses {
		socket, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		t.Cleanup(func() { _ = socket.Close() })
		addresses[i] = socket.Addr().String()
		sockets = append(sockets, socket)
	}
	for _, socket := range sockets {
		require.NoError(t, socket.Close())
	}
	var secret [32]byte
	_, err := rand.Read(secret[:])
	require.NoError(t, err)
	tokenFile := filepath.Join(t.TempDir(), "peer-token")
	require.NoError(t, os.WriteFile(tokenFile, []byte(base64.RawURLEncoding.EncodeToString(secret[:])), 0600))

	var nodes [3]testClusterBroker
	for i := range nodes {
		cfg := config.DefaultConfig()
		cfg.Storage.Path = t.TempDir()
		cfg.Network.Address = addresses[i+3]
		cfg.Cluster = config.ClusterConfig{
			ID: "named-exchange", NodeID: uint64(i + 1), PeerAddress: addresses[i], Seeds: []string{"1@" + addresses[0]},
			Shards: 4, TokenFile: tokenFile, AllowLoopbackPlaintext: true,
		}
		nodes[i].config = cfg
		t.Cleanup(func() { nodes[i].close(t) })
	}
	_, err = openCluster(nodes[0].config, true)
	require.NoError(t, err)
	nodes[0].start(t, false)
	const queue = "cluster-topic-worker"
	const exchange = "cluster-topic-source"
	first := openClusterChannel(t, waitQueue(t, &nodes[0], queue, false))
	require.NoError(t, first.ExchangeDeclare(exchange, "topic", true, false, false, false, nil))
	require.NoError(t, first.QueueBind(queue, "jobs.*", exchange, false, nil))
	for i := 1; i < len(nodes); i++ {
		nodes[i].start(t, false)
		waitClusterBroker(t, &nodes[i])
	}

	publisher := openClusterChannel(t, waitQueue(t, &nodes[1], queue, true))
	require.NoError(t, publisher.ExchangeDeclarePassive(exchange, "topic", true, false, false, false, nil))
	require.NoError(t, publisher.Confirm(false))
	for _, body := range []string{"first", "second"} {
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		confirm, err := publisher.PublishWithDeferredConfirmWithContext(ctx, exchange, "jobs.green", true, false, amqp.Publishing{
			Body: []byte(body), ContentType: "text/plain", DeliveryMode: amqp.Persistent,
		})
		require.NoError(t, err)
		require.NotNil(t, confirm)
		accepted, err := confirm.WaitContext(ctx)
		cancel()
		require.NoError(t, err)
		require.True(t, accepted)
	}

	reader := openClusterChannel(t, waitQueue(t, &nodes[2], queue, true))
	require.NoError(t, reader.Qos(1, 0, false))
	const consumer = "worker"
	deliveries, err := reader.Consume(queue, consumer, false, false, false, false, nil)
	require.NoError(t, err)
	read := func(body string) amqp.Delivery {
		select {
		case delivery, ok := <-deliveries:
			require.True(t, ok)
			require.Equal(t, body, string(delivery.Body))
			require.Equal(t, exchange, delivery.Exchange)
			require.Equal(t, "jobs.green", delivery.RoutingKey)
			return delivery
		case <-time.After(5 * time.Second):
			t.Fatalf("missing delivery %q", body)
			return amqp.Delivery{}
		}
	}
	delivery := read("first")
	snapshot, err := first.QueueInspect(queue)
	require.NoError(t, err)
	require.Equal(t, 1, snapshot.Messages)
	require.Equal(t, 1, snapshot.Consumers)
	require.NoError(t, delivery.Ack(false))
	read("second")
	require.NoError(t, reader.Cancel(consumer, false))
	redelivery := getClusterMessage(t, first, queue, "second", true)
	require.NoError(t, redelivery.Ack(false))
	snapshot, err = first.QueueInspect(queue)
	require.NoError(t, err)
	require.Zero(t, snapshot.Messages)
	require.Zero(t, snapshot.Consumers)
}
