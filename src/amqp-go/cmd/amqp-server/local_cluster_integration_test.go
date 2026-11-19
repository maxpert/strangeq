package main

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/config"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
)

func TestLocalAMQPPromotesWithoutReplacingGatewayAndSurvivesSeedLoss(t *testing.T) {
	nodes := newTestCluster(t, 3, 1, 2)
	transport := nodes[0].config.Cluster
	nodes[0].config.Cluster = config.ClusterConfig{Local: true, Shards: 2}
	nodes[0].start(t, false)
	const queue = "local-promoted-bytes"
	connection := waitQueue(t, &nodes[0], queue, false)
	writer := openClusterChannel(t, connection)
	require.NoError(t, writer.Confirm(false))
	originalNode, originalGateway := nodes[0].node, nodes[0].broker
	bodies := make([]string, 0, 48)
	ids := make([]string, 0, 48)
	confirmBody := func(label, body string) {
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		defer cancel()
		confirm, err := writer.PublishWithDeferredConfirmWithContext(ctx, "", queue, true, false, amqp.Publishing{DeliveryMode: amqp.Persistent, MessageId: label, Body: []byte(body)})
		require.NoError(t, err)
		require.NotNil(t, confirm)
		ok, err := confirm.WaitContext(ctx)
		require.NoError(t, err)
		require.True(t, ok)
	}
	publish := func(label string) {
		body := label + "\x00\xff" + strings.Repeat(label+"-payload-", 1024)
		bodies = append(bodies, body)
		ids = append(ids, label)
		confirmBody(label, body)
	}
	const probeID = "probe-settled-on-original-connection"
	const probeBody = "probe-body\x00\xff"
	confirmBody(probeID, probeBody)
	publish("before-activation")
	lineage, id := nodes[0].node.Identity()
	activation := *nodes[0].config
	_, port, err := net.SplitHostPort(activation.Network.Address)
	require.NoError(t, err)
	activation.Network.Port, err = strconv.Atoi(port)
	require.NoError(t, err)
	activation.Cluster = transport
	activation.Cluster.Local = true
	activation.Cluster.ID = ""
	activation.Cluster.NodeID = 0
	activation.Cluster.Seeds = nil
	configPath := filepath.Join(t.TempDir(), "broker.yaml")
	require.NoError(t, activation.Save(configPath))
	require.NoError(t, reloadClusterPeers(nodes[0].node, nodes[0].config, configPath))
	require.Same(t, originalNode, nodes[0].node)
	require.Same(t, originalGateway, nodes[0].broker)
	require.False(t, connection.IsClosed())
	publish("after-activation")
	waitVoters := func(count int) {
		require.Eventually(t, func() bool {
			for i := 0; i < count; i++ {
				statuses := nodes[i].node.LocalGroupStatuses()
				if len(statuses) != 3 {
					return false
				}
				for _, st := range statuses {
					if len(st.Voters) != count || len(st.Learners) != 0 || st.ConfigIndex == 0 || st.Applied < st.ConfigIndex {
						return false
					}
				}
			}
			return true
		}, 30*time.Second, 20*time.Millisecond, "catalog and every data group must apply %d voters", count)
	}
	for i := 1; i < 3; i++ {
		nodes[i].config.Cluster.ID = lineage
		nodes[i].config.Cluster.NodeID = id + uint64(i)
		nodes[i].config.Cluster.Seeds = []string{fmt.Sprintf("%d@%s", id, transport.PeerAddress)}
		nodes[i].config.Cluster.Discovery = nil
		nodes[i].start(t, false)
		publish(fmt.Sprintf("during-join-%d", i))
		waitVoters(i + 1)
		require.False(t, connection.IsClosed())
		publish(fmt.Sprintf("applied-%d-voters", i+1))
		if i == 1 {
			// Consume and settle an independently tracked pre-activation body
			// through the same live connection after both voters apply it.
			probe := getClusterMessage(t, writer, queue, probeBody, false)
			require.Equal(t, probeID, probe.MessageId)
			require.NoError(t, probe.Ack(false))
			waitClusterQueueDepth(t, &nodes[0], queue, uint64(len(bodies)), 0)
		}
	}
	nodes[0].close(t)
	// No source node or seed listener remains. Both survivors must form quorum.
	reader := openClusterChannel(t, waitQueue(t, &nodes[1], queue, true))
	for i, body := range bodies {
		message := getClusterMessage(t, reader, queue, body, false)
		require.Equal(t, ids[i], message.MessageId)
		require.NoError(t, message.Ack(false))
	}
	waitClusterQueueDepth(t, &nodes[1], queue, 0, 0)
}

func TestLocalStorageCannotFallBackToLegacyStartup(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Storage.Path = t.TempDir()
	cfg.Cluster = config.ClusterConfig{Local: true, Shards: 1}
	node, err := openCluster(cfg, false)
	require.NoError(t, err)
	require.NoError(t, node.Close())
	cfg.Cluster = config.ClusterConfig{}
	_, err = openCluster(cfg, false)
	require.ErrorContains(t, err, "persistent cluster storage requires")
}
