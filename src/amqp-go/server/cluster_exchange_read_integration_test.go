package server

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/internal/cluster"
	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/internal/clusterraft"
	"github.com/stretchr/testify/require"
)

func TestClusterExchangeDescribeFollowsBindWithoutRaftWrite(t *testing.T) {
	nodes, directories := openExchangeReadNodes(t)
	const queueName = "read-index-worker"
	const exchangeName = "read-index-source"
	const shardKey = "\x00exchange:" + exchangeName
	const bindingKey = "accepted"
	var preferred uint64
	require.Eventually(t, func() bool {
		for _, node := range nodes {
			status, expected, ok := node.LocalQueueShardStatus(shardKey)
			if node.Ready() != nil || !ok || status.Leader == 0 || status.Leader != expected ||
				len(status.Voters) != len(nodes) || status.Applied < 2 {
				return false
			}
			preferred = expected
		}
		return true
	}, 8*time.Second, 25*time.Millisecond, "the preferred leader must be established at all voters")
	// Catalog reports share these extents with the data shard. Wait for both
	// groups' actual configurations to be durably reported at every node so
	// background placement reconciliation has no outstanding write to make.
	waitExchangeReadPlacementReports(t, nodes, directories)
	leader := nodes[preferred-1]
	follower := nodes[preferred%uint64(len(nodes))]
	require.NotSame(t, leader, follower)
	writer := NewClusterBroker(leader)
	queue, err := writer.DeclareQueue(queueName, true, false, false, nil)
	require.NoError(t, err)
	require.NoError(t, writer.DeclareExchange(exchangeName, "direct", true, false, false, nil))
	journalDirectory := filepath.Join(directories[preferred-1], "raft-journal")
	beforeBind := exchangeReadExtentDigests(t, journalDirectory)
	require.NoError(t, writer.BindQueue(queueName, exchangeName, bindingKey, nil))
	before := exchangeReadExtentDigests(t, journalDirectory)
	require.NotEqual(t, beforeBind, before, "a real binding write must change the monitored journal extents")
	prior, _, ok := leader.LocalQueueShardStatus(shardKey)
	require.True(t, ok)
	require.Equal(t, preferred, prior.Leader)
	require.Equal(t, prior.Commit, prior.Applied, "completed bind must be locally applied")

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	router := NewClusterBroker(follower)
	targets, err := router.route(ctx, exchangeName, bindingKey, nil)
	require.NoError(t, err)
	require.Equal(t, []clusterTarget{{Queue: queueName, Incarnation: clusterqueue.QueueID(queue.Ordinal)}}, targets)
	after, _, ok := leader.LocalQueueShardStatus(shardKey)
	require.True(t, ok)
	require.Equal(t, preferred, after.Leader)
	require.Equal(t, prior.Commit, after.Commit, "routing a named exchange must not append a describe proposal")
	require.Equal(t, prior.Applied, after.Applied)
	require.Equal(t, before, exchangeReadExtentDigests(t, journalDirectory), "routing a named exchange must leave the active journal extents unchanged")
}

func exchangeReadExtentDigests(t *testing.T, directory string) map[string][sha256.Size]byte {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(directory, "extent-*.raft-journal"))
	require.NoError(t, err)
	require.NotEmpty(t, paths, "the monitored node must have active segmented journal extents")
	digests := make(map[string][sha256.Size]byte, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		require.NotEmpty(t, data, "the monitored journal extent must contain actual Raft records")
		digests[filepath.Base(path)] = sha256.Sum256(data)
	}
	return digests
}

func waitExchangeReadPlacementReports(t *testing.T, nodes []*cluster.Node, directories []string) {
	t.Helper()
	type placement struct{ Group, Index uint64 }
	type catalogView struct {
		Epoch      uint64
		Catalog    placement
		Placements []placement
	}
	require.Eventually(t, func() bool {
		var epoch uint64
		var reference map[uint64]clusterraft.Status
		for i, node := range nodes {
			raw, err := os.ReadFile(filepath.Join(directories[i], "catalog-cache"))
			var view catalogView
			if err != nil || json.Unmarshal(raw, &view) != nil || view.Epoch == 0 || len(view.Placements) != 1 {
				return false
			}
			statuses := node.LocalGroupStatuses()
			if len(statuses) != 2 {
				return false
			}
			for _, placement := range append(view.Placements, view.Catalog) {
				status, ok := statuses[placement.Group]
				if !ok || placement.Index == 0 || placement.Index != status.ConfigIndex || status.Leader == 0 ||
					len(status.Voters) != len(nodes) || len(status.Learners) != 0 || status.Commit != status.Applied {
					return false
				}
				if reference != nil {
					other := reference[placement.Group]
					if status.Leader != other.Leader || status.Commit != other.Commit {
						return false
					}
				}
			}
			if reference == nil {
				reference, epoch = statuses, view.Epoch
			} else if view.Epoch != epoch {
				return false
			}
		}
		return true
	}, 8*time.Second, 25*time.Millisecond, "all catalog and data placement reports must be committed, applied, and cached before observing the shared journal")
}

func TestClusterMachineQueryAcceptsOnlyExchangeDescribe(t *testing.T) {
	machine := NewClusterMachine()
	const name = "read-only-description"
	declare, err := exchangeRequest(clusterExchangeCommand{
		Kind: exchangeDeclare, Name: name, Exchange: clusterExchange{Name: name, Type: "direct", Durable: true},
	})
	require.NoError(t, err)
	var declaration clusterExchangeReply
	require.NoError(t, json.Unmarshal(machine.Apply(declare), &declaration))
	require.Empty(t, declaration.Code)
	before, err := machine.Snapshot()
	require.NoError(t, err)

	removeExchange, err := exchangeRequest(clusterExchangeCommand{Kind: exchangeDelete, Name: name})
	require.NoError(t, err)
	_, err = machine.Query(removeExchange)
	require.Error(t, err, "read query must reject an exchange deletion")
	queueWrite, err := clusterqueue.EncodeCommand(clusterqueue.QueueCommand{Kind: clusterqueue.DeclareQueue, Queue: "write-via-query"})
	require.NoError(t, err)
	_, err = machine.Query(queueWrite)
	require.Error(t, err, "read query must reject a queue declaration")
	for _, item := range []struct {
		name, code string
	}{
		{name: name},
		{name: "amq.direct"},
		{name: "missing-exchange", code: "missing"},
		{code: "invalid"},
	} {
		describe, err := exchangeRequest(clusterExchangeCommand{Kind: exchangeDescribe, Name: item.name})
		require.NoError(t, err)
		raw, err := machine.Query(describe)
		require.NoError(t, err)
		var result clusterExchangeReply
		require.NoError(t, json.Unmarshal(raw, &result))
		require.Equal(t, item.code, result.Code)
		if item.code == "" {
			require.Equal(t, item.name, result.Exchange.Name)
		}
		require.Equal(t, machine.Apply(describe), raw, "old describe log entries have the same result")
	}
	after, err := machine.Snapshot()
	require.NoError(t, err)
	require.Equal(t, before, after, "queries cannot mutate the shard state")
}

func openExchangeReadNodes(t *testing.T) ([]*cluster.Node, []string) {
	t.Helper()
	const replicas = 3
	var listeners [replicas]net.Listener
	peers := make([]cluster.Peer, replicas)
	for i := range peers {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners[i] = listener
		t.Cleanup(func() { _ = listener.Close() })
		peers[i] = cluster.Peer{ID: uint64(i + 1), Address: listener.Addr().String()}
	}
	for _, listener := range listeners {
		require.NoError(t, listener.Close())
	}
	var secret [32]byte
	_, err := rand.Read(secret[:])
	require.NoError(t, err)
	token := base64.RawURLEncoding.EncodeToString(secret[:])
	configs := make([]cluster.Config, replicas)
	directories := make([]string, replicas)
	for i, peer := range peers {
		directories[i] = t.TempDir()
		configs[i] = cluster.Config{
			ClusterID: "exchange-read-index", Peer: peer, Seeds: peers, Listen: peer.Address,
			Directory: directories[i], Shards: 1, Token: token, AllowInsecure: true,
			NewMachine: func(uint64) clusterraft.Machine { return NewClusterMachine() },
		}
		// No ExpiryCommand in this fixture. A maintenance proposal would
		// advance the monitored data shard independently of routing.
		require.NoError(t, cluster.Initialize(configs[i]))
	}
	nodes := make([]*cluster.Node, replicas)
	for i, config := range configs {
		node, err := cluster.Open(config)
		require.NoError(t, err)
		nodes[i] = node
		t.Cleanup(func() { require.NoError(t, node.Close()) })
	}
	return nodes, directories
}
