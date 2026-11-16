package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestClusterAMQPThreeBrokerFailover(t *testing.T) {
	for _, role := range []string{"leader", "follower"} {
		t.Run(role, func(t *testing.T) {
			const queue = "failover"
			nodes := newTestCluster(t, 3, 3, 1)
			for i := range nodes {
				_, err := openCluster(nodes[i].config, true)
				require.NoError(t, err)
			}
			for i := range nodes {
				nodes[i].start(t, false)
			}
			leader := waitShardLeader(t, nodes, queue, -1, true)
			victim := leader
			if role == "follower" {
				victim = (leader + 1) % len(nodes)
			}
			publisher := openClusterChannel(t, nodes[leader].dial(t))
			_, err := publisher.QueueDeclare(queue, true, false, false, false, nil)
			require.NoError(t, err)
			require.NoError(t, publisher.Confirm(false))
			publishClusterConfirm(t, publisher, queue, "before-outage")

			nodes[victim].close(t)
			active := waitShardLeader(t, nodes, queue, victim, false)
			if role == "leader" {
				require.NotEqual(t, leader, active)
			} else {
				require.Equal(t, leader, active)
			}
			follower := 3 - victim - active // The indices of the three brokers sum to three.
			writer := openClusterChannel(t, nodes[follower].dial(t))
			require.NoError(t, writer.Confirm(false))
			publishClusterConfirm(t, writer, queue, "during-outage")
			reader := openClusterChannel(t, nodes[active].dial(t))
			message := getClusterMessage(t, reader, queue, "before-outage", false)
			require.NoError(t, message.Ack(false))
			_, err = reader.QueueInspect(queue)
			require.NoError(t, err)
			checkpoint, _, ok := nodes[active].node.LocalQueueShardStatus(queue)
			require.True(t, ok)

			nodes[victim].start(t, false)
			waitShardLeader(t, nodes, queue, -1, true)
			require.Eventually(t, func() bool {
				status, _, ok := nodes[victim].node.LocalQueueShardStatus(queue)
				return ok && status.Applied >= checkpoint.Applied
			}, 5*time.Second, 20*time.Millisecond, "returning broker must apply the outage writes and acknowledgment")
			returned := openClusterChannel(t, nodes[victim].dial(t))
			message = getClusterMessage(t, returned, queue, "during-outage", false)
			require.NoError(t, message.Ack(false))
			_, err = returned.QueueInspect(queue)
			require.NoError(t, err)
			require.NoError(t, returned.Confirm(false))
			publishClusterConfirm(t, returned, queue, "after-rejoin")
			message = getClusterMessage(t, reader, queue, "after-rejoin", false)
			require.NoError(t, message.Ack(false))
			_, found, err := reader.Get(queue, false)
			require.NoError(t, err)
			require.False(t, found)
		})
	}
}

func waitShardLeader(t *testing.T, nodes []testClusterBroker, queue string, offline int, preferAssignedLeader bool) int {
	t.Helper()
	var leader uint64
	require.Eventually(t, func() bool {
		leader = 0
		for i := range nodes {
			if i == offline {
				continue
			}
			status, preferred, ok := nodes[i].node.LocalQueueShardStatus(queue)
			if !ok || status.Leader == 0 || len(status.Voters) != len(nodes) || int(status.Leader)-1 == offline {
				return false
			}
			if preferAssignedLeader && status.Leader != preferred {
				return false
			}
			if leader != 0 && leader != status.Leader {
				return false
			}
			leader = status.Leader
		}
		return leader > 0 && leader <= uint64(len(nodes))
	}, 5*time.Second, 20*time.Millisecond, "online brokers must agree on an online leader")
	return int(leader) - 1
}
