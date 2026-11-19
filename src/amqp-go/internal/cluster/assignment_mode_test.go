//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package cluster

import (
	"os"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/internal/clusterwire"
	"github.com/stretchr/testify/require"
)

func TestLegacyClusterRejectsAssignmentCommandsBeforeProposal(t *testing.T) {
	config := journalTestNodeConfig(t, true)
	require.NoError(t, Initialize(config))
	node, err := Open(config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, node.Close()) })
	const queue = "mode-fence"
	require.Eventually(t, func() bool {
		if node.Ready() != nil {
			return false
		}
		status, _, ok := node.LocalQueueShardStatus(queue)
		return ok && status.Leader == config.Peer.ID && status.Applied > 1
	}, 4*time.Second, 20*time.Millisecond)
	before, _, ok := node.LocalQueueShardStatus(queue)
	require.True(t, ok)
	open := clusterqueue.QueueCommand{
		Kind: clusterqueue.OpenQueueSession, Queue: queue, Incarnation: 1,
		Owner: "registered-consumer", Session: 1, Consumer: true, AutoAssign: true, Prefetch: 2, Deadline: 1,
	}
	credit := clusterqueue.QueueCommand{
		Kind: clusterqueue.SetAssignmentCredit, Queue: queue, Incarnation: 1,
		Owner: open.Owner, Session: 1, Sequence: 1, Prefetch: 1,
	}
	for _, command := range []clusterqueue.QueueCommand{open, credit} {
		wire, err := clusterqueue.EncodeCommand(command)
		require.NoError(t, err)
		local, err := node.Submit(t.Context(), queue, wire)
		require.NoError(t, err)
		remote, err := node.client.CommandForGroup(t.Context(), config.Listen, before.GroupID, wire)
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(remote), 9)
		for _, body := range [][]byte{local, remote[9:]} {
			reply, err := clusterqueue.DecodeResponse(body)
			require.NoError(t, err)
			require.ErrorIs(t, reply.Err(), clusterqueue.ErrInvalidCommand)
		}
	}
	after, _, ok := node.LocalQueueShardStatus(queue)
	require.True(t, ok)
	require.Equal(t, before.Applied, after.Applied)
	require.Equal(t, before.Commit, after.Commit)
}

func TestAssignmentsRequireFreshJournalAndMatchingPeerMode(t *testing.T) {
	legacy := journalTestNodeConfig(t, true)
	require.NoError(t, Initialize(legacy))
	before := journalTestStorageDigest(t, legacy.Directory)
	incompatible := legacy
	incompatible.ExperimentalAssignments = true
	_, err := Open(incompatible)
	require.ErrorContains(t, err, "storage mode")
	require.Equal(t, before, journalTestStorageDigest(t, legacy.Directory))

	fresh := journalTestNodeConfig(t, true)
	fresh.ExperimentalAssignments = true
	require.NoError(t, Initialize(fresh))
	without := fresh
	without.ExperimentalAssignments = false
	_, err = Open(without)
	require.ErrorContains(t, err, "storage mode")

	node, err := Open(fresh)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, node.Close()) })
	require.True(t, node.SupportsAssignments())
	body, err := node.client.Membership(t.Context(), fresh.Listen, nil)
	require.NoError(t, err)
	require.NotEmpty(t, body)

	peer, err := clusterwire.NewClient(clusterwire.Config{
		Listen: fresh.Listen, ClusterID: fresh.ClusterID, Token: fresh.Token, AllowInsecure: true,
		ProtocolVersion: ackAndClaimPeerProtocol,
	})
	require.NoError(t, err)
	t.Cleanup(peer.Close)
	_, err = peer.Membership(t.Context(), fresh.Listen, nil)
	require.ErrorContains(t, err, "HTTP status 401")
}

func TestSmallBodyCompressionFencesLegacyStorageAndPeers(t *testing.T) {
	enableSmallBodies := func(cfg Config) Config {
		cfg.ExperimentalJournalSmallBodyCompression = true
		return cfg
	}
	legacy := journalTestNodeConfig(t, true)
	legacy.ExperimentalAssignments = true
	legacy.ExperimentalJournalBodyCompression = true
	require.NoError(t, Initialize(legacy))
	before := journalTestStorageDigest(t, legacy.Directory)
	err := Initialize(enableSmallBodies(legacy))
	require.ErrorContains(t, err, "storage mode")
	require.Equal(t, before, journalTestStorageDigest(t, legacy.Directory))

	fresh := journalTestNodeConfig(t, true)
	fresh.ExperimentalAssignments = true
	fresh.ExperimentalJournalBodyCompression = true
	small := enableSmallBodies(fresh)
	require.NoError(t, Initialize(small))
	node, err := Open(small)
	require.NoError(t, err)
	defer func() { require.NoError(t, node.Close()) }()
	body, err := node.client.Membership(t.Context(), small.Listen, nil)
	require.NoError(t, err)
	require.NotEmpty(t, body)

	peer, err := clusterwire.NewClient(clusterwire.Config{
		Listen: small.Listen, ClusterID: small.ClusterID, Token: small.Token, AllowInsecure: true,
		ProtocolVersion: compressedBodyPeerProtocol,
	})
	require.NoError(t, err)
	t.Cleanup(peer.Close)
	_, err = peer.Membership(t.Context(), small.Listen, nil)
	require.ErrorContains(t, err, "HTTP status 401")
	require.NoError(t, node.Close())
	before = journalTestStorageDigest(t, small.Directory)
	_, err = Open(fresh)
	require.ErrorContains(t, err, "storage mode")
	require.Equal(t, before, journalTestStorageDigest(t, small.Directory))

	require.NoError(t, os.Remove(node.path(clusterStorageModeFile)))
	before = journalTestStorageDigest(t, small.Directory)
	_, err = Open(small)
	require.ErrorContains(t, err, "lost its persistent storage mode")
	_, err = Open(fresh)
	require.ErrorContains(t, err, "storage mode")
	require.Equal(t, before, journalTestStorageDigest(t, small.Directory))
}
