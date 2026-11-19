//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterraft"
	"github.com/stretchr/testify/require"
)

func dualStreamTestConfig(t *testing.T) Config {
	t.Helper()
	cfg := journalTestNodeConfig(t, true)
	cfg.ExperimentalRaftRangeSync = true
	cfg.ExperimentalRaftJournalDualStream = true
	return cfg
}

func dualStreamTestPath(cfg Config, stream int) string {
	return filepath.Join(cfg.Directory, fmt.Sprintf("raft-journal-%d", stream))
}

func TestDualJournalNodeUsesFreshStableParityStorage(t *testing.T) {
	cfg := dualStreamTestConfig(t)
	require.NoError(t, Initialize(cfg))
	for stream := range 2 {
		require.FileExists(t, filepath.Join(dualStreamTestPath(cfg, stream), "active.raft-journal"),
			"explicit two-stream mode must provision both independent journals")
	}
	require.NoDirExists(t, filepath.Join(cfg.Directory, "raft-journal"))
	mode, err := os.ReadFile(filepath.Join(cfg.Directory, "raft-storage-mode-v1"))
	require.NoError(t, err)
	require.Equal(t, "journal-two-stream-parity-v1", string(mode))

	assertInventory := func() {
		t.Helper()
		for stream, ids := range [][]uint64{{2}, {1, catalogID}} {
			owner, err := clusterraft.OpenNodeJournalWithRangeSync(dualStreamTestPath(cfg, stream), false)
			require.NoError(t, err)
			actual := owner.GroupIDs()
			require.NoError(t, owner.Close())
			require.Equal(t, ids, actual, "group parity must remain stable on disk")
		}
	}
	assertInventory()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	first, err := Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = first.Close() })
	for id := uint64(1); id <= cfg.Shards; id++ {
		g := first.group(id)
		require.NotNil(t, g)
		require.NoError(t, g.Campaign(ctx))
		_, err := g.Propose(ctx, []byte(fmt.Sprintf("stream body %d", id)))
		require.NoError(t, err)
	}
	require.NoError(t, first.Close())
	assertInventory()
	second, err := Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close() })
	for id := uint64(1); id <= cfg.Shards; id++ {
		snapshot, err := second.group(id).MachineSnapshot(ctx)
		require.NoError(t, err)
		require.JSONEq(t, fmt.Sprintf(`["stream body %d"]`, id), string(snapshot))
	}
	require.NoError(t, second.Close())
	assertInventory()
}

func TestDualJournalNodeResumesPartialProvisioning(t *testing.T) {
	cfg := dualStreamTestConfig(t)
	stub := &Node{cfg: cfg}
	name, mode, err := stub.storageIdentity()
	require.NoError(t, err)
	require.NoError(t, stub.claimStorageMode(mode))
	require.NoError(t, provisionFile(stub.path(name), encode(identity{
		Peer: cfg.Peer, Cluster: cfg.ClusterID, Shards: cfg.Shards, Seeds: cfg.Seeds,
	})))
	first, err := clusterraft.OpenNodeJournalWithRangeSync(dualStreamTestPath(cfg, 0), true)
	require.NoError(t, err)
	require.NoError(t, first.Close())
	require.NoDirExists(t, dualStreamTestPath(cfg, 1))
	require.NoFileExists(t, stub.path(journalProvisionedFile))

	require.NoError(t, Initialize(cfg))
	require.FileExists(t, filepath.Join(dualStreamTestPath(cfg, 1), "active.raft-journal"))
	pair, err := clusterraft.OpenNodeJournalPairWithRangeSync(cfg.Directory, false, cfg.Peer.ID)
	require.NoError(t, err)
	defer func() { require.NoError(t, pair.Close()) }()
	require.Equal(t, []uint64{1, 2, catalogID}, pair.GroupIDs())
}

func TestDualJournalNodeRejectsModeChangeAndMissingStream(t *testing.T) {
	cfg := dualStreamTestConfig(t)
	require.NoError(t, Initialize(cfg))
	before := journalTestStorageDigest(t, cfg.Directory)
	wrong := cfg
	wrong.ExperimentalRaftJournalDualStream = false
	node, err := Open(wrong)
	if node != nil {
		_ = node.Close()
	}
	require.ErrorContains(t, err, "storage mode")
	require.ErrorContains(t, Initialize(wrong), "storage mode")
	require.Equal(t, before, journalTestStorageDigest(t, cfg.Directory))

	for stream := range 2 {
		active := filepath.Join(dualStreamTestPath(cfg, stream), "active.raft-journal")
		backup := filepath.Join(t.TempDir(), "active.raft-journal")
		require.NoError(t, os.Rename(active, backup))
		node, err := Open(cfg)
		if node != nil {
			_ = node.Close()
		}
		require.ErrorContains(t, err, "lost its established persistent raft journal")
		require.NoFileExists(t, active)
		require.NoError(t, os.Rename(backup, active))
	}
	reopened, err := Open(cfg)
	require.NoError(t, err, "failed open must release every stream owner it acquired")
	require.NoError(t, reopened.Close())
}

type failedJournalOwner struct{ nodeJournalOwner }

func (failedJournalOwner) Failure() error { return errors.New("the other journal stream failed") }

func TestDualJournalNodeRejectsCommandsAfterOtherStreamFails(t *testing.T) {
	cfg := dualStreamTestConfig(t)
	require.NoError(t, Initialize(cfg))
	running, err := Open(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, running.Close()) })

	const groupID = 2
	group := running.group(groupID)
	require.NotNil(t, group)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, group.Campaign(ctx))
	require.Eventually(t, func() bool {
		status := group.Status()
		return status.Leader == cfg.Peer.ID && status.Applied == status.Commit
	}, time.Second, 5*time.Millisecond)
	before := group.Status()

	// Use a real healthy group and inject the other owner's terminal error
	// at the Node boundary without changing the running Node's owner.
	receiver := &Node{
		cfg: cfg, groups: map[uint64]*clusterraft.Group{groupID: group},
		journal: failedJournalOwner{running.journal},
	}
	request := binary.BigEndian.AppendUint64(nil, groupID)
	request = append(request, "must not enter the healthy stream"...)
	wire, err := receiver.Command(ctx, request)
	require.NoError(t, err)
	require.NotEmpty(t, wire)
	require.Equal(t, unavailable, wire[0], "a forwarded command must stop at node-wide failure")
	require.Equal(t, before.Commit, group.Status().Commit)
	require.Equal(t, before.Applied, group.Status().Applied)
}

func TestDualJournalNodeAcquiresBothStreamOwners(t *testing.T) {
	cfg := dualStreamTestConfig(t)
	require.NoError(t, Initialize(cfg))
	owner, err := clusterraft.OpenNodeJournalWithRangeSync(dualStreamTestPath(cfg, 1), false)
	require.NoError(t, err)
	t.Cleanup(func() { _ = owner.Close() })
	node, err := Open(cfg)
	if node != nil {
		_ = node.Close()
	}
	require.ErrorContains(t, err, "lock node raft journal")
	available, err := clusterraft.OpenNodeJournalWithRangeSync(dualStreamTestPath(cfg, 0), false)
	require.NoError(t, err, "stream 1 lock failure must release stream 0")
	require.NoError(t, available.Close())
	require.NoError(t, owner.Close())
	reopened, err := Open(cfg)
	require.NoError(t, err)
	require.NoError(t, reopened.Close())
}

func TestDualJournalNodeRejectsSwappedStreamsAndLostMode(t *testing.T) {
	cfg := dualStreamTestConfig(t)
	require.NoError(t, Initialize(cfg))
	zero, one := dualStreamTestPath(cfg, 0), dualStreamTestPath(cfg, 1)
	hold := filepath.Join(cfg.Directory, "swap-hold")
	require.NoError(t, os.Rename(zero, hold))
	require.NoError(t, os.Rename(one, zero))
	require.NoError(t, os.Rename(hold, one))
	node, err := Open(cfg)
	if node != nil {
		_ = node.Close()
	}
	require.ErrorContains(t, err, "routing or owner")
	// Restore the physical mapping, then remove only the authority claim.
	require.NoError(t, os.Rename(zero, hold))
	require.NoError(t, os.Rename(one, zero))
	require.NoError(t, os.Rename(hold, one))
	require.NoError(t, os.Remove(filepath.Join(cfg.Directory, "raft-storage-mode-v1")))
	before := journalTestStorageDigest(t, cfg.Directory)
	node, err = Open(cfg)
	if node != nil {
		_ = node.Close()
	}
	require.ErrorContains(t, err, "lost its persistent storage mode")
	require.Equal(t, before, journalTestStorageDigest(t, cfg.Directory))
}
