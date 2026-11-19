package cluster

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterraft"
	"github.com/stretchr/testify/require"
)

func localTestConfig(t *testing.T) Config {
	return Config{Directory: t.TempDir(), Shards: 1, NewMachine: func(uint64) clusterraft.Machine { return &testMachine{} }}
}

func TestLocalIdentityAndBindFailureRetainUsableJournal(t *testing.T) {
	cfg := localTestConfig(t)
	node, err := OpenLocal(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, node.Close()) })
	lineage, id := node.Identity()
	require.NotEmpty(t, lineage)
	require.NotZero(t, id)
	require.Nil(t, node.client)
	require.Nil(t, node.wire)
	require.Nil(t, node.transport.Load())
	submitEventually(t, node, "q", "before", `["before"]`)
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer occupied.Close()
	activation := Config{Peer: Peer{Address: occupied.Addr().String()}, Token: base64.RawURLEncoding.EncodeToString(make([]byte, 32)), AllowInsecure: true}
	require.ErrorContains(t, node.ActivatePeers(activation), "activate peer listener")
	require.Nil(t, node.transport.Load())
	require.Empty(t, address(node.catalog.view(), id))
	require.FileExists(t, filepath.Join(cfg.Directory, peerActivationFile))
	submitEventually(t, node, "q", "after-failure", `["before","after-failure"]`)
	require.NoError(t, occupied.Close())
	require.NoError(t, node.ActivatePeers(activation))
	require.Equal(t, activation.Peer.Address, address(node.catalog.view(), id))
	require.True(t, node.transport.Load().ready.Load())
	require.NoError(t, node.Close())
	cfg.Peer = activation.Peer
	cfg.Token, cfg.AllowInsecure = activation.Token, true
	reopened, err := OpenLocal(cfg)
	require.NoError(t, err)
	node = reopened
	gotLineage, gotID := node.Identity()
	require.Equal(t, lineage, gotLineage)
	require.Equal(t, id, gotID)
	submitEventually(t, node, "q", "after-restart", `["before","after-failure","after-restart"]`)
}

func TestLocalMissingIdentityRejectsUnknownDataBeforeWrites(t *testing.T) {
	for _, legacy := range []string{"identity.json", "raft-journal", "messages.wal"} {
		t.Run(legacy, func(t *testing.T) {
			cfg := localTestConfig(t)
			path := filepath.Join(cfg.Directory, legacy)
			require.NoError(t, os.WriteFile(path, []byte("foreign authority"), 0600))
			_, err := OpenLocal(cfg)
			require.ErrorContains(t, err, "without its persistent identity")
			entries, err := os.ReadDir(cfg.Directory)
			require.NoError(t, err)
			require.Len(t, entries, 1)
		})
	}
}

func TestLocalActivatedThreeVoterColdRestartNeedsNoInitialQuorum(t *testing.T) {
	cfg := localTestConfig(t)
	seed, err := OpenLocal(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = seed.Close() })
	token := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	addressFor := func() string {
		s, e := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, e)
		a := s.Addr().String()
		require.NoError(t, s.Close())
		return a
	}
	activation := Config{Peer: Peer{Address: addressFor()}, Token: token, AllowInsecure: true}
	require.NoError(t, seed.ActivatePeers(activation))
	lineage, id := seed.Identity()
	configs := []Config{cfg, {}, {}}
	configs[0].Peer, configs[0].Token, configs[0].AllowInsecure = activation.Peer, token, true
	nodes := []*Node{seed, nil, nil}
	for i := 1; i < 3; i++ {
		configs[i] = Config{ClusterID: lineage, Peer: Peer{ID: id + uint64(i), Address: addressFor()}, Seeds: []Peer{{ID: id, Address: activation.Peer.Address}}, Directory: t.TempDir(), Shards: 1, Token: token, AllowInsecure: true, NewMachine: cfg.NewMachine}
		configs[i].Listen = configs[i].Peer.Address
		nodes[i], err = Open(configs[i])
		require.NoError(t, err)
		j := i
		t.Cleanup(func() { _ = nodes[j].Close() })
	}
	require.Eventually(t, func() bool {
		for _, n := range nodes {
			statuses := n.LocalGroupStatuses()
			if len(statuses) != 2 {
				return false
			}
			for _, s := range statuses {
				if len(s.Voters) != 3 || len(s.Learners) != 0 || s.ConfigIndex == 0 {
					return false
				}
			}
		}
		return true
	}, 30*time.Second, 30*time.Millisecond)
	for _, n := range nodes {
		require.NoError(t, n.Close())
	}
	// Seed transport must reopen while every other voter is still offline.
	seed, err = OpenLocal(configs[0])
	require.NoError(t, err)
	nodes[0] = seed
	require.True(t, seed.transport.Load().ready.Load())
	for _, st := range seed.LocalGroupStatuses() {
		require.Len(t, st.Voters, 3)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	_, err = seed.Submit(ctx, "q", []byte("must-not-confirm-with-one-vote"))
	cancel()
	require.Error(t, err, "lost transport must not change persisted quorum")
	for i := 1; i < 3; i++ {
		nodes[i], err = Open(configs[i])
		require.NoError(t, err)
	}
	submitEventually(t, seed, "q", "cold-restart", `["cold-restart"]`)
}

// These are the optional interfaces discovered by clusterwire.Server. A gate
// must preserve both or forwarding assembles and copies the complete reply.
var _ interface {
	CommandParts(context.Context, []byte) ([]byte, []byte, error)
	ReadCommandParts(context.Context, []byte) ([]byte, []byte, error)
} = (*activationHandler)(nil)

type localQueryTestMachine struct{ testMachine }

func (m *localQueryTestMachine) Query([]byte) ([]byte, error) { return m.Snapshot() }

func TestLocalActivationGatePreservesSplitReplyHandlers(t *testing.T) {
	cfg := localTestConfig(t)
	cfg.NewMachine = func(uint64) clusterraft.Machine { return &localQueryTestMachine{} }
	node, err := OpenLocal(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, node.Close()) })
	submitEventually(t, node, "q", "one", `["one"]`)
	handler := &activationHandler{node: node, transport: &peerTransport{}}
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	command := binary.BigEndian.AppendUint64(nil, 1)
	command = append(command, []byte("two")...)
	for _, parts := range []func(context.Context, []byte) ([]byte, []byte, error){handler.CommandParts, handler.ReadCommandParts} {
		prefix, body, err := parts(ctx, command)
		require.ErrorIs(t, err, ErrUnavailable)
		require.Nil(t, prefix)
		require.Nil(t, body)
	}
	handler.transport.ready.Store(true)
	prefix, body, err := handler.CommandParts(ctx, command)
	require.NoError(t, err)
	require.Len(t, prefix, 9)
	require.Equal(t, committed, prefix[0])
	require.JSONEq(t, `["one","two"]`, string(body))
	prefix, body, err = handler.ReadCommandParts(ctx, binary.BigEndian.AppendUint64(nil, 1))
	require.NoError(t, err)
	require.Len(t, prefix, 9)
	require.Equal(t, committed, prefix[0])
	require.JSONEq(t, `["one","two"]`, string(body))
}

type localRestoreObservedMachine struct {
	testMachine
	observe  sync.Once
	restored chan<- []byte
	release  <-chan struct{}
}

func (m *localRestoreObservedMachine) Restore(data []byte) error {
	// The source cannot acknowledge snapshot installation or promote this
	// learner until its real state-machine restore finishes.
	m.observe.Do(func() {
		m.restored <- append([]byte(nil), data...)
		<-m.release
	})
	return m.testMachine.Restore(data)
}

func TestLocalActivationInstallsCompactedSnapshotAndLiveTail(t *testing.T) {
	cfg := localTestConfig(t)
	seed, err := OpenLocal(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, seed.Close()) })
	submitEventually(t, seed, "q", "base-one", `["base-one"]`)
	submitEventually(t, seed, "q", "base-two", `["base-one","base-two"]`)
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	require.NoError(t, seed.group(1).Snapshot(ctx), "establish a durable compacted base before peer activation")
	cancel()
	submitEventually(t, seed, "q", "before-activation-tail", `["base-one","base-two","before-activation-tail"]`)
	socket, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	seedAddress := socket.Addr().String()
	require.NoError(t, socket.Close())
	token := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	require.NoError(t, seed.ActivatePeers(Config{Peer: Peer{Address: seedAddress}, Token: token, AllowInsecure: true}))
	lineage, id := seed.Identity()
	socket, err = net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	joinAddress := socket.Addr().String()
	require.NoError(t, socket.Close())
	restored := make(chan []byte, 1)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	// Register unblocking after peer cleanup too, so a failed assertion never
	// waits for a machine restore while closing its journal.
	defer unblock()
	learnerID := id + 1
	learner, err := Open(Config{ClusterID: lineage, Peer: Peer{ID: learnerID, Address: joinAddress}, Listen: joinAddress, Seeds: []Peer{{ID: id, Address: seedAddress}}, Directory: t.TempDir(), Shards: 1, Token: token, AllowInsecure: true,
		NewMachine: func(uint64) clusterraft.Machine {
			return &localRestoreObservedMachine{restored: restored, release: release}
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { unblock(); require.NoError(t, learner.Close()) })
	select {
	case snapshot := <-restored:
		var values []string
		require.NoError(t, json.Unmarshal(snapshot, &values))
		require.GreaterOrEqual(t, len(values), 2, "fresh learner must restore the compacted base")
		require.Equal(t, []string{"base-one", "base-two"}, values[:2], "snapshot must contain the forced compacted base; the pre-join tail may arrive in the snapshot or log suffix")
	case <-time.After(20 * time.Second):
		t.Fatal("fresh learner never installed a Raft snapshot")
	}
	status := seed.group(1).Status()
	require.Contains(t, status.Learners, learnerID, "restore must run while the peer is still a learner")
	require.Equal(t, []uint64{id}, status.Voters, "snapshot import cannot count an unready learner as a voter")
	submitEventually(t, seed, "q", "during-snapshot-tail", `["base-one","base-two","before-activation-tail","during-snapshot-tail"]`)
	unblock()
	require.Eventually(t, func() bool {
		g := learner.group(1)
		if g == nil {
			return false
		}
		status := g.Status()
		return contains(status.Voters, learnerID) && len(status.Learners) == 0 && status.Applied >= seed.group(1).Status().Commit
	}, 20*time.Second, 20*time.Millisecond, "learner must apply the live log suffix before completing promotion")
	ctx, cancel = context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()
	snapshot, err := learner.group(1).MachineSnapshot(ctx)
	require.NoError(t, err)
	require.JSONEq(t, `["base-one","base-two","before-activation-tail","during-snapshot-tail"]`, string(snapshot))
}

func TestLocalActivationCacheFailureKeepsSingletonUsableAndRetries(t *testing.T) {
	cfg := localTestConfig(t)
	node, err := OpenLocal(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, node.Close()) })
	submitEventually(t, node, "q", "before", `["before"]`)
	// Serialize with the install path so the directory reliably blocks the
	// durable cache rename, including concurrent maintenance reports.
	func() {
		node.viewMu.Lock()
		defer node.viewMu.Unlock()
		err := os.Remove(node.path("catalog-cache"))
		require.True(t, err == nil || os.IsNotExist(err))
		require.NoError(t, os.Mkdir(node.path("catalog-cache"), 0700))
	}()
	socket, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	endpoint := socket.Addr().String()
	require.NoError(t, socket.Close())
	activation := Config{Peer: Peer{Address: endpoint}, Token: base64.RawURLEncoding.EncodeToString(make([]byte, 32)), AllowInsecure: true}
	require.ErrorContains(t, node.ActivatePeers(activation), "committed peer endpoint could not be installed")
	_, id := node.Identity()
	require.Equal(t, endpoint, address(node.catalog.view(), id), "activation command must have reached its committed boundary")
	require.Empty(t, address(node.view(), id), "blocked cache cannot publish the endpoint")
	require.Nil(t, node.transport.Load(), "failed publication must leave enrollment closed")
	require.NoError(t, node.Ready())
	submitEventually(t, node, "q", "after-cache-failure", `["before","after-cache-failure"]`)
	require.NoError(t, os.Remove(node.path("catalog-cache")))
	require.NoError(t, node.ActivatePeers(activation))
	require.Equal(t, endpoint, address(node.view(), id))
	require.True(t, node.transport.Load().ready.Load())
}
