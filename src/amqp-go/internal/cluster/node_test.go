package cluster

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterraft"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
)

func sortMembers(ids []uint64) []uint64 { slices.Sort(ids); return ids }

func TestOpenRejectsReservedRaftIDBeforeEnrollment(t *testing.T) {
	cfg := Config{
		ClusterID: "reserved-peer-id", Directory: t.TempDir(), Shards: 1,
		Peer: Peer{raft.LocalAppendThread, "127.0.0.1:30002"}, Listen: "127.0.0.1:0",
		Seeds: []Peer{{1, "127.0.0.1:30001"}}, AllowInsecure: true,
		Token:      base64.RawURLEncoding.EncodeToString(make([]byte, 32)),
		NewMachine: func(uint64) clusterraft.Machine { return &testMachine{} },
	}
	node, err := Open(cfg)
	if node != nil {
		t.Cleanup(func() { require.NoError(t, node.Close()) })
	}
	require.ErrorContains(t, err, "valid peer")
	files, err := os.ReadDir(cfg.Directory)
	require.NoError(t, err)
	require.Empty(t, files)
}

func TestFourNodeRebalanceAndReplicaIdentityFence(t *testing.T) {
	token := make([]byte, 32)
	_, err := rand.Read(token)
	require.NoError(t, err)
	peers := make([]Peer, 4)
	sockets := make([]net.Listener, len(peers))
	for i := range peers {
		socket, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		sockets[i] = socket
		peers[i] = Peer{uint64(i + 1), socket.Addr().String()}
	}
	for _, socket := range sockets {
		require.NoError(t, socket.Close())
	}

	dir := t.TempDir()
	nodes := make([]*Node, 4)
	configs := make([]Config, 4)
	for i, peer := range peers {
		configs[i] = Config{
			ClusterID: "runtime-test", Peer: peer, Listen: peer.Address,
			Directory: filepath.Join(dir, fmt.Sprint(peer.ID)),
			Seeds:     peers[:3], Shards: 16,
			Token: base64.RawURLEncoding.EncodeToString(token), AllowInsecure: true,
			NewMachine: func(uint64) clusterraft.Machine { return &testMachine{} },
		}
		if i < 3 {
			require.NoError(t, Initialize(configs[i]))
		}
	}
	for i := range configs {
		n, err := Open(configs[i])
		require.NoError(t, err)
		nodes[i] = n
		t.Cleanup(func() { require.NoError(t, n.Close()) })
	}
	joiner := nodes[3]
	require.Eventually(t, func() bool { return joiner.Ready() == nil }, 20*time.Second, 30*time.Millisecond)
	var local, remote string
	for i := range 1024 {
		q := fmt.Sprintf("queue-%d", i)
		if contains(rank(joiner.view(), groupFor(q, 16)), 4) {
			local = q
		} else {
			remote = q
		}
	}
	require.NotEmpty(t, local)
	require.NotEmpty(t, remote)
	for _, q := range []string{local, remote} {
		id := groupFor(q, 16)
		require.Eventually(t, func() bool {
			v := joiner.view()
			p := v.Placements[id-1]
			return p.Index > 0 && slices.Equal(p.Voters, sortMembers(rank(v, id))) && len(p.Learners) == 0
		}, 30*time.Second, 40*time.Millisecond)
	}
	require.Nil(t, joiner.group(groupFor(remote, 16)))
	for _, q := range []string{local, remote} {
		submitEventually(t, joiner, q, "one", `["one"]`)
		submitEventually(t, nodes[0], q, "two", `["one","two"]`)
	}

	require.NoError(t, joiner.Close())
	disk := configs[3].Directory
	require.NoError(t, os.RemoveAll(filepath.Join(disk, journalDirectory)))
	_, err = Open(configs[3])
	require.ErrorContains(t, err, "persistent raft journal")

	require.NoError(t, os.RemoveAll(disk))
	replacement, err := Open(configs[3])
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, replacement.Close()) })
	require.Eventually(t, func() bool { return len(replacement.view().Nodes) == 4 }, 10*time.Second, 30*time.Millisecond)
	require.Error(t, replacement.Ready())
	require.Nil(t, replacement.group(groupFor(local, 16)))

	require.NoError(t, nodes[0].Close())
	require.NoError(t, os.RemoveAll(configs[0].Directory))
	_, err = Open(configs[0])
	require.ErrorContains(t, err, "missing persistent identity")
	require.ErrorContains(t, Initialize(configs[0]), "cluster is running")
}

func submitEventually(t *testing.T, n *Node, q, body, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		result, err := n.Submit(ctx, q, []byte(body))
		cancel()
		if (errors.Is(err, ErrUnavailable) || errors.Is(err, ErrUncertain)) && time.Now().Before(deadline) {
			time.Sleep(40 * time.Millisecond)
			continue
		}
		require.NoError(t, err)
		require.JSONEq(t, want, string(result))
		return
	}
}

type testMachine struct{ values []string }

func (m *testMachine) Apply(data []byte) []byte {
	if !slices.Contains(m.values, string(data)) {
		m.values = append(m.values, string(data))
	}
	out, _ := m.Snapshot()
	return out
}
func (m *testMachine) Snapshot() ([]byte, error) { return json.Marshal(m.values) }
func (m *testMachine) Restore(data []byte) error { return json.Unmarshal(data, &m.values) }

func TestOneVoterGrowsToSevenViaSurvivingPeer(t *testing.T) {
	addrs := make([]string, 7)
	listeners := make([]net.Listener, len(addrs))
	for i := range addrs {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		t.Cleanup(func() { _ = l.Close() })
		listeners[i] = l
		addrs[i] = l.Addr().String()
	}
	for _, l := range listeners {
		require.NoError(t, l.Close())
	}
	token := make([]byte, 32)
	_, err := rand.Read(token)
	require.NoError(t, err)
	nodes := make([]*Node, len(addrs))
	configs := make([]Config, len(addrs))
	for i, addr := range addrs {
		configs[i] = Config{
			ClusterID: "single-to-seven", Shards: defaultShards, Token: base64.RawURLEncoding.EncodeToString(token),
			Peer: Peer{uint64(i + 1), addr}, Listen: addr, Directory: t.TempDir(),
			Seeds: []Peer{{1, addrs[0]}}, AllowInsecure: true,
			NewMachine: func(uint64) clusterraft.Machine { return &testMachine{} },
		}
		if i >= 4 {
			configs[i].Discovery = []string{addrs[1]}
		}
	}
	require.NoError(t, Initialize(configs[0]))
	start := func(i int) {
		node, err := Open(configs[i])
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, node.Close()) })
		nodes[i] = node
	}
	start(0)
	const queue = "durable-before-expansion"
	owner := groupFor(queue, configs[0].Shards)
	submitEventually(t, nodes[0], queue, "one", `["one"]`)
	for i := 1; i < 3; i++ {
		start(i)
		require.Eventually(t, func() bool {
			v := nodes[i].view()
			return len(v.Catalog.Voters) == i+1 && len(groupPlacement(v, owner).Voters) == i+1
		}, 18*time.Second, 50*time.Millisecond)
	}
	start(3)
	require.Eventually(t, func() bool {
		return nodes[3].Ready() == nil && len(nodes[3].view().Catalog.Voters) == 3
	}, 15*time.Second, 50*time.Millisecond)
	require.NoError(t, nodes[0].Close())
	for i := 4; i < 7; i++ {
		start(i)
	}
	require.Eventually(t, func() bool {
		for _, n := range nodes[1:] {
			if n.Ready() != nil || len(n.view().Nodes) != 7 {
				return false
			}
		}
		return true
	}, 28*time.Second, 50*time.Millisecond)
	submitEventually(t, nodes[6], queue, "seven", `["one","seven"]`)
	// The only original seed is offline, while two subsequently enrolled
	// catalog voters still serve requests. A blank replacement directory must
	// not let that original voter reinitialize under the same identity.
	replacement := configs[0]
	replacement.Directory = t.TempDir()
	replacement.Discovery = []string{addrs[1]}
	require.ErrorContains(t, Initialize(replacement), "cluster is running")
	start(0)

	require.Eventually(t, func() bool {
		v := nodes[6].view()
		leaders := make(map[uint64]bool)
		for _, p := range v.Placements {
			desired := rank(v, p.Group)
			preferred := desired[0]
			if !slices.Equal(p.Voters, sortMembers(desired)) || len(p.Learners) > 0 {
				return false
			}
			g := nodes[preferred-1].group(p.Group)
			if g == nil || g.Status().Leader != preferred {
				return false
			}
			for _, node := range nodes {
				if !contains(desired, node.cfg.Peer.ID) {
					if node.group(p.Group) != nil {
						return false
					}
					if _, err := os.Stat(node.store(p.Group)); !errors.Is(err, os.ErrNotExist) {
						return false
					}
				}
			}
			leaders[preferred] = true
		}
		return len(leaders) == len(nodes)
	}, 40*time.Second, 50*time.Millisecond)
	submitEventually(t, nodes[0], queue, "returned", `["one","seven","returned"]`)
}
