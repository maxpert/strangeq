package cluster

import (
	"context"
	"encoding/base64"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterraft"
	"github.com/maxpert/amqp-go/internal/clusterwire"
	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
)

type raftReceiptMachine struct {
	testMachine
	block   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func (m *raftReceiptMachine) Snapshot() ([]byte, error) {
	if m.block.Load() {
		close(m.entered)
		<-m.release
	}
	return m.testMachine.Snapshot()
}

type raftReceiptHandler struct {
	*Node
	request chan context.Context
}

func (h *raftReceiptHandler) Raft(ctx context.Context, id uint64, messages []*pb.Message) error {
	h.request <- ctx
	return h.Node.Raft(ctx, id, messages)
}

func TestReplicationReceiptPrecedesExclusiveCheckpoint(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	const shard = 1
	replies := make(chan *pb.Message, 4)
	machine := &raftReceiptMachine{entered: make(chan struct{}), release: make(chan struct{})}
	group, err := clusterraft.Open(clusterraft.Config{
		ID: 2, GroupID: shard, Dir: t.TempDir(), Peers: []uint64{1, 2},
		Machine: machine, TickInterval: -1,
		Send: func(msg *pb.Message) error { replies <- msg; return nil },
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, group.Close()) })
	require.NoError(t, group.Step(ctx, raftReceiptAppend(1, 1)))
	awaitRaftReceiptAppendReply(t, ctx, replies, 2)
	require.Equal(t, uint64(2), group.Status().Applied)

	cfg := clusterwire.Config{
		Listen: "127.0.0.1:0", ClusterID: "raft-receipt-test", AllowInsecure: true,
		Token: base64.RawURLEncoding.EncodeToString(make([]byte, 32)), Timeout: 5 * time.Second,
	}
	peers := []Peer{{1, "127.0.0.1:30101"}, {2, "127.0.0.1:30102"}}
	catalog, err := newCatalog(cfg.ClusterID, shard, peers)
	require.NoError(t, err)
	client, err := clusterwire.NewClient(cfg)
	require.NoError(t, err)
	nodeCtx, stopNode := context.WithCancel(t.Context())
	node := &Node{
		cfg: Config{Peer: peers[1], Shards: shard}, catalog: catalog, client: client,
		ctx: nodeCtx, cancel: stopNode, groups: map[uint64]*clusterraft.Group{shard: group},
	}
	node.current.Store(&catalog.initial)
	require.True(t, node.enrolled(node.view()))
	t.Cleanup(func() { require.NoError(t, node.Close()) })
	handler := &raftReceiptHandler{Node: node, request: make(chan context.Context, 1)}
	node.wire, err = clusterwire.NewServer(cfg, handler)
	require.NoError(t, err)
	require.NoError(t, node.wire.Start())
	address := node.wire.Addr().String()

	machine.block.Store(true)
	var releaseOnce sync.Once
	releaseCheckpoint := func() {
		releaseOnce.Do(func() {
			machine.block.Store(false)
			close(machine.release)
		})
	}
	defer releaseCheckpoint()
	checkpoint := make(chan error, 1)
	go func() { checkpoint <- group.Snapshot(ctx) }()
	select {
	case <-machine.entered:
		// The follower holds its Raft group lock before persisting the checkpoint.
	case <-ctx.Done():
		t.Fatal("the follower did not enter the exclusive checkpoint")
	}

	receipt := make(chan error, 1)
	go func() {
		batch := []*pb.Message{raftReceiptAppend(2, 2), raftReceiptAppend(3, 2)}
		receipt <- client.Raft(ctx, address, shard, batch)
	}()
	var requestCtx context.Context
	select {
	case requestCtx = <-handler.request:
	case <-ctx.Done():
		select {
		case err := <-receipt:
			t.Fatalf("peer request exited before reaching the Raft handler: %v", err)
		default:
			t.Fatal("peer request did not reach the Raft handler")
		}
	}
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	select {
	case err := <-receipt:
		require.NoError(t, err)
	case reply := <-replies:
		t.Fatalf("follower replied before its group could persist the append: %v", reply)
	case <-deadline.C:
		t.Fatal("peer receipt waited for the exclusive Raft checkpoint")
	}
	select {
	case <-requestCtx.Done():
	case <-ctx.Done():
		t.Fatal("the peer receipt did not release the HTTP request context")
	}
	select {
	case reply := <-replies:
		t.Fatalf("receipt was mistaken for durable Raft completion: %v", reply)
	default:
	}

	releaseCheckpoint()
	select {
	case err := <-checkpoint:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("the follower did not complete its unblocked checkpoint")
	}
	awaitRaftReceiptAppendReply(t, ctx, replies, 3)
	awaitRaftReceiptAppendReply(t, ctx, replies, 4)
	require.Equal(t, uint64(4), group.Status().Applied, "the entire accepted batch must survive its HTTP request context in order")
	require.NoError(t, node.Close())
	require.Empty(t, node.receipts.groups, "Node.Close must join and remove the group inbox")
	require.Zero(t, node.receipts.count)
	require.Zero(t, node.receipts.bytes)
}

func raftReceiptAppend(previous, previousTerm uint64) *pb.Message {
	return &pb.Message{
		Type: pb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(2)),
		Index: new(previous), LogTerm: new(previousTerm), Commit: new(previous + 1),
		Entries: []*pb.Entry{{Type: pb.EntryNormal.Enum(), Index: new(previous + 1), Term: new(uint64(2))}},
	}
}

func awaitRaftReceiptAppendReply(t *testing.T, ctx context.Context, replies <-chan *pb.Message, index uint64) {
	t.Helper()
	select {
	case reply := <-replies:
		require.Equal(t, pb.MsgAppResp, reply.GetType())
		require.Equal(t, index, reply.GetIndex())
		require.False(t, reply.GetReject())
	case <-ctx.Done():
		t.Fatalf("the follower did not durably acknowledge append at index %d", index)
	}
}
