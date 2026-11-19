package cluster

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterwire"
	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type raftSendBatchRequest struct {
	group    uint64
	messages []*pb.Message
	release  chan struct{}
}

type raftSendBatchPeer struct {
	routingPeer
	ordinary, snapshots chan raftSendBatchRequest
}

func (p raftSendBatchPeer) Raft(ctx context.Context, group uint64, messages []*pb.Message) error {
	received := p.ordinary
	if messages[0].GetType() == pb.MsgSnap {
		received = p.snapshots
	}
	request := raftSendBatchRequest{group: group, messages: messages, release: make(chan struct{})}
	select {
	case received <- request:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-request.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestRaftSendBatchesAdjacentQueuedPackets(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	receiver := raftSendBatchPeer{
		ordinary: make(chan raftSendBatchRequest, 1), snapshots: make(chan raftSendBatchRequest, 1),
	}
	cfg := clusterwire.Config{
		Listen: "127.0.0.1:0", ClusterID: "raft-send-batch", AllowInsecure: true,
		Token: base64.RawURLEncoding.EncodeToString(make([]byte, 32)),
	}
	peer, err := clusterwire.NewServer(cfg, receiver)
	require.NoError(t, err)
	require.NoError(t, peer.Start())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, peer.Close(ctx))
	})
	client, err := clusterwire.NewClient(cfg)
	require.NoError(t, err)
	nodeCtx, stopNode := context.WithCancel(ctx)
	const first, other, target = 1, 3, 2
	node := &Node{client: client, ctx: nodeCtx, cancel: stopNode, lanes: make(map[uint64][3]chan packet)}
	node.current.Store(&view{Nodes: []Peer{{1, "127.0.0.1:1"}, {target, peer.Addr().String()}}})
	t.Cleanup(func() { require.NoError(t, node.Close()) })

	var charged int64
	enqueue := func(group uint64, message *pb.Message) {
		t.Helper()
		require.NoError(t, node.send(group, message))
		charged += int64(proto.Size(message))
	}
	expectCharges := func() {
		t.Helper()
		require.Equal(t, charged, node.sendBytes.Load(), "only completed requests release their outgoing packet bytes")
	}
	finish := func(request raftSendBatchRequest) {
		for _, message := range request.messages {
			charged -= int64(proto.Size(message))
		}
		close(request.release)
	}

	blocker := raftSendTestAppend(1)
	enqueue(first, blocker)
	blocked := awaitRaftSendBatch(t, ctx, receiver.ordinary, first, blocker)
	a, b, c, d, e := raftSendTestAppend(2), raftSendTestAppend(3), raftSendTestAppend(1),
		raftSendTestAppend(4), raftSendTestAppend(5)
	enqueue(first, a)
	enqueue(first, b)
	enqueue(other, c)
	enqueue(first, d)
	enqueue(first, e)

	snap1, snap2, snap3 := raftSendTestSnapshot(10), raftSendTestSnapshot(11), raftSendTestSnapshot(12)
	enqueue(first, snap1)
	blockedSnapshot := awaitRaftSendBatch(t, ctx, receiver.snapshots, first, snap1)
	enqueue(first, snap2)
	enqueue(first, snap3)

	node.sendMu.Lock()
	lanes := node.lanes[target]
	ordinaryQueued, snapshotsQueued := len(lanes[first%2]), len(lanes[2])
	node.sendMu.Unlock()
	require.Equal(t, 5, ordinaryQueued, "the entire ordinary burst must be ready before releasing the sender")
	require.Equal(t, 2, snapshotsQueued, "snapshots must already be queued on their isolated lane")
	expectCharges()

	finish(blocked)
	firstBatch := awaitRaftSendBatch(t, ctx, receiver.ordinary, first, a, b)
	expectCharges()
	finish(firstBatch)
	between := awaitRaftSendBatch(t, ctx, receiver.ordinary, other, c)
	expectCharges()
	finish(between)
	lastBatch := awaitRaftSendBatch(t, ctx, receiver.ordinary, first, d, e)
	expectCharges()

	// Keep the last ordinary request active while the snapshot lane advances,
	// so only the snapshot bytes can change at each observation.
	finish(blockedSnapshot)
	secondSnapshot := awaitRaftSendBatch(t, ctx, receiver.snapshots, first, snap2)
	expectCharges()
	finish(secondSnapshot)
	thirdSnapshot := awaitRaftSendBatch(t, ctx, receiver.snapshots, first, snap3)
	expectCharges()

	finish(lastBatch)
	finish(thirdSnapshot)
	require.NoError(t, node.Close())
	require.Zero(t, charged)
	expectCharges()
}

func awaitRaftSendBatch(t *testing.T, ctx context.Context, requests <-chan raftSendBatchRequest, group uint64, messages ...*pb.Message) raftSendBatchRequest {
	t.Helper()
	select {
	case request := <-requests:
		require.Equal(t, group, request.group, "the send lane reordered or merged distinct groups")
		require.Len(t, request.messages, len(messages), "adjacent same-shard Raft packets did not share one signed peer RPC")
		for i, message := range messages {
			require.True(t, proto.Equal(message, request.messages[i]), "Raft packet %d changed or moved", i)
		}
		return request
	case <-ctx.Done():
		t.Fatalf("peer did not receive the expected Raft RPC for group %d", group)
		return raftSendBatchRequest{}
	}
}

func raftSendTestAppend(previous uint64) *pb.Message {
	logTerm := uint64(2)
	if previous == 1 {
		logTerm = 1
	}
	return &pb.Message{
		Type: pb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(2)),
		Index: new(previous), LogTerm: new(logTerm), Commit: new(previous),
		Entries: []*pb.Entry{{Type: pb.EntryNormal.Enum(), Index: new(previous + 1), Term: new(uint64(2))}},
	}
}

func raftSendTestSnapshot(index uint64) *pb.Message {
	return &pb.Message{
		Type: pb.MsgSnap.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(2)),
		Snapshot: &pb.Snapshot{
			Metadata: &pb.SnapshotMetadata{
				Index: new(index), Term: new(uint64(2)), ConfState: &pb.ConfState{Voters: []uint64{1, 2}},
			},
			Data: []byte("snapshot state"),
		},
	}
}
