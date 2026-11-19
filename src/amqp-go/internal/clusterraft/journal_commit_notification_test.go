package clusterraft

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
)

func TestJournalMatchedFollowerReceivesCommitWithoutAnotherAppendFence(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cluster := newTestCluster(t)
	gate := &journalGroupSyncGate{started: make(chan struct{}, 1), release: make(chan struct{})}
	defer close(gate.release)
	followerFile := &journalCommitFenceFile{gate: gate}
	leader := openJournalQuorumMember(t, cluster, openTestJournalGroupNode(t, t.TempDir(), nil), 1, []uint64{1, 2})
	follower := openJournalQuorumMember(t, cluster, openTestJournalGroupNode(t, t.TempDir(), func(file *os.File) journalFile {
		followerFile.File = file
		gate.File = file
		return followerFile
	}), 2, []uint64{1, 2})
	require.NoError(t, leader.Campaign(ctx))
	cluster.drain()
	require.Equal(t, uint64(1), leader.Leader())
	commit := leader.Status().Commit + 1
	writes := followerFile.syncs.Load()
	result := make(chan response, 1)
	go func() {
		body, err := leader.Propose(ctx, []byte("new durable entry"))
		result <- response{body: body, err: err}
	}()
	appendMessage := awaitJournalQuorumMessage(t, ctx, cluster.out, result, func(message *pb.Message) bool {
		return message.GetFrom() == 1 && message.GetTo() == 2 && message.GetType() == pb.MsgApp &&
			len(message.GetEntries()) != 0 && message.GetEntries()[0].GetIndex() == commit
	})
	require.NoError(t, waitTestLocalWrites(ctx, leader))
	require.NoError(t, follower.Step(ctx, appendMessage))
	ack := awaitJournalQuorumMessage(t, ctx, cluster.out, result, func(message *pb.Message) bool {
		return message.GetFrom() == 2 && message.GetTo() == 1 && message.GetType() == pb.MsgAppResp &&
			!message.GetReject() && message.GetIndex() == commit
	})
	require.Equal(t, writes+1, followerFile.syncs.Load(), "the entry reply must still follow a full fence")
	require.NoError(t, leader.Step(ctx, ack))
	applied := awaitTestReadResult(t, ctx, result)
	require.NoError(t, applied.err)
	require.Equal(t, []byte("applied new durable entry"), applied.body)

	notice := awaitJournalQuorumMessage(t, ctx, cluster.out, nil, func(message *pb.Message) bool {
		return message.GetFrom() == 1 && message.GetTo() == 2 && message.GetCommit() == commit
	})
	require.Equal(t, pb.MsgHeartbeat, notice.GetType(), "a matched peer needs commit progress rather than another append")
	require.Empty(t, notice.GetContext(), "the notification must not resolve a ReadIndex")
	require.NoError(t, follower.Step(ctx, notice))
	require.Equal(t, commit, follower.Status().Applied)
	require.Equal(t, writes+1, followerFile.syncs.Load(), "commit-only progress can share a later durable fence")
	func() {
		follower.mu.Lock()
		defer follower.mu.Unlock()
		store := follower.store
		require.Equal(t, commit-1, store.state.hard.GetCommit())
		require.Equal(t, commit, store.pendingCommit)
	}()

	// A crash can lose the scalar before the next fence. The acknowledged
	// log entry is still present, and renewed commit information reapplies it.
	recoveredNode := openTestJournalGroupNode(t, captureJournalQuorumPrefix(t, followerFile), nil)
	recovered := openJournalQuorumMember(t, newTestCluster(t), recoveredNode, 2, nil)
	require.Equal(t, commit-1, recovered.Status().Applied)
	require.NoError(t, recovered.Step(ctx, notice))
	body, err := recovered.MachineSnapshot(ctx)
	require.NoError(t, err)
	require.JSONEq(t, `["new durable entry"]`, string(body))

	// Lose the route before the heartbeat reply arrives. It must trigger a
	// real append probe, whose response still waits on the follower's fence.
	heartbeatReply := awaitJournalQuorumMessage(t, ctx, cluster.out, nil, func(message *pb.Message) bool {
		return message.GetFrom() == 2 && message.GetTo() == 1 && message.GetType() == pb.MsgHeartbeatResp
	})
	require.NoError(t, leader.change(ctx, func() error {
		leader.node.ReportUnreachable(2)
		return nil
	}))
	require.NoError(t, leader.Step(ctx, heartbeatReply))
	probe := awaitJournalQuorumMessage(t, ctx, cluster.out, nil, func(message *pb.Message) bool {
		return message.GetFrom() == 1 && message.GetTo() == 2 && message.GetType() == pb.MsgApp
	})
	require.Empty(t, probe.GetEntries())
	gate.enabled.Store(true)
	require.NoError(t, follower.Step(ctx, probe))
	reply := awaitJournalCommitResponseFence(t, ctx, gate, cluster.out)
	require.Equal(t, pb.MsgAppResp, reply.GetType())
	require.NoError(t, leader.Step(ctx, reply))
	leader.mu.Lock()
	defer leader.mu.Unlock()
	leader.node.WithProgress(func(id uint64, _ raft.ProgressType, progress tracker.Progress) {
		if id == 2 {
			require.Equal(t, tracker.StateReplicate, progress.State)
		}
	})
}
