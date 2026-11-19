package clusterraft

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestJournalDeferredCommitQuorumCrashRecoversConfirmedCommandBeforeRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cluster := newTestCluster(t)
	files := make(map[uint64]*journalCommitFenceFile)
	for _, id := range []uint64{1, 2, 3} {
		file := &journalCommitFenceFile{}
		files[id] = file
		node := openTestJournalGroupNode(t, t.TempDir(), func(underlying *os.File) journalFile {
			file.File = underlying
			return file
		})
		openJournalQuorumMember(t, cluster, node, id, []uint64{1, 2, 3})
	}
	leader := cluster.groups[1]
	require.NoError(t, leader.Campaign(ctx))
	cluster.drain()
	require.Equal(t, uint64(1), leader.Leader())
	previous := leader.Status().Commit
	for _, group := range cluster.groups {
		require.Equal(t, previous, group.Status().Applied)
	}

	const command = "confirmed publish"
	confirmed := previous + 1
	before := files[1].syncs.Load()
	proposal := make(chan response, 1)
	go func() {
		body, err := leader.Propose(ctx, []byte(command))
		proposal <- response{body: body, err: err}
	}()
	// Give only voter 2 the new entry. Voter 3 and every later commit
	// notification remain disconnected until after the crash images exist.
	appendMessage := awaitJournalQuorumMessage(t, ctx, cluster.out, proposal, func(message *pb.Message) bool {
		if message.GetFrom() != 1 || message.GetTo() != 2 || message.GetType() != pb.MsgApp {
			return false
		}
		for _, entry := range message.GetEntries() {
			if entry.GetIndex() == confirmed && len(entry.GetData()) >= len(key{}) &&
				bytes.Equal(entry.GetData()[len(key{}):], []byte(command)) {
				return true
			}
		}
		return false
	})
	require.Equal(t, previous, appendMessage.GetCommit())
	// Finish the leader's entry write before releasing the follower's ack.
	// This prevents an exclusive waiter from flushing the later commit debt.
	require.NoError(t, waitTestLocalWrites(ctx, leader))
	require.Equal(t, int64(1), files[1].syncs.Load()-before)
	select {
	case result := <-proposal:
		t.Fatalf("proposal completed without a durable quorum: %+v", result)
	default:
	}
	require.NoError(t, cluster.groups[2].Step(ctx, appendMessage))
	require.NoError(t, waitTestLocalWrites(ctx, cluster.groups[2]))
	ack := awaitJournalQuorumMessage(t, ctx, cluster.out, proposal, func(message *pb.Message) bool {
		return message.GetFrom() == 2 && message.GetTo() == 1 && message.GetType() == pb.MsgAppResp &&
			!message.GetReject() && message.GetIndex() == confirmed
	})
	require.Equal(t, previous, cluster.groups[2].Status().Commit)
	require.NoError(t, leader.Step(ctx, ack))
	result := awaitTestReadResult(t, ctx, proposal)
	require.NoError(t, result.err)
	require.Equal(t, []byte("applied "+command), result.body)
	require.Equal(t, confirmed, leader.Status().Applied)
	require.Equal(t, int64(1), files[1].syncs.Load()-before, "the acknowledged command must still have no standalone commit sync")
	func() {
		leader.mu.Lock()
		defer leader.mu.Unlock()
		store := leader.store
		require.Equal(t, previous, store.state.hard.GetCommit(), "the crash boundary must precede commit-floor persistence")
		require.Equal(t, confirmed, store.pendingCommit, "the acknowledged commit must still be deferred")
	}()

	// Copy each observed successful-sync prefix before any clean close can
	// persist the leader's debt. The original transport is never drained again.
	images := make(map[uint64]string)
	for _, id := range []uint64{1, 2, 3} {
		images[id] = captureJournalQuorumPrefix(t, files[id])
	}
	for _, group := range cluster.groups {
		require.NoError(t, group.Close())
	}

	restarted := newTestCluster(t)
	for _, id := range []uint64{1, 2, 3} {
		node := openTestJournalGroupNode(t, images[id], nil)
		group := openJournalQuorumMember(t, restarted, node, id, nil)
		require.Equal(t, previous, group.Status().Commit, "restart must use the copied prefix, not the clean-close commit marker")
		require.Equal(t, previous, group.Status().Applied)
		last, err := group.store.LastIndex()
		require.NoError(t, err)
		if id == 3 {
			require.Equal(t, previous, last, "the third voter never received the confirmed command")
		} else {
			require.Equal(t, confirmed, last, "both members of the acknowledging quorum must retain the command on disk")
		}
	}

	// Elect the former follower from the lower recovered commit floor. Hold
	// its first new-term replication so leadership alone cannot authorize Read.
	replacement := restarted.groups[2]
	runs := &atomic.Int32{}
	read := awaitTestReadResult(t, ctx, startTestRead(ctx, replacement, runs))
	require.ErrorIs(t, read.err, ErrNotLeader)
	require.NoError(t, replacement.Campaign(ctx))
	for replacement.Leader() != 2 {
		select {
		case message := <-restarted.out:
			restarted.deliver(message)
		case <-ctx.Done():
			t.Fatal("the durable follower did not become the replacement leader")
		}
	}
	require.Equal(t, previous, replacement.Status().Applied)
	read = awaitTestReadResult(t, ctx, startTestRead(ctx, replacement, runs))
	require.ErrorIs(t, read.err, ErrBusy, "the new leader must apply a current-term entry before reading")
	require.Zero(t, runs.Load(), "neither restart nor election alone may invoke the read callback")
	restarted.drain()
	require.GreaterOrEqual(t, replacement.Status().Applied, confirmed)

	// A recovered current-term leader still needs this read's own quorum
	// response. Observing and holding that response establishes the fence.
	reading := startTestRead(ctx, replacement, runs)
	heartbeat := awaitJournalQuorumMessage(t, ctx, restarted.out, reading, func(message *pb.Message) bool {
		return message.GetFrom() == 2 && message.GetTo() == 1 && message.GetType() == pb.MsgHeartbeat && len(message.GetContext()) != 0
	})
	requireReadStillPending(t, reading, runs)
	require.NoError(t, restarted.groups[1].Step(ctx, heartbeat))
	heartbeatReply := awaitJournalQuorumMessage(t, ctx, restarted.out, reading, func(message *pb.Message) bool {
		return message.GetFrom() == 1 && message.GetTo() == 2 && message.GetType() == pb.MsgHeartbeatResp &&
			bytes.Equal(message.GetContext(), heartbeat.GetContext())
	})
	requireReadStillPending(t, reading, runs)
	require.NoError(t, replacement.Step(ctx, heartbeatReply))
	read = awaitTestReadResult(t, ctx, reading)
	require.NoError(t, read.err)
	require.JSONEq(t, `["confirmed publish"]`, string(read.body), "a quorum read after crash recovery must include the previously confirmed command exactly once")
	require.Equal(t, int32(1), runs.Load())
}

func openJournalQuorumMember(t *testing.T, cluster *testCluster, node *journalGroupNode, id uint64, peers []uint64) *Group {
	t.Helper()
	group := openTestJournalGroup(t, node, Config{
		ID: id, GroupID: 11, Dir: filepath.Join(node.dir, "group-11"), Peers: peers,
		Machine: &testMachine{}, TickInterval: -1, DeferJournalCommit: true,
		Send: func(message *pb.Message) error { cluster.out <- message; return nil },
	})
	cluster.groups[id] = group
	return group
}

func awaitJournalQuorumMessage(t *testing.T, ctx context.Context, messages <-chan *pb.Message, result <-chan response, matches func(*pb.Message) bool) *pb.Message {
	t.Helper()
	for {
		select {
		case message := <-messages:
			if matches(message) {
				return message
			}
		case early := <-result:
			t.Fatalf("operation completed before the held quorum message: %+v", early)
		case <-ctx.Done():
			t.Fatal("the expected quorum message was not emitted")
		}
	}
}

func captureJournalQuorumPrefix(t *testing.T, file *journalCommitFenceFile) string {
	t.Helper()
	dir := t.TempDir()
	end := file.end.Load()
	journal, err := os.ReadFile(file.Name())
	require.NoError(t, err)
	require.Positive(t, end)
	require.LessOrEqual(t, end, int64(len(journal)))
	require.NoError(t, os.WriteFile(filepath.Join(dir, journalActiveName), journal[:end], 0600))
	snapshots, err := os.ReadDir(filepath.Dir(file.Name()))
	require.NoError(t, err)
	for _, snapshot := range snapshots {
		if !strings.HasPrefix(snapshot.Name(), journalSnapshotPrefix) {
			continue
		}
		data, err := os.ReadFile(filepath.Join(filepath.Dir(file.Name()), snapshot.Name()))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, snapshot.Name()), data, 0600))
	}
	return dir
}
