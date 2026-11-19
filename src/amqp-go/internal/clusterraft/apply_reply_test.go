package clusterraft

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestApplyCommittedSkipsFollowerReplyAndReplaysAfterTransfer(t *testing.T) {
	ctx, cluster, machines, claim := newReplyTestCluster(t)
	leader, follower := cluster.groups[1], cluster.groups[2]
	original := submitReplyTestCommand(t, ctx, cluster, leader, claim)
	decoded, err := clusterqueue.DecodeResponse(original)
	require.NoError(t, err)
	require.NoError(t, decoded.Err())
	require.Len(t, decoded.Deliveries, 2)
	require.Equal(t, []byte("first body"), decoded.Deliveries[0].Body)
	require.Equal(t, []byte("second body"), decoded.Deliveries[1].Body)
	before := assertReplyTestSnapshots(t, ctx, cluster, leader)

	observed := observeReplyTestMachine(follower, machines[2])
	require.Zero(t, observed.legacy, "follower encoded the committed claim through legacy Apply")
	require.Equal(t, 1, observed.skipped, "follower must apply the claim without a local reply")
	require.Zero(t, observed.replies)
	require.Equal(t, 1, observeReplyTestMachine(leader, machines[1]).replies, "origin must encode the local reply")

	require.NoError(t, leader.TransferLeadership(ctx, 2))
	cluster.drain()
	require.Equal(t, uint64(2), follower.Leader(), "retry must reach the previous follower as leader")
	retry := submitReplyTestCommand(t, ctx, cluster, follower, claim)
	require.Equal(t, original, retry, "retry must return the same attempt IDs and body bytes")
	require.Equal(t, before, assertReplyTestSnapshots(t, ctx, cluster, follower), "retry cannot claim the bodies again")
	require.Equal(t, 1, observeReplyTestMachine(follower, machines[2]).replies)
}

func TestApplyCommittedSkipsCanceledOriginReplyUntilRetry(t *testing.T) {
	ctx, cluster, machines, claim := newReplyTestCluster(t)
	leader := cluster.groups[1]
	data, err := clusterqueue.EncodeCommand(claim)
	require.NoError(t, err)
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := leader.Propose(readCtx, data)
		result <- err
	}()

	priorCommit := leader.Status().Commit
	var held []*pb.Message
	var index uint64
	for index == 0 {
		select {
		case message := <-cluster.out:
			held = append(held, message)
			if message.GetType() != pb.MsgApp || message.GetFrom() != 1 {
				continue
			}
			for _, entry := range message.GetEntries() {
				if len(entry.GetData()) >= len(key{}) && bytes.Equal(entry.GetData()[len(key{}):], data) {
					index = entry.GetIndex()
				}
			}
		case <-ctx.Done():
			t.Fatal("did not observe the claim enter the proposal log")
		}
	}
	require.Greater(t, index, priorCommit)
	require.Equal(t, priorCommit, leader.Status().Commit, "no follower may acknowledge before cancellation")
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-ctx.Done():
		t.Fatal("proposal waiter did not exit on cancellation")
	}
	leader.mu.Lock()
	pending := len(leader.pending)
	leader.mu.Unlock()
	require.Zero(t, pending, "local waiter must be gone before the held replication is delivered")
	for _, message := range held {
		cluster.deliver(message)
	}
	cluster.drain()
	require.Equal(t, index, leader.Status().Applied, "canceling a caller cannot discard an accepted claim")
	before := assertReplyTestSnapshots(t, ctx, cluster, leader)
	observed := observeReplyTestMachine(leader, machines[1])
	require.Zero(t, observed.legacy, "canceled origin encoded the committed claim through legacy Apply")
	require.Equal(t, 1, observed.skipped)
	require.Zero(t, observed.replies)

	retry := submitReplyTestCommand(t, ctx, cluster, leader, claim)
	decoded, err := clusterqueue.DecodeResponse(retry)
	require.NoError(t, err)
	require.NoError(t, decoded.Err())
	require.Len(t, decoded.Deliveries, 2)
	require.Equal(t, clusterqueue.AttemptID(1), decoded.Deliveries[0].Attempt)
	require.Equal(t, clusterqueue.AttemptID(2), decoded.Deliveries[1].Attempt)
	require.Equal(t, []byte("first body"), decoded.Deliveries[0].Body)
	require.Equal(t, []byte("second body"), decoded.Deliveries[1].Body)
	require.Equal(t, before, assertReplyTestSnapshots(t, ctx, cluster, leader), "retry cannot repeat the canceled claim's state transition")
}

type replyObservation struct{ legacy, skipped, replies int }

type replyTestMachine struct {
	clusterqueue.Machine
	observed replyObservation
}

func (m *replyTestMachine) Apply(data []byte) []byte {
	if command, err := clusterqueue.DecodeCommand(data); err == nil && command.Kind == clusterqueue.ClaimBatch {
		m.observed.legacy++
	}
	return m.Machine.Apply(data)
}

func (m *replyTestMachine) observeCommitted(data []byte, wantReply bool) {
	if command, err := clusterqueue.DecodeCommand(data); err == nil && command.Kind == clusterqueue.ClaimBatch {
		if wantReply {
			m.observed.replies++
		} else {
			m.observed.skipped++
		}
	}
}

func (m *replyTestMachine) ApplyCommitted(data []byte, wantReply bool) ([]byte, error) {
	m.observeCommitted(data, wantReply)
	return m.Machine.ApplyCommitted(data, wantReply)
}

func (m *replyTestMachine) ApplyJournaledReply(data []byte, wantReply bool, source io.ReaderAt) ([]byte, func() ([]byte, error), error) {
	m.observeCommitted(data, wantReply)
	return m.Machine.ApplyJournaledReply(data, wantReply, source)
}

func observeReplyTestMachine(group *Group, machine *replyTestMachine) replyObservation {
	group.mu.Lock()
	defer group.mu.Unlock()
	return machine.observed
}

func newReplyTestCluster(t *testing.T) (context.Context, *testCluster, map[uint64]*replyTestMachine, clusterqueue.QueueCommand) {
	t.Helper()
	cluster := newTestCluster(t)
	peers := []uint64{1, 2, 3}
	machines := make(map[uint64]*replyTestMachine, len(peers))
	for _, id := range peers {
		machine := &replyTestMachine{}
		group, err := Open(Config{
			ID: id, GroupID: 1, Dir: filepath.Join(cluster.dir, string(rune('a'+id))), Peers: peers, Machine: machine,
			TickInterval: -1, Send: func(message *pb.Message) error { cluster.out <- message; return nil },
		})
		require.NoError(t, err)
		machines[id], cluster.groups[id] = machine, group
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	t.Cleanup(cancel)
	leader := cluster.groups[1]
	require.NoError(t, leader.Campaign(ctx))
	cluster.drain()
	require.Equal(t, uint64(1), leader.Leader())
	const queue = "reply-retention"
	declare := submitReplyTestCommand(t, ctx, cluster, leader, clusterqueue.QueueCommand{Kind: clusterqueue.DeclareQueue, Queue: queue})
	declared, err := clusterqueue.DecodeResponse(declare)
	require.NoError(t, err)
	require.NoError(t, declared.Err())
	session := clusterqueue.QueueCommand{Queue: queue, Incarnation: declared.Queue.Incarnation, Session: 1, Owner: "reader"}
	open := session
	open.Kind, open.Prefetch, open.Consumer, open.Deadline = clusterqueue.OpenQueueSession, 2, true, time.Now().Add(time.Second).UnixNano()
	_ = submitReplyTestCommand(t, ctx, cluster, leader, open)
	for _, body := range []string{"first body", "second body"} {
		_ = submitReplyTestCommand(t, ctx, cluster, leader, clusterqueue.QueueCommand{
			Kind: clusterqueue.PublishMessage, Queue: queue, Body: []byte(body),
		})
	}
	session.Kind, session.Sequence, session.ClaimLimit, session.ClaimBytes = clusterqueue.ClaimBatch, 1, 2, 1<<20
	return ctx, cluster, machines, session
}

func submitReplyTestCommand(t *testing.T, ctx context.Context, cluster *testCluster, group *Group, command clusterqueue.QueueCommand) []byte {
	t.Helper()
	data, err := clusterqueue.EncodeCommand(command)
	require.NoError(t, err)
	done := make(chan response, 1)
	go func() {
		body, err := group.Propose(ctx, data)
		done <- response{body: body, err: err}
	}()
	result := cluster.await(done)
	require.NoError(t, result.err)
	decoded, err := clusterqueue.DecodeResponse(result.body)
	require.NoError(t, err)
	require.NoError(t, decoded.Err())
	return result.body
}

func assertReplyTestSnapshots(t *testing.T, ctx context.Context, cluster *testCluster, leader *Group) []byte {
	t.Helper()
	// Two ticks guarantee a heartbeat that propagates the committed index
	// to every follower, including a follower outside the first quorum.
	for range 2 {
		require.NoError(t, leader.Tick(ctx))
		cluster.drain()
	}
	commit := leader.Status().Applied
	want, err := leader.MachineSnapshot(ctx)
	require.NoError(t, err)
	for id, group := range cluster.groups {
		require.Equal(t, commit, group.Status().Applied, "replica %d must apply through the same index", id)
		got, err := group.MachineSnapshot(ctx)
		require.NoError(t, err)
		require.Equal(t, want, got, "replica %d must keep exact queue state", id)
	}
	return want
}
