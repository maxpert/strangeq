package clusterraft

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestReadIndexNeedsQuorumAndDoesNotPersist(t *testing.T) {
	ctx, cluster, leader := newReadTestCluster(t)
	lastBefore, err := leader.store.LastIndex()
	require.NoError(t, err)
	statsBefore := testJournalEnd(leader)
	runs := &atomic.Int32{}
	result := startTestRead(ctx, leader, runs)
	heartbeat := awaitReadTestHeartbeat(t, ctx, cluster, result)
	require.Empty(t, runs.Load(), "query cannot execute before a matching quorum response")

	// A valid reply from a voter that does not cover this read must be ignored.
	require.NoError(t, leader.Step(ctx, &pb.Message{
		Type: pb.MsgHeartbeatResp.Enum(), From: heartbeat.To, To: heartbeat.From, Term: heartbeat.Term,
	}))
	requireReadStillPending(t, result, runs)

	answerReadTestHeartbeat(t, ctx, cluster, heartbeat)
	completed := awaitTestReadResult(t, ctx, result)
	require.NoError(t, completed.err)
	require.JSONEq(t, `["applied before read"]`, string(completed.body))
	require.Equal(t, int32(1), runs.Load())
	require.NoError(t, waitTestLocalWrites(ctx, leader))
	lastAfter, err := leader.store.LastIndex()
	require.NoError(t, err)
	require.Equal(t, lastBefore, lastAfter, "read must not append a command")
	statsAfter := testJournalEnd(leader)
	require.Equal(t, statsBefore, statsAfter, "read must not write the Raft store")
}

func TestReadIndexCancellationBoundsAbandonedFences(t *testing.T) {
	ctx, cluster, leader := newReadTestCluster(t)
	leader.mu.Lock()
	leader.cfg.MaxPending = 2
	leader.mu.Unlock()

	canceledRuns := &atomic.Int32{}
	firstCtx, cancelFirst := context.WithCancel(ctx)
	defer cancelFirst()
	first := startTestRead(firstCtx, leader, canceledRuns)
	firstHeartbeat := awaitReadTestHeartbeat(t, ctx, cluster, first)
	cancelFirst()
	require.ErrorIs(t, awaitTestReadResult(t, ctx, first).err, context.Canceled)

	queuedCtx, cancelQueued := context.WithCancel(ctx)
	defer cancelQueued()
	queued := startTestRead(queuedCtx, leader, canceledRuns)
	requirePendingReadCount(t, leader, 1)
	_, err := leader.Read(ctx, func(Machine) ([]byte, error) {
		canceledRuns.Add(1)
		return nil, nil
	})
	require.ErrorIs(t, err, ErrBusy, "the abandoned fence and queued read must both count toward admission")

	cancelQueued()
	require.ErrorIs(t, awaitTestReadResult(t, ctx, queued).err, context.Canceled)
	liveRuns := &atomic.Int32{}
	live := startTestRead(ctx, leader, liveRuns)
	requirePendingReadCount(t, leader, 1)

	// The canceled queued read freed its slot. The surviving waiter still
	// needs a fresh quorum response after the abandoned fence is retired.
	answerReadTestHeartbeat(t, ctx, cluster, firstHeartbeat)
	liveHeartbeat := awaitReadTestHeartbeat(t, ctx, cluster, live)
	require.NotEqual(t, firstHeartbeat.GetContext(), liveHeartbeat.GetContext())
	answerReadTestHeartbeat(t, ctx, cluster, firstHeartbeat)
	requireReadStillPending(t, live, liveRuns)
	answerReadTestHeartbeat(t, ctx, cluster, liveHeartbeat)
	require.NoError(t, awaitTestReadResult(t, ctx, live).err)
	require.Equal(t, int32(1), liveRuns.Load())
	require.Zero(t, canceledRuns.Load())
}

func TestReadIndexAbortsWhenLeaderIsDeposed(t *testing.T) {
	ctx, cluster, leader := newReadTestCluster(t)
	runs := &atomic.Int32{}
	result := startTestRead(ctx, leader, runs)
	heartbeat := awaitReadTestHeartbeat(t, ctx, cluster, result)
	queued := startTestRead(ctx, leader, runs)
	requirePendingReadCount(t, leader, 2)
	require.NoError(t, leader.Step(ctx, &pb.Message{
		Type: pb.MsgHeartbeat.Enum(), From: new(uint64(2)), To: new(uint64(1)), Term: new(heartbeat.GetTerm() + 1),
		Commit: new(leader.Status().Commit),
	}))
	require.ErrorIs(t, awaitTestReadResult(t, ctx, result).err, ErrLeadershipLost)
	require.ErrorIs(t, awaitTestReadResult(t, ctx, queued).err, ErrLeadershipLost)
	require.NoError(t, waitTestLocalWrites(ctx, leader))
	require.Equal(t, uint64(2), leader.Leader())
	answerReadTestHeartbeat(t, ctx, cluster, heartbeat)
	require.ErrorIs(t, awaitTestReadResult(t, ctx, startTestRead(ctx, leader, runs)).err, ErrNotLeader)
	require.Zero(t, runs.Load())
}

func TestReadIndexWaitsForMatchingAppliedFence(t *testing.T) {
	ctx, cluster, leader := newReadTestCluster(t)
	runs := &atomic.Int32{}
	result := startTestRead(ctx, leader, runs)
	_ = awaitReadTestHeartbeat(t, ctx, cluster, result)
	var id key
	var applied uint64
	func() {
		leader.mu.Lock()
		defer leader.mu.Unlock()
		require.Len(t, leader.reads, 1)
		for request := range leader.reads {
			id = request
		}
		applied = leader.applied
		unrelated := id
		unrelated[0] ^= 1
		leader.recordReadStates([]raft.ReadState{{Index: applied, RequestCtx: unrelated[:]}})
		leader.finishReads(leader.node.BasicStatus())
	}()
	requireReadStillPending(t, result, runs)

	// The transport test verifies the real wire fence. Here inject a valid
	// result that is ahead of application to isolate the apply predicate.
	leader.mu.Lock()
	leader.recordReadStates([]raft.ReadState{{Index: applied + 1, RequestCtx: id[:]}})
	leader.finishReads(leader.node.BasicStatus())
	leader.mu.Unlock()
	requireReadStillPending(t, result, runs)

	// Discard this read's unanswered wire heartbeats. Application of the
	// next real command, not a second fence, must release the query.
	for len(cluster.out) > 0 {
		<-cluster.out
	}
	require.NoError(t, cluster.commit(func() error {
		_, err := leader.Propose(ctx, []byte("at the read fence"))
		return err
	}))
	completed := awaitTestReadResult(t, ctx, result)
	require.NoError(t, completed.err)
	require.JSONEq(t, `["applied before read", "at the read fence"]`, string(completed.body))
	require.Equal(t, int32(1), runs.Load())
}

func TestReadIndexRejectsLeaderWithoutAppliedCurrentTerm(t *testing.T) {
	ctx, _, leader := newReadTestElection(t)
	func() {
		leader.mu.Lock()
		defer leader.mu.Unlock()
		term, err := leader.store.Term(leader.applied)
		require.NoError(t, err)
		require.NotEqual(t, leader.node.BasicStatus().GetTerm(), term, "current-term entry must remain unapplied")
	}()
	runs := &atomic.Int32{}
	read := awaitTestReadResult(t, ctx, startTestRead(ctx, leader, runs))
	require.ErrorIs(t, read.err, ErrBusy)
	require.Zero(t, runs.Load())
}

func newReadTestElection(t *testing.T) (context.Context, *testCluster, *Group) {
	t.Helper()
	cluster := newTestCluster(t)
	for _, id := range []uint64{1, 2, 3} {
		cluster.open(id, []uint64{1, 2, 3})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	leader := cluster.groups[1]
	require.NoError(t, leader.Campaign(ctx))
	for leader.Leader() != 1 {
		select {
		case message := <-cluster.out:
			cluster.deliver(message)
		case <-ctx.Done():
			t.Fatal("read fixture leader did not win election")
		}
	}
	return ctx, cluster, leader
}

func newReadTestCluster(t *testing.T) (context.Context, *testCluster, *Group) {
	t.Helper()
	ctx, cluster, leader := newReadTestElection(t)
	cluster.drain()
	require.NoError(t, cluster.commit(func() error {
		_, err := leader.Propose(ctx, []byte("applied before read"))
		return err
	}))
	require.NoError(t, waitTestLocalWrites(ctx, leader))
	cluster.drain()
	return ctx, cluster, leader
}

func startTestRead(ctx context.Context, group *Group, runs *atomic.Int32) <-chan response {
	done := make(chan response, 1)
	go func() {
		body, err := group.Read(ctx, func(machine Machine) ([]byte, error) {
			runs.Add(1)
			return machine.Snapshot()
		})
		done <- response{body: body, err: err}
	}()
	return done
}

func awaitReadTestHeartbeat(t *testing.T, ctx context.Context, cluster *testCluster, result <-chan response) *pb.Message {
	t.Helper()
	for {
		select {
		case m := <-cluster.out:
			if m.GetFrom() == 1 && m.GetTo() == 2 && m.GetType() == pb.MsgHeartbeat && len(m.GetContext()) > 0 {
				return m
			}
		case read := <-result:
			t.Fatalf("read returned without requesting a quorum fence: %v", read.err)
		case <-ctx.Done():
			t.Fatal("read never requested a quorum fence")
		}
	}
}

func answerReadTestHeartbeat(t *testing.T, ctx context.Context, cluster *testCluster, heartbeat *pb.Message) {
	t.Helper()
	require.NoError(t, cluster.groups[2].Step(ctx, heartbeat))
	for {
		select {
		case m := <-cluster.out:
			if m.GetFrom() == 2 && m.GetTo() == 1 && m.GetType() == pb.MsgHeartbeatResp && bytes.Equal(m.GetContext(), heartbeat.GetContext()) {
				require.NoError(t, cluster.groups[1].Step(ctx, m))
				return
			}
		case <-ctx.Done():
			t.Fatal("voter did not return matching heartbeat response")
		}
	}
}

func awaitTestReadResult(t *testing.T, ctx context.Context, result <-chan response) response {
	t.Helper()
	select {
	case done := <-result:
		return done
	case <-ctx.Done():
		t.Fatal("read did not finish")
		return response{}
	}
}

func requireReadStillPending(t *testing.T, result <-chan response, runs *atomic.Int32) {
	t.Helper()
	select {
	case done := <-result:
		t.Fatalf("read completed without a matching quorum response: %+v", done)
	default:
	}
	require.Zero(t, runs.Load(), "query cannot execute before a matching quorum response")
}

func requirePendingReadCount(t *testing.T, leader *Group, count int) {
	t.Helper()
	require.Eventually(t, func() bool {
		leader.mu.Lock()
		defer leader.mu.Unlock()
		return len(leader.reads) == count && leader.appending == nil
	}, time.Second, time.Millisecond, "expected %d admitted reads with no deferred Ready writes", count)
}
