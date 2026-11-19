package clusterraft

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestReadIndexAfterRemovingLastOtherVoter(t *testing.T) {
	cluster := newTestCluster(t)
	leader := cluster.open(1, []uint64{1, 2})
	follower := cluster.open(2, []uint64{1, 2})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, leader.Campaign(ctx))
	cluster.drain()
	require.EqualValues(t, 1, leader.Leader())
	require.NoError(t, cluster.commit(func() error {
		_, err := leader.Propose(ctx, []byte("before removal"))
		return err
	}))
	require.NoError(t, waitTestLocalWrites(ctx, leader))
	cluster.drain()
	term := func() uint64 {
		leader.mu.Lock()
		defer leader.mu.Unlock()
		return leader.node.BasicStatus().GetTerm()
	}
	lastIndex := func() uint64 {
		leader.mu.Lock()
		defer leader.mu.Unlock()
		index, err := leader.store.LastIndex()
		require.NoError(t, err)
		return index
	}
	originalTerm := term()

	firstCtx, cancelFirst := context.WithCancel(ctx)
	defer cancelFirst()
	firstRuns := &atomic.Int32{}
	first := startTestRead(firstCtx, leader, firstRuns)
	heartbeat := awaitReadTestHeartbeat(t, ctx, cluster, first)
	require.Equal(t, originalTerm, heartbeat.GetTerm())
	require.NoError(t, follower.Step(ctx, heartbeat))
	require.NoError(t, waitTestLocalWrites(ctx, follower))
	var heldReply *pb.Message
	for heldReply == nil {
		select {
		case message := <-cluster.out:
			if message.GetFrom() == 2 && message.GetTo() == 1 && message.GetType() == pb.MsgHeartbeatResp &&
				bytes.Equal(message.GetContext(), heartbeat.GetContext()) {
				heldReply = message
			} else {
				cluster.deliver(message)
			}
		case result := <-first:
			t.Fatalf("read completed before its quorum reply was delivered: %+v", result)
		case <-ctx.Done():
			t.Fatal("follower did not produce the quorum reply to hold")
		}
	}
	requireReadStillPending(t, first, firstRuns)

	// Automatic ticks are disabled by newTestCluster. Only the matching
	// heartbeat response above is withheld; real append acknowledgements
	// still commit the public membership operation under the old quorum.
	require.NoError(t, cluster.commit(func() error { return leader.Remove(ctx, 2) }))
	require.NoError(t, waitTestLocalWrites(ctx, leader))
	status := leader.Status()
	require.Equal(t, []uint64{1}, status.Voters)
	require.Empty(t, status.Learners)
	require.EqualValues(t, 1, status.Leader)
	require.Equal(t, originalTerm, term(), "removal must not recover the read by changing terms")

	require.NoError(t, cluster.commit(func() error {
		_, err := leader.Propose(ctx, []byte("committed as singleton"))
		return err
	}))
	require.NoError(t, waitTestLocalWrites(ctx, leader))
	beforeRead := leader.Status()
	require.Equal(t, beforeRead.Commit, beforeRead.Applied)
	beforeReadIndex := lastIndex()

	// This request starts after the removal and the later command complete.
	// It needs the singleton's fresh read fence: the old quorum reply stays
	// outside the transport, and no fabricated ReadState releases either read.
	freshCtx, cancelFresh := context.WithTimeout(ctx, time.Second)
	defer cancelFresh()
	var freshRuns atomic.Int32
	body, err := leader.Read(freshCtx, func(machine Machine) ([]byte, error) {
		freshRuns.Add(1)
		if leader.applied < beforeRead.Commit {
			return nil, fmt.Errorf("read ran at applied index %d before completed singleton index %d", leader.applied, beforeRead.Commit)
		}
		return machine.Snapshot()
	})
	require.NoError(t, err, "a new singleton read must not wait for a removed voter's old quorum reply")
	require.JSONEq(t, `["before removal", "committed as singleton"]`, string(body))
	require.EqualValues(t, 1, freshRuns.Load())
	require.Equal(t, originalTerm, term(), "the fresh read must finish in the unchanged leader term")
	require.Equal(t, beforeRead.Applied, leader.Status().Applied)
	require.Equal(t, beforeReadIndex, lastIndex(), "the read must not append a replicated command")
	require.Equal(t, originalTerm, heldReply.GetTerm(), "the original quorum reply remains withheld")

	// The baseline leaves the original read pending; an implementation may
	// also finish it using the singleton quorum. Neither outcome may leak it.
	cancelFirst()
	previous := awaitTestReadResult(t, ctx, first)
	if previous.err != nil {
		require.ErrorIs(t, previous.err, context.Canceled)
	}
	require.LessOrEqual(t, firstRuns.Load(), int32(1))
}
