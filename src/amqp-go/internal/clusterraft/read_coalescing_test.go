package clusterraft

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestReadIndexCoalescesOnlyLaterRequestsBehindActiveFence(t *testing.T) {
	ctx, cluster, leader := newReadTestCluster(t)
	readFenceContexts := func() []string {
		// Send runs under this lock. All admitted, non-deferred heartbeats
		// are visible in the transport before we inspect it.
		leader.mu.Lock()
		defer leader.mu.Unlock()
		var contexts []string
		for len(cluster.out) != 0 {
			message := <-cluster.out
			if message.GetFrom() == 1 && message.GetType() == pb.MsgHeartbeat && len(message.GetContext()) != 0 {
				contexts = append(contexts, string(message.GetContext()))
			}
		}
		return contexts
	}

	firstRuns := &atomic.Int32{}
	first := startTestRead(ctx, leader, firstRuns)
	firstHeartbeat := awaitReadTestHeartbeat(t, ctx, cluster, first)
	for _, fence := range readFenceContexts() {
		require.Equal(t, string(firstHeartbeat.GetContext()), fence)
	}

	// Both copies of the first fence are withheld. Commit a real mutation
	// before the later reads start, while that older fence is still open.
	require.NoError(t, cluster.commit(func() error {
		_, err := leader.Propose(ctx, []byte("committed between fences"))
		return err
	}))
	require.NoError(t, waitTestLocalWrites(ctx, leader))
	cluster.drain()
	requireReadStillPending(t, first, firstRuns)

	secondRuns, thirdRuns := &atomic.Int32{}, &atomic.Int32{}
	second := startTestRead(ctx, leader, secondRuns)
	third := startTestRead(ctx, leader, thirdRuns)
	require.Eventually(t, func() bool {
		leader.mu.Lock()
		defer leader.mu.Unlock()
		return len(leader.reads) == 3 && leader.appending == nil
	}, time.Second, time.Millisecond, "all three reads must be admitted with no deferred Ready writes")

	require.Empty(t, readFenceContexts(), "later reads must wait behind the active quorum fence")
	answerReadTestHeartbeat(t, ctx, cluster, firstHeartbeat)
	require.NoError(t, awaitTestReadResult(t, ctx, first).err)
	require.Equal(t, int32(1), firstRuns.Load())

	secondHeartbeat := awaitReadTestHeartbeat(t, ctx, cluster, second)
	require.NotEqual(t, firstHeartbeat.GetContext(), secondHeartbeat.GetContext(), "later reads need a fresh quorum fence")
	for _, fence := range readFenceContexts() {
		require.Equal(t, string(secondHeartbeat.GetContext()), fence, "later reads must share the fresh fence")
	}
	requireReadStillPending(t, second, secondRuns)
	requireReadStillPending(t, third, thirdRuns)

	answerReadTestHeartbeat(t, ctx, cluster, secondHeartbeat)
	for _, result := range []<-chan response{second, third} {
		read := awaitTestReadResult(t, ctx, result)
		require.NoError(t, read.err)
		require.JSONEq(t, `["applied before read", "committed between fences"]`, string(read.body))
	}
	require.Equal(t, int32(1), secondRuns.Load())
	require.Equal(t, int32(1), thirdRuns.Load())
	require.Empty(t, readFenceContexts(), "the one fresh fence must complete both later reads")
}
