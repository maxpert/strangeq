package clusterraft

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReadIndexKeepsOldFenceAcrossRepeatedVoterChanges(t *testing.T) {
	cluster := newTestCluster(t)
	leader := cluster.open(1, []uint64{1, 2})
	cluster.open(2, []uint64{1, 2})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, leader.Campaign(ctx))
	cluster.drain()
	require.EqualValues(t, 1, leader.Leader())
	require.NoError(t, cluster.commit(func() error {
		_, err := leader.Propose(ctx, []byte("before shrink"))
		return err
	}))
	require.NoError(t, waitTestLocalWrites(ctx, leader))
	cluster.drain()

	runs := &atomic.Int32{}
	first := startTestRead(ctx, leader, runs)
	oldHeartbeat := awaitReadTestHeartbeat(t, ctx, cluster, first)
	requireReadStillPending(t, first, runs)

	var final <-chan response
	var finalRuns *atomic.Int32
	for round := range 2 {
		// Keep oldHeartbeat out of the transport while the same pair of
		// members commits a shrink followed by a new promotion.
		require.NoError(t, cluster.commit(func() error { return leader.Remove(ctx, 2) }))
		require.NoError(t, waitTestLocalWrites(ctx, leader))
		require.Equal(t, []uint64{1}, leader.Status().Voters)
		waiting := first
		if round != 0 {
			waiting = final
		}
		completed := awaitTestReadResult(t, ctx, waiting)
		require.NoError(t, completed.err)
		require.JSONEq(t, `["before shrink"]`, string(completed.body))

		// A detached receipt must leave the only caller slot usable
		// while the local voter can confirm reads without any peer.
		leader.mu.Lock()
		leader.cfg.MaxPending = 1
		leader.mu.Unlock()
		body, err := leader.Read(ctx, func(machine Machine) ([]byte, error) { return machine.Snapshot() })
		require.NoError(t, err)
		require.JSONEq(t, `["before shrink"]`, string(body))

		require.NoError(t, cluster.commit(func() error { return leader.AddLearner(ctx, 2) }))
		require.NoError(t, waitTestLocalWrites(ctx, leader))
		cluster.drain()
		require.True(t, leader.CaughtUp(2))
		require.NoError(t, cluster.commit(func() error { return leader.Promote(ctx, 2) }))
		require.NoError(t, waitTestLocalWrites(ctx, leader))
		cluster.drain()
		require.Equal(t, []uint64{1, 2}, leader.Status().Voters)

		_, err = leader.Read(ctx, func(machine Machine) ([]byte, error) { return machine.Snapshot() })
		require.ErrorIs(t, err, ErrBusy, "the unresolved multisite fence must count again after promotion")
		leader.mu.Lock()
		leader.cfg.MaxPending = 2
		leader.mu.Unlock()
		finalRuns = &atomic.Int32{}
		final = startTestRead(ctx, leader, finalRuns)
		requirePendingReadCount(t, leader, 1)
		require.Empty(t, cluster.out, "re-expansion must not send a second multisite fence")
		requireReadStillPending(t, final, finalRuns)
	}

	// This fixture controls delivery and disables the automatic ticker.
	// Two ticks emit the still-unanswered request to the current voters.
	require.NoError(t, leader.Tick(ctx))
	require.NoError(t, leader.Tick(ctx))
	retried := awaitReadTestHeartbeat(t, ctx, cluster, final)
	require.Equal(t, oldHeartbeat.GetTerm(), retried.GetTerm())
	require.Equal(t, oldHeartbeat.GetContext(), retried.GetContext())
	answerReadTestHeartbeat(t, ctx, cluster, retried)
	requireReadStillPending(t, final, finalRuns)

	fresh := awaitReadTestHeartbeat(t, ctx, cluster, final)
	require.Equal(t, oldHeartbeat.GetTerm(), fresh.GetTerm())
	require.NotEqual(t, oldHeartbeat.GetContext(), fresh.GetContext())
	answerReadTestHeartbeat(t, ctx, cluster, fresh)
	completed := awaitTestReadResult(t, ctx, final)
	require.NoError(t, completed.err)
	require.JSONEq(t, `["before shrink"]`, string(completed.body))
	require.EqualValues(t, 1, finalRuns.Load())
	require.EqualValues(t, 1, runs.Load())
}
