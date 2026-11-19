package clusterraft

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
)

type testAppliedWaiter interface {
	WaitApplied(context.Context, func(Machine) (bool, error)) error
}

func startAppliedWaitTest(ctx context.Context, group *Group, value string) (<-chan bool, <-chan error) {
	observed := make(chan bool, 4)
	done := make(chan error, 1)
	go func() {
		waiter, ok := any(group).(testAppliedWaiter)
		if !ok {
			done <- errors.New("group has no applied readiness wait")
			return
		}
		done <- waiter.WaitApplied(ctx, func(machine Machine) (bool, error) {
			if group.mu.TryLock() {
				group.mu.Unlock()
				return false, errors.New("readiness predicate ran outside the group lock")
			}
			ready := slices.Contains(machine.(*testMachine).entries, value)
			select {
			case observed <- ready:
			default:
			}
			return ready, nil
		})
	}()
	return observed, done
}

func expectAppliedWaitObservation(t *testing.T, ctx context.Context, observed <-chan bool, done <-chan error, ready bool) {
	t.Helper()
	select {
	case value := <-observed:
		require.Equal(t, ready, value)
	case err := <-done:
		t.Fatalf("wait returned before evaluating applied readiness: %v", err)
	case <-ctx.Done():
		t.Fatal("wait did not evaluate applied readiness")
	}
}

func awaitAppliedWaitTest(t *testing.T, ctx context.Context, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		t.Fatal("applied readiness wait did not exit")
		return ctx.Err()
	}
}

func TestWaitAppliedUsesAppliedConditionWithoutWriting(t *testing.T) {
	ctx, cluster, leader := newReadTestCluster(t)
	before, err := leader.store.LastIndex()
	require.NoError(t, err)
	beforeStats := testBoltDB(leader).Stats()
	observed, done := startAppliedWaitTest(ctx, leader, "newly applied")
	expectAppliedWaitObservation(t, ctx, observed, done, false)
	leader.mu.Lock()
	index, err := leader.store.LastIndex()
	leader.mu.Unlock()
	require.NoError(t, err)
	require.Equal(t, before, index, "parking a readiness wait does not propose a Raft entry")
	afterStats := testBoltDB(leader).Stats()
	require.Equal(t, beforeStats.TxStats.GetWrite(), afterStats.TxStats.GetWrite(), "parking does not write the Raft store")

	require.NoError(t, cluster.commit(func() error {
		_, err := leader.Propose(ctx, []byte("newly applied"))
		return err
	}))
	require.NoError(t, awaitAppliedWaitTest(t, ctx, done))
	select {
	case ready := <-observed:
		require.True(t, ready, "successful wait must observe the applied value")
	default:
		t.Fatal("wait returned without observing a ready predicate")
	}
	require.NoError(t, waitTestLocalWrites(ctx, leader))
	index, err = leader.store.LastIndex()
	require.NoError(t, err)
	require.Equal(t, before+1, index, "only the actual proposal added a log entry")

	// A publish that commits before subscription is visible immediately.
	observed, done = startAppliedWaitTest(ctx, leader, "newly applied")
	require.NoError(t, awaitAppliedWaitTest(t, ctx, done))
	select {
	case ready := <-observed:
		require.True(t, ready)
	default:
		t.Fatal("an already-applied condition must be observed before returning")
	}
}

func TestWaitAppliedCancellationReturnsWithoutWriting(t *testing.T) {
	ctx, _, leader := newReadTestCluster(t)
	before, err := leader.store.LastIndex()
	require.NoError(t, err)
	beforeStats := testBoltDB(leader).Stats()
	waitCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	observed, done := startAppliedWaitTest(waitCtx, leader, "absent")
	expectAppliedWaitObservation(t, ctx, observed, done, false)
	cancel()
	require.ErrorIs(t, awaitAppliedWaitTest(t, ctx, done), context.Canceled)
	index, err := leader.store.LastIndex()
	require.NoError(t, err)
	require.Equal(t, before, index)
	afterStats := testBoltDB(leader).Stats()
	require.Equal(t, beforeStats.TxStats.GetWrite(), afterStats.TxStats.GetWrite())
}

func TestWaitAppliedEndsOnLeaderLoss(t *testing.T) {
	ctx, _, leader := newReadTestCluster(t)
	observed, done := startAppliedWaitTest(ctx, leader, "absent")
	expectAppliedWaitObservation(t, ctx, observed, done, false)
	leader.mu.Lock()
	status := leader.node.BasicStatus()
	leader.mu.Unlock()
	require.NoError(t, leader.Step(ctx, &pb.Message{
		Type: pb.MsgHeartbeat.Enum(), From: new(uint64(2)), To: new(uint64(1)),
		Term: new(status.GetTerm() + 1), Commit: new(status.GetCommit()),
	}))
	require.ErrorIs(t, awaitAppliedWaitTest(t, ctx, done), ErrLeadershipLost)
	require.EqualValues(t, 2, leader.Leader())
}
