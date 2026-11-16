package clusterraft

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

type testMachine struct{ entries []string }

func (m *testMachine) Apply(entry []byte) []byte {
	m.entries = append(m.entries, string(entry))
	return []byte("applied " + string(entry))
}
func (m *testMachine) Snapshot() ([]byte, error) { return json.Marshal(m.entries) }
func (m *testMachine) Restore(data []byte) error { return json.Unmarshal(data, &m.entries) }

type testCluster struct {
	t      *testing.T
	dir    string
	groups map[uint64]*Group
	out    chan *raftpb.Message
}

func TestDurableQuorumAndRestart(t *testing.T) {
	cluster := newTestCluster(t)
	for _, id := range []uint64{1, 2, 3} {
		cluster.open(id, []uint64{1, 2, 3})
	}
	ctx := context.Background()
	require.NoError(t, cluster.groups[1].Campaign(ctx))
	cluster.drain()
	require.Equal(t, uint64(1), cluster.groups[1].Leader())

	proposalCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	reply := make(chan response, 1)
	go func() {
		body, err := cluster.groups[1].Propose(proposalCtx, []byte("durable"))
		reply <- response{body: body, err: err}
	}()

	var appendMessage *raftpb.Message
	for appendMessage == nil {
		select {
		case message := <-cluster.out:
			if message.GetTo() == 2 && message.GetType() == raftpb.MsgApp && len(message.GetEntries()) > 0 {
				appendMessage = message
			}
		case <-time.After(3 * time.Second):
			t.Fatal("leader did not replicate")
		}
	}
	select {
	case <-reply:
		t.Fatal("leader replied before durable quorum")
	default:
	}
	require.NoError(t, cluster.groups[2].Step(ctx, appendMessage))
	cluster.drain()
	select {
	case result := <-reply:
		require.NoError(t, result.err)
		require.Equal(t, []byte("applied durable"), result.body)
	case <-time.After(time.Second):
		t.Fatal("leader did not reply after durable quorum")
	}
	require.NoError(t, cluster.groups[1].Snapshot(ctx))
	for _, id := range []uint64{1, 2, 3} {
		require.NoError(t, cluster.groups[id].Close())
		cluster.open(id, nil)
	}
	snapshot, err := cluster.groups[1].MachineSnapshot(ctx)
	require.NoError(t, err)
	require.JSONEq(t, `["durable"]`, string(snapshot))
	require.Equal(t, []uint64{1, 2, 3}, cluster.groups[1].Status().Voters)
}

func TestSnapshotCatchesUpJoiningLearner(t *testing.T) {
	cluster := newTestCluster(t)
	for _, id := range []uint64{1, 2, 3} {
		cluster.open(id, []uint64{1, 2, 3})
	}
	ctx := context.Background()
	require.NoError(t, cluster.groups[1].Campaign(ctx))
	cluster.drain()
	leader := cluster.groups[1]
	require.Equal(t, uint64(1), leader.Leader())
	require.NoError(t, cluster.commit(func() error { _, err := leader.Propose(ctx, []byte("in-snapshot")); return err }))
	require.NoError(t, leader.Snapshot(ctx))

	learner := cluster.open(4, nil)
	require.NoError(t, cluster.commit(func() error { return leader.AddLearner(ctx, 4) }))
	require.True(t, leader.CaughtUp(4))
	require.NoError(t, cluster.commit(func() error { return leader.Promote(ctx, 4) }))
	require.Equal(t, []uint64{1, 2, 3, 4}, leader.Status().Voters)
	snapshot, err := learner.MachineSnapshot(ctx)
	require.NoError(t, err)
	require.JSONEq(t, `["in-snapshot"]`, string(snapshot))
}

func TestLeadershipTransferRejectsUnacceptedProposalForRetry(t *testing.T) {
	cluster := newTestCluster(t)
	for _, id := range []uint64{1, 2, 3} {
		cluster.open(id, []uint64{1, 2, 3})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	oldLeader, nextLeader := cluster.groups[1], cluster.groups[2]
	require.NoError(t, oldLeader.Campaign(ctx))
	cluster.drain()
	require.NoError(t, oldLeader.TransferLeadership(ctx, 2))

	// Hold the transfer message in transit. The old leader still reports
	// itself as leader, but Raft refuses new entries during this interval.
	const body = "safe-retry"
	commit := oldLeader.Status().Commit
	_, err := oldLeader.Propose(ctx, []byte(body))
	require.ErrorIs(t, err, ErrBusy)
	require.NotErrorIs(t, err, ErrLeadershipLost)
	require.Equal(t, commit, oldLeader.Status().Commit)

	cluster.drain()
	require.Equal(t, uint64(2), nextLeader.Leader())
	require.NoError(t, cluster.commit(func() error {
		_, err := nextLeader.Propose(ctx, []byte(body))
		return err
	}))
	snapshot, err := nextLeader.MachineSnapshot(ctx)
	require.NoError(t, err)
	require.JSONEq(t, `["safe-retry"]`, string(snapshot))
}

func TestNewLeaderCanRetryConfigurationDiscardedAsNoOp(t *testing.T) {
	cluster := newTestCluster(t)
	for _, id := range []uint64{1, 2, 3} {
		cluster.open(id, []uint64{1, 2, 3})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	oldLeader, nextLeader := cluster.groups[1], cluster.groups[2]
	require.NoError(t, oldLeader.Campaign(ctx))
	cluster.drain()

	// Leave an unapplied configuration in the surviving replica's log.
	old := make(chan error, 1)
	go func() { old <- oldLeader.AddLearner(ctx, 5) }()
	for range 2 {
		select {
		case message := <-cluster.out:
			require.Equal(t, raftpb.MsgApp, message.GetType())
			if message.GetTo() == 2 {
				require.NoError(t, nextLeader.Step(ctx, message))
			}
		case <-ctx.Done():
			t.Fatal("leader did not send uncommitted configuration")
		}
	}
	require.NoError(t, oldLeader.Close())
	delete(cluster.groups, 1)
	cluster.drain()
	require.ErrorIs(t, <-old, ErrLeadershipLost)

	// Expire the previous leader's lease, then elect the up-to-date replica
	// without delivering its first append to the other surviving voter.
	for range 10 {
		require.NoError(t, nextLeader.Tick(ctx))
		require.NoError(t, cluster.groups[3].Tick(ctx))
	}
	require.NoError(t, nextLeader.Campaign(ctx))
	for nextLeader.Leader() != 2 {
		select {
		case message := <-cluster.out:
			cluster.deliver(message)
		case <-ctx.Done():
			t.Fatal("surviving replica did not become leader")
		}
	}

	// Raft replaces this configuration request with a no-op while the old
	// configuration is unapplied. Wait for local admission before unblocking
	// replication so the no-op path cannot be skipped by the test scheduler.
	discarded := make(chan response, 1)
	go func() { discarded <- response{err: nextLeader.AddLearner(ctx, 4)} }()
	require.Eventually(t, func() bool {
		nextLeader.mu.Lock()
		defer nextLeader.mu.Unlock()
		return nextLeader.configuration.index != 0
	}, time.Second, time.Millisecond)

	require.ErrorIs(t, cluster.await(discarded).err, ErrConfChangePending)
	require.Equal(t, []uint64{5}, nextLeader.Status().Learners)
	require.NoError(t, cluster.commit(func() error { return nextLeader.Remove(ctx, 5) }))
	require.NoError(t, cluster.commit(func() error { return nextLeader.AddLearner(ctx, 4) }))
	require.Equal(t, []uint64{4}, nextLeader.Status().Learners)
}

func TestSingleVoterGrowsAcrossSevenNodesWithConcurrentCommands(t *testing.T) {
	cluster := newTestCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	leader := cluster.open(1, []uint64{1})
	require.NoError(t, leader.Campaign(ctx))
	require.Equal(t, uint64(1), leader.Leader())
	command := "before-join"
	result, err := leader.Propose(ctx, []byte(command))
	require.NoError(t, err)
	require.Equal(t, []byte("applied "+command), result)

	// The original voter recovers its configuration and data without
	// receiving another bootstrap configuration.
	require.NoError(t, leader.Close())
	leader = cluster.open(1, nil)
	require.NoError(t, leader.Campaign(ctx))
	want := []string{command}
	voters := []uint64{1}
	for id := uint64(2); id <= 7; id++ {
		cluster.open(id, nil)
		command = "joining-" + strconv.FormatUint(id, 10)
		if id == 2 {
			// Admission commits with the old singleton quorum. Promotion
			// changes the next command's quorum to both voters.
			require.NoError(t, cluster.commit(func() error { return leader.AddLearner(ctx, id) }))
			require.True(t, leader.CaughtUp(id))
			require.NoError(t, cluster.commit(func() error { return leader.Promote(ctx, id) }))
			cluster.verifyTwoVoterQuorum(ctx, leader, id, command)
		} else {
			// Keep a normal command and an admission in flight together.
			// Holding transport prevents either from committing until both
			// are in the leader log.
			cluster.joinWithCommand(ctx, leader, id, command)
			require.True(t, leader.CaughtUp(id))
			require.NoError(t, cluster.commit(func() error { return leader.Promote(ctx, id) }))
		}
		want = append(want, command)
		voters = append(voters, id)
		if len(voters) == 4 {
			remove := voters[1]
			require.NoError(t, cluster.commit(func() error { return leader.Remove(ctx, remove) }))
			require.NoError(t, cluster.groups[remove].Close())
			delete(cluster.groups, remove)
			voters = append(voters[:1], voters[2:]...)
		}
		for _, voter := range voters {
			group := cluster.groups[voter]
			require.Equal(t, voters, group.Status().Voters)
			raw, err := group.MachineSnapshot(ctx)
			require.NoError(t, err)
			var entries []string
			require.NoError(t, json.Unmarshal(raw, &entries))
			require.Equal(t, want, entries)
		}
	}
}

func (c *testCluster) verifyTwoVoterQuorum(ctx context.Context, leader *Group, peer uint64, command string) {
	c.t.Helper()
	commit := leader.Status().Commit
	result := make(chan response, 1)
	go func() {
		body, err := leader.Propose(ctx, []byte(command))
		result <- response{body: body, err: err}
	}()
	select {
	case message := <-c.out:
		require.Equal(c.t, peer, message.GetTo())
		require.Equal(c.t, raftpb.MsgApp, message.GetType())
		require.Equal(c.t, commit, leader.Status().Commit, "the leader must wait for its second voter")
		c.deliver(message)
	case <-ctx.Done():
		c.t.Fatal("the leader did not replicate to its second voter")
	}
	confirmed := c.await(result)
	require.NoError(c.t, confirmed.err)
	require.Equal(c.t, []byte("applied "+command), confirmed.body)
}

func (c *testCluster) joinWithCommand(ctx context.Context, leader *Group, id uint64, command string) {
	c.t.Helper()
	commandResult := make(chan response, 1)
	admitted := make(chan response, 1)
	go func() {
		body, err := leader.Propose(ctx, []byte(command))
		commandResult <- response{body: body, err: err}
	}()
	go func() { admitted <- response{err: leader.AddLearner(ctx, id)} }()

	var queued []*raftpb.Message
	var normal, configuration bool
	for !normal || !configuration {
		select {
		case message := <-c.out:
			for _, entry := range message.GetEntries() {
				normal = normal || entry.GetType() == raftpb.EntryNormal
				configuration = configuration || entry.GetType() == raftpb.EntryConfChange
			}
			queued = append(queued, message)
		case <-ctx.Done():
			c.t.Fatal("both proposals must enter the log before replication")
		}
	}
	for _, message := range queued {
		c.deliver(message)
	}
	result := c.await(commandResult)
	require.NoError(c.t, result.err)
	require.Equal(c.t, []byte("applied "+command), result.body)
	require.NoError(c.t, c.await(admitted).err)
}

func TestReplicaStorageFailureDoesNotAcknowledgeAppend(t *testing.T) {
	cluster := newTestCluster(t)
	for _, id := range []uint64{1, 2, 3} {
		cluster.open(id, []uint64{1, 2, 3})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	leader, failed := cluster.groups[1], cluster.groups[2]
	require.NoError(t, leader.Campaign(ctx))
	cluster.drain()
	require.NoError(t, failed.db.Close())

	reply := make(chan error, 1)
	go func() {
		_, err := leader.Propose(ctx, []byte("replicated"))
		reply <- err
	}()

	var toFailed, toHealthy *raftpb.Message
	for toFailed == nil || toHealthy == nil {
		select {
		case message := <-cluster.out:
			require.Equal(t, raftpb.MsgApp, message.GetType())
			switch message.GetTo() {
			case 2:
				toFailed = message
			case 3:
				toHealthy = message
			}
		case <-ctx.Done():
			t.Fatal("leader did not append")
		}
	}
	require.Error(t, failed.Step(ctx, toFailed))
	require.Equal(t, uint64(0), failed.Leader())
	select {
	case sent := <-cluster.out:
		t.Fatalf("failed replica sent %s after storage failure", sent.GetType())
	case err := <-reply:
		t.Fatalf("leader returned before a durable majority: %v", err)
	default:
	}
	require.NoError(t, cluster.groups[3].Step(ctx, toHealthy))
	for {
		select {
		case message := <-cluster.out:
			if message.GetTo() != 2 {
				cluster.deliver(message)
			}
		case err := <-reply:
			require.NoError(t, err)
			return
		case <-ctx.Done():
			t.Fatal("leader did not commit with a durable majority")
		}
	}
}

func newTestCluster(t *testing.T) *testCluster {
	c := &testCluster{t: t, dir: t.TempDir(), groups: make(map[uint64]*Group), out: make(chan *raftpb.Message, 4096)}
	t.Cleanup(func() {
		for _, group := range c.groups {
			_ = group.Close()
		}
	})
	return c
}
func (c *testCluster) open(id uint64, peers []uint64) *Group {
	c.t.Helper()
	group, err := Open(Config{
		ID: id, GroupID: 1, Dir: filepath.Join(c.dir, string(rune('a'+id))), Peers: peers, Machine: &testMachine{},
		TickInterval: -1, Send: func(msg *raftpb.Message) error { c.out <- msg; return nil },
	})
	require.NoError(c.t, err)
	c.groups[id] = group
	return group
}
func (c *testCluster) deliver(message *raftpb.Message) {
	c.t.Helper()
	if group := c.groups[message.GetTo()]; group != nil {
		require.NoError(c.t, group.Step(context.Background(), message))
	}
	if message.GetType() == raftpb.MsgSnap {
		require.NoError(c.t, c.groups[message.GetFrom()].ReportSnapshot(context.Background(), message.GetTo(), c.groups[message.GetTo()] != nil))
	}
}
func (c *testCluster) drain() {
	c.t.Helper()
	for range 4096 {
		select {
		case message := <-c.out:
			c.deliver(message)
		default:
			return
		}
	}
	c.t.Fatal("Raft transport did not quiesce")
}
func (c *testCluster) commit(apply func() error) error {
	c.t.Helper()
	done := make(chan response, 1)
	go func() { done <- response{err: apply()} }()
	return c.await(done).err
}

func (c *testCluster) await(done <-chan response) response {
	c.t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		select {
		case message := <-c.out:
			c.deliver(message)
		case result := <-done:
			c.drain()
			return result
		case <-timer.C:
			c.t.Fatal("command did not commit")
		}
	}
}
