package clusterraft

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestAsyncRaftReplicatesWhileLeaderWriteIsBlocked(t *testing.T) {
	cluster := newTestCluster(t)
	for _, id := range []uint64{1, 2, 3} {
		cluster.open(id, []uint64{1, 2, 3})
	}
	leader, follower := cluster.groups[1], cluster.groups[2]
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, leader.Campaign(ctx))
	for leader.Leader() != 1 {
		select {
		case message := <-cluster.out:
			cluster.deliver(message)
		case <-ctx.Done():
			t.Fatal("leader was not elected")
		}
	}
	require.NoError(t, cluster.commit(func() error {
		_, err := leader.Propose(ctx, []byte("fixture fence"))
		return err
	}))

	command := []byte("replicate before local persistence")
	var target atomic.Uint64
	type diskObservation struct {
		first, second []byte
		err           error
	}
	followerAck := make(chan diskObservation, 1)
	var firstAck sync.Once
	follower.mu.Lock()
	send := follower.cfg.Send
	follower.cfg.Send = func(message *pb.Message) error {
		if index := target.Load(); index != 0 && message.GetType() == pb.MsgAppResp &&
			message.GetTo() == 1 && message.GetIndex() >= index && !message.GetReject() {
			firstAck.Do(func() {
				data, err := readAsyncTestEntry(testJournalDiskFor(follower), index)
				followerAck <- diskObservation{first: data, err: err}
			})
		}
		return send(message)
	}
	follower.mu.Unlock()

	leaderApply := make(chan diskObservation, 1)
	var firstApply sync.Once
	leader.mu.Lock()
	leader.cfg.Machine = &asyncTestApplyObserver{Machine: leader.cfg.Machine, observe: func(data []byte) {
		if bytes.Equal(data, command) {
			firstApply.Do(func() {
				index := target.Load()
				leaderData, err := readAsyncTestEntry(testJournalDiskFor(leader), index)
				if err != nil {
					leaderApply <- diskObservation{err: err}
					return
				}
				followerData, err := readAsyncTestEntry(testJournalDiskFor(follower), index)
				leaderApply <- diskObservation{first: leaderData, second: followerData, err: err}
			})
		}
	}}
	leader.mu.Unlock()

	// Block both nodes at their real segmented extent write boundary before
	// either append can reach the journal durability fence.
	releaseLeader, leaderEntered := holdAsyncTestWrite(t, testJournalDiskFor(leader))
	releaseFollower, followerEntered := holdAsyncTestWrite(t, testJournalDiskFor(follower))
	result := make(chan response, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		body, err := leader.Propose(ctx, command)
		result <- response{body: body, err: err}
	}()
	<-started

	var msg *pb.Message
	var proposal *pb.Entry
	for proposal == nil {
		select {
		case out := <-cluster.out:
			if out.GetType() != pb.MsgApp || out.GetFrom() != 1 || out.GetTo() != 2 {
				continue
			}
			for _, entry := range out.GetEntries() {
				if len(entry.GetData()) >= len(key{}) && bytes.Equal(entry.GetData()[len(key{}):], command) {
					msg, proposal = out, entry
					break
				}
			}
		case <-ctx.Done():
			t.Fatal("leader replication must not wait for its local storage")
		}
	}
	index := proposal.GetIndex()
	target.Store(index)
	want, err := proto.Marshal(proposal)
	require.NoError(t, err)
	awaitAsyncTestWrite(t, ctx, leaderEntered)
	data, err := readAsyncTestEntry(testJournalDiskFor(leader), index)
	require.NoError(t, err)
	require.Empty(t, data, "leader's append must still be blocked when replication leaves")

	stepAsyncTestGroup(t, ctx, follower, msg)
	awaitAsyncTestWrite(t, ctx, followerEntered)
	data, err = readAsyncTestEntry(testJournalDiskFor(follower), index)
	require.NoError(t, err)
	require.Empty(t, data, "follower's real extent write must be blocked before releasing its durability fence")
	select {
	case reply := <-result:
		t.Fatalf("peer receipt completed an undurable proposal: %+v", reply)
	default:
	}
	releaseFollower()

	var ack *pb.Message
	for ack == nil {
		select {
		case out := <-cluster.out:
			if out.GetType() == pb.MsgAppResp && out.GetFrom() == 2 && out.GetTo() == 1 &&
				out.GetIndex() >= index && !out.GetReject() {
				ack = out
			}
		case <-ctx.Done():
			t.Fatal("follower did not acknowledge after its write became durable")
		}
	}
	select {
	case observed := <-followerAck:
		require.NoError(t, observed.err)
		require.Equal(t, want, observed.first, "follower emitted a Raft acknowledgement before its durable append")
	case <-ctx.Done():
		t.Fatal("follower acknowledgement did not pass through the observation hook")
	}
	stepAsyncTestGroup(t, ctx, leader, ack)
	select {
	case reply := <-result:
		t.Fatalf("one durable replica completed the proposal: %+v", reply)
	default:
	}
	releaseLeader()

	select {
	case reply := <-result:
		require.NoError(t, reply.err)
		require.Equal(t, append([]byte("applied "), command...), reply.body)
	case <-ctx.Done():
		t.Fatal("a durable majority did not complete the proposal")
	}
	select {
	case observed := <-leaderApply:
		require.NoError(t, observed.err)
		require.Equal(t, want, observed.first, "leader applied before local durability")
		require.Equal(t, want, observed.second, "leader applied before a durable quorum existed")
	case <-ctx.Done():
		t.Fatal("proposal returned without applying at the leader")
	}
}

func TestAsyncRaftCloseReleasesProposalBeforeJoiningBlockedWriter(t *testing.T) {
	cluster := newTestCluster(t)
	for _, id := range []uint64{1, 2, 3} {
		cluster.open(id, []uint64{1, 2, 3})
	}
	leader := cluster.groups[1]
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, leader.Campaign(ctx))
	cluster.drain()
	require.Equal(t, uint64(1), leader.Leader())
	require.NoError(t, cluster.commit(func() error {
		_, err := leader.Propose(ctx, []byte("fixture fence"))
		return err
	}))

	release, entered := holdAsyncTestWrite(t, testJournalDiskFor(leader))
	awaiting := make(chan error, 1)
	go func() {
		_, err := leader.Propose(ctx, []byte("pending during Close"))
		awaiting <- err
	}()
	var appendPending bool
	for !appendPending {
		select {
		case message := <-cluster.out:
			if message.GetType() == pb.MsgApp && message.GetFrom() == 1 {
				for _, entry := range message.GetEntries() {
					appendPending = appendPending || bytes.HasSuffix(entry.GetData(), []byte("pending during Close"))
				}
			}
		case <-ctx.Done():
			t.Fatal("blocked writer must leave a pending proposal before Close")
		}
	}
	awaitAsyncTestWrite(t, ctx, entered)
	closed := make(chan error, 1)
	go func() { closed <- leader.Close() }()
	select {
	case <-leader.Done():
	case <-ctx.Done():
		t.Fatal("Close could not stop the group while its writer was blocked")
	}
	select {
	case err := <-awaiting:
		require.ErrorIs(t, err, ErrLeadershipLost)
		require.NotErrorIs(t, err, context.DeadlineExceeded)
	case <-ctx.Done():
		t.Fatal("Close did not release the pending proposal before joining the writer")
	}
	select {
	case err := <-closed:
		t.Fatalf("Close completed before the writer could release its store: %v", err)
	default:
	}
	release()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("Close did not finish after the write blocker was released")
	}
}

func TestAsyncRaftExclusiveWaitReservesAdmissionUntilCanceled(t *testing.T) {
	cluster := newTestCluster(t)
	leader := cluster.open(1, []uint64{1})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, leader.Campaign(ctx))
	require.Equal(t, uint64(1), leader.Leader())

	_, entered := holdAsyncTestWrite(t, testJournalDiskFor(leader))
	// Submit a no-op through the normal RawNode admission path. Synchronous
	// campaign is finished, and the new append cannot finish under the lease.
	require.NoError(t, leader.change(ctx, func() error { return leader.node.Propose(nil) }))
	awaitAsyncTestWrite(t, ctx, entered)
	leader.mu.Lock()
	appendJob := leader.appending
	leader.mu.Unlock()
	require.NotNil(t, appendJob, "exclusive control must enter the active writer wait")

	snapshotCtx, cancelSnapshot := context.WithCancel(ctx)
	defer cancelSnapshot()
	syncWait := &asyncTestWaitContext{Context: snapshotCtx, waiting: make(chan struct{})}
	snapshotResult := make(chan error, 1)
	go func() { snapshotResult <- leader.Snapshot(syncWait) }()
	select {
	case <-syncWait.waiting:
	case <-ctx.Done():
		t.Fatal("snapshot did not enter its active-writer wait")
	}
	require.ErrorIs(t, leader.Tick(ctx), ErrBusy, "an exclusive waiter must reserve the next admission opportunity")
	cancelSnapshot()
	select {
	case err := <-snapshotResult:
		require.ErrorIs(t, err, context.Canceled)
	case <-ctx.Done():
		t.Fatal("canceling the snapshot did not release its waiter")
	}
	select {
	case <-appendJob.done:
		t.Fatal("the durable writer must remain blocked when ordinary admission resumes")
	default:
	}
	require.NoError(t, leader.Tick(ctx), "canceling the last exclusive waiter must restore ordinary admission")
}

type asyncTestWaitContext struct {
	context.Context
	observed sync.Once
	waiting  chan struct{}
}

func (ctx *asyncTestWaitContext) Done() <-chan struct{} {
	ctx.observed.Do(func() { close(ctx.waiting) })
	return ctx.Context.Done()
}

type asyncTestApplyObserver struct {
	Machine
	observe func([]byte)
}

func (m *asyncTestApplyObserver) Apply(data []byte) []byte {
	m.observe(data)
	return m.Machine.Apply(data)
}

func stepAsyncTestGroup(t *testing.T, ctx context.Context, group *Group, message *pb.Message) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- group.Step(ctx, message) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("accepting a peer message waited for blocked local persistence")
	}
}

func holdAsyncTestWrite(t *testing.T, disk *testJournalDisk) (func(), <-chan struct{}) {
	t.Helper()
	gate := &testJournalWriteGate{entered: make(chan struct{}), released: make(chan struct{})}
	disk.gate.Store(gate)
	var once sync.Once
	release := func() { once.Do(func() { disk.gate.Store(nil); close(gate.released) }) }
	t.Cleanup(release)
	return release, gate.entered
}

func awaitAsyncTestWrite(t *testing.T, ctx context.Context, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("storage worker did not enter the held extent write")
	}
}

func readAsyncTestEntry(disk *testJournalDisk, index uint64) ([]byte, error) {
	reader := disk.node.reader.(*journalSegmentReader)
	reader.store.mu.RLock()
	end := reader.store.durableEnd
	reader.store.mu.RUnlock()
	var data []byte
	_, err := scanJournal(reader, end, end, 1, func(records []journalRecord) error {
		for _, record := range records {
			delta, err := decodeJournalGroupOp(record)
			if err != nil {
				return err
			}
			for _, ref := range delta.entries {
				if ref.index == index {
					data = make([]byte, ref.size)
					if err := readJournalAt(reader, data, ref.offset); err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
	return data, err
}
