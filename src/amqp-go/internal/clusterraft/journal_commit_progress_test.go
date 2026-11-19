package clusterraft

import (
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

type journalCommitFenceFile struct {
	*os.File
	syncs atomic.Int64
	end   atomic.Int64
	gate  *journalGroupSyncGate
}

func (f *journalCommitFenceFile) Sync() error {
	var err error
	if f.gate != nil {
		err = f.gate.Sync()
	} else {
		err = syncJournalFile(f.File)
	}
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		return err
	}
	f.end.Store(info.Size())
	f.syncs.Add(1)
	return nil
}

func openJournalCommitFixture(t *testing.T, ctx context.Context) (*Group, *journalCommitFenceFile) {
	t.Helper()
	dir := t.TempDir()
	file := &journalCommitFenceFile{}
	node := openTestJournalGroupNode(t, dir, func(underlying *os.File) journalFile {
		file.File = underlying
		return file
	})
	group := openTestJournalGroup(t, node, Config{
		ID: 1, GroupID: 11, Dir: filepath.Join(dir, "group-11"),
		Peers: []uint64{1}, Machine: &testMachine{}, TickInterval: -1, DeferJournalCommit: true,
	})
	require.NoError(t, group.Campaign(ctx))
	require.Equal(t, uint64(1), group.Leader())
	return group, file
}

func proposeWithOneJournalFence(t *testing.T, ctx context.Context, group *Group, file *journalCommitFenceFile, data string) uint64 {
	t.Helper()
	before := file.syncs.Load()
	result, err := group.Propose(ctx, []byte(data))
	require.NoError(t, err)
	require.Equal(t, []byte("applied "+data), result)
	status := group.Status()
	require.Equal(t, status.Commit, status.Applied)
	require.Equal(t, int64(1), file.syncs.Load()-before, "commit-only progress must not issue a standalone journal sync")
	return status.Commit
}

// Copy the stable prefix while the live writer is still open. A clean close
// can publish more state than a power loss after the last successful sync.
func openJournalCommitCrashImage(t *testing.T, source *Group, file *journalCommitFenceFile) *Group {
	t.Helper()
	dir := t.TempDir()
	journal, err := os.ReadFile(file.Name())
	require.NoError(t, err)
	end := file.end.Load()
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
	node := openTestJournalGroupNode(t, dir, nil)
	return openTestJournalGroup(t, node, Config{
		ID: source.cfg.ID, GroupID: source.cfg.GroupID, Dir: filepath.Join(dir, "group-11"),
		Machine: &testMachine{}, TickInterval: -1, DeferJournalCommit: source.cfg.DeferJournalCommit,
	})
}

func TestJournalCommitProgressSkipsStandaloneSyncAndReplaysDurableLog(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	group, file := openJournalCommitFixture(t, ctx)
	previous := group.Status().Commit
	confirmed := proposeWithOneJournalFence(t, ctx, group, file, "confirmed body")
	require.Equal(t, previous+1, confirmed)

	reopened := openJournalCommitCrashImage(t, group, file)
	require.Equal(t, previous, reopened.Status().Commit, "an absent commit marker must not be invented on disk")
	last, err := reopened.store.LastIndex()
	require.NoError(t, err)
	require.Equal(t, confirmed, last, "confirmation must have persisted the actual log entry")
	queried := false
	_, err = reopened.Read(ctx, func(Machine) ([]byte, error) {
		queried = true
		return nil, nil
	})
	require.ErrorIs(t, err, ErrNotLeader)
	require.False(t, queried, "restart must not serve a read before electing a leader in a new term")
	require.NoError(t, reopened.Campaign(ctx))
	snapshot, err := reopened.MachineSnapshot(ctx)
	require.NoError(t, err)
	require.JSONEq(t, `["confirmed body"]`, string(snapshot))
}

func TestJournalCommitProgressFoldsDebtBeforeNextWriteAndCheckpoint(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	group, file := openJournalCommitFixture(t, ctx)
	first := proposeWithOneJournalFence(t, ctx, group, file, "first body")
	second := proposeWithOneJournalFence(t, ctx, group, file, "second body")
	require.Equal(t, first+1, second)

	beforeCheckpoint := openJournalCommitCrashImage(t, group, file)
	require.Equal(t, first, beforeCheckpoint.Status().Commit, "the next durable Ready must fold the previous commit floor into its write")
	snapshot, err := beforeCheckpoint.MachineSnapshot(ctx)
	require.NoError(t, err)
	require.JSONEq(t, `["first body"]`, string(snapshot))

	require.NoError(t, group.Snapshot(ctx))
	checkpoint := openJournalCommitCrashImage(t, group, file)
	status := checkpoint.Status()
	require.Equal(t, second, status.Commit, "the checkpoint must persist the commit floor for its trimmed prefix")
	require.Equal(t, second, status.Applied)
	firstIndex, err := checkpoint.store.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, second+1, firstIndex, "the replayed checkpoint must cover the trimmed prefix")
	snapshot, err = checkpoint.MachineSnapshot(ctx)
	require.NoError(t, err)
	require.JSONEq(t, `["first body", "second body"]`, string(snapshot))
}

func TestJournalSettlementAndPublishShareDurableWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	gate := &journalGroupSyncGate{started: make(chan struct{}, 1), release: make(chan struct{}, 1)}
	defer close(gate.release)
	file := &journalCommitFenceFile{gate: gate}
	node := openTestJournalGroupNode(t, t.TempDir(), func(underlying *os.File) journalFile {
		file.File, gate.File = underlying, underlying
		return file
	})
	group := openTestJournalGroup(t, node, Config{
		ID: 1, GroupID: 11, Dir: filepath.Join(t.TempDir(), "group-11"),
		Peers: []uint64{1}, Machine: &testMachine{}, TickInterval: -1, DeferJournalCommit: true,
		GatherProposal: func(data []byte) time.Duration {
			switch string(data) {
			case "settle":
				return time.Hour
			case "lone settle":
				return 125 * time.Microsecond
			}
			return 0
		},
	})
	require.NoError(t, group.Campaign(ctx))
	before := file.syncs.Load()
	gate.enabled.Store(true)
	settlement := startJournalGatherProposal(ctx, group, "settle")
	waitJournalGatherAdmission(t, group)
	publish := startJournalGatherProposal(ctx, group, "publish")
	select {
	case <-gate.started:
	case <-ctx.Done():
		t.Fatal("the journal write did not reach the durability fence")
	}
	for _, outcome := range []<-chan response{settlement, publish} {
		select {
		case got := <-outcome:
			t.Fatalf("proposal completed before the journal fence: %+v", got)
		default:
		}
	}
	gate.enabled.Store(false)
	gate.release <- struct{}{}
	for i, outcome := range []<-chan response{settlement, publish} {
		got := awaitTestReadResult(t, ctx, outcome)
		require.NoError(t, got.err)
		require.Equal(t, []byte([]string{"applied settle", "applied publish"}[i]), got.body)
	}
	require.Equal(t, int64(1), file.syncs.Load()-before, "independent settlement and publish should share one durable write")
	image := openJournalCommitCrashImage(t, group, file)
	require.NoError(t, image.Campaign(ctx))
	body, err := image.MachineSnapshot(ctx)
	require.NoError(t, err)
	require.JSONEq(t, `["settle", "publish"]`, string(body))

	body, err = group.Propose(ctx, []byte("lone settle"))
	require.NoError(t, err)
	require.Equal(t, []byte("applied lone settle"), body)
	require.Equal(t, int64(2), file.syncs.Load()-before, "the idle timer must flush a standalone settlement")

	settlement = startJournalGatherProposal(ctx, group, "settle")
	waitJournalGatherAdmission(t, group)
	require.NoError(t, group.Snapshot(ctx))
	require.NoError(t, awaitTestReadResult(t, ctx, settlement).err)
	image = openJournalCommitCrashImage(t, group, file)
	body, err = image.MachineSnapshot(ctx)
	require.NoError(t, err)
	require.JSONEq(t, `["settle", "publish", "lone settle", "settle"]`, string(body))

	settlement = startJournalGatherProposal(ctx, group, "settle")
	waitJournalGatherAdmission(t, group)
	beforeClose := file.syncs.Load()
	require.NoError(t, group.Close())
	require.ErrorIs(t, awaitTestReadResult(t, ctx, settlement).err, ErrLeadershipLost)
	require.Equal(t, beforeClose, file.syncs.Load(), "the stopped group must discard the gathered proposal")
}

func TestJournalSettlementAndPublishShareFollowerWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cluster := newTestCluster(t)
	leaderNode := openTestJournalGroupNode(t, t.TempDir(), nil)
	leader := openJournalQuorumMember(t, cluster, leaderNode, 1, []uint64{1, 2})
	leader.cfg.GatherProposal = func(data []byte) time.Duration {
		if string(data) == "settle" {
			return time.Hour
		}
		return 0
	}
	gate := &journalGroupSyncGate{started: make(chan struct{}, 1), release: make(chan struct{}, 1)}
	defer close(gate.release)
	file := &journalCommitFenceFile{gate: gate}
	followerNode := openTestJournalGroupNode(t, t.TempDir(), func(underlying *os.File) journalFile {
		file.File, gate.File = underlying, underlying
		return file
	})
	follower := openJournalQuorumMember(t, cluster, followerNode, 2, []uint64{1, 2})
	require.NoError(t, leader.Campaign(ctx))
	cluster.drain()
	require.Equal(t, uint64(1), leader.Leader())
	before := file.syncs.Load()

	settlement := startJournalGatherProposal(ctx, leader, "settle")
	waitJournalGatherAdmission(t, leader)
	publish := startJournalGatherProposal(ctx, leader, "publish")
	appendMessage := awaitJournalQuorumMessage(t, ctx, cluster.out, settlement, func(message *pb.Message) bool {
		return message.GetType() == pb.MsgApp && message.GetFrom() == 1 &&
			message.GetTo() == 2 && len(message.GetEntries()) != 0
	})
	entries := appendMessage.GetEntries()
	require.Len(t, entries, 2, "an idle follower should receive both commands in one Raft append")
	require.Equal(t, []byte("settle"), entries[0].GetData()[len(key{}):])
	require.Equal(t, []byte("publish"), entries[1].GetData()[len(key{}):])
	require.NoError(t, waitTestLocalWrites(ctx, leader))
	gate.enabled.Store(true)
	require.NoError(t, follower.Step(ctx, appendMessage))
	select {
	case <-gate.started:
	case <-ctx.Done():
		t.Fatal("the follower did not reach its journal fence")
	}
	for _, outcome := range []<-chan response{settlement, publish} {
		select {
		case got := <-outcome:
			t.Fatalf("proposal completed before the follower fence: %+v", got)
		default:
		}
	}
	select {
	case message := <-cluster.out:
		t.Fatalf("follower sent %v with its journal fence held", message)
	default:
	}
	gate.enabled.Store(false)
	gate.release <- struct{}{}
	reply := awaitJournalQuorumMessage(t, ctx, cluster.out, settlement, func(message *pb.Message) bool {
		return message.GetType() == pb.MsgAppResp && message.GetFrom() == 2 && !message.GetReject()
	})
	require.Equal(t, entries[1].GetIndex(), reply.GetIndex())
	require.Equal(t, before+1, file.syncs.Load())
	require.NoError(t, leader.Step(ctx, reply))
	for i, outcome := range []<-chan response{settlement, publish} {
		got := awaitTestReadResult(t, ctx, outcome)
		require.NoError(t, got.err)
		require.Equal(t, []byte([]string{"applied settle", "applied publish"}[i]), got.body)
	}
}

func TestJournalSettlementVectorRejectionKeepsPerCommandAdmission(t *testing.T) {
	for _, tc := range []struct {
		name        string
		prefixBytes int
		firstFits   bool
	}{
		{name: "only settlement fits", prefixBytes: raftAppendMessageBytes - 64, firstFits: true},
		{name: "neither fits", prefixBytes: raftAppendMessageBytes - 16},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			cluster := newTestCluster(t)
			for _, id := range []uint64{1, 2} {
				node := openTestJournalGroupNode(t, t.TempDir(), nil)
				cluster.groups[id] = openTestJournalGroup(t, node, Config{
					ID: id, GroupID: 11, Dir: filepath.Join(node.dir, "group-11"),
					Peers: []uint64{1, 2}, Machine: &testMachine{}, TickInterval: -1, DeferJournalCommit: true,
					MaxEntryBytes: raftAppendMessageBytes / 4, Send: func(message *pb.Message) error { cluster.out <- message; return nil },
					GatherProposal: func(data []byte) time.Duration {
						if data[0] == 's' {
							return time.Hour
						}
						return 0
					},
				})
			}
			leader := cluster.groups[1]
			require.NoError(t, leader.Campaign(ctx))
			cluster.drain()
			require.Equal(t, uint64(1), leader.Leader())
			applied := leader.Status().Applied

			// Fill the uncommitted allowance without delivering
			// appends to the second voter. Both next commands charge 40 bytes.
			for i := range 8 {
				data := append(make([]byte, len(key{})), strings.Repeat("d", tc.prefixBytes/8-len(key{}))...)
				data[0] = byte(i + 1)
				require.NoError(t, leader.change(ctx, func() error { return leader.node.Propose(data) }))
				require.NoError(t, waitTestLocalWrites(ctx, leader))
			}
			settlement := strings.Repeat("s", 24)
			settled := startJournalGatherProposal(ctx, leader, settlement)
			waitJournalGatherAdmission(t, leader)
			_, err := leader.Propose(ctx, []byte(strings.Repeat("p", 24)))
			require.ErrorIs(t, err, ErrBusy)
			if tc.firstFits {
				got := cluster.await(settled)
				require.NoError(t, got.err)
				require.Equal(t, []byte("applied "+settlement), got.body)
				require.Equal(t, applied+9, leader.Status().Applied)
			} else {
				require.ErrorIs(t, awaitTestReadResult(t, ctx, settled).err, ErrBusy)
				require.Equal(t, applied, leader.Status().Applied)
			}
		})
	}
}

func startJournalGatherProposal(ctx context.Context, group *Group, data string) <-chan response {
	outcome := make(chan response, 1)
	go func() {
		body, err := group.Propose(ctx, []byte(data))
		outcome <- response{body: body, err: err}
	}()
	return outcome
}

func waitJournalGatherAdmission(t *testing.T, group *Group) {
	t.Helper()
	require.Eventually(t, func() bool {
		group.mu.Lock()
		defer group.mu.Unlock()
		return len(group.pending) == 1
	}, time.Second, time.Millisecond)
}

func TestJournalCommitProgressVoteResponseWaitsForTermVoteSync(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	group, file, sent := openJournalCommitFollower(t)
	gate := file.gate
	defer close(gate.release)
	gate.enabled.Store(true)
	require.NoError(t, group.Step(ctx, &pb.Message{
		Type: pb.MsgVote.Enum(), From: new(uint64(1)), To: new(uint64(2)),
		Term: new(uint64(2)), Index: new(uint64(1)), LogTerm: new(uint64(1)),
	}))
	reply := awaitJournalCommitResponseFence(t, ctx, gate, sent)
	require.Equal(t, pb.MsgVoteResp, reply.GetType())
	require.Equal(t, uint64(2), reply.GetTerm())
	require.False(t, reply.GetReject())
	require.NoError(t, waitTestLocalWrites(ctx, group))
	reopened := openJournalCommitCrashImage(t, group, file)
	hard, _, err := reopened.store.InitialState()
	require.NoError(t, err)
	require.Equal(t, uint64(2), hard.GetTerm())
	require.Equal(t, uint64(1), hard.GetVote())
}

func TestJournalCommitProgressAppendResponseWaitsForCommitOnlySync(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	group, file, sent := openJournalCommitFollower(t)
	gate := file.gate
	defer close(gate.release)
	require.NoError(t, group.Step(ctx, &pb.Message{
		Type: pb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(2)),
		Index: new(uint64(1)), LogTerm: new(uint64(1)), Commit: new(uint64(1)),
		Entries: []*pb.Entry{{
			Type: pb.EntryNormal.Enum(), Term: new(uint64(2)), Index: new(uint64(2)),
			Data: append(make([]byte, len(key{})), []byte("replicated body")...),
		}},
	}))
	initial := awaitJournalCommitResponse(t, ctx, sent)
	require.Equal(t, pb.MsgAppResp, initial.GetType())
	require.Equal(t, uint64(2), initial.GetIndex())
	require.False(t, initial.GetReject())
	require.NoError(t, waitTestLocalWrites(ctx, group))
	require.Equal(t, uint64(1), group.Status().Commit)

	gate.enabled.Store(true)
	require.NoError(t, group.Step(ctx, &pb.Message{
		Type: pb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(2)), Term: new(uint64(2)),
		Index: new(uint64(2)), LogTerm: new(uint64(2)), Commit: new(uint64(2)),
	}))
	reply := awaitJournalCommitResponseFence(t, ctx, gate, sent)
	require.Equal(t, pb.MsgAppResp, reply.GetType())
	require.Equal(t, uint64(2), reply.GetIndex())
	require.False(t, reply.GetReject())
	require.NoError(t, waitTestLocalWrites(ctx, group))
	reopened := openJournalCommitCrashImage(t, group, file)
	require.Equal(t, uint64(2), reopened.Status().Commit)
	snapshot, err := reopened.MachineSnapshot(ctx)
	require.NoError(t, err)
	require.JSONEq(t, `["replicated body"]`, string(snapshot))
}

func openJournalCommitFollower(t *testing.T) (*Group, *journalCommitFenceFile, <-chan *pb.Message) {
	t.Helper()
	dir := t.TempDir()
	gate := &journalGroupSyncGate{started: make(chan struct{}, 1), release: make(chan struct{}, 1)}
	file := &journalCommitFenceFile{gate: gate}
	node := openTestJournalGroupNode(t, dir, func(underlying *os.File) journalFile {
		file.File = underlying
		gate.File = underlying
		return file
	})
	sent := make(chan *pb.Message, 4)
	group := openTestJournalGroup(t, node, Config{
		ID: 2, GroupID: 11, Dir: filepath.Join(dir, "group-11"),
		Peers: []uint64{1, 2, 3}, Machine: &testMachine{}, TickInterval: -1, DeferJournalCommit: true,
		Send: func(message *pb.Message) error {
			sent <- message
			return nil
		},
	})
	return group, file, sent
}

func awaitJournalCommitResponseFence(t *testing.T, ctx context.Context, gate *journalGroupSyncGate, sent <-chan *pb.Message) *pb.Message {
	t.Helper()
	select {
	case <-gate.started:
	case response := <-sent:
		t.Fatalf("raft released %v without a durable journal fence", response)
	case <-ctx.Done():
		t.Fatal("raft did not reach the expected journal fence")
	}
	select {
	case response := <-sent:
		t.Fatalf("raft released %v while the durable journal fence was held", response)
	default:
	}
	gate.enabled.Store(false)
	gate.release <- struct{}{}
	return awaitJournalCommitResponse(t, ctx, sent)
}

func awaitJournalCommitResponse(t *testing.T, ctx context.Context, sent <-chan *pb.Message) *pb.Message {
	t.Helper()
	select {
	case reply := <-sent:
		return reply
	case <-ctx.Done():
		t.Fatal("raft did not send the expected response")
		return nil
	}
}
