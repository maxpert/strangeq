package clusterraft

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
)

func openInlineReadGroup(t *testing.T, campaign bool) (*journalGroupNode, *Group) {
	t.Helper()
	dir := t.TempDir()
	node := openPrototypeProcessNode(t, dir, "inline-read-kernel", true, true, nil)
	group := openTestJournalGroup(t, node, Config{ID: 1, GroupID: 11, Dir: filepath.Join(dir, "group"), Peers: []uint64{1}, Machine: &testMachine{}, TickInterval: -1, ProcessDurability: true, DeferJournalCommit: true})
	if campaign {
		require.NoError(t, group.Campaign(t.Context()))
		require.NoError(t, waitTestLocalWrites(t.Context(), group))
	}
	return node, group
}

func requireInlineReadFallback(t *testing.T, g *Group) {
	t.Helper()
	called := false
	_, handled, err := g.tryProcessSingletonRead(t.Context(), func(Machine) ([]byte, error) { called = true; return nil, nil })
	require.False(t, handled)
	require.NoError(t, err)
	require.False(t, called)
}

func TestProcessSingletonReadRunsInlineWithoutFence(t *testing.T) {
	_, g := openInlineReadGroup(t, true)
	_, err := g.Propose(t.Context(), []byte("committed"))
	require.NoError(t, err)
	require.NoError(t, waitTestLocalWrites(t.Context(), g))
	before := testJournalEnd(g)
	calls := 0
	body, err := g.Read(t.Context(), func(m Machine) ([]byte, error) {
		calls++
		require.Zero(t, g.readFence)
		require.Empty(t, g.reads)
		require.Zero(t, g.readTerm)
		return m.Snapshot()
	})
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	require.JSONEq(t, `["committed"]`, string(body))
	require.Equal(t, before, testJournalEnd(g))
	_, err = g.Read(t.Context(), nil)
	require.Error(t, err)
}

func TestProcessSingletonReadWaitsForElectionAndCommittedApply(t *testing.T) {
	_, g := openInlineReadGroup(t, false)
	requireInlineReadFallback(t, g)
	// Elect without driving storage/application. The newly elected leader has
	// no persisted, applied current-term proof yet.
	g.mu.Lock()
	campaignErr := g.node.Campaign()
	g.mu.Unlock()
	require.NoError(t, campaignErr)
	requireInlineReadFallback(t, g)
	require.NoError(t, waitTestLocalWrites(t.Context(), g))
	_, handled, err := g.tryProcessSingletonRead(t.Context(), func(Machine) ([]byte, error) { return []byte("current term"), nil })
	require.True(t, handled)
	require.NoError(t, err)
	_, err = g.Propose(t.Context(), []byte("new commit"))
	require.NoError(t, err)
	require.NoError(t, waitTestLocalWrites(t.Context(), g))
	g.mu.Lock()
	applied := g.applied
	g.applied--
	behind, committed := g.applied, g.node.BasicStatus().GetCommit()
	g.mu.Unlock()
	require.Less(t, behind, committed)
	requireInlineReadFallback(t, g)
	g.mu.Lock()
	g.applied = applied
	base := g.store.state.snapshot.metadata.GetIndex()
	ref := &g.store.state.entries[applied-base-1]
	term := ref.term
	ref.term = term - 1
	g.mu.Unlock()
	requireInlineReadFallback(t, g)
	g.mu.Lock()
	ref.term = term
	g.mu.Unlock()
}

func TestProcessSingletonReadOrderingAndMembershipFallback(t *testing.T) {
	_, g := openInlineReadGroup(t, true)
	cases := map[string]func() func(){
		"gathered": func() func() {
			old := g.gatheredProposal
			g.gatheredProposal = []byte("pending")
			return func() { g.gatheredProposal = old }
		},
		"exclusive": func() func() { g.exclusiveWaiters++; return func() { g.exclusiveWaiters-- } },
		"configuration": func() func() {
			g.configuration = configProposal{index: 99}
			return func() { g.configuration = configProposal{} }
		},
		"read-fence":    func() func() { g.readFence = readFence{issued: true}; return func() { g.readFence = readFence{} } },
		"retired-fence": func() func() { g.retiredRead = &key{1}; return func() { g.retiredRead = nil } },
		"read-waiter":   func() func() { g.reads = map[key]*raftRead{{1}: {}}; return func() { g.reads = nil } },
		"read-term":     func() func() { g.readTerm = 99; return func() { g.readTerm = 0 } },
		"voters": func() func() {
			old := g.conf
			g.conf = &pb.ConfState{Voters: []uint64{1, 2}}
			return func() { g.conf = old }
		},
		"other-voter": func() func() {
			old := g.conf
			g.conf = &pb.ConfState{Voters: []uint64{2}}
			return func() { g.conf = old }
		},
		"learner": func() func() {
			old := g.conf
			g.conf = &pb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}
			return func() { g.conf = old }
		},
		"learners-next": func() func() {
			old := g.conf
			g.conf = &pb.ConfState{Voters: []uint64{1}, LearnersNext: []uint64{2}}
			return func() { g.conf = old }
		},
		"outgoing": func() func() {
			old := g.conf
			g.conf = &pb.ConfState{Voters: []uint64{1}, VotersOutgoing: []uint64{1}}
			return func() { g.conf = old }
		},
		"auto-leave": func() func() {
			old := g.conf
			g.conf = &pb.ConfState{Voters: []uint64{1}, AutoLeave: new(true)}
			return func() { g.conf = old }
		},
		"strict-config":  func() func() { g.cfg.ProcessDurability = false; return func() { g.cfg.ProcessDurability = true } },
		"wrong-local-id": func() func() { g.cfg.ID = 2; return func() { g.cfg.ID = 1 } },
	}
	for name, alter := range cases {
		t.Run(name, func(t *testing.T) {
			g.mu.Lock()
			undo := alter()
			g.mu.Unlock()
			defer func() { g.mu.Lock(); undo(); g.mu.Unlock() }()
			requireInlineReadFallback(t, g)
		})
	}
}

func TestProcessSingletonReadCancellationCloseAndStorageFault(t *testing.T) {
	t.Run("canceled", func(t *testing.T) {
		_, g := openInlineReadGroup(t, true)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		called := false
		_, err := g.Read(ctx, func(Machine) ([]byte, error) { called = true; return nil, nil })
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, called)
	})
	t.Run("closed", func(t *testing.T) {
		_, g := openInlineReadGroup(t, true)
		require.NoError(t, g.Close())
		called := false
		_, err := g.Read(t.Context(), func(Machine) ([]byte, error) { called = true; return nil, nil })
		require.ErrorIs(t, err, ErrStopped)
		require.False(t, called)
	})
	t.Run("typed-fault", func(t *testing.T) {
		_, g := openInlineReadGroup(t, true)
		calls := 0
		_, err := g.Read(t.Context(), func(Machine) ([]byte, error) { calls++; panic(&raftStoreReadError{io.ErrUnexpectedEOF}) })
		require.ErrorIs(t, err, ErrStopped)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		require.Equal(t, 1, calls)
		select {
		case <-g.Done():
		default:
			t.Fatal("storage fault did not stop group")
		}
	})
}

func TestProcessSingletonReadPromotionUsesOriginalFence(t *testing.T) {
	node, g := openInlineReadGroup(t, true)
	require.NoError(t, node.writer.requireFullDurability())
	requireInlineReadFallback(t, g)
	calls := 0
	_, err := g.Read(t.Context(), func(Machine) ([]byte, error) {
		calls++
		require.True(t, g.readFence.issued, "withdrawn shared latch must retain original ReadIndex path")
		return nil, nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, calls)
}

func TestProcessSingletonReadQuorumFallback(t *testing.T) {
	cluster := newTestCluster(t)
	for _, id := range []uint64{1, 2, 3} {
		disk := &testJournalDisk{}
		node, err := openSegmentedGroupNode(filepath.Join(cluster.dir, fmt.Sprint(id)), true, false, journalServingExtentBytes, journalGroupMaxFileBytes, nil, journalSegmentHooks{writeAt: disk.writeAt})
		require.NoError(t, err)
		// Install the immutable shared latch before the first group write starts.
		node.writer.durability = &journalDurability{}
		group, err := node.openGroup(Config{ID: id, GroupID: 1, Dir: filepath.Join(cluster.dir, fmt.Sprint(id)), Peers: []uint64{1, 2, 3}, Machine: &testMachine{}, TickInterval: -1, ProcessDurability: true, Send: func(message *pb.Message) error { cluster.out <- message; return nil }})
		require.NoError(t, err)
		group.store.closeOwner = true
		disk.node = node
		testJournalDisks.Store(group, disk)
		t.Cleanup(func() { testJournalDisks.Delete(group) })
		cluster.groups[id] = group
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
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
	cluster.drain()
	require.NoError(t, cluster.commit(func() error { _, err := leader.Propose(ctx, []byte("applied before read")); return err }))
	require.NoError(t, waitTestLocalWrites(ctx, leader))
	cluster.drain()
	runs := &atomic.Int32{}
	result := startTestRead(ctx, leader, runs)
	heartbeat := awaitReadTestHeartbeat(t, ctx, cluster, result)
	requireReadStillPending(t, result, runs)
	answerReadTestHeartbeat(t, ctx, cluster, heartbeat)
	require.NoError(t, awaitTestReadResult(t, ctx, result).err)
	require.Equal(t, int32(1), runs.Load())
}

func TestProcessSingletonCapturedReadKeepsPinAndCloseWait(t *testing.T) {
	_, g := openInlineReadGroup(t, true)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	done := make(chan response, 1)
	go func() {
		body, err := g.ReadCaptured(ctx, func(Machine) (func() ([]byte, error), int, error) {
			require.Zero(t, g.readFence)
			return func() ([]byte, error) { close(entered); <-release; return []byte("captured"), nil }, captureJournalReplyBytes, nil
		})
		done <- response{body: body, err: err}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("captured encoder never started")
	}
	_, err := g.Propose(ctx, []byte("while encoding"))
	require.NoError(t, err)
	closed := make(chan error, 1)
	go func() { closed <- g.Close() }()
	select {
	case <-g.Done():
	case <-ctx.Done():
		t.Fatal("close did not start")
	}
	select {
	case <-closed:
		t.Fatal("close returned before captured encoder released its pin")
	default:
	}
	close(release)
	select {
	case got := <-done:
		require.NoError(t, got.err)
		require.Equal(t, []byte("captured"), got.body)
	case <-ctx.Done():
		t.Fatal("captured result did not complete")
	}
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("close did not finish after capture")
	}
}
