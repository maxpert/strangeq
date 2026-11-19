package clusterraft

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func openTestJournalGroupNode(t *testing.T, dir string, wrap func(*os.File) journalFile) *journalGroupNode {
	t.Helper()
	node, err := openJournalGroupNode(dir, wrap)
	require.NoError(t, err)
	t.Cleanup(func() { _ = node.close() })
	return node
}

func openTestJournalGroup(t *testing.T, node *journalGroupNode, config Config) *Group {
	t.Helper()
	group, err := node.openGroup(config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = group.Close() })
	return group
}

func TestJournalGroupRestartsFromCheckpointAndTailWithoutBolt(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	dir := t.TempDir()
	config := Config{
		ID: 1, GroupID: 11, Dir: filepath.Join(dir, "group-11"),
		Peers: []uint64{1}, Machine: &testMachine{}, TickInterval: -1,
	}
	node := openTestJournalGroupNode(t, dir, nil)
	group := openTestJournalGroup(t, node, config)
	require.NoError(t, group.Campaign(ctx))
	body, err := group.Propose(ctx, []byte("checkpoint body"))
	require.NoError(t, err)
	require.Equal(t, []byte("applied checkpoint body"), body)
	require.NoError(t, group.Snapshot(ctx))
	body, err = group.Propose(ctx, []byte("tail body"))
	require.NoError(t, err)
	require.Equal(t, []byte("applied tail body"), body)
	require.NoError(t, waitTestLocalWrites(ctx, group))
	committed := group.Status().Commit
	require.NoError(t, group.Close())
	require.NoError(t, node.close())

	config.Peers = nil
	config.Machine = &testMachine{}
	reopened := openTestJournalGroupNode(t, dir, nil)
	restored := openTestJournalGroup(t, reopened, config)
	snapshot, err := restored.MachineSnapshot(ctx)
	require.NoError(t, err)
	require.JSONEq(t, `["checkpoint body", "tail body"]`, string(snapshot))
	require.Equal(t, committed, restored.Status().Commit)
	require.Equal(t, []uint64{1}, restored.Status().Voters)
	require.NoFileExists(t, filepath.Join(config.Dir, dbName), "journal-backed Group used bbolt")
	info, err := os.Stat(filepath.Join(dir, journalActiveName))
	require.NoError(t, err)
	require.Positive(t, info.Size(), "checkpoint and committed tail must have a physical journal")
}

type journalGroupSyncGate struct {
	*os.File
	enabled atomic.Bool
	started chan struct{}
	release chan struct{}
}

func (f *journalGroupSyncGate) Sync() error {
	if f.enabled.Load() {
		f.started <- struct{}{}
		<-f.release
	}
	return f.File.Sync()
}

func waitJournalGroupFence(t *testing.T, ctx context.Context, gate *journalGroupSyncGate, result <-chan response) {
	t.Helper()
	select {
	case early := <-result:
		t.Fatalf("proposal finished before reaching the shared journal fence: %+v", early)
	case <-gate.started:
	case <-ctx.Done():
		t.Fatal("journal group never reached the requested fence")
	}
}

func proposeJournalGroup(ctx context.Context, group *Group, data string) <-chan response {
	replies := make(chan response, 1)
	go func() {
		body, err := group.Propose(ctx, []byte(data))
		replies <- response{body: body, err: err}
	}()
	return replies
}

func TestJournalGroupsShareDurableFence(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	dir := t.TempDir()
	gate := &journalGroupSyncGate{started: make(chan struct{}, 16), release: make(chan struct{}, 16)}
	node := openTestJournalGroupNode(t, dir, func(file *os.File) journalFile {
		gate.File = file
		return gate
	})
	// Deferred before the registered group cleanups run, even on assertion failure.
	defer close(gate.release)

	groups := make([]*Group, 0, 3)
	for i, name := range []string{"blocker", "first", "second"} {
		group := openTestJournalGroup(t, node, Config{
			ID: 1, GroupID: uint64(i + 1), Dir: filepath.Join(dir, name),
			Peers: []uint64{1}, Machine: &testMachine{}, TickInterval: -1,
		})
		require.NoError(t, group.Campaign(ctx))
		require.NoError(t, waitTestLocalWrites(ctx, group))
		groups = append(groups, group)
	}

	gate.enabled.Store(true)
	blocker := proposeJournalGroup(ctx, groups[0], "unblocking write")
	waitJournalGroupFence(t, ctx, gate, blocker)
	first := proposeJournalGroup(ctx, groups[1], "first shared write")
	second := proposeJournalGroup(ctx, groups[2], "second shared write")
	require.Eventually(t, func() bool {
		node.writer.mu.Lock()
		defer node.writer.mu.Unlock()
		var firstQueued, secondQueued bool
		for _, request := range node.writer.queue {
			firstQueued = firstQueued || request.record.group == 2
			secondQueued = secondQueued || request.record.group == 3
		}
		return firstQueued && secondQueued
	}, time.Second, time.Millisecond, "both groups must enter journal admission while the first sync remains gated")
	gate.release <- struct{}{}
	waitJournalGroupFence(t, ctx, gate, first)
	for _, pending := range []<-chan response{first, second} {
		select {
		case early := <-pending:
			t.Fatalf("group confirmed with the shared durability fence held: %+v", early)
		default:
		}
	}
	info, err := node.reader.Stat()
	require.NoError(t, err)
	shared := false
	_, err = scanJournal(node.reader, info.Size(), -1, 1, func(batch []journalRecord) error {
		var firstPresent, secondPresent bool
		for _, record := range batch {
			firstPresent = firstPresent || record.group == 2
			secondPresent = secondPresent || record.group == 3
		}
		shared = shared || firstPresent && secondPresent
		return nil
	})
	require.NoError(t, err)
	require.True(t, shared, "the pending groups must share one physical frame and fence")

	gate.enabled.Store(false)
	gate.release <- struct{}{}
	for i, reply := range []<-chan response{blocker, first, second} {
		select {
		case result := <-reply:
			require.NoError(t, result.err)
			require.Equal(t, []byte([]string{"applied unblocking write", "applied first shared write", "applied second shared write"}[i]), result.body)
		case <-ctx.Done():
			t.Fatal("proposal did not complete after the journal fence")
		}
	}
}

type failedJournalSyncFile struct {
	*os.File
	enabled atomic.Bool
	cause   error
}

func (f *failedJournalSyncFile) Sync() error {
	if f.enabled.Load() {
		return f.cause
	}
	return f.File.Sync()
}

func failJournalGroupWriter(t *testing.T, ctx context.Context) (string, *journalGroupNode, *Group, *Group, error) {
	t.Helper()
	dir := t.TempDir()
	failure := errors.New("injected node journal sync failure")
	file := &failedJournalSyncFile{cause: failure}
	node := openTestJournalGroupNode(t, dir, func(underlying *os.File) journalFile {
		file.File = underlying
		return file
	})
	active := openTestJournalGroup(t, node, Config{
		ID: 1, GroupID: 10, Dir: filepath.Join(dir, "active"),
		Peers: []uint64{1}, Machine: &testMachine{}, TickInterval: -1,
	})
	idle := openTestJournalGroup(t, node, Config{
		ID: 1, GroupID: 11, Dir: filepath.Join(dir, "idle"),
		Peers: []uint64{1}, Machine: &testMachine{}, TickInterval: -1,
	})
	require.NoError(t, active.Campaign(ctx))
	require.NoError(t, idle.Campaign(ctx))
	require.NoError(t, waitTestLocalWrites(ctx, active))
	require.NoError(t, waitTestLocalWrites(ctx, idle))
	file.enabled.Store(true)
	_, err := active.Propose(ctx, []byte("unconfirmed before storage failure"))
	require.ErrorIs(t, err, failure)
	select {
	case <-node.writer.done:
	case <-ctx.Done():
		t.Fatal("failed journal writer did not finish")
	}
	return dir, node, active, idle, failure
}

func TestJournalWriterFailureRetainsNodeOwnershipUntilClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	dir, node, active, idle, failure := failJournalGroupWriter(t, ctx)
	expectLocked := func() {
		t.Helper()
		competing, err := openJournalGroupNode(dir, nil)
		if competing != nil {
			_ = competing.close()
		}
		require.ErrorContains(t, err, "lock node raft journal", "the failed writer released ownership before Node close")
	}
	expectLocked()
	require.ErrorContains(t, node.close(), "after all group handles")
	require.ErrorIs(t, active.Close(), failure)
	require.ErrorIs(t, idle.Close(), failure)
	expectLocked()
	require.ErrorIs(t, node.close(), failure)
	next, err := openJournalGroupNode(dir, nil)
	require.NoError(t, err, "explicit close must make the journal available for a new owner")
	require.NoError(t, next.close())
}

func TestJournalWriterFailureStopsIdleGroup(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, _, _, idle, failure := failJournalGroupWriter(t, ctx)
	state, err := idle.MachineSnapshot(ctx)
	require.ErrorIs(t, err, failure, "an idle group returned state after its node journal failed")
	require.Empty(t, state)
	select {
	case <-idle.Done():
	case <-ctx.Done():
		t.Fatal("idle Group was not stopped after its node journal failed")
	}
}
