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

func checkpointJournalGroup(t *testing.T, ctx context.Context, group *Group, body string) {
	t.Helper()
	_, err := group.Propose(ctx, []byte(body))
	require.NoError(t, err)
	require.NoError(t, group.Snapshot(ctx))
}

func requireJournalSnapshot(t *testing.T, group *Group, contents string) string {
	t.Helper()
	group.mu.Lock()
	defer group.mu.Unlock()
	store := group.store.(*journalGroupStore)
	snapshot, err := store.Snapshot()
	require.NoError(t, err)
	_, data, err := decodeSnapshot(snapshot)
	require.NoError(t, err)
	require.JSONEq(t, contents, string(data))
	return filepath.Join(store.node.dir, store.state.snapshot.name)
}

func TestJournalCheckpointReclaimsSnapshotAfterDurableReplacement(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	dir := t.TempDir()
	gate := &journalGroupSyncGate{started: make(chan struct{}, 1), release: make(chan struct{})}
	node := openTestJournalGroupNode(t, dir, func(file *os.File) journalFile {
		gate.File = file
		return gate
	})
	defer close(gate.release)

	firstConfig := Config{
		ID: 1, GroupID: 11, Dir: filepath.Join(dir, "first"),
		Peers: []uint64{1}, Machine: &testMachine{}, TickInterval: -1,
	}
	otherConfig := Config{
		ID: 1, GroupID: 22, Dir: filepath.Join(dir, "other"),
		Peers: []uint64{1}, Machine: &testMachine{}, TickInterval: -1,
	}
	first := openTestJournalGroup(t, node, firstConfig)
	other := openTestJournalGroup(t, node, otherConfig)
	require.NoError(t, first.Campaign(ctx))
	require.NoError(t, other.Campaign(ctx))
	checkpointJournalGroup(t, ctx, first, "original")
	checkpointJournalGroup(t, ctx, other, "other group")
	previous := requireJournalSnapshot(t, first, `["original"]`)
	otherBlob := requireJournalSnapshot(t, other, `["other group"]`)
	_, err := first.Propose(ctx, []byte("replacement"))
	require.NoError(t, err)
	require.NoError(t, waitTestLocalWrites(ctx, first))
	require.NoError(t, waitTestLocalWrites(ctx, other))

	gate.enabled.Store(true)
	checkpoint := make(chan error, 1)
	go func() { checkpoint <- first.Snapshot(ctx) }()
	select {
	case <-gate.started:
	case err := <-checkpoint:
		t.Fatalf("checkpoint finished before reaching its journal fence: %v", err)
	case <-ctx.Done():
		t.Fatal("checkpoint never reached its journal fence")
	}
	require.FileExists(t, previous, "the old blob is live until the replacement reference is durable")
	require.FileExists(t, otherBlob)
	gate.enabled.Store(false)
	select {
	case gate.release <- struct{}{}:
	case <-ctx.Done():
		t.Fatal("checkpoint did not receive the journal fence release")
	}
	select {
	case err := <-checkpoint:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("checkpoint did not complete after releasing its journal fence")
	}

	require.NoFileExists(t, previous, "a successful durable checkpoint must reclaim its previous blob")
	replacement := requireJournalSnapshot(t, first, `["original","replacement"]`)
	require.FileExists(t, replacement)
	require.Equal(t, otherBlob, requireJournalSnapshot(t, other, `["other group"]`))
	require.NoError(t, first.Close())
	require.NoError(t, other.Close())
	require.NoError(t, node.close())

	reopened := openTestJournalGroupNode(t, dir, nil)
	firstConfig.Peers, firstConfig.Machine = nil, &testMachine{}
	otherConfig.Peers, otherConfig.Machine = nil, &testMachine{}
	restored := openTestJournalGroup(t, reopened, firstConfig)
	otherRestored := openTestJournalGroup(t, reopened, otherConfig)
	require.Equal(t, replacement, requireJournalSnapshot(t, restored, `["original","replacement"]`))
	require.Equal(t, otherBlob, requireJournalSnapshot(t, otherRestored, `["other group"]`))
}

type journalCheckpointAppendFailure struct {
	*os.File
	enabled atomic.Bool
	cause   error
}

func (f *journalCheckpointAppendFailure) WriteAt(data []byte, offset int64) (int, error) {
	if f.enabled.Load() {
		return 0, f.cause
	}
	return f.File.WriteAt(data, offset)
}

func TestJournalCheckpointAppendFailurePreservesOldSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	dir := t.TempDir()
	failure := &journalCheckpointAppendFailure{cause: errors.New("injected rejection before snapshot reference append")}
	node := openTestJournalGroupNode(t, dir, func(file *os.File) journalFile {
		failure.File = file
		return failure
	})
	config := Config{
		ID: 1, GroupID: 11, Dir: filepath.Join(dir, "group"),
		Peers: []uint64{1}, Machine: &testMachine{}, TickInterval: -1,
	}
	group := openTestJournalGroup(t, node, config)
	require.NoError(t, group.Campaign(ctx))
	checkpointJournalGroup(t, ctx, group, "original")
	previous := requireJournalSnapshot(t, group, `["original"]`)
	_, err := group.Propose(ctx, []byte("committed tail"))
	require.NoError(t, err)
	require.NoError(t, waitTestLocalWrites(ctx, group))

	failure.enabled.Store(true)
	require.ErrorIs(t, group.Snapshot(ctx), failure.cause)
	require.FileExists(t, previous, "failing to append the new reference cannot retire the old blob")
	files, err := filepath.Glob(filepath.Join(dir, journalSnapshotPrefix+"*"))
	require.NoError(t, err)
	require.Greater(t, len(files), 1, "the failed checkpoint staged an unreferenced snapshot")
	require.ErrorIs(t, group.Close(), failure.cause)
	require.ErrorIs(t, node.close(), failure.cause)

	reopened := openTestJournalGroupNode(t, dir, nil)
	config.Peers, config.Machine = nil, &testMachine{}
	restored := openTestJournalGroup(t, reopened, config)
	require.Equal(t, previous, requireJournalSnapshot(t, restored, `["original"]`))
	machine, err := restored.MachineSnapshot(ctx)
	require.NoError(t, err)
	require.JSONEq(t, `["original","committed tail"]`, string(machine))
	files, err = filepath.Glob(filepath.Join(dir, journalSnapshotPrefix+"*"))
	require.NoError(t, err)
	require.Equal(t, []string{previous}, files)
}

type journalCheckpointSyncFailure struct {
	*os.File
	enabled atomic.Bool
	cause   error
}

func (f *journalCheckpointSyncFailure) Sync() error {
	if f.enabled.Load() {
		return f.cause
	}
	return f.File.Sync()
}

func TestJournalCheckpointReopenSyncsBeforeReclaiming(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	dir := t.TempDir()
	failure := &journalCheckpointSyncFailure{cause: errors.New("injected error after writing journal frame")}
	wrap := func(file *os.File) journalFile {
		failure.File = file
		return failure
	}
	node := openTestJournalGroupNode(t, dir, wrap)
	config := Config{
		ID: 1, GroupID: 11, Dir: filepath.Join(dir, "group"),
		Peers: []uint64{1}, Machine: &testMachine{}, TickInterval: -1,
	}
	group := openTestJournalGroup(t, node, config)
	require.NoError(t, group.Campaign(ctx))
	checkpointJournalGroup(t, ctx, group, "original")
	previous := requireJournalSnapshot(t, group, `["original"]`)
	_, err := group.Propose(ctx, []byte("replacement"))
	require.NoError(t, err)
	require.NoError(t, waitTestLocalWrites(ctx, group))

	failure.enabled.Store(true)
	require.ErrorIs(t, group.Snapshot(ctx), failure.cause)
	require.ErrorIs(t, group.Close(), failure.cause)
	require.ErrorIs(t, node.close(), failure.cause)
	require.FileExists(t, previous)
	_, err = openJournalGroupNode(dir, wrap)
	require.ErrorIs(t, err, failure.cause)
	require.FileExists(t, previous, "failed startup sync cannot retire the prior durable reference")

	reopened := openTestJournalGroupNode(t, dir, nil)
	config.Peers, config.Machine = nil, &testMachine{}
	restored := openTestJournalGroup(t, reopened, config)
	replacement := requireJournalSnapshot(t, restored, `["original","replacement"]`)
	require.NotEqual(t, previous, replacement)
	require.NoFileExists(t, previous)
}
