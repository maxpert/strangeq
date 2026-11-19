package clusterraft

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type failedInitialSnapshot struct{ testMachine }

func (*failedInitialSnapshot) Snapshot() ([]byte, error) {
	return nil, errors.New("initial snapshot failed")
}

func TestStoreCreationCanRetryAfterInitialSnapshotFailure(t *testing.T) {
	dir := t.TempDir()
	config := Config{ID: 1, GroupID: 3, Dir: dir, Peers: []uint64{1}, Machine: &failedInitialSnapshot{}}
	group, err := Open(config)
	require.Nil(t, group)
	require.ErrorContains(t, err, "initial snapshot failed")
	config.Machine = &testMachine{}
	group, err = Open(config)
	require.NoError(t, err)
	require.NoError(t, group.Close())
}

func TestStoreCreationRefusesLegacyAuthority(t *testing.T) {
	config := Config{ID: 1, GroupID: 3, Dir: t.TempDir(), Peers: []uint64{1}, Machine: &testMachine{}}
	require.NoError(t, os.WriteFile(filepath.Join(config.Dir, "raft.db"), []byte("legacy authority"), 0600))
	before, err := os.ReadFile(filepath.Join(config.Dir, "raft.db"))
	require.NoError(t, err)
	_, err = Open(config)
	require.ErrorContains(t, err, "fresh directory")
	after, err := os.ReadFile(filepath.Join(config.Dir, "raft.db"))
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestStoreReplacesUncommittedSuffixAndPrunesCheckpointPrefix(t *testing.T) {
	config := Config{ID: 1, GroupID: 3, Dir: t.TempDir(), Peers: []uint64{1, 2, 3}, Machine: &testMachine{}}
	node, err := OpenNodeJournal(config.Dir, true)
	require.NoError(t, err)
	store, _, err := node.store.openStore(config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.close(); _ = node.Close() })

	entry := func(index, term uint64, body string) *pb.Entry {
		return &pb.Entry{Index: new(index), Term: new(term), Data: []byte(body)}
	}
	writeTestReady(t, store, raft.Ready{
		HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(uint64(2))},
		Entries:   []*pb.Entry{entry(2, 2, "committed"), entry(3, 2, "old 3"), entry(4, 2, "old 4"), entry(5, 2, "old 5"), entry(6, 2, "old 6")},
	})
	writeTestReady(t, store, raft.Ready{
		HardState: &pb.HardState{Term: new(uint64(3)), Commit: new(uint64(3))},
		Entries:   []*pb.Entry{entry(3, 3, "new 3"), entry(4, 3, "new 4")},
	})
	last, err := store.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(4), last)
	entries, err := store.Entries(2, last+1, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, entries, 3)
	require.Equal(t, []byte("committed"), entries[0].GetData())
	require.Equal(t, []byte("new 3"), entries[1].GetData())
	require.Equal(t, []byte("new 4"), entries[2].GetData())

	require.NoError(t, store.checkpoint(3, &pb.ConfState{Voters: []uint64{1, 2, 3}}, encodeSnapshot(1, []byte("[]"))))
	require.NoError(t, store.close())
	require.NoError(t, node.Close())
	node, err = OpenNodeJournal(config.Dir, false)
	require.NoError(t, err)
	var snapshot *pb.Snapshot
	store, snapshot, err = node.store.openStore(config)
	require.NoError(t, err)
	first, err := store.FirstIndex()
	require.NoError(t, err)
	last, err = store.LastIndex()
	require.NoError(t, err)
	entries, err = store.Entries(first, last+1, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, uint64(4), entries[0].GetIndex())
	require.Equal(t, uint64(3), snapshot.GetMetadata().GetIndex())
	require.Equal(t, uint64(3), store.state.hard.GetCommit())
}

func writeTestReady(t *testing.T, store *journalGroupStore, ready raft.Ready) {
	t.Helper()
	write, err := store.planReady(ready)
	require.NoError(t, err)
	delta, err := store.persistReady(write)
	require.NoError(t, err)
	store.publishReady(write, delta)
}

func TestGroupReadsStableEntriesFromDiskWithinByteLimit(t *testing.T) {
	config := Config{ID: 1, GroupID: 3, Dir: t.TempDir(), Peers: []uint64{1}, Machine: &testMachine{}, TickInterval: -1}
	node, err := OpenNodeJournal(config.Dir, true)
	require.NoError(t, err)
	store, _, err := node.store.openStore(config)
	require.NoError(t, err)
	entries := []*pb.Entry{
		{Index: new(uint64(2)), Term: new(uint64(1)), Data: []byte("first")},
		{Index: new(uint64(3)), Term: new(uint64(1)), Data: bytes.Repeat([]byte{'x'}, 128)},
	}
	writeTestReady(t, store, raft.Ready{Entries: entries})
	require.NoError(t, store.close())
	require.NoError(t, node.Close())

	group, err := Open(config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, group.Close()) })
	storage := group.store
	read, err := storage.Entries(2, 4, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, read, 2)
	require.Equal(t, entries[1].GetData(), read[1].GetData())
	corrupt := make([]byte, 128)
	corruptTestJournalEntry(t, group, 3)
	read, err = storage.Entries(2, 4, 1)
	require.NoError(t, err)
	require.Len(t, read, 1, "Raft requires the first entry even when it exceeds the limit")
	require.Equal(t, entries[0].GetData(), read[0].GetData())
	firstSize := uint64(proto.Size(entries[0]))
	budget := firstSize + 1
	require.Greater(t, budget, firstSize, "the first entry must not exhaust the combined budget")
	require.Greater(t, uint64(len(corrupt)), budget-firstSize, "the second entry must exceed the remaining budget")
	read, err = storage.Entries(2, 4, budget)
	require.NoError(t, err, "an oversized second entry must not be decoded")
	require.Len(t, read, 1)
	require.Equal(t, entries[0].GetData(), read[0].GetData())
	_, err = storage.Entries(3, 4, 1)
	require.Error(t, err, "Raft storage must read the requested entry from disk")
}

func TestGroupReadsStableSnapshotFromDisk(t *testing.T) {
	config := Config{ID: 1, GroupID: 3, Dir: t.TempDir(), Peers: []uint64{1}, Machine: &testMachine{}, TickInterval: -1}
	group, err := Open(config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, group.Close()) })
	storage := group.store
	snapshot, err := storage.Snapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(1), snapshot.GetMetadata().GetIndex())
	require.NotEmpty(t, snapshot.GetData())
	ref := group.store.state.snapshot
	require.NoError(t, os.WriteFile(filepath.Join(config.Dir, ref.name), []byte{0}, 0600))
	_, err = storage.Snapshot()
	require.Error(t, err, "Raft storage must read the requested snapshot from disk")
}
