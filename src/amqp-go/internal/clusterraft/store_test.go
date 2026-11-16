package clusterraft

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestStoreReplacesUncommittedSuffixAndPrunesCheckpointPrefix(t *testing.T) {
	config := Config{ID: 1, GroupID: 3, Dir: t.TempDir(), Peers: []uint64{1, 2, 3}, Machine: &testMachine{}}
	db, err := createStore(config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	entry := func(index, term uint64, body string) *pb.Entry {
		return &pb.Entry{Index: new(index), Term: new(term), Data: []byte(body)}
	}
	require.NoError(t, saveReady(db, raft.Ready{
		HardState: &pb.HardState{Term: new(uint64(2)), Commit: new(uint64(2))},
		Entries:   []*pb.Entry{entry(2, 2, "committed"), entry(3, 2, "old 3"), entry(4, 2, "old 4"), entry(5, 2, "old 5"), entry(6, 2, "old 6")},
	}))
	require.NoError(t, saveReady(db, raft.Ready{
		HardState: &pb.HardState{Term: new(uint64(3)), Commit: new(uint64(3))},
		Entries:   []*pb.Entry{entry(3, 3, "new 3"), entry(4, 3, "new 4")},
	}))
	state, err := loadStore(db)
	require.NoError(t, err)
	require.Len(t, state.entries, 3)
	require.Equal(t, []byte("committed"), state.entries[0].GetData())
	require.Equal(t, []byte("new 3"), state.entries[1].GetData())
	require.Equal(t, []byte("new 4"), state.entries[2].GetData())

	snapshot := &pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{Index: new(uint64(3)), Term: new(uint64(3)), ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}},
		Data:     encodeSnapshot(1, []byte("[]")),
	}
	require.NoError(t, saveSnapshot(db, snapshot))
	state, err = loadStore(db)
	require.NoError(t, err)
	require.Len(t, state.entries, 1)
	require.Equal(t, uint64(4), state.entries[0].GetIndex())
	require.Equal(t, uint64(3), state.snapshot.GetMetadata().GetIndex())
	require.Equal(t, uint64(3), state.hard.GetCommit())
}
