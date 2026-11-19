package clusterraft

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestJournalRecentlyPublishedEntryAvoidsPayloadReallocation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	dir := t.TempDir()
	node := openTestJournalGroupNode(t, dir, nil)
	group := openTestJournalGroup(t, node, Config{
		ID: 1, GroupID: 11, Dir: filepath.Join(dir, "group-11"),
		Peers: []uint64{1}, Machine: &testMachine{}, TickInterval: -1,
	})
	require.NoError(t, group.Campaign(ctx))
	body := bytes.Repeat([]byte{0xaa, 0x24}, 32<<10)
	_, err := group.Propose(ctx, body)
	require.NoError(t, err)
	require.NoError(t, waitTestLocalWrites(ctx, group))

	group.mu.Lock()
	defer group.mu.Unlock()
	index := group.applied
	entries, err := group.store.Entries(index, index+1, 1<<20)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.True(t, bytes.Equal(body, entries[0].GetData()[len(key{}):]))
	entries[0] = nil
	entries, err = group.store.Entries(index, index+1, 1<<20)
	require.NoError(t, err)
	require.NotNil(t, entries[0], "changing one returned slice must leave later reads intact")
	measure := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := group.store.Entries(index, index+1, 1<<20); err != nil {
				b.Fatal(err)
			}
		}
	})
	require.Less(t, measure.AllocedBytesPerOp(), int64(len(body)/4),
		"using the recently published entry should not decode and allocate its full payload again")
}

func TestJournalRecentEntryCacheFollowsPublishedUncommittedReplacement(t *testing.T) {
	node := openTestJournalGroupNode(t, t.TempDir(), nil)
	store, _, err := node.openStore(Config{ID: 1, GroupID: 11, Dir: filepath.Join(node.dir, "unused")})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.close()) })
	appendEntry := func(term uint64, body string) (readyWrite, journalGroupDelta, *pb.Entry) {
		t.Helper()
		entry := &pb.Entry{Index: new(uint64(1)), Term: new(term), Data: []byte(body)}
		write, err := store.planReady(raft.Ready{HardState: &pb.HardState{Term: new(term)}, Entries: []*pb.Entry{entry}})
		require.NoError(t, err)
		delta, err := store.persistReady(write)
		require.NoError(t, err)
		require.Len(t, delta.entries, 1)
		require.Nil(t, node.recentEntries.lookup(delta.entries[0]), "a synced append is invisible until the Group publishes it")
		return write, delta, entry
	}
	write, delta, first := appendEntry(1, "original")
	store.publishReady(write, delta)
	previous, err := store.Entries(1, 2, 1<<20)
	require.NoError(t, err)
	require.Same(t, first, previous[0])

	write, delta, replacement := appendEntry(2, "replacement")
	store.publishReady(write, delta)
	entries, err := store.Entries(1, 2, 1<<20)
	require.NoError(t, err)
	require.Same(t, replacement, entries[0], "the old index must resolve to the new physical entry")
	require.Equal(t, []byte("original"), previous[0].GetData(), "replacement must not mutate a previously returned entry")

	node.recentEntries.clear()
	entries, err = store.Entries(1, 2, 1<<20)
	require.NoError(t, err)
	require.NotSame(t, replacement, entries[0], "a cache miss decodes the persisted entry")
	require.Equal(t, replacement.GetData(), entries[0].GetData())
}
