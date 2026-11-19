package clusterraft

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	journalReadFixtureSlots = 4
	journalReadFixtureMax   = (1 << 20) + (4 << 10)
)

func openJournalReadFixtureStore(t *testing.T, node *journalGroupNode, group uint64, bodies ...[]byte) *journalGroupStore {
	t.Helper()
	store, _, err := node.openStore(Config{ID: 1, GroupID: group, Dir: filepath.Join(node.dir, "unused")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.close() })
	entries := make([]*pb.Entry, len(bodies))
	for i, body := range bodies {
		entries[i] = &pb.Entry{Index: new(uint64(i + 1)), Term: new(uint64(1)), Data: body}
	}
	ready, err := store.planReady(raft.Ready{
		HardState: &pb.HardState{Term: new(uint64(1)), Commit: new(uint64(len(entries)))},
		Entries:   entries,
	})
	require.NoError(t, err)
	delta, err := store.persistReady(ready)
	require.NoError(t, err)
	store.publishReady(ready, delta)
	return store
}

func journalReadFixtureDecodeAllocs(t *testing.T, index uint64, data []byte) float64 {
	t.Helper()
	encoded, err := proto.Marshal(&pb.Entry{Index: new(index), Term: new(uint64(1)), Data: data})
	require.NoError(t, err)
	var decoded []*pb.Entry
	allocs := testing.AllocsPerRun(16, func() {
		entry := &pb.Entry{}
		if err := proto.Unmarshal(encoded, entry); err != nil {
			t.Fatal(err)
		}
		decoded = append([]*pb.Entry(nil), entry)
	})
	require.Equal(t, data, decoded[0].GetData())
	return allocs
}

func TestJournalEntriesReusesScratchAcrossGroupsWithoutAliasing(t *testing.T) {
	node := openTestJournalGroupNode(t, t.TempDir(), nil)
	first := bytes.Repeat([]byte{0x25}, 512<<10)
	second := bytes.Repeat([]byte{0x68}, len(first))
	oversize := bytes.Repeat([]byte{0x73}, 2*journalReadFixtureMax)
	left := openJournalReadFixtureStore(t, node, 11, first, oversize)
	right := openJournalReadFixtureStore(t, node, 22, second)
	node.recentEntries.clear() // Exercise disk decode and scratch ownership.

	held, err := left.Entries(1, 2, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, held, 1)
	for range 2 * journalReadFixtureSlots {
		reads, err := right.Entries(1, 2, ^uint64(0))
		require.NoError(t, err)
		require.Len(t, reads, 1)
		require.Equal(t, second, reads[0].GetData())
	}
	require.Equal(t, first, held[0].GetData(), "a later Entries call cannot overwrite an earlier decoded body")

	var reads []*pb.Entry
	oversizeAllocs := testing.AllocsPerRun(16, func() {
		var err error
		reads, err = left.Entries(2, 3, ^uint64(0))
		if err != nil {
			t.Fatal(err)
		}
	})
	require.Equal(t, oversize, reads[0].GetData())
	require.Greater(t, oversizeAllocs, journalReadFixtureDecodeAllocs(t, 2, oversize),
		"an oversize read must keep its scratch ephemeral")

	warm := []*journalGroupStore{left, right}
	next := 0
	steadyAllocs := testing.AllocsPerRun(16, func() {
		var err error
		reads, err = warm[next].Entries(1, 2, ^uint64(0))
		if err != nil {
			t.Fatal(err)
		}
		next = 1 - next
	})
	require.LessOrEqual(t, steadyAllocs, journalReadFixtureDecodeAllocs(t, 1, first),
		"steady journal reads must allocate only their decoded entries")
	require.Equal(t, first, held[0].GetData())
	reads[0].Data[0] ^= 0xff
	require.Equal(t, first, held[0].GetData(), "returned Entries calls must own their decoded buffers")
}

func borrowJournalReadFixtureScratch(node *journalGroupNode, size int) ([]byte, func()) {
	loan := node.readScratch.acquire(size)
	return loan.buffer[:size], func() { node.readScratch.release(loan) }
}

func TestJournalReadScratchKeepsOnlyBoundedNodeSlots(t *testing.T) {
	node := openTestJournalGroupNode(t, t.TempDir(), nil)
	leased := make(map[*byte]bool, journalReadFixtureSlots)
	var release []func()
	for range journalReadFixtureSlots {
		scratch, giveBack := borrowJournalReadFixtureScratch(node, journalReadFixtureMax)
		require.False(t, leased[&scratch[0]], "one buffer cannot back two active readers")
		leased[&scratch[0]] = true
		release = append(release, giveBack)
	}
	extra, releaseExtra := borrowJournalReadFixtureScratch(node, journalReadFixtureMax)
	large, releaseLarge := borrowJournalReadFixtureScratch(node, 2*journalReadFixtureMax)
	require.False(t, leased[&extra[0]])
	require.False(t, leased[&large[0]])
	releaseExtra()
	releaseLarge()
	for _, giveBack := range release {
		giveBack()
	}

	for range journalReadFixtureSlots {
		scratch, giveBack := borrowJournalReadFixtureScratch(node, journalReadFixtureMax)
		defer giveBack()
		require.LessOrEqual(t, cap(scratch), journalReadFixtureMax)
		require.True(t, leased[&scratch[0]], "only the original node slots may be reused")
		delete(leased, &scratch[0])
	}
	require.Empty(t, leased)
}
