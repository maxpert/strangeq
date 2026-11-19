package clusterraft

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type checkpointCostFixture struct {
	testMachine
	cost      uint64
	snapshots int
}

func (m *checkpointCostFixture) CheckpointCostBytes() uint64 { return m.cost }

func (m *checkpointCostFixture) Apply(data []byte) []byte {
	if string(data) == "drained" {
		m.cost = 0
	}
	return m.testMachine.Apply(data)
}

func (m *checkpointCostFixture) Snapshot() ([]byte, error) {
	m.snapshots++
	return m.testMachine.Snapshot()
}

func openCheckpointCostGroup(t *testing.T, ctx context.Context, entries uint64) (*journalGroupNode, *Group, *checkpointCostFixture) {
	t.Helper()
	dir := t.TempDir()
	node := openTestJournalGroupNode(t, dir, nil)
	machine := &checkpointCostFixture{cost: 8 << 10}
	group := openTestJournalGroup(t, node, Config{
		ID: 1, GroupID: 11, Dir: filepath.Join(dir, "group-11"), Peers: []uint64{1}, Machine: machine,
		TickInterval: -1, SnapshotBytes: 4 << 10, SnapshotEntries: entries, BackgroundJournalCheckpoints: true,
	})
	require.NoError(t, group.Campaign(ctx))
	require.NoError(t, group.Snapshot(ctx))
	return node, group, machine
}

func TestJournalCheckpointCostFollowsCurrentLiveState(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	_, group, machine := openCheckpointCostGroup(t, ctx, 1024)
	base := group.Status().Applied
	before := machine.snapshots

	_, err := group.Propose(ctx, []byte(strings.Repeat("w", 32<<10)))
	require.NoError(t, err)
	require.NoError(t, waitTestLocalWrites(ctx, group))
	group.mu.Lock()
	expensive := machine.snapshots
	group.mu.Unlock()
	require.Equal(t, before, expensive, "a fresh expensive image must not checkpoint at the legacy byte threshold")

	_, err = group.Propose(ctx, []byte("drained"))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		group.mu.Lock()
		defer group.mu.Unlock()
		return group.snapshotIndex == base+2
	}, 2*time.Second, time.Millisecond, "the remaining tail must checkpoint once the current image becomes cheap")
}

func TestJournalCheckpointCostForcesBoundedAppliedTailWhileSlotBusy(t *testing.T) {
	for _, tc := range []struct {
		name      string
		body      string
		entries   uint64
		proposals int
	}{
		{name: "bytes", body: strings.Repeat("w", 64<<10), entries: 1024, proposals: 4},
		{name: "entries", body: "metadata only", entries: 1, proposals: 64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
			defer cancel()
			node, group, _ := openCheckpointCostGroup(t, ctx, tc.entries)
			base := group.Status().Applied
			require.True(t, node.routineSnapshots.tryAcquire(88), "another group must own the routine slot")
			defer node.routineSnapshots.release()
			for range tc.proposals {
				_, err := group.Propose(ctx, []byte(tc.body))
				require.NoError(t, err)
			}
			require.NoError(t, waitTestLocalWrites(ctx, group))
			group.mu.Lock()
			through := group.snapshotIndex
			group.mu.Unlock()
			require.Equal(t, base+uint64(tc.proposals), through,
				"applied replay debt must force a checkpoint even when the routine slot is occupied")
		})
	}
}
