package clusterraft

import (
	"context"
	"encoding/json"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type checkpointCaptureFixture struct {
	testMachine
	armed   atomic.Bool
	started chan struct{}
	release chan struct{}
}

func (m *checkpointCaptureFixture) encode(entries []string) ([]byte, error) {
	if m.armed.CompareAndSwap(true, false) {
		close(m.started)
		<-m.release
	}
	return json.Marshal(entries)
}

func (m *checkpointCaptureFixture) Snapshot() ([]byte, error) {
	return m.encode(m.entries)
}

func (m *checkpointCaptureFixture) CaptureSnapshot() func([]byte) ([]byte, error) {
	entries := slices.Clone(m.entries)
	return func(prefix []byte) ([]byte, error) {
		data, err := m.encode(entries)
		return append(prefix, data...), err
	}
}

func TestRoutineJournalCheckpointEncodingKeepsCommittedTailMoving(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	dir := t.TempDir()
	machine := &checkpointCaptureFixture{started: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(machine.release) }) }
	defer unblock()
	node := openTestJournalGroupNode(t, dir, nil)
	config := Config{
		ID: 1, GroupID: 11, Dir: filepath.Join(dir, "group-11"), Peers: []uint64{1}, Machine: machine,
		TickInterval: -1, SnapshotBytes: 4 << 10, SnapshotEntries: 1000, BackgroundJournalCheckpoints: true,
	}
	group := openTestJournalGroup(t, node, config)
	require.NoError(t, group.Campaign(ctx))
	require.NoError(t, group.Snapshot(ctx))
	throughCapture := group.Status().Applied + 1
	large, tail := strings.Repeat("a", 8<<10), "committed during encode"
	machine.armed.Store(true)
	first := proposeJournalGroup(ctx, group, large)
	select {
	case <-machine.started:
	case <-ctx.Done():
		t.Fatal("checkpoint never began encoding the captured state")
	}

	next := proposeJournalGroup(ctx, group, tail)
	select {
	case result := <-next:
		require.NoError(t, result.err)
		require.Equal(t, []byte("applied "+tail), result.body)
	case <-time.After(time.Second):
		t.Fatal("routine checkpoint encoding prevented the committed tail from advancing")
	}
	unblock()
	select {
	case result := <-first:
		require.NoError(t, result.err)
	case <-ctx.Done():
		t.Fatal("the first proposal did not complete")
	}
	require.Eventually(t, func() bool {
		group.mu.Lock()
		defer group.mu.Unlock()
		return group.snapshotIndex == throughCapture
	}, 2*time.Second, time.Millisecond, "the staged snapshot should publish only its captured index")
	require.NoError(t, group.Close())
	require.NoError(t, node.close())

	config.Peers, config.Machine = nil, &testMachine{}
	restored := openTestJournalGroup(t, openTestJournalGroupNode(t, dir, nil), config)
	state, err := restored.MachineSnapshot(ctx)
	require.NoError(t, err)
	var entries []string
	require.NoError(t, json.Unmarshal(state, &entries))
	require.Equal(t, []string{large, tail}, entries, "the later command must replay once from the retained log")
}
