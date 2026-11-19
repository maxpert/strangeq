package clusterraft

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type commandSourceFixture struct {
	testMachine
	sources []io.ReaderAt
}

func (m *commandSourceFixture) ApplyJournaled(data []byte, wantReply bool, source io.ReaderAt) ([]byte, error) {
	m.sources = append(m.sources, source)
	reply := m.Apply(data)
	if !wantReply {
		reply = nil
	}
	return reply, nil
}

func requireJournaledCommand(t *testing.T, source io.ReaderAt, command []byte) {
	t.Helper()
	start := len(command) / 3
	dst := make([]byte, len(command)-start)
	n, err := source.ReadAt(dst, int64(start))
	require.NoError(t, err)
	require.Equal(t, len(dst), n)
	require.Equal(t, command[start:], dst)
	n, err = source.ReadAt(make([]byte, 1), int64(len(command)))
	require.ErrorIs(t, err, io.EOF)
	require.Zero(t, n)
}

func TestJournaledCommandSourceSurvivesCheckpointAndTailReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	dir := t.TempDir()
	machine := &commandSourceFixture{}
	config := Config{
		ID: 1, GroupID: 11, Dir: filepath.Join(dir, "group-11"),
		Peers: []uint64{1}, Machine: machine, TickInterval: -1,
	}
	gate := &journalGroupSyncGate{started: make(chan struct{}, 2), release: make(chan struct{}, 2)}
	node := openTestJournalGroupNode(t, dir, func(file *os.File) journalFile {
		gate.File = file
		return gate
	})
	defer close(gate.release)
	group := openTestJournalGroup(t, node, config)
	require.NoError(t, group.Campaign(ctx))
	first := bytes.Repeat([]byte("committed before checkpoint "), 2400)
	second := bytes.Repeat([]byte("committed after checkpoint "), 2400)
	_, err := group.Propose(ctx, first)
	require.NoError(t, err)
	require.NoError(t, group.Snapshot(ctx))
	gate.enabled.Store(true)
	reply := proposeJournalGroup(ctx, group, string(second))
	waitJournalGroupFence(t, ctx, gate, reply)
	group.mu.Lock()
	sourcesBeforeSync := len(machine.sources)
	group.mu.Unlock()
	require.Equal(t, 1, sourcesBeforeSync, "the source must remain unavailable while its append fence is held")
	gate.enabled.Store(false)
	gate.release <- struct{}{}
	select {
	case result := <-reply:
		require.NoError(t, result.err)
	case <-ctx.Done():
		t.Fatal("the proposal did not complete after the durable append")
	}
	require.NoError(t, waitTestLocalWrites(ctx, group))
	require.Len(t, machine.sources, 2, "both committed commands must expose their stable journal ranges")
	requireJournaledCommand(t, machine.sources[0], first)
	requireJournaledCommand(t, machine.sources[1], second)
	require.NoError(t, group.Close())
	require.NoError(t, node.close())

	machine = &commandSourceFixture{}
	config.Peers, config.Machine = nil, machine
	reopened := openTestJournalGroupNode(t, dir, nil)
	_ = openTestJournalGroup(t, reopened, config)
	require.Len(t, machine.sources, 1, "replay must restore the snapshot and expose the stable tail range")
	requireJournaledCommand(t, machine.sources[0], second)
}
