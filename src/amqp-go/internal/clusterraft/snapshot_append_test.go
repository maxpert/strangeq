package clusterraft

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type appendCheckpointMachine struct {
	testMachine
	legacyErr   error
	appendCalls int
	prefix      []byte
	restored    []byte
}

func (m *appendCheckpointMachine) Snapshot() ([]byte, error) {
	if m.legacyErr != nil {
		return nil, m.legacyErr
	}
	return m.testMachine.Snapshot()
}

func (m *appendCheckpointMachine) AppendSnapshot(dst []byte) ([]byte, error) {
	m.appendCalls++
	m.prefix = bytes.Clone(dst)
	data, err := m.testMachine.Snapshot()
	return append(dst, data...), err
}

func (m *appendCheckpointMachine) Restore(data []byte) error {
	m.restored = bytes.Clone(data)
	return m.testMachine.Restore(data)
}

func TestGroupCheckpointAppendsSnapshotWithRaftPrefix(t *testing.T) {
	ctx := context.Background()
	machine := &appendCheckpointMachine{}
	config := Config{
		ID: 1, GroupID: 3, Dir: t.TempDir(), Peers: []uint64{1},
		Machine: machine, TickInterval: -1,
	}
	group, err := Open(config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = group.Close() })
	require.NoError(t, group.Campaign(ctx))
	reply, err := group.Propose(ctx, []byte("checkpointed state"))
	require.NoError(t, err)
	require.Equal(t, []byte("applied checkpointed state"), reply)
	require.NoError(t, waitTestLocalWrites(ctx, group))

	status := group.Status()
	before, err := group.store.Snapshot()
	require.NoError(t, err)
	require.Less(t, before.GetMetadata().GetIndex(), status.Applied, "the explicit snapshot must cover a newly applied command")
	require.Positive(t, status.ConfigIndex)
	require.LessOrEqual(t, status.ConfigIndex, status.Applied)

	wantPrefix := binary.BigEndian.AppendUint64([]byte("SQRaft01"), status.ConfigIndex)
	wantBody := []byte(`["checkpointed state"]`)
	group.mu.Lock()
	appendCalls := machine.appendCalls
	machine.legacyErr = errors.New("legacy snapshot callback used for explicit checkpoint")
	group.mu.Unlock()
	require.NoError(t, group.Snapshot(ctx), "checkpoint must use the available append callback")

	require.Equal(t, appendCalls+1, machine.appendCalls)
	require.Equal(t, wantPrefix, machine.prefix)
	checkpoint, err := group.store.Snapshot()
	require.NoError(t, err)
	require.Equal(t, status.Applied, checkpoint.GetMetadata().GetIndex())
	require.Equal(t, append(wantPrefix, wantBody...), checkpoint.GetData())
	require.NoError(t, group.Close())

	restored := &appendCheckpointMachine{}
	config.Peers, config.Machine = nil, restored
	reopened, err := Open(config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	require.Equal(t, status.ConfigIndex, reopened.Status().ConfigIndex)
	require.Equal(t, status.Applied, reopened.Status().Applied)
	require.Equal(t, wantBody, restored.restored)
}
