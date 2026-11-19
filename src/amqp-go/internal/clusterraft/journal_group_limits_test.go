package clusterraft

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestJournalRejectsUnsupportedEntryLimitBeforeProvisioning(t *testing.T) {
	dir := t.TempDir()
	node := openTestJournalGroupNode(t, dir, nil)
	group, err := node.openGroup(Config{
		ID: 1, GroupID: 1, Dir: filepath.Join(dir, "group"), Peers: []uint64{1},
		Machine: &testMachine{}, TickInterval: -1, MaxEntryBytes: maxJournalFrameBytes,
	})
	if group != nil {
		_ = group.Close()
	}
	require.ErrorIs(t, err, ErrTooLarge, "unsupported journal maximum reached group provisioning")
	info, err := os.Stat(filepath.Join(dir, journalActiveName))
	require.NoError(t, err)
	require.Zero(t, info.Size(), "a rejected journal configuration changed the store")
}

func TestJournalRejectsOversizedFollowerEntryBeforeRaftMutation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	dir := t.TempDir()
	node := openTestJournalGroupNode(t, dir, nil)
	// The pinned Raft setup requires MaxInflightBytes (4x this value) to
	// cover the broker's 1 MiB MaxSizePerMsg before accepting any input.
	const maxEntryBytes = 256 << 10
	follower := openTestJournalGroup(t, node, Config{
		ID: 2, GroupID: 1, Dir: filepath.Join(dir, "group"), Peers: []uint64{1, 2},
		Machine: &testMachine{}, TickInterval: -1, MaxEntryBytes: maxEntryBytes,
	})
	message := &pb.Message{
		Type: pb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(2)),
		Term: new(uint64(2)), Index: new(uint64(1)), LogTerm: new(uint64(1)), Commit: new(uint64(1)),
		Entries: []*pb.Entry{{Index: new(uint64(2)), Term: new(uint64(2)), Data: make([]byte, maxEntryBytes+1)}},
	}
	require.ErrorIs(t, follower.Step(ctx, message), ErrTooLarge, "oversized peer entry reached RawNode.Step")
	follower.mu.Lock()
	status := follower.node.BasicStatus()
	follower.mu.Unlock()
	require.Equal(t, uint64(1), status.GetTerm(), "rejected append changed raft term")
	require.Equal(t, uint64(1), status.GetCommit(), "rejected append advanced commit")

	message.Entries[0].Data = make([]byte, maxEntryBytes)
	require.NoError(t, follower.Step(ctx, message), "the configured maximum remains supported")
	require.NoError(t, waitTestLocalWrites(ctx, follower))
	index, err := follower.store.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(2), index)
}

func TestJournalReadyAdmissionReservesEncodingOverhead(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	dir := t.TempDir()
	gate := &journalGroupSyncGate{started: make(chan struct{}, 16), release: make(chan struct{}, 16)}
	node := openTestJournalGroupNode(t, dir, func(file *os.File) journalFile {
		gate.File = file
		return gate
	})
	defer close(gate.release)
	const maxEntry = 256 << 10
	follower := openTestJournalGroup(t, node, Config{
		ID: 2, GroupID: 1, Dir: filepath.Join(dir, "group"), Peers: []uint64{1, 2},
		Machine: &testMachine{}, TickInterval: -1, MaxEntryBytes: maxEntry,
	})
	appendAt := func(previous, previousTerm uint64, size int) *pb.Message {
		return &pb.Message{
			Type: pb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(2)),
			Term: new(uint64(2)), Index: new(previous), LogTerm: new(previousTerm), Commit: new(uint64(1)),
			Entries: []*pb.Entry{{Index: new(previous + 1), Term: new(uint64(2)), Data: make([]byte, size)}},
		}
	}
	gate.enabled.Store(true)
	require.NoError(t, follower.Step(ctx, appendAt(1, 1, 8)))
	select {
	case <-gate.started:
	case <-ctx.Done():
		t.Fatal("follower did not hold the initial journal fence")
	}
	var encoded int
	for i := range 4 {
		message := appendAt(uint64(2+i), 2, maxEntry-96)
		encoded += proto.Size(message)
		// The existing input accounting can accept all four messages while
		// storage waits. The journal additionally reserves record overhead.
		require.LessOrEqual(t, encoded, 4*maxEntry)
		err := follower.Step(ctx, message)
		if i < 3 {
			require.NoError(t, err)
		} else {
			require.ErrorIs(t, err, ErrBusy, "journal framing allowance was not charged before RawNode.Step")
		}
	}
	gate.enabled.Store(false)
	gate.release <- struct{}{}
	require.NoError(t, waitTestLocalWrites(ctx, follower))
	index, err := follower.store.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(5), index, "only the first three buffered entries reached storage")
}
