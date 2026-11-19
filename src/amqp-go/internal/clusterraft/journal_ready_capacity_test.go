package clusterraft

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterproposal"
	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestJournalReadyAdmissionUsesSegmentCapacity(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	follower, disk := openReadyCapacityFollower(t)
	const entryBytes = 12 << 20
	release, entered := holdAsyncTestWrite(t, disk)
	require.NoError(t, follower.Step(ctx, readyCapacityAppend(1, 1, []byte("held"))))
	awaitAsyncTestWrite(t, ctx, entered)

	body := make([]byte, entryBytes)
	require.NoError(t, follower.Step(ctx, readyCapacityAppend(2, 2, body)))
	require.NoError(t, follower.Step(ctx, readyCapacityAppend(3, 2, body)))
	retry := readyCapacityAppend(4, 2, body)
	retryErr := follower.Step(ctx, retry)
	release()
	// With an unbounded Ready the accumulated 36 MiB fails here, after
	// every input has already entered the unstable Raft log.
	require.NoError(t, waitTestLocalWrites(ctx, follower), "a full Ready must not stop the group")
	require.ErrorIs(t, retryErr, ErrBusy, "the first append that does not fit must remain retryable")
	index, err := follower.store.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(4), index, "the rejected append must not enter the Raft log")

	require.NoError(t, follower.Step(ctx, retry))
	require.NoError(t, waitTestLocalWrites(ctx, follower))
	index, err = follower.store.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(5), index)
}

func TestJournalReadyRejectsOversizedSegmentInputBeforeRaftMutation(t *testing.T) {
	follower, _ := openReadyCapacityFollower(t)
	allocation := make([]byte, journalGroupMaxEntryBytes)
	owned, err := clusterproposal.Adopt(allocation)
	require.NoError(t, err)
	submission, err := owned.Begin()
	require.NoError(t, err)
	message := readyCapacityAppend(1, 1, allocation)

	t.Run("proposal", func(t *testing.T) {
		_, err := follower.Propose(t.Context(), submission.Command())
		require.ErrorIs(t, err, ErrTooLarge)
	})
	t.Run("owned proposal", func(t *testing.T) {
		_, err := follower.ProposeOwned(t.Context(), submission)
		require.ErrorIs(t, err, ErrTooLarge)
		require.True(t, submission.Available())
	})
	t.Run("peer receipt", func(t *testing.T) {
		require.ErrorIs(t, follower.CheckPeerInputSize(message, uint64(proto.Size(message))), ErrTooLarge)
	})
	t.Run("peer append", func(t *testing.T) {
		require.ErrorIs(t, follower.Step(t.Context(), message), ErrTooLarge)
		require.Equal(t, uint64(1), follower.Status().Term)
	})
}

func TestJournalReadyAdmissionFlushesGatherBeforeSegmentOverflow(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	dir := t.TempDir()
	const extentBytes = 256 << 10
	node, err := openSegmentedGroupNode(dir, true, false, extentBytes, journalGroupMaxFileBytes, nil, journalSegmentHooks{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = node.close() })
	machine := &testMachine{}
	group := openTestJournalGroup(t, node, Config{
		ID: 1, GroupID: 1, Dir: dir, Peers: []uint64{1}, Machine: machine, TickInterval: -1,
		GatherProposal: func([]byte) time.Duration { return time.Hour },
	})
	require.NoError(t, group.Campaign(ctx))
	first, second := strings.Repeat("a", 160<<10), strings.Repeat("b", 112<<10)
	firstResult := startJournalGatherProposal(ctx, group, first)
	require.Eventually(t, func() bool {
		group.mu.Lock()
		defer group.mu.Unlock()
		return group.gatheredProposal != nil && group.appending == nil
	}, time.Second, time.Millisecond)
	_, err = group.Propose(ctx, []byte(second))
	require.NoError(t, err, "the second proposal must flush the first before it can overflow the next Ready")
	require.NoError(t, awaitTestReadResult(t, ctx, firstResult).err)
	require.Equal(t, []string{first, second}, machine.entries)
}

func openReadyCapacityFollower(t *testing.T) (*Group, *testJournalDisk) {
	t.Helper()
	const extentBytes = 32 << 20
	dir := t.TempDir()
	disk := &testJournalDisk{}
	node, err := openSegmentedGroupNode(dir, true, false, extentBytes, journalGroupMaxFileBytes, nil,
		journalSegmentHooks{writeAt: disk.writeAt})
	require.NoError(t, err)
	t.Cleanup(func() { _ = node.close() })
	require.Equal(t, extentBytes-journalFrameHeaderSize-journalFrameTrailerSize, node.writer.limits.pendingBytes)
	follower := openTestJournalGroup(t, node, Config{
		ID: 2, GroupID: 1, Dir: dir, Peers: []uint64{1, 2}, Machine: &testMachine{},
		TickInterval: -1, MaxEntryBytes: journalGroupMaxEntryBytes,
	})
	return follower, disk
}

func readyCapacityAppend(previous, previousTerm uint64, data []byte) *pb.Message {
	return &pb.Message{
		Type: pb.MsgApp.Enum(), From: new(uint64(1)), To: new(uint64(2)),
		Term: new(uint64(2)), Index: new(previous), LogTerm: new(previousTerm), Commit: new(uint64(1)),
		Entries: []*pb.Entry{{Index: new(previous + 1), Term: new(uint64(2)), Data: data}},
	}
}
