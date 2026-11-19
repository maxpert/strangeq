package clusterraft

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/stretchr/testify/require"
)

func TestRoutineJournalCheckpointDoesNotStallCommittedClaim(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	dir := t.TempDir()
	started, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var armed atomic.Bool
	node := openTestJournalGroupNode(t, dir, nil)
	syncFile := node.syncFile
	node.syncFile = func(file *os.File) error {
		if armed.CompareAndSwap(true, false) {
			close(started)
			<-release
		}
		return syncFile(file)
	}
	config := Config{
		ID: 1, GroupID: 11, Dir: filepath.Join(dir, "group-11"), Peers: []uint64{1},
		Machine: &clusterqueue.Machine{}, TickInterval: -1,
		SnapshotBytes: 4 << 10, SnapshotEntries: 1000, BackgroundJournalCheckpoints: true,
	}
	group := openTestJournalGroup(t, node, config)
	require.NoError(t, group.Campaign(ctx))
	encode := func(command clusterqueue.QueueCommand) []byte {
		t.Helper()
		wire, err := clusterqueue.EncodeCommand(command)
		require.NoError(t, err)
		return wire
	}
	apply := func(command clusterqueue.QueueCommand) clusterqueue.QueueResponse {
		t.Helper()
		reply, err := group.Propose(ctx, encode(command))
		require.NoError(t, err)
		decoded, err := clusterqueue.DecodeResponse(reply)
		require.NoError(t, err)
		require.NoError(t, decoded.Err())
		return decoded
	}
	declared := apply(clusterqueue.QueueCommand{Kind: clusterqueue.DeclareQueue, Queue: "checkpoint-claim"})
	session := clusterqueue.QueueCommand{
		Kind: clusterqueue.OpenQueueSession, Queue: "checkpoint-claim", Incarnation: declared.Queue.Incarnation,
		Owner: "reader", Session: 1, Consumer: true, Prefetch: 1, Deadline: 100,
	}
	apply(session)
	require.NoError(t, group.Snapshot(ctx))
	throughPublish := group.Status().Applied + 1
	body := bytes.Repeat([]byte{0xaa, 0x55}, 4<<10)
	publish := encode(clusterqueue.QueueCommand{Kind: clusterqueue.PublishMessage, Queue: session.Queue, Body: body})
	armed.Store(true)
	published := proposeJournalGroup(ctx, group, string(publish))
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("the routine checkpoint never reached its snapshot file sync")
	}
	session.Kind, session.Sequence, session.Prefetch, session.Consumer, session.Deadline = clusterqueue.ClaimMessage, 1, 0, false, 0
	claim := encode(session)
	result := proposeJournalGroup(ctx, group, string(claim))
	var claimed []byte
	select {
	case reply := <-result:
		require.NoError(t, reply.err)
		claimed = reply.body
	case <-time.After(time.Second):
		t.Fatal("the routine checkpoint blocked a claim while its file sync was held")
	}
	decoded, err := clusterqueue.DecodeResponse(claimed)
	require.NoError(t, err)
	require.NoError(t, decoded.Err())
	require.Equal(t, body, decoded.Result.Delivery.Body)
	require.EqualValues(t, 1, decoded.Result.Delivery.Attempt)
	unblock()
	select {
	case reply := <-published:
		require.NoError(t, reply.err)
	case <-ctx.Done():
		t.Fatal("the publish did not complete")
	}
	require.Eventually(t, func() bool {
		group.mu.Lock()
		defer group.mu.Unlock()
		return group.snapshotIndex == throughPublish
	}, 2*time.Second, time.Millisecond, "the staged snapshot must cover the publish, with the claim in its tail")
	require.NoError(t, group.Close())
	require.NoError(t, node.close())

	config.Peers, config.Machine = nil, &clusterqueue.Machine{}
	reopened := openTestJournalGroupNode(t, dir, nil)
	restored := openTestJournalGroup(t, reopened, config)
	require.NoError(t, restored.Campaign(ctx))
	retry, err := restored.Propose(ctx, claim)
	require.NoError(t, err)
	require.Equal(t, claimed, retry, "restart must replay the same per-session outcome from the snapshot and tail")
}
