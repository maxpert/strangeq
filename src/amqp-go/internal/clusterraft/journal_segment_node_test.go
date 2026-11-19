package clusterraft

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/stretchr/testify/require"
)

func TestSegmentedJournalCanReuseCapacityAfterDurableAcknowledgments(t *testing.T) {
	checkSegmentedJournalCapacityReuse(t, false)
}

func TestSegmentedJournalReclaimsLaterFilesWhileOldBodyRemainsLive(t *testing.T) {
	checkSegmentedJournalCapacityReuse(t, true)
}

func checkSegmentedJournalCapacityReuse(t *testing.T, keepOldBody bool) {
	t.Helper()
	const extentBytes, retainedBytes = 256 << 10, 1 << 20
	dir := t.TempDir()
	cfg := Config{
		ID: 1, GroupID: 11, Dir: filepath.Join(dir, "group"), Peers: []uint64{1},
		Machine: &clusterqueue.Machine{}, TickInterval: -1, DeferJournalCommit: true,
	}
	node, err := openSegmentedGroupNode(dir, true, false, extentBytes, retainedBytes, nil, journalSegmentHooks{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = node.close() })
	group := openTestJournalGroup(t, node, cfg)
	require.NoError(t, group.Campaign(t.Context()))
	apply := func(group *Group, command clusterqueue.QueueCommand) clusterqueue.QueueResponse {
		t.Helper()
		request, err := clusterqueue.EncodeCommand(command)
		require.NoError(t, err)
		reply, err := group.Propose(t.Context(), request)
		require.NoError(t, err)
		result, err := clusterqueue.DecodeResponse(reply)
		require.NoError(t, err)
		require.NoError(t, result.Err())
		return result
	}
	queue := apply(group, clusterqueue.QueueCommand{Kind: clusterqueue.DeclareQueue, Queue: "jobs"}).Queue
	session := clusterqueue.QueueCommand{Kind: clusterqueue.OpenQueueSession, Queue: "jobs", Incarnation: queue.Incarnation,
		Session: 1, Owner: "reader", Consumer: true, Prefetch: 1, Deadline: 100}
	apply(group, session)
	var pinned clusterqueue.QueueInfo
	pinnedBody := bytes.Repeat([]byte{0xfe}, 64<<10)
	if keepOldBody {
		pinned = apply(group, clusterqueue.QueueCommand{Kind: clusterqueue.DeclareQueue, Queue: "pinned"}).Queue
		apply(group, clusterqueue.QueueCommand{
			Kind: clusterqueue.PublishMessage, Queue: "pinned", Incarnation: pinned.Incarnation, Body: pinnedBody,
		})
	}
	for i := range 24 {
		body := bytes.Repeat([]byte{byte(i)}, 64<<10)
		apply(group, clusterqueue.QueueCommand{Kind: clusterqueue.PublishMessage, Queue: "jobs", Incarnation: queue.Incarnation, Body: body})
		op := clusterqueue.QueueCommand{Kind: clusterqueue.ClaimMessage, Queue: "jobs", Incarnation: queue.Incarnation,
			Session: session.Session, Owner: session.Owner, Sequence: uint64(2*i + 1)}
		delivery := apply(group, op).Result.Delivery
		require.Equal(t, body, delivery.Body)
		op.Kind, op.Sequence, op.Attempt = clusterqueue.AckMessage, op.Sequence+1, delivery.Attempt
		apply(group, op)
		require.NoError(t, group.Snapshot(t.Context()))
	}
	require.Eventually(t, func() bool {
		apply(group, clusterqueue.QueueCommand{Kind: clusterqueue.DescribeQueue, Queue: "jobs"})
		manifest, err := loadJournalSegmentManifest(dir)
		if err != nil || manifest.extents[len(manifest.extents)-1].base <= retainedBytes {
			return false
		}
		if keepOldBody {
			require.Zero(t, manifest.extents[0].base, "the live body still owns the first segment")
			return len(manifest.extents) > 1 && manifest.extents[1].base > manifest.extents[0].sealedBytes
		}
		return manifest.extents[0].base > 0
	}, 2*time.Second, 10*time.Millisecond, "checkpoint publication must reclaim covered segments during service")
	require.NoError(t, group.Close())
	require.NoError(t, node.close())
	cfg.Machine = &clusterqueue.Machine{}
	reopened, err := openSegmentedGroupNode(dir, false, false, extentBytes, retainedBytes, nil, journalSegmentHooks{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.close()) })
	recovered := openTestJournalGroup(t, reopened, cfg)
	require.NoError(t, recovered.Campaign(t.Context()))
	state := apply(recovered, clusterqueue.QueueCommand{Kind: clusterqueue.DescribeQueue, Queue: "jobs"}).Queue
	require.Zero(t, state.Ready)
	require.Zero(t, state.Unacknowledged)
	if keepOldBody {
		apply(recovered, clusterqueue.QueueCommand{
			Kind: clusterqueue.OpenQueueSession, Queue: "pinned", Incarnation: pinned.Incarnation,
			Session: 1, Owner: "reader", Consumer: true, Prefetch: 1, Deadline: 100,
		})
		delivery := apply(recovered, clusterqueue.QueueCommand{
			Kind: clusterqueue.ClaimMessage, Queue: "pinned", Incarnation: pinned.Incarnation,
			Session: 1, Owner: "reader", Sequence: 1,
		}).Result.Delivery
		require.Equal(t, pinnedBody, delivery.Body)
	}
}

func TestSegmentedJournalIdleShardCheckpointsWhenSharedLogAdvances(t *testing.T) {
	const extentBytes = 128 << 10
	dir := t.TempDir()
	node, err := openSegmentedGroupNode(dir, true, false, extentBytes, 2<<20, nil, journalSegmentHooks{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, node.close()) })
	open := func(id uint64, machine Machine) *Group {
		group := openTestJournalGroup(t, node, Config{
			ID: 1, GroupID: id, Dir: dir, Peers: []uint64{1}, Machine: machine, TickInterval: -1,
			BackgroundJournalCheckpoints: true,
		})
		require.NoError(t, group.Campaign(t.Context()))
		require.NoError(t, group.Snapshot(t.Context()))
		return group
	}
	idle := open(11, &checkpointCostFixture{})
	busy := open(12, &checkpointCostFixture{})
	_, err = idle.Propose(t.Context(), []byte("one small command"))
	require.NoError(t, err)
	require.NoError(t, waitTestLocalWrites(t.Context(), idle))
	idle.mu.Lock()
	before, applied, bytes := idle.snapshotIndex, idle.applied, idle.appliedLogBytes
	idle.mu.Unlock()
	require.Greater(t, applied, before)
	require.Less(t, bytes, idle.cfg.SnapshotBytes)

	body := make([]byte, 64<<10)
	for range 12 {
		_, err = busy.Propose(t.Context(), body)
		require.NoError(t, err)
		// Leave one compaction candidate independent of the idle shard's
		// checkpoint. No more commands go through the idle group.
		require.NoError(t, busy.Snapshot(t.Context()))
	}
	store := node.reader.(*journalSegmentReader).store
	require.Positive(t, store.checkpointFloor.Load(), "busy shard must advance the shared append frontier")
	require.Eventually(t, func() bool {
		require.NoError(t, idle.Tick(t.Context()))
		idle.mu.Lock()
		defer idle.mu.Unlock()
		return idle.snapshotIndex == applied
	}, 2*time.Second, 10*time.Millisecond, "the idle shard must checkpoint without receiving more writes")
}

func TestSegmentedJournalCheckpointUsesStableAppendMetadataAcrossRotations(t *testing.T) {
	const extent = 256 << 10
	dir := t.TempDir()
	started, release := make(chan struct{}), make(chan struct{})
	var startOnce, releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	node, err := openSegmentedGroupNode(dir, true, false, extent, 8<<20, nil, journalSegmentHooks{
		syncFile: func(file *os.File) error {
			if strings.HasPrefix(filepath.Base(file.Name()), journalCheckpointPrefix) {
				startOnce.Do(func() {
					close(started)
					<-release
				})
			}
			return syncJournalFile(file)
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = node.close() })
	cfg := Config{ID: 1, GroupID: 11, Dir: dir, Peers: []uint64{1}, Machine: &testMachine{}, TickInterval: -1}
	group := openTestJournalGroup(t, node, cfg)
	require.NoError(t, group.Campaign(t.Context()))

	var expected []string
	appendBodies := func(count int) {
		t.Helper()
		for range count {
			body := bytes.Repeat([]byte{'a' + byte(len(expected))}, 64<<10)
			_, err := group.Propose(t.Context(), body)
			require.NoError(t, err)
			expected = append(expected, string(body))
		}
	}
	appendBodies(6)
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("checkpoint worker did not reach the index sync")
	}
	before, err := loadJournalSegmentManifest(dir)
	require.NoError(t, err)
	require.Empty(t, before.checkpoint.name, "the staged index has no authority before its sync")
	appendBodies(8)
	after, err := loadJournalSegmentManifest(dir)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(after.extents), len(before.extents)+2, "writer must roll while the index worker is held")
	unblock()
	require.NoError(t, group.Close())
	require.NoError(t, node.close())
	after, err = loadJournalSegmentManifest(dir)
	require.NoError(t, err)
	require.True(t, after.checkpoint.valid())

	reopened, err := openSegmentedGroupNode(dir, false, false, extent, 8<<20, nil, journalSegmentHooks{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.close() })
	cfg.Machine = &testMachine{}
	restored := openTestJournalGroup(t, reopened, cfg)
	snapshot, err := restored.MachineSnapshot(t.Context())
	require.NoError(t, err)
	var values []string
	require.NoError(t, json.Unmarshal(snapshot, &values))
	require.Equal(t, expected, values)
}

func TestSegmentedJournalCanIndexRecoveredSealBeforeAnyNewAppend(t *testing.T) {
	dir, first, _, _ := seedRetainedCheckpointFixture(t)
	extent := int64(len(first))
	before, err := loadJournalSegmentManifest(dir)
	require.NoError(t, err)
	require.Empty(t, before.checkpoint.name)
	require.Len(t, before.extents, 2)

	node, err := openSegmentedGroupNode(dir, false, false, extent, 4096, nil, journalSegmentHooks{})
	require.NoError(t, err)
	require.NoError(t, node.close())
	after, err := loadJournalSegmentManifest(dir)
	require.NoError(t, err)
	require.Equal(t, extent, after.checkpoint.end)
	require.Equal(t, uint64(1), after.checkpoint.sequence)
	require.Len(t, after.extents, 1, "the recovered sealed extent has no live entry references")

	reopened, err := openSegmentedGroupNode(dir, false, false, extent, 4096, nil, journalSegmentHooks{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.close()) })
	require.Contains(t, reopened.groups, uint64(11), "group from the sealed index")
	require.Contains(t, reopened.groups, uint64(22), "group from the active replay tail")
}
