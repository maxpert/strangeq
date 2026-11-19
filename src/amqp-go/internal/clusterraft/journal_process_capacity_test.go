package clusterraft

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	pb "go.etcd.io/raft/v3/raftpb"
)

// A completed metadata build must release an idle process stream's covered
// extents while its open suffix remains unfenced. Otherwise its active peer
// exhausts their shared quota despite the reclaimable durable prefix.
func TestJournalProcessIdleStreamReclaimsCompletedCheckpointForPeer(t *testing.T) {
	checkJournalProcessIdleStreamCheckpoint(t, false)
}

func TestJournalProcessIdleStreamChainsNewerSealedCheckpoint(t *testing.T) {
	checkJournalProcessIdleStreamCheckpoint(t, true)
}

func checkJournalProcessIdleStreamCheckpoint(t *testing.T, chain bool) {
	t.Helper()
	const extentBytes = 64 << 10
	retainedBytes := int64(160 << 10)
	if chain {
		retainedBytes = 216 << 10
	}
	quota := &journalSegmentQuota{limit: retainedBytes, streams: 2}
	started, release, built := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce, builtOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var checkpointSynced atomic.Bool
	mode := &journalDurability{}
	open := func(name string, hooks journalSegmentHooks) *journalGroupNode {
		t.Helper()
		hooks.sharedQuota = quota
		node, err := openSegmentedGroupNode(filepath.Join(t.TempDir(), name), true, false, extentBytes, retainedBytes, nil, hooks)
		require.NoError(t, err)
		node.writer.durability = mode
		t.Cleanup(func() { _ = node.close() })
		return node
	}
	idle := open("idle", journalSegmentHooks{
		syncFile: func(file *os.File) error {
			if strings.HasPrefix(filepath.Base(file.Name()), journalCheckpointPrefix) && !checkpointSynced.Load() {
				close(started)
				<-release
				if err := syncJournalFile(file); err != nil {
					return err
				}
				checkpointSynced.Store(true)
				return nil
			}
			return syncJournalFile(file)
		},
		syncDir: func(dir string) error {
			if err := syncDirectory(dir); err != nil {
				return err
			}
			if checkpointSynced.Load() {
				builtOnce.Do(func() { close(built) })
			}
			return nil
		},
	})
	active := open("active", journalSegmentHooks{})
	persist := func(node *journalGroupNode, group uint64, op journalGroupOp, process bool) error {
		delta, err := node.persistMode(group, op, process)
		if err != nil {
			return err
		}
		return node.replayDelta(group, delta)
	}
	create := func(node *journalGroupNode, group uint64) {
		require.NoError(t, persist(node, group, journalGroupOp{kind: journalGroupCreate, identity: journalCheckpointFixtureIdentity(group)}, false))
	}
	appendEntry := func(node *journalGroupNode, group, index uint64, size int, process bool) error {
		return persist(node, group, journalGroupOp{kind: journalGroupReady,
			hard:    &pb.HardState{Term: new(uint64(1)), Commit: new(index)},
			entries: []*pb.Entry{{Index: new(index), Term: new(uint64(1)), Data: bytes.Repeat([]byte{byte(index)}, size)}},
		}, process)
	}
	create(idle, 11)
	require.NoError(t, appendEntry(idle, 11, 1, 48<<10, true))
	checkpointThrough := func(index uint64) {
		snapshot, err := idle.stageSnapshot(&pb.Snapshot{Metadata: &pb.SnapshotMetadata{
			Index: new(index), Term: new(uint64(1)), ConfState: &pb.ConfState{Voters: []uint64{1}},
		}, Data: encodeSnapshot(1, nil)})
		require.NoError(t, err)
		require.NoError(t, persist(idle, 11, journalGroupOp{kind: journalGroupSnap, snapshot: snapshot}, false))
	}
	if !chain {
		checkpointThrough(1)
	}
	// Strict entry 2 seals the first extent and starts a build.
	require.NoError(t, appendEntry(idle, 11, 2, 24<<10, false))
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("checkpoint worker did not reach its file fence")
	}
	// A newer compacted cut must be built after the held older checkpoint:
	// its old entry references still protect the first extent until then.
	size := 1 << 10
	if chain {
		checkpointThrough(2)
		size = 48 << 10
	}
	// The next process write dirties the open suffix while the build is held.
	require.NoError(t, appendEntry(idle, 11, 3, size, true))
	storage := idle.reader.(*journalSegmentReader).store
	storage.mu.RLock()
	durable, completed := storage.durableEnd, storage.completedEnd
	first := storage.path(storage.manifest.extents[0].id)
	firstEnd := storage.manifest.extents[0].base + storage.manifest.extents[0].sealedBytes
	storage.mu.RUnlock()
	require.Greater(t, completed, durable)
	require.FileExists(t, first)
	unblock()
	select {
	case <-built:
	case <-time.After(3 * time.Second):
		t.Fatal("checkpoint worker did not finish its durable artifact")
	}
	// No request is sent to the idle stream after this point. Completion must
	// wake its owner to publish and reclaim, without syncing the process suffix.
	require.Eventually(t, func() bool { _, err := os.Stat(first); return os.IsNotExist(err) }, 2*time.Second, time.Millisecond,
		"idle process writer stranded a completed sealed-prefix checkpoint")
	storage.mu.RLock()
	require.Equal(t, durable, storage.durableEnd, "metadata publication must not fence the dirty process suffix")
	require.Greater(t, storage.completedEnd, storage.durableEnd)
	require.LessOrEqual(t, storage.manifest.checkpoint.end, storage.durableEnd)
	if chain {
		require.Greater(t, storage.manifest.checkpoint.end, firstEnd, "idle writer must finish the newer compacted seal too")
	}
	storage.mu.RUnlock()
	create(active, 12)
	require.NoError(t, appendEntry(active, 12, 1, 48<<10, true))
	require.NoError(t, appendEntry(active, 12, 2, 24<<10, true), "active peer must reuse capacity reclaimed by the idle stream")
	require.NoError(t, idle.writer.failure())
	require.NoError(t, active.writer.failure())
}
