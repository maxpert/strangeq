package clusterraft

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/stretchr/testify/require"
)

func openPrototypeProcessNode(t *testing.T, dir, boot string, create, process bool, syncs *atomic.Int64) *journalGroupNode {
	t.Helper()
	hooks := journalSegmentHooks{processRoots: process, processBootID: boot}
	if syncs != nil {
		hooks.syncFile = func(file *os.File) error { syncs.Add(1); return syncJournalFile(file) }
	}
	node, err := openSegmentedGroupNode(dir, create, false, 64<<10, 2<<20, nil, hooks)
	require.NoError(t, err)
	if process {
		node.writer.durability = &journalDurability{}
	} else {
		require.NoError(t, node.writer.requireFullDurability())
	}
	t.Cleanup(func() { _ = node.close() })
	return node
}
func copyPrototypeImage(t *testing.T, source string) string {
	t.Helper()
	target := t.TempDir()
	files, err := os.ReadDir(source)
	require.NoError(t, err)
	for _, file := range files {
		if file.IsDir() || file.Name() == journalSegmentLockName {
			continue
		}
		data, err := os.ReadFile(filepath.Join(source, file.Name()))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(target, file.Name()), data, 0600))
	}
	return target
}

// This first integrated proof uses the real queue Group, routine Group.Snapshot
// compaction, real retained body references and one physical volume. Images are
// captured before either owner close can accidentally certify the live suffix.
func TestPrototypeDualRootsRecoverLiveSurvivorAndExactPhysicalState(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	dir := t.TempDir()
	var syncs atomic.Int64
	node := openPrototypeProcessNode(t, dir, "kernel-A", true, true, &syncs)
	group := openTestJournalGroup(t, node, Config{ID: 1, GroupID: 11, Dir: filepath.Join(dir, "group"), Peers: []uint64{1}, Machine: &clusterqueue.Machine{}, TickInterval: -1, DeferJournalCommit: true, ProcessDurability: true, SnapshotBytes: 1 << 30})
	require.NoError(t, group.Campaign(ctx))
	apply := func(command clusterqueue.QueueCommand) clusterqueue.QueueResponse {
		wire, err := clusterqueue.EncodeCommand(command)
		require.NoError(t, err)
		reply, err := group.Propose(ctx, wire)
		require.NoError(t, err)
		result, err := clusterqueue.DecodeResponse(reply)
		require.NoError(t, err)
		require.NoError(t, result.Err())
		return result
	}
	held := apply(clusterqueue.QueueCommand{Kind: clusterqueue.DeclareQueue, Queue: "held"}).Queue
	work := apply(clusterqueue.QueueCommand{Kind: clusterqueue.DeclareQueue, Queue: "work"}).Queue
	session := clusterqueue.QueueCommand{Kind: clusterqueue.OpenQueueSession, Queue: "work", Incarnation: work.Incarnation, Session: 1, Owner: "reader", Consumer: true, Prefetch: 1, Deadline: 100}
	apply(session)
	require.NoError(t, group.Snapshot(ctx))
	// A strict owner request certifies the previous empty-queue state and forks
	// its last inode before any disposable process bodies can append to it.
	fence, err := node.writer.enqueueEncoded(1, 0, true, nil, func([]byte) error { return nil })
	require.NoError(t, err)
	require.NoError(t, (<-fence).err)
	require.NoError(t, waitTestLocalWrites(ctx, group))
	physical, err := readProcessRecoveryRoot(filepath.Join(dir, processPhysicalRoot))
	require.NoError(t, err)
	physicalManifest, err := decodeJournalSegmentManifest(physical.Manifest)
	require.NoError(t, err)
	physicalCommit := group.Status().Commit
	oldSnapshot := physical.Snapshots[0]
	before := syncs.Load()
	survivor := bytes.Repeat([]byte{0xa3}, 24<<10)
	apply(clusterqueue.QueueCommand{Kind: clusterqueue.PublishMessage, Queue: "held", Incarnation: held.Incarnation, Body: survivor})
	var seenExtents = make(map[uint64]bool)
	storage := node.reader.(*journalSegmentReader).store
	for i := range 16 {
		body := bytes.Repeat([]byte{byte(i + 1)}, 24<<10)
		apply(clusterqueue.QueueCommand{Kind: clusterqueue.PublishMessage, Queue: "work", Incarnation: work.Incarnation, Body: body})
		op := clusterqueue.QueueCommand{Kind: clusterqueue.ClaimMessage, Queue: "work", Incarnation: work.Incarnation, Session: 1, Owner: "reader", Sequence: uint64(2*i + 1)}
		delivery := apply(op).Result.Delivery
		require.Equal(t, body, delivery.Body)
		op.Kind, op.Sequence, op.Attempt = clusterqueue.AckMessage, op.Sequence+1, delivery.Attempt
		apply(op)
		require.NoError(t, group.Snapshot(ctx))
		storage.mu.RLock()
		for _, extent := range storage.manifest.extents {
			seenExtents[extent.id] = true
		}
		storage.mu.RUnlock()
	}
	require.NoError(t, waitTestLocalWrites(ctx, group))
	require.Equal(t, before, syncs.Load(), "normal process rolling and Group compaction cannot fence disposable bodies")
	require.FileExists(t, filepath.Join(dir, oldSnapshot), "previous physical snapshot must survive live compaction")
	for _, extent := range physicalManifest.extents {
		require.FileExists(t, storage.path(extent.id), "physical-root extent closure must remain owned")
	}
	require.Eventually(t, func() bool {
		for id := range seenExtents {
			if _, err := os.Stat(storage.path(id)); os.IsNotExist(err) {
				return true
			}
		}
		return false
	}, time.Second, time.Millisecond, "normal compaction must dispose some process-only extent bytes")
	liveDir := copyPrototypeImage(t, dir)
	resetDir := copyPrototypeImage(t, dir)
	intentDir := copyPrototypeImage(t, dir)
	missingDir := copyPrototypeImage(t, dir)
	capDir := copyPrototypeImage(t, dir)
	mismatchDir := copyPrototypeImage(t, dir)
	require.NoError(t, os.Remove(filepath.Join(missingDir, processLiveRoot)))
	_, err = openSegmentedGroupNode(missingDir, false, false, 64<<10, 2<<20, nil, journalSegmentHooks{processRoots: true, processBootID: "kernel-A"})
	require.Error(t, err, "same-kernel recovery cannot silently fall back when its live root is missing")
	check := func(image, boot string, process bool, expectSurvivor bool) {
		t.Helper()
		restoredNode := openPrototypeProcessNode(t, image, boot, false, process, nil)
		restored := openTestJournalGroup(t, restoredNode, Config{ID: 1, GroupID: 11, Dir: filepath.Join(image, "group"), Machine: &clusterqueue.Machine{}, TickInterval: -1, ProcessDurability: process})
		if expectSurvivor {
			require.Greater(t, restored.Status().Commit, physicalCommit)
		} else {
			require.Equal(t, physicalCommit, restored.Status().Commit, "host reset must select exact previous physical commit")
		}
		require.NoError(t, restored.Campaign(ctx))
		wire, err := clusterqueue.EncodeCommand(clusterqueue.QueueCommand{Kind: clusterqueue.DescribeQueue, Queue: "held"})
		require.NoError(t, err)
		reply, err := restored.Propose(ctx, wire)
		require.NoError(t, err)
		state, err := clusterqueue.DecodeResponse(reply)
		require.NoError(t, err)
		require.NoError(t, state.Err())
		if expectSurvivor {
			require.EqualValues(t, 1, state.Queue.Ready)
			request, err := clusterqueue.EncodeCommand(clusterqueue.QueueCommand{Kind: clusterqueue.OpenQueueSession, Queue: "held", Incarnation: held.Incarnation, Session: 1, Owner: "survivor-reader", Consumer: true, Prefetch: 1, Deadline: 100})
			require.NoError(t, err)
			reply, err := restored.Propose(ctx, request)
			require.NoError(t, err)
			response, err := clusterqueue.DecodeResponse(reply)
			require.NoError(t, err)
			require.NoError(t, response.Err())
			request, err = clusterqueue.EncodeCommand(clusterqueue.QueueCommand{Kind: clusterqueue.ClaimMessage, Queue: "held", Incarnation: held.Incarnation, Session: 1, Owner: "survivor-reader", Sequence: 1})
			require.NoError(t, err)
			reply, err = restored.Propose(ctx, request)
			require.NoError(t, err)
			response, err = clusterqueue.DecodeResponse(reply)
			require.NoError(t, err)
			require.NoError(t, response.Err())
			require.Equal(t, survivor, response.Result.Delivery.Body)
		} else {
			require.Zero(t, state.Queue.Ready)
		}
		if !process {
			cert, err := readProcessRecoveryRoot(filepath.Join(image, processPhysicalRoot))
			require.NoError(t, err)
			require.Greater(t, cert.End, physical.End, "restart after intent must certify the selected same-kernel live closure")
		}
	}
	// The physical last inode deliberately contains a later dirty live suffix.
	physicalActive := physicalManifest.extents[len(physicalManifest.extents)-1]
	capPath := filepath.Join(capDir, fmt.Sprintf("extent-%016x.raft-journal", physicalActive.id))
	info, err := os.Stat(capPath)
	require.NoError(t, err)
	require.Greater(t, info.Size(), physical.End-physicalActive.base, "fault proof needs a real suffix beyond the physical cut")
	beforeCap := errors.New("crash boundary before physical truncate")
	_, err = openSegmentedGroupNode(capDir, false, false, 64<<10, 2<<20, nil, journalSegmentHooks{processRoots: true, processBootID: "kernel-B", beforePhysicalRecoveryCap: func(*os.File) error { return beforeCap }})
	require.ErrorIs(t, err, beforeCap)
	unchanged, err := readProcessRecoveryRoot(filepath.Join(capDir, processPhysicalRoot))
	require.NoError(t, err)
	require.Equal(t, "kernel-A", unchanged.Boot, "interrupted cap cannot publish new boot identity")
	infoAfter, err := os.Stat(capPath)
	require.NoError(t, err)
	require.Equal(t, info.Size(), infoAfter.Size())
	check(capDir, "kernel-B", true, false)
	// A valid-checksum but inconsistent physical sequence fails after opening
	// and scanning the active descriptor. That descriptor must still close.
	bad, err := readProcessRecoveryRoot(filepath.Join(mismatchDir, processPhysicalRoot))
	require.NoError(t, err)
	bad.Next++
	codec := &journalProcessRoots{dir: mismatchDir, strict: journalSegmentHooks{syncFile: syncJournalFile, syncDir: syncDirectory}}
	require.NoError(t, codec.writeRoot(processPhysicalRoot, bad, true))
	var opened *os.File
	_, err = openSegmentedGroupNode(mismatchDir, false, false, 64<<10, 2<<20, nil, journalSegmentHooks{processRoots: true, processBootID: "kernel-B", beforePhysicalRecoveryCap: func(file *os.File) error { opened = file; return nil }})
	require.ErrorContains(t, err, "physical root cut mismatch")
	require.NotNil(t, opened)
	_, err = opened.Stat()
	require.ErrorIs(t, err, os.ErrClosed, "post-scan recovery errors cannot leak the active descriptor")
	check(liveDir, "kernel-A", true, true)
	check(resetDir, "kernel-B", true, false)
	check(intentDir, "kernel-A", false, true)
}

func TestPrototypeDualRootSIGKILLHelper(t *testing.T) {
	dir := os.Getenv("STRANGEQ_PROTO_CHILD_DIR")
	if dir == "" {
		t.Skip("subprocess helper")
	}
	node := openPrototypeProcessNode(t, dir, "same-kernel", true, true, nil)
	group := openTestJournalGroup(t, node, Config{ID: 1, GroupID: 11, Dir: filepath.Join(dir, "group"), Peers: []uint64{1}, Machine: &clusterqueue.Machine{}, TickInterval: -1, ProcessDurability: true, DeferJournalCommit: true})
	require.NoError(t, group.Campaign(t.Context()))
	apply := func(command clusterqueue.QueueCommand) clusterqueue.QueueResponse {
		wire, err := clusterqueue.EncodeCommand(command)
		require.NoError(t, err)
		reply, err := group.Propose(t.Context(), wire)
		require.NoError(t, err)
		result, err := clusterqueue.DecodeResponse(reply)
		require.NoError(t, err)
		require.NoError(t, result.Err())
		return result
	}
	queue := apply(clusterqueue.QueueCommand{Kind: clusterqueue.DeclareQueue, Queue: "survivor"}).Queue
	require.NoError(t, group.Snapshot(t.Context()))
	fence, err := node.writer.enqueueEncoded(1, 0, true, nil, func([]byte) error { return nil })
	require.NoError(t, err)
	require.NoError(t, (<-fence).err)
	apply(clusterqueue.QueueCommand{Kind: clusterqueue.PublishMessage, Queue: "survivor", Incarnation: queue.Incarnation, Body: bytes.Repeat([]byte{0xb7}, 24<<10)})
	require.NoError(t, group.Snapshot(t.Context()))
	require.NoError(t, waitTestLocalWrites(t.Context(), group))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "child.ready"), []byte("confirmed survivor, not closed"), 0600))
	select {}
}

func TestPrototypeDualRootActualSIGKILLKeepsLiveSurvivor(t *testing.T) {
	dir := t.TempDir()
	executable, err := os.Executable()
	require.NoError(t, err)
	log, err := os.CreateTemp(t.TempDir(), "child-log")
	require.NoError(t, err)
	defer log.Close()
	child := exec.Command(executable, "-test.run=^TestPrototypeDualRootSIGKILLHelper$", "-test.timeout=20s")
	child.Env = append(os.Environ(), "STRANGEQ_PROTO_CHILD_DIR="+dir)
	child.Stdout = log
	child.Stderr = log
	require.NoError(t, child.Start())
	defer child.Process.Kill()
	ready := false
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		if _, err := os.Stat(filepath.Join(dir, "child.ready")); err == nil {
			ready = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !ready {
		_ = child.Process.Kill()
		_ = child.Wait()
		data, _ := os.ReadFile(log.Name())
		t.Fatalf("child never confirmed survivor: %s", data)
	}
	require.NoError(t, child.Process.Kill())
	require.Error(t, child.Wait(), "helper must exit by actual SIGKILL before cleanup")
	node := openPrototypeProcessNode(t, dir, "same-kernel", false, true, nil)
	group := openTestJournalGroup(t, node, Config{ID: 1, GroupID: 11, Dir: filepath.Join(dir, "group"), Machine: &clusterqueue.Machine{}, TickInterval: -1, ProcessDurability: true})
	require.NoError(t, group.Campaign(t.Context()))
	apply := func(command clusterqueue.QueueCommand) clusterqueue.QueueResponse {
		wire, err := clusterqueue.EncodeCommand(command)
		require.NoError(t, err)
		reply, err := group.Propose(t.Context(), wire)
		require.NoError(t, err)
		result, err := clusterqueue.DecodeResponse(reply)
		require.NoError(t, err)
		require.NoError(t, result.Err())
		return result
	}
	queue := apply(clusterqueue.QueueCommand{Kind: clusterqueue.DescribeQueue, Queue: "survivor"}).Queue
	require.EqualValues(t, 1, queue.Ready)
	apply(clusterqueue.QueueCommand{Kind: clusterqueue.OpenQueueSession, Queue: "survivor", Incarnation: queue.Incarnation, Session: 1, Owner: "reader", Consumer: true, Prefetch: 1, Deadline: 100})
	delivery := apply(clusterqueue.QueueCommand{Kind: clusterqueue.ClaimMessage, Queue: "survivor", Incarnation: queue.Incarnation, Session: 1, Owner: "reader", Sequence: 1}).Result.Delivery
	require.Equal(t, bytes.Repeat([]byte{0xb7}, 24<<10), delivery.Body)
}

func TestPrototypePairPromotionCertifiesEveryReachableClosure(t *testing.T) {
	root := t.TempDir()
	quota := &journalSegmentQuota{limit: 4 << 20, streams: 2}
	mode := &journalDurability{}
	entered, release := make(chan struct{}, 2), make(chan struct{})
	var armed atomic.Bool
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	pair, err := openJournalPair(root, 1, func(dir string, index int, budget *journalBudget) (*journalGroupNode, error) {
		node, err := openSegmentedGroupNode(dir, true, false, 64<<10, quota.limit, budget, journalSegmentHooks{processRoots: true, processBootID: "pair-kernel", sharedQuota: quota, syncFile: func(file *os.File) error {
			if armed.Load() && strings.Contains(filepath.Base(file.Name()), journalSnapshotPrefix) {
				entered <- struct{}{}
				<-release
			}
			return syncJournalFile(file)
		}})
		if err == nil {
			node.writer.durability = mode
		}
		return node, err
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = pair.Close() })
	for i := range 2 {
		group, err := pair.OpenGroup(Config{ID: 1, GroupID: uint64(10 + i), Dir: filepath.Join(root, fmt.Sprint(i)), Peers: []uint64{1}, Machine: &testMachine{}, TickInterval: -1, ProcessDurability: true})
		require.NoError(t, err)
		t.Cleanup(func() { _ = group.Close() })
		require.NoError(t, group.Campaign(t.Context()))
		_, err = group.Propose(t.Context(), bytes.Repeat([]byte{byte(i + 1)}, 24<<10))
		require.NoError(t, err)
		require.NoError(t, group.Snapshot(t.Context()))
	}
	// No active descriptor need be dirty after this metadata-only snapshot
	// publication: closure certification must still fence reachable snapshots.
	armed.Store(true)
	done := make(chan error, 1)
	go func() { done <- pair.RequireFullDurability() }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("pair promotion did not reach reachable snapshot fence")
	}
	select {
	case <-done:
		t.Fatal("pair promotion returned while closure fence was held")
	default:
	}
	require.True(t, mode.strict.Load())
	unblock()
	armed.Store(false)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("pair did not finish all closure fences")
	}
	for _, stream := range pair.streams {
		storage := stream.store.reader.(*journalSegmentReader).store
		certificate, err := readProcessRecoveryRoot(filepath.Join(storage.dir, processPhysicalRoot))
		require.NoError(t, err)
		storage.mu.RLock()
		require.Equal(t, storage.completedEnd, certificate.End)
		storage.mu.RUnlock()
		require.False(t, storage.processRoots.process.Load())
	}
}

func TestPrototypeRepeatedCertificatesKeepClosureBounded(t *testing.T) {
	node := openPrototypeProcessNode(t, t.TempDir(), "repeat-kernel", true, true, nil)
	group := openTestJournalGroup(t, node, Config{ID: 1, GroupID: 11, Dir: filepath.Join(node.dir, "group"), Peers: []uint64{1}, Machine: &testMachine{}, TickInterval: -1, ProcessDurability: false, SnapshotBytes: 1 << 30})
	require.NoError(t, group.Campaign(t.Context()))
	for i := range maxJournalSegmentCount + 16 {
		_, err := group.Propose(t.Context(), []byte("x"))
		require.NoError(t, err, "strict batch %d cannot consume an artificial extent", i)
		if i%16 == 0 {
			require.NoError(t, group.Snapshot(t.Context()))
		}
	}
	require.NoError(t, group.Snapshot(t.Context()))
	fence, err := node.writer.enqueueEncoded(1, 0, true, nil, func([]byte) error { return nil })
	require.NoError(t, err)
	require.NoError(t, (<-fence).err)
	_, err = group.Propose(t.Context(), []byte("sweep"))
	require.NoError(t, err)
	require.NoError(t, waitTestLocalWrites(t.Context(), group))
	storage := node.reader.(*journalSegmentReader).store
	physical, err := readProcessRecoveryRoot(filepath.Join(node.dir, processPhysicalRoot))
	require.NoError(t, err)
	manifest, err := decodeJournalSegmentManifest(physical.Manifest)
	require.NoError(t, err)
	require.Less(t, len(manifest.extents), 8, "certificate cannot union old recovery roots forever")
	files, err := os.ReadDir(node.dir)
	require.NoError(t, err)
	snapshots, indexes, extents := 0, 0, 0
	for _, file := range files {
		switch {
		case strings.HasPrefix(file.Name(), journalSnapshotPrefix):
			snapshots++
		case strings.HasPrefix(file.Name(), journalCheckpointPrefix):
			indexes++
		case strings.HasPrefix(file.Name(), "extent-"):
			extents++
		}
	}
	require.LessOrEqual(t, snapshots, 2, "released physical snapshots must be revisited by owning Group")
	require.LessOrEqual(t, indexes, 2, "released physical indexes must be unlinked and uncharged")
	require.Less(t, extents, 8, "old physical extent pins must be released after newer certificate")
	storage.mu.RLock()
	require.Less(t, len(storage.manifest.extents), 8)
	storage.mu.RUnlock()
}

func TestPrototypeOrphanLiveRootNeverFallsBackToOrdinaryManifest(t *testing.T) {
	dir := t.TempDir()
	node := openPrototypeProcessNode(t, dir, "orphan-kernel", true, true, nil)
	require.NoError(t, node.close())
	require.NoError(t, os.Remove(filepath.Join(dir, processPhysicalRoot)))
	_, err := OpenNodeJournalWithOptions(dir, false, JournalOptions{})
	require.ErrorContains(t, err, "orphan process live root")
	_, err = openSegmentedGroupNode(dir, false, false, 64<<10, 2<<20, nil, journalSegmentHooks{})
	require.ErrorContains(t, err, "orphan process live root")
}

func TestPrototypeIdleGroupRetiredSnapshotReapedByPeerCertificate(t *testing.T) {
	node := openPrototypeProcessNode(t, t.TempDir(), "idle-owner-kernel", true, true, nil)
	open := func(id uint64, process bool) *Group {
		group := openTestJournalGroup(t, node, Config{ID: 1, GroupID: id, Dir: filepath.Join(node.dir, fmt.Sprint(id)), Peers: []uint64{1}, Machine: &testMachine{}, TickInterval: -1, ProcessDurability: process, SnapshotBytes: 1 << 30})
		require.NoError(t, group.Campaign(t.Context()))
		return group
	}
	idle, peer := open(11, true), open(12, false)
	_, err := idle.Propose(t.Context(), []byte("old snapshot"))
	require.NoError(t, err)
	require.NoError(t, idle.Snapshot(t.Context()))
	fence, err := node.writer.enqueueEncoded(1, 0, true, nil, func([]byte) error { return nil })
	require.NoError(t, err)
	require.NoError(t, (<-fence).err)
	idle.mu.Lock()
	old := idle.store.state.snapshot
	idle.mu.Unlock()
	roots := node.reader.(*journalSegmentReader).store.processRoots
	require.True(t, roots.pinsName(old.name))
	_, err = idle.Propose(t.Context(), []byte("replacement snapshot"))
	require.NoError(t, err)
	require.NoError(t, idle.Snapshot(t.Context()))
	// The owner has now passed through reclaimSnapshot after publication. It
	// issues no further command, snapshot or close before the peer certifies.
	require.FileExists(t, filepath.Join(node.dir, old.name))
	roots.mu.RLock()
	_, proven := roots.retiredSnapshots[old.name]
	roots.mu.RUnlock()
	require.True(t, proven)
	before := node.snapshotBytes.Load()
	_, err = peer.Propose(t.Context(), []byte("peer strict certificate"))
	require.NoError(t, err)
	require.NoFileExists(t, filepath.Join(node.dir, old.name), "peer certificate must release a proven-retired idle owner's snapshot")
	require.Equal(t, before-int64(old.size), node.snapshotBytes.Load())
	require.Zero(t, roots.pendingSnapshotCount.Load(), "proven-retired names do not need an idle owner's publication retry")
}
