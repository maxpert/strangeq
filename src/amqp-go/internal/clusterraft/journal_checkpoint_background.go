package clusterraft

import (
	"fmt"
	"sync"
	"time"

	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Only one group owns a routine checkpoint at a time. During that work,
// remember one other due group and give it a brief opportunity to retry.
// An idle or closed nominee cannot hold the slot indefinitely.
type journalCheckpointAdmission struct {
	mu         sync.Mutex
	active     bool
	next       uint64
	yieldUntil time.Time
}

func (a *journalCheckpointAdmission) tryAcquire(group uint64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.active {
		if a.next == 0 {
			a.next = group
		}
		return false
	}
	if a.next != 0 && a.next != group && time.Now().Before(a.yieldUntil) {
		return false
	}
	a.active = true
	a.next = 0
	return true
}

func (a *journalCheckpointAdmission) release() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.active = false
	if a.next != 0 {
		a.yieldUntil = time.Now().Add(25 * time.Millisecond)
	}
}

// Group.mu protects the job and reference. A nonempty ref means staging has
// finished and publication is waiting for a safe append boundary.
type routineJournalCheckpoint struct {
	index, term, configIndex uint64
	ref                      journalSnapshotRef
}

func (g *Group) checkpointDue() bool {
	return g.applied-g.snapshotIndex >= g.cfg.SnapshotEntries || g.logBytes >= g.cfg.SnapshotBytes
}

func (g *Group) maintainCheckpoint(synchronous bool) error {
	store, journal := g.store.(*journalGroupStore)
	background := journal && g.cfg.BackgroundJournalCheckpoints && !synchronous
	if g.configIndex > g.snapshotIndex {
		return g.checkpoint()
	}
	if background {
		if err := g.publishRoutineCheckpoint(store); err != nil {
			return err
		}
	}
	due, force := g.checkpointPressure()
	if force || (!background && due) {
		return g.checkpoint()
	}
	if !background || !due || g.routineCheckpoint != nil || g.applied == g.snapshotIndex {
		return nil
	}
	return g.startRoutineCheckpoint(store)
}

func (g *Group) startRoutineCheckpoint(store *journalGroupStore) error {
	if !store.node.routineSnapshots.tryAcquire(store.group) {
		return nil
	}
	started := false
	defer func() {
		if !started {
			store.node.routineSnapshots.release()
		}
	}()
	index := g.applied
	term, err := store.checkpointTerm(index)
	if err != nil {
		return err
	}
	snapshot := &pb.Snapshot{
		Metadata: &pb.SnapshotMetadata{Index: new(index), Term: new(term), ConfState: proto.CloneOf(g.conf)},
	}
	var encode func() ([]byte, error)
	if machine, ok := g.cfg.Machine.(interface {
		CaptureSnapshot() func([]byte) ([]byte, error)
	}); ok {
		captured := machine.CaptureSnapshot()
		prefix := encodeSnapshot(g.configIndex, nil)
		encode = func() ([]byte, error) { return captured(prefix) }
	} else {
		snapshot.Data, err = g.checkpointData()
		if err != nil {
			return err
		}
	}
	g.routineCheckpoint = &routineJournalCheckpoint{index: index, term: term, configIndex: g.configIndex}
	g.checkpointWorkers.Go(func() { g.stageRoutineCheckpoint(store, snapshot, encode) })
	started = true
	return nil
}

func (g *Group) stageRoutineCheckpoint(store *journalGroupStore, snapshot *pb.Snapshot, encode func() ([]byte, error)) {
	var ref journalSnapshotRef
	var err error
	if encode != nil {
		snapshot.Data, err = encode()
	}
	if err == nil {
		ref, err = store.node.stageSnapshot(snapshot)
	}
	snapshot.Data = nil
	g.mu.Lock()
	defer g.mu.Unlock()
	stopped := g.stopped()
	g.routineCheckpoint.ref = ref
	if stopped != nil || g.routineCheckpointObsolete() {
		g.releaseRoutineCheckpoint(true)
		return
	}
	if err != nil {
		g.releaseRoutineCheckpoint(true)
		g.stop(fmt.Errorf("stage routine raft checkpoint: %w", err))
		return
	}
	if err := g.publishRoutineCheckpoint(store); err != nil {
		g.stop(err)
	}
}

func (g *Group) routineCheckpointObsolete() bool {
	work := g.routineCheckpoint
	return work.index <= g.snapshotIndex || work.index > g.applied || work.configIndex != g.configIndex
}

func (g *Group) discardObsoleteRoutineCheckpoint() {
	if work := g.routineCheckpoint; work != nil && work.ref.name != "" && g.routineCheckpointObsolete() {
		g.releaseRoutineCheckpoint(true)
	}
}

func (g *Group) releaseRoutineCheckpoint(discard bool) {
	store := g.store.(*journalGroupStore)
	work := g.routineCheckpoint
	g.routineCheckpoint = nil
	if discard {
		store.reclaimSnapshot(work.ref)
	}
	store.node.routineSnapshots.release()
}

func (g *Group) publishRoutineCheckpoint(store *journalGroupStore) error {
	work := g.routineCheckpoint
	if work == nil || work.ref.name == "" || g.appending != nil {
		return nil
	}
	if g.routineCheckpointObsolete() {
		g.releaseRoutineCheckpoint(true)
		return nil
	}
	term, err := store.checkpointTerm(work.index)
	if err != nil || term != work.term {
		g.releaseRoutineCheckpoint(true)
		return err
	}
	if err := store.syncCommit(); err != nil {
		g.releaseRoutineCheckpoint(true)
		return err
	}
	err = store.persistCheckpoint(work.ref)
	// Once a checkpoint record may have reached disk, replay can select its
	// blob even if the write or sync reported an error.
	g.releaseRoutineCheckpoint(false)
	if err != nil {
		return err
	}
	g.recordCheckpoint(work.index)
	return nil
}
