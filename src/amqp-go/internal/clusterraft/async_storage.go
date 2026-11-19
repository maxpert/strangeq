package clusterraft

import (
	"errors"
	"fmt"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
	"google.golang.org/protobuf/proto"
)

type appendJob struct {
	write         readyWrite
	append, apply *pb.Message
	snapshots     []*pb.Message
	done          chan struct{}
}

// flush owns the consensus and application state. It accepts only one append
// job at a time. Ordinary outbound messages have no storage dependency and
// may leave before the job. Responses inside storage messages stay ordered.
func (g *Group) flush(syncWrites bool) error {
	for g.appending == nil && g.node.HasReady() {
		ready := g.node.Ready()
		g.recordReadStates(ready.ReadStates)
		job := &appendJob{}
		var delta journalGroupDelta
		for _, message := range ready.Messages {
			switch message.GetType() {
			case pb.MsgStorageAppend:
				job.append = message
			case pb.MsgStorageApply:
				job.apply = message
			case pb.MsgSnap:
				// An apply can advance the membership configuration. Check
				// snapshot freshness after applying the preceding entries.
				job.snapshots = append(job.snapshots, message)
			default:
				g.send(message)
			}
		}
		if job.append != nil {
			write, err := g.store.planReady(raft.Ready{
				Entries: job.append.GetEntries(), Snapshot: job.append.GetSnapshot(),
				HardState: &pb.HardState{Term: job.append.Term, Vote: job.append.Vote, Commit: job.append.Commit},
				MustSync:  ready.MustSync,
			})
			if err != nil {
				return fmt.Errorf("plan raft append: %w", err)
			}
			if journal, ok := g.store.(*journalGroupStore); ok && g.cfg.DeferJournalCommit &&
				!syncWrites && len(job.append.GetResponses()) == 0 && journal.deferCommit(write) {
				// The journal already holds the entries this update commits.
				job.append = nil
			} else {
				job.write = write
				// Replacements and snapshot installs change previously visible
				// keys. Keep those writes exclusive with all Raft storage reads.
				if !syncWrites && !write.replacesLog && needsDurableWrite(write.ready) {
					job.done = make(chan struct{})
					g.appending = job
					g.appendJobs <- job
					return nil
				}
				delta, err = g.store.persistReady(write)
				if err != nil {
					return fmt.Errorf("persist raft entries: %w", err)
				}
			}
		}
		if err := g.finishReady(job, delta, syncWrites); err != nil {
			return err
		}
	}
	return nil
}

func (g *Group) runWriter() {
	for {
		select {
		case <-g.done:
			return
		case <-g.storeFailed:
			g.mu.Lock()
			g.stopped()
			g.mu.Unlock()
			return
		case job := <-g.appendJobs:
			delta, err := g.store.persistReady(job.write)
			if err := g.finishAppend(job, delta, err); err != nil {
				return
			}
		}
	}
}

func (g *Group) finishAppend(job *appendJob, delta journalGroupDelta, persistErr error) (err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	defer close(job.done)
	defer g.stopOnStoreReadError(&err)
	g.appending = nil
	g.bufferedOps, g.bufferedBytes = 0, 0
	if persistErr != nil {
		failure := fmt.Errorf("persist raft entries: %w", persistErr)
		if g.terminal != nil {
			g.terminalCause = errors.Join(g.terminalCause, failure)
			return g.terminalCause
		}
		g.stop(failure)
		return failure
	}
	if g.terminal != nil {
		return g.terminal
	}
	if err := g.finishReady(job, delta, g.exclusiveWaiters > 0); err != nil {
		g.stop(err)
		return err
	}
	// Complete the Ready work already admitted behind this write. An
	// exclusive waiter closed ordinary admission and needs a fully idle
	// writer before changing the visible log range.
	return g.drive(g.exclusiveWaiters > 0)
}

func (g *Group) finishReady(job *appendJob, delta journalGroupDelta, syncWrites bool) error {
	if err := g.stopped(); err != nil {
		return err
	}
	if job.append != nil {
		g.store.publishReady(job.write, delta)
		if snapshot := job.append.GetSnapshot(); !raft.IsEmptySnap(snapshot) {
			if err := g.restore(snapshot); err != nil {
				return err
			}
		}
		if journal, ok := g.store.(*journalGroupStore); ok && job.write.replacesLog {
			g.logBytes, g.appliedLogBytes = journal.remainingLogBytes(g.applied)
		} else {
			for _, entry := range job.append.GetEntries() {
				g.logBytes += uint64(proto.Size(entry))
			}
		}
		if err := g.releaseResponses(job.append); err != nil {
			return err
		}
	}
	if job.apply != nil {
		for _, entry := range job.apply.GetEntries() {
			if err := g.apply(entry); err != nil {
				return err
			}
		}
		if err := g.releaseResponses(job.apply); err != nil {
			return err
		}
	}
	// Membership and learner snapshot freshness still use the synchronous
	// path. Only staged blob I/O for a routine journal checkpoint can overlap
	// appends, and publication waits for this Ready's delta to be visible.
	if err := g.maintainCheckpoint(syncWrites || len(job.snapshots) != 0); err != nil {
		return err
	}
	for _, message := range job.snapshots {
		g.send(message)
	}
	return nil
}

func (g *Group) releaseResponses(job *pb.Message) error {
	for _, response := range job.GetResponses() {
		if response.GetTo() == g.cfg.ID {
			if err := g.node.Step(response); err != nil && !errors.Is(err, raft.ErrStepPeerNotFound) {
				return err
			}
		} else {
			g.send(response)
		}
	}
	return nil
}

func (g *Group) send(message *pb.Message) {
	if notice := g.matchedCommitHeartbeat(message); notice != nil {
		message = notice
	}
	if message.GetType() == pb.MsgSnap && message.GetSnapshot().GetMetadata().GetIndex() < g.configIndex {
		g.node.ReportSnapshot(message.GetTo(), raft.SnapshotFailure)
		return
	}
	if err := g.cfg.Send(message); err != nil {
		if message.GetType() == pb.MsgSnap {
			g.node.ReportSnapshot(message.GetTo(), raft.SnapshotFailure)
		} else {
			g.node.ReportUnreachable(message.GetTo())
		}
	}
}

// A follower's earlier append response already proves this prefix durable.
// The pinned Raft implementation sends the same bounded commit index in its
// ordinary heartbeats. Empty appends used for probing or recovering inflights
// must still receive their ordinary append responses.
func (g *Group) matchedCommitHeartbeat(message *pb.Message) *pb.Message {
	if !g.cfg.DeferJournalCommit || !g.journalInput || message.GetType() != pb.MsgApp ||
		message.GetFrom() != g.cfg.ID || message.GetTo() == g.cfg.ID ||
		len(message.GetEntries()) != 0 || len(message.GetContext()) != 0 || !raft.IsEmptySnap(message.GetSnapshot()) ||
		message.GetCommit() == 0 || message.GetCommit() > message.GetIndex() {
		return nil
	}
	status := g.node.BasicStatus()
	if status.RaftState != raft.StateLeader || status.GetTerm() != message.GetTerm() || status.GetCommit() < message.GetCommit() {
		return nil
	}
	// Keep the first commit notification of a new term on the ordinary
	// append path. Startup and membership recovery already use that fence.
	if term, err := g.store.Term(min(g.applied, message.GetCommit()-1)); err != nil || term != status.GetTerm() {
		return nil
	}
	matched := false
	g.node.WithProgress(func(id uint64, typ raft.ProgressType, progress tracker.Progress) {
		if id == message.GetTo() && typ == raft.ProgressTypePeer && progress.RecentActive &&
			progress.State == tracker.StateReplicate && !progress.IsPaused() {
			matched = progress.Next != 0 && progress.Next-1 == progress.Match && message.GetIndex() == progress.Match
		}
	})
	if !matched {
		return nil
	}
	return &pb.Message{
		Type: pb.MsgHeartbeat.Enum(), From: message.From, To: message.To,
		Term: message.Term, Commit: message.Commit,
	}
}
