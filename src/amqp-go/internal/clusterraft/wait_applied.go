package clusterraft

import (
	"context"
	"errors"

	"go.etcd.io/raft/v3"
)

const maxAppliedWaiters = 256

type appliedWaitSignal struct {
	changed chan struct{}
	index   uint64
	term    uint64
}

// WaitApplied waits for an advisory predicate on the leader's applied state.
// It does not establish a quorum fence. Predicate observation and subscription
// occur under the group lock. The predicate may not mutate or retain Machine.
func (g *Group) WaitApplied(ctx context.Context, predicate func(Machine) (bool, error)) error {
	if predicate == nil {
		return errors.New("applied readiness wait requires a predicate")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := g.stopped(); err != nil {
		return err
	}
	status := g.node.BasicStatus()
	if status.RaftState != raft.StateLeader {
		return ErrNotLeader
	}
	term := status.GetTerm()
	if g.appliedWaiters >= min(maxAppliedWaiters, g.cfg.MaxPending) {
		return ErrBusy
	}
	g.appliedWaiters++
	defer func() {
		g.appliedWaiters--
		if g.appliedWaiters == 0 {
			g.appliedWait = nil
		}
	}()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := g.stopped(); err != nil {
			return err
		}
		status := g.node.BasicStatus()
		if status.RaftState != raft.StateLeader || status.GetTerm() != term {
			return ErrLeadershipLost
		}
		ready, err := predicate(g.cfg.Machine)
		if ready || err != nil {
			return err
		}
		if g.appliedWait == nil {
			g.appliedWait = &appliedWaitSignal{changed: make(chan struct{}), index: g.applied, term: term}
		}
		changed := g.appliedWait.changed
		g.mu.Unlock()
		select {
		case <-changed:
		case <-g.done:
		case <-ctx.Done():
		}
		g.mu.Lock()
	}
}

// drive is reached after applying Ready entries and after asynchronous
// append completion. No signal is allocated when there are no parked waits.
func (g *Group) notifyAppliedWaiters(status raft.BasicStatus) {
	wait := g.appliedWait
	if wait != nil && (wait.index != g.applied || wait.term != status.GetTerm() || status.RaftState != raft.StateLeader) {
		close(wait.changed)
		g.appliedWait = nil
	}
}
