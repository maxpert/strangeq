package clusterraft

import (
	"context"
	"crypto/rand"
	"errors"
	"slices"

	"go.etcd.io/raft/v3"
)

type raftRead struct {
	ctx    context.Context
	query  func(Machine) ([]byte, error)
	result chan response
	issued bool
}

const singletonReadSuffix byte = 1

type readFence struct {
	id        key
	index     uint64
	waiters   int
	issued    bool
	confirmed bool
	singleton bool
}

// Read runs a read-only query after a quorum fence has reached the state machine.
// The query runs under the group lock and must not mutate or retain the machine.
func (g *Group) Read(ctx context.Context, query func(Machine) ([]byte, error)) ([]byte, error) {
	if query == nil {
		return nil, errors.New("raft read requires a query")
	}
	var id key
	if _, err := rand.Read(id[:]); err != nil {
		return nil, err
	}
	read := &raftRead{ctx: ctx, query: query, result: make(chan response, 1)}
	if err := g.change(ctx, func() error {
		status := g.node.BasicStatus()
		leader, singleton := g.readQuorum(status)
		if !leader {
			return ErrNotLeader
		}
		// Wait for a current-term applied entry before submitting to Raft.
		// This keeps our reads out of its pre-commit queue across elections.
		term, err := g.store.Term(g.applied)
		if err != nil || term == 0 || term != status.GetTerm() {
			return ErrBusy
		}
		if g.readTerm != 0 && g.readTerm != term {
			g.failReads(ErrLeadershipLost)
		}
		pending := len(g.reads)
		if g.readFence.issued && !g.readFence.singleton && !g.readFence.confirmed && g.readFence.waiters == 0 ||
			g.retiredRead != nil && !singleton {
			// RawNode retains an unanswered multisite fence when its callers leave.
			pending++
		}
		if pending >= g.cfg.MaxPending {
			return ErrBusy
		}
		if g.reads == nil {
			g.reads = make(map[key]*raftRead)
		}
		g.readTerm = term
		g.reads[id] = read
		if !g.readFence.issued && (g.retiredRead == nil || singleton) {
			read.issued = true
			g.issueReadFence(id, 1, singleton)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	select {
	case done := <-read.result:
		return done.body, done.err
	case <-ctx.Done():
		g.mu.Lock()
		if g.reads[id] == read {
			delete(g.reads, id)
			if read.issued {
				g.readFence.waiters--
			}
		}
		g.mu.Unlock()
		return nil, ctx.Err()
	}
}

func (g *Group) readQuorum(status raft.BasicStatus) (leader, singleton bool) {
	if status.RaftState != raft.StateLeader {
		return false, false
	}
	voters, outgoing := g.conf.GetVoters(), g.conf.GetVotersOutgoing()
	if len(voters) == 1 && len(outgoing) == 0 {
		local := voters[0] == g.cfg.ID
		return local, local
	}
	return slices.Contains(voters, g.cfg.ID) || slices.Contains(outgoing, g.cfg.ID), false
}

func (g *Group) issueReadFence(id key, waiters int, singleton bool) {
	g.readFence = readFence{id: id, waiters: waiters, issued: true, singleton: singleton}
	requestCtx := id[:]
	if singleton {
		// A singleton can re-fence the same callers while Raft still holds
		// their original multisite request.
		requestCtx = append(requestCtx, singletonReadSuffix)
	}
	g.node.ReadIndex(requestCtx)
}

func (g *Group) recordReadStates(states []raft.ReadState) {
	for _, state := range states {
		ctx := state.RequestCtx
		if g.retiredRead != nil && len(ctx) == len(key{}) && key(ctx) == *g.retiredRead {
			g.retiredRead = nil
			continue
		}
		fence := &g.readFence
		if !fence.issued || fence.confirmed {
			continue
		}
		if fence.singleton {
			if len(ctx) != len(key{})+1 || ctx[len(key{})] != singletonReadSuffix {
				continue
			}
			ctx = ctx[:len(key{})]
		}
		if len(ctx) == len(key{}) && key(ctx) == fence.id {
			fence.index = state.Index
			fence.confirmed = true
		}
	}
}

// finishReads reports whether it submitted a new fence that drive must flush.
func (g *Group) finishReads(status raft.BasicStatus) bool {
	if g.readTerm == 0 {
		return false
	}
	leader, singleton := g.readQuorum(status)
	if !leader || status.GetTerm() != g.readTerm {
		g.failReads(ErrLeadershipLost)
		return false
	}
	fence := g.readFence
	switch {
	case !fence.issued:
	case fence.confirmed && fence.index <= g.applied:
		g.completeReads()
	case singleton && !fence.singleton && !fence.confirmed:
		// Raft still needs a heartbeat response to retire the old request.
		// A new local fence lets current callers finish without that response.
		// On re-expansion the old request is the only outstanding multisite fence.
		g.retiredRead = &fence.id
	default:
		return false
	}
	g.readFence = readFence{}
	return g.issueQueuedReads(singleton)
}

func (g *Group) completeReads() {
	for id, read := range g.reads {
		if !read.issued {
			continue
		}
		delete(g.reads, id)
		if err := read.ctx.Err(); err != nil {
			read.result <- response{err: err}
			continue
		}
		body, err := read.query(g.cfg.Machine)
		read.result <- response{body: body, err: err}
		// A local storage fault can stop the group from inside the query.
		// stop has already completed the other read waiters.
		if g.terminal != nil {
			return
		}
	}
}

func (g *Group) issueQueuedReads(singleton bool) bool {
	if g.retiredRead != nil && !singleton {
		return false
	}
	var next key
	var waiters int
	for id, read := range g.reads {
		if err := read.ctx.Err(); err != nil {
			delete(g.reads, id)
			read.result <- response{err: err}
			continue
		}
		read.issued = true
		next = id
		waiters++
	}
	if waiters == 0 {
		if g.retiredRead == nil {
			g.readTerm = 0
		}
		return false
	}
	g.issueReadFence(next, waiters, singleton)
	return true
}

func (g *Group) failReads(err error) {
	for _, read := range g.reads {
		read.result <- response{err: err}
	}
	g.reads = nil
	g.readFence = readFence{}
	g.readTerm = 0
	g.retiredRead = nil
}
