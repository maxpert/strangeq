package clusterraft

import (
	"context"

	"go.etcd.io/raft/v3"
)

// A process singleton has no remote quorum to contact. Once its current-term
// commit is persisted and applied, the query can linearize here under g.mu.
// Any ordering/fence/membership uncertainty retains the existing ReadIndex path.
// ReadCaptured still supplies its normal pin/callback through this same query.
func (g *Group) tryProcessSingletonRead(ctx context.Context, query func(Machine) ([]byte, error)) (body []byte, handled bool, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	defer g.stopOnStoreReadError(&err)
	if !g.cfg.ProcessDurability || !g.store.node.writer.durability.processAllowed() {
		return nil, false, nil
	}
	if err := g.stopped(); err != nil {
		return nil, true, err
	}
	if err := ctx.Err(); err != nil {
		return nil, true, err
	}
	if g.gatheredProposal != nil || g.exclusiveWaiters != 0 || g.configuration != (configProposal{}) ||
		len(g.reads) != 0 || g.readFence != (readFence{}) || g.retiredRead != nil || g.readTerm != 0 {
		return nil, false, nil
	}
	status := g.node.BasicStatus()
	if status.RaftState != raft.StateLeader || status.Lead != g.cfg.ID || status.ID != g.cfg.ID ||
		status.GetTerm() == 0 || g.applied < status.GetCommit() {
		return nil, false, nil
	}
	conf := g.conf
	if len(conf.GetVoters()) != 1 || conf.GetVoters()[0] != g.cfg.ID ||
		len(conf.GetVotersOutgoing()) != 0 || len(conf.GetLearners()) != 0 || len(conf.GetLearnersNext()) != 0 || conf.GetAutoLeave() {
		return nil, false, nil
	}
	// Mark handled before the store read: typed read panics must stop this
	// group and return its terminal error, never enter a second read path.
	handled = true
	term, termErr := g.store.Term(g.applied)
	if termErr != nil || term == 0 || term != status.GetTerm() {
		return nil, false, nil
	}
	body, err = query(g.cfg.Machine)
	return body, true, err
}
