package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"slices"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterraft"
)

func sortMembers(ids []uint64) []uint64 { slices.Sort(ids); return ids }

func (n *Node) maintain() {
	defer n.wg.Done()
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	completed := make(chan uint64, 8)
	working := map[uint64]bool{}
	lastExpiry := map[uint64]time.Time{}
	var nextRead time.Time
	for {
		select {
		case <-n.ctx.Done():
			return
		case id := <-completed:
			delete(working, id)
		case now := <-tick.C:
			if !now.Before(nextRead) {
				n.syncCatalog()
				nextRead = now.Add(time.Second)
			}
			n.adjustLocal()
			n.groupsMu.RLock()
			for id, g := range n.groups {
				if len(working) >= cap(completed) {
					break
				}
				if working[id] || g.Status().Leader != n.cfg.Peer.ID {
					continue
				}
				working[id] = true
				expire := id != catalogID && n.cfg.ExpiryCommand != nil && now.Sub(lastExpiry[id]) > 5*time.Second
				if expire {
					lastExpiry[id] = now
				}
				n.wg.Add(1)
				go func() {
					defer n.wg.Done()
					defer func() {
						select {
						case completed <- id:
						case <-n.ctx.Done():
						}
					}()
					n.reconcile(id, g)
					if expire {
						ctx, cancel := context.WithTimeout(n.ctx, time.Second)
						defer cancel()
						_, _ = g.Propose(ctx, n.cfg.ExpiryCommand(now))
					}
				}()
			}
			n.groupsMu.RUnlock()
		}
	}
}

func (n *Node) syncCatalog() {
	if local := n.group(catalogID); local != nil && local.Status().ConfigIndex > 0 {
		n.install(n.catalog.view())
		select {
		case <-local.Done():
		default:
			return
		}
	}

	v := n.view()
	addresses := make([]string, 0, len(v.Catalog.Voters)+len(n.cfg.Discovery)+len(n.catalog.initial.Nodes))
	for _, id := range v.Catalog.Voters {
		addresses = append(addresses, address(v, id))
	}
	addresses = append(addresses, n.cfg.Discovery...)
	for _, peer := range n.catalog.initial.Nodes {
		addresses = append(addresses, peer.Address)
	}
	seen := make(map[string]bool, len(addresses))
	for _, p := range addresses {
		if p == "" || p == n.cfg.Peer.Address || seen[p] {
			continue
		}
		seen[p] = true
		ctx, cancel := context.WithTimeout(n.ctx, time.Second)
		raw, err := n.client.Membership(ctx, p, nil)
		cancel()
		if err != nil {
			continue
		}
		var v view
		if json.Unmarshal(raw, &v) == nil {
			n.install(v)
		}
	}
	v = n.view()
	if n.enrolled(v) || address(v, n.cfg.Peer.ID) != "" {
		return
	}
	ctx, cancel := context.WithTimeout(n.ctx, 2*time.Second)
	defer cancel()
	n.catalogWrite(ctx, catalogCommand{Join: &n.cfg.Peer, Nonce: n.nonce})
}

func (n *Node) catalogWrite(ctx context.Context, cmd catalogCommand) {
	raw, err := n.submit(ctx, catalogID, encode(cmd))
	if err != nil {
		return
	}
	var reply catalogReply
	if json.Unmarshal(raw, &reply) == nil && reply.Error == "" {
		n.install(reply.View)
	}
}

func (n *Node) adjustLocal() {
	v := n.view()
	if !n.enrolled(v) {
		return
	}
	n.ensureLocal(v, v.Catalog)
	for _, p := range v.Placements {
		n.ensureLocal(v, p)
	}
}

func (n *Node) ensureLocal(v view, p placement) {
	me := n.cfg.Peer.ID
	wanted := contains(desiredMembers(v, p.Group), me)
	member := contains(p.Voters, me) || contains(p.Learners, me)
	g := n.group(p.Group)
	if g == nil && (wanted || member) {
		// Current voters must reopen durable state. An empty store is only
		// eligible when placement asks for a new learner.
		if !contains(p.Voters, me) {
			_ = n.openGroup(p.Group, nil)
		}
	}
	if g != nil && !wanted && !member && p.Index > 0 && p.Index >= g.Status().ConfigIndex {
		_ = n.removeLocal(p.Group, g)
	}
}

func (n *Node) removeLocal(group uint64, g *clusterraft.Group) error {
	if err := durableFile(n.retired(group), []byte{1}); err != nil {
		return err
	}
	n.groupsMu.Lock()
	delete(n.groups, group)
	n.groupsMu.Unlock()
	if n.journal != nil {
		// The durable retirement marker fences the old group's records.
		// This prototype retains the shared journal until Node closes.
		return g.Close()
	}
	return errors.Join(g.Close(), os.RemoveAll(n.store(group)))
}

func (n *Node) reconcile(id uint64, g *clusterraft.Group) {
	st := g.Status()
	if st.Leader != n.cfg.Peer.ID || len(st.Voters) < 1 {
		return
	}
	v := n.view()
	p := groupPlacement(v, id)
	ctx, cancel := context.WithTimeout(n.ctx, 2*time.Second)
	defer cancel()
	if st.ConfigIndex > p.Index {
		r := placement{Group: id, Index: st.ConfigIndex, Voters: st.Voters, Learners: st.Learners}
		n.catalogWrite(ctx, catalogCommand{Report: &r})
		return
	}
	if st.ConfigIndex < p.Index {
		return
	}
	wanted := desiredMembers(v, id)
	for _, learner := range st.Learners {
		if !contains(wanted, learner) {
			_ = g.Remove(ctx, learner)
		} else if g.CaughtUp(learner) {
			_ = g.Promote(ctx, learner)
		}
		return
	}
	if len(st.Voters) > len(wanted) {
		for _, voter := range st.Voters {
			if voter != st.Leader && !contains(wanted, voter) {
				_ = g.Remove(ctx, voter)
				return
			}
		}
		for _, target := range wanted {
			if target != st.Leader && g.CaughtUp(target) {
				_ = g.TransferLeadership(ctx, target)
				return
			}
		}
		return
	}
	for _, target := range wanted {
		if !contains(st.Voters, target) {
			_ = g.AddLearner(ctx, target)
			return
		}
	}
	if id != catalogID && wanted[0] != st.Leader && g.CaughtUp(wanted[0]) {
		_ = g.TransferLeadership(ctx, wanted[0])
	}
}
