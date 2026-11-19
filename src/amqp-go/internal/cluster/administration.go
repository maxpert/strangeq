package cluster

import (
	"context"
	"slices"
	"strconv"
	"time"

	"github.com/maxpert/amqp-go/internal/clusteradmin"
	"github.com/maxpert/amqp-go/internal/clusterraft"
)

// Observe exposes one broker's local catalog and Raft observations. A catalog
// member's presence is independent of the reachability of its broker process.
func (n *Node) Observe(ctx context.Context) (clusteradmin.Status, error) {
	if err := ctx.Err(); err != nil {
		return clusteradmin.Status{}, err
	}
	view := n.view()
	observer := strconv.FormatUint(n.cfg.Peer.ID, 10)
	report := clusteradmin.Status{
		ClusterID: n.cfg.ClusterID,
		Observer:  observer,
		Nodes:     make([]clusteradmin.Node, 0, len(view.Nodes)),
	}
	for _, peer := range view.Nodes {
		node := clusteradmin.Node{
			ID:      strconv.FormatUint(peer.ID, 10),
			Address: peer.Address,
			State:   "unknown",
		}
		if peer.ID == n.cfg.Peer.ID {
			ready := n.Ready() == nil
			node.Ready = &ready
			node.State = "running"
		}
		report.Nodes = append(report.Nodes, node)
	}

	n.groupsMu.RLock()
	groups := make([]*clusterraft.Group, 0, len(n.groups))
	for _, group := range n.groups {
		groups = append(groups, group)
	}
	n.groupsMu.RUnlock()
	report.Groups = make([]clusteradmin.Group, 0, len(groups))
	for _, group := range groups {
		if err := ctx.Err(); err != nil {
			return clusteradmin.Status{}, err
		}
		report.Groups = append(report.Groups, observedGroup(observer, group.Status()))
	}
	slices.SortFunc(report.Groups, func(a, b clusteradmin.Group) int {
		if a.Catalog != b.Catalog {
			if a.Catalog {
				return -1
			}
			return 1
		}
		return compareNodeID(a.ID, b.ID)
	})
	report.ObservedAt = time.Now().UTC()
	return report, nil
}

func compareNodeID(a, b string) int {
	if len(a) != len(b) {
		return len(a) - len(b)
	}
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func observedGroup(observer string, status clusterraft.Status) clusteradmin.Group {
	group := clusteradmin.Group{
		ID:          strconv.FormatUint(status.GroupID, 10),
		Catalog:     status.GroupID == catalogID,
		Observer:    observer,
		Term:        strconv.FormatUint(status.Term, 10),
		Voters:      decimalIDs(status.Voters),
		Learners:    decimalIDs(status.Learners),
		Commit:      strconv.FormatUint(status.Commit, 10),
		Applied:     strconv.FormatUint(status.Applied, 10),
		ConfigIndex: strconv.FormatUint(status.ConfigIndex, 10),
	}
	if len(status.Voters) > 0 {
		group.Quorum = len(status.Voters)/2 + 1
	}
	if status.Leader != 0 {
		group.Leader = strconv.FormatUint(status.Leader, 10)
	}
	if status.Progress != nil {
		active := 0
		for _, voter := range status.Voters {
			if voter == status.ID || status.Progress[voter].RecentActive {
				active++
			}
		}
		group.ActiveVoters = &active
	}
	return group
}

func decimalIDs(ids []uint64) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = strconv.FormatUint(id, 10)
	}
	return out
}
