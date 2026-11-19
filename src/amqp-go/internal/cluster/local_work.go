package cluster

import (
	"context"

	"github.com/maxpert/amqp-go/internal/clusterraft"
)

// WorkShard identifies the data shard that owns a queue key.
func (n *Node) WorkShard(queue string) uint64 { return groupFor(queue, n.cfg.Shards) }

// LocalWorkShards finds locally led data shards with work using a cheap
// advisory predicate. The subsequent read must still pass a quorum fence.
func (n *Node) LocalWorkShards(pending func(clusterraft.Machine) bool) []uint64 {
	if n.Ready() != nil {
		return nil
	}
	var shards []uint64
	for id := uint64(1); id <= n.cfg.Shards; id++ {
		if group := n.group(id); group != nil && group.HasLocalWork(pending) {
			shards = append(shards, id)
		}
	}
	return shards
}

func (n *Node) ReadLocalWork(ctx context.Context, id uint64, capture func(clusterraft.Machine) (func() ([]byte, error), int, error)) ([]byte, error) {
	if group := n.group(id); group != nil {
		return group.ReadCaptured(ctx, capture)
	}
	return nil, ErrUnavailable
}

func (n *Node) SubmitLocalWork(ctx context.Context, id uint64, data []byte) ([]byte, error) {
	if group := n.group(id); group != nil {
		return group.Propose(ctx, data)
	}
	return nil, ErrUnavailable
}
