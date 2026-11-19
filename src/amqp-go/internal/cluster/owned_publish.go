package cluster

import (
	"context"

	"github.com/maxpert/amqp-go/internal/clusterproposal"
)

// SupportsOwnedPublishes advertises only the isolated local process path.
// Recheck during routing: activation can occur after the server encodes a burst.
func (n *Node) SupportsOwnedPublishes() bool {
	return n.cfg.local && n.cfg.ProcessDurability && n.peerClient() == nil
}

// SubmitOwned consumes a final publish allocation exactly once, even on error.
// Command routing, limits, safe rejection retries, redirects and ambiguous
// results use the existing route machinery. Remote/noneligible attempts keep
// the unchanged command bytes and ordinary proposal-copy behavior.
func (n *Node) SubmitOwned(ctx context.Context, queue string, command *clusterproposal.Owned) ([]byte, error) {
	owned, err := command.Begin()
	if err != nil {
		return nil, err
	}
	defer owned.Discard()
	data := owned.Command()
	if err := n.Ready(); err != nil {
		return nil, err
	}
	id := groupFor(queue, n.cfg.Shards)
	if !n.SupportsOwnedPublishes() {
		return n.submit(ctx, id, data)
	}
	return n.retryRoute(ctx, id, data, func(ctx context.Context, id uint64, data []byte) ([]byte, error) {
		return n.tryRouteOwned(ctx, id, data, routeCommand, owned)
	})
}
