package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterraft"
	"github.com/maxpert/amqp-go/internal/clusterwire"
)

type reply struct {
	code   byte
	leader uint64
	body   []byte
}

const (
	committed byte = iota
	redirect
	unavailable
	uncertain
	oversize
)

func (n *Node) Submit(ctx context.Context, queue string, data []byte) ([]byte, error) {
	if err := n.Ready(); err != nil {
		return nil, err
	}
	return n.submit(ctx, groupFor(queue, n.cfg.Shards), data)
}

func (n *Node) SubmitEachShard(ctx context.Context, data []byte) ([][]byte, error) {
	if err := n.Ready(); err != nil {
		return nil, err
	}
	out := make([][]byte, 0, n.cfg.Shards)
	for id := uint64(1); id <= n.cfg.Shards; id++ {
		response, err := n.submit(ctx, id, data)
		if err != nil {
			return nil, err
		}
		out = append(out, response)
	}
	return out, nil
}

func (n *Node) submit(ctx context.Context, id uint64, data []byte) ([]byte, error) {
	if len(data) > maxEntryBytes-128 {
		return nil, clusterraft.ErrTooLarge
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	for {
		response, err := n.trySubmit(ctx, id, data)
		if !errors.Is(err, ErrUnavailable) {
			return response, err
		}
		timer := time.NewTimer(40 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ErrUnavailable
		case <-timer.C:
		}
	}
}

func (n *Node) trySubmit(ctx context.Context, id uint64, data []byte) ([]byte, error) {
	v := n.view()
	n.routeMu.RLock()
	targets := []uint64{n.routes[id]}
	n.routeMu.RUnlock()
	if g := n.group(id); g != nil {
		targets = append(targets, g.Status().Leader, n.cfg.Peer.ID)
	}
	targets = append(targets, groupPlacement(v, id).Voters...)
	if id == catalogID {
		for _, p := range n.catalog.initial.Nodes {
			targets = append(targets, p.ID)
		}
	}
	var frame []byte
	visited := make(map[uint64]bool)
	for len(targets) > 0 && ctx.Err() == nil {
		peer := targets[0]
		targets = targets[1:]
		addr := address(v, peer)
		if addr == "" || visited[peer] {
			continue
		}
		visited[peer] = true
		var response reply
		if peer == n.cfg.Peer.ID {
			response = n.execute(ctx, id, data)
		} else {
			if frame == nil {
				frame = make([]byte, 8, 8+len(data))
				binary.BigEndian.PutUint64(frame, id)
				frame = append(frame, data...)
			}
			b, err := n.client.Command(ctx, addr, frame)
			if err != nil {
				n.routeMu.Lock()
				if n.routes[id] == peer {
					delete(n.routes, id)
				}
				n.routeMu.Unlock()
				if errors.Is(err, clusterwire.ErrUnsent) {
					continue
				}
				return nil, fmt.Errorf("%w: %v", ErrUncertain, err)
			}
			if len(b) < 9 || b[0] > oversize {
				return nil, ErrUncertain
			}
			response = reply{b[0], binary.BigEndian.Uint64(b[1:9]), b[9:]}
		}
		switch response.code {
		case committed:
			n.routeMu.Lock()
			n.routes[id] = peer
			n.routeMu.Unlock()
			return response.body, nil
		case redirect, unavailable:
			if response.leader != 0 && !visited[response.leader] {
				targets = append([]uint64{response.leader}, targets...)
			}
		case oversize:
			return nil, clusterraft.ErrTooLarge
		default:
			return nil, ErrUncertain
		}
	}
	return nil, ErrUnavailable
}

func (n *Node) execute(ctx context.Context, id uint64, data []byte) reply {
	g := n.group(id)
	if g == nil {
		return reply{code: unavailable}
	}
	leader := g.Status().Leader
	if leader != n.cfg.Peer.ID {
		return reply{code: redirect, leader: leader}
	}
	result, err := g.Propose(ctx, data)
	switch {
	case err == nil:
		return reply{body: result}
	case errors.Is(err, clusterraft.ErrNotLeader):
		return reply{code: redirect, leader: g.Status().Leader}
	case errors.Is(err, clusterraft.ErrBusy):
		return reply{code: unavailable}
	case errors.Is(err, clusterraft.ErrTooLarge):
		return reply{code: oversize}
	default:
		return reply{code: uncertain}
	}
}

func (n *Node) Command(ctx context.Context, data []byte) ([]byte, error) {
	if len(data) < 8 {
		return nil, errors.New("invalid shard command")
	}
	id := binary.BigEndian.Uint64(data[:8])
	if !n.validGroup(id) {
		return nil, errors.New("invalid shard ID")
	}
	response := n.execute(ctx, id, data[8:])
	frame := make([]byte, 9, 9+len(response.body))
	frame[0] = response.code
	binary.BigEndian.PutUint64(frame[1:], response.leader)
	return append(frame, response.body...), nil
}

func (n *Node) Membership(_ context.Context, data []byte) ([]byte, error) {
	if len(data) != 0 || !n.enrolled(n.view()) {
		return nil, ErrUnavailable
	}
	return encode(n.view()), nil
}
