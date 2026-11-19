package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterproposal"
	"github.com/maxpert/amqp-go/internal/clusterqueue"
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
	invalidRead
	waitIdle
	waitQueueMissing
	waitQueueChanged
)

type routeMode uint8

const (
	routeCommand routeMode = iota
	routeRead
	routeWait
)

var errInvalidRead = errors.New("invalid shard read")

func (n *Node) Submit(ctx context.Context, queue string, data []byte) ([]byte, error) {
	if err := n.Ready(); err != nil {
		return nil, err
	}
	return n.submit(ctx, groupFor(queue, n.cfg.Shards), data)
}

// Query routes a read-only request to the shard leader and waits for its quorum fence.
func (n *Node) Query(ctx context.Context, key string, data []byte) ([]byte, error) {
	if err := n.Ready(); err != nil {
		return nil, err
	}
	return n.retryRoute(ctx, groupFor(key, n.cfg.Shards), data, n.tryQuery)
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
	return n.retryRoute(ctx, id, data, n.trySubmit)
}

func (n *Node) retryRoute(ctx context.Context, id uint64, data []byte, try func(context.Context, uint64, []byte) ([]byte, error)) ([]byte, error) {
	if len(data) > maxEntryBytes-128 {
		return nil, clusterraft.ErrTooLarge
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	for {
		response, err := try(ctx, id, data)
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
	return n.tryRoute(ctx, id, data, routeCommand)
}

func (n *Node) tryQuery(ctx context.Context, id uint64, data []byte) ([]byte, error) {
	return n.tryRoute(ctx, id, data, routeRead)
}

func (n *Node) tryRoute(ctx context.Context, id uint64, data []byte, mode routeMode) ([]byte, error) {
	return n.tryRouteOwned(ctx, id, data, mode, nil)
}

func (n *Node) tryRouteOwned(ctx context.Context, id uint64, data []byte, mode routeMode, owned *clusterproposal.Submission) ([]byte, error) {
	return n.tryRouteWithAssignment(ctx, id, data, mode, owned, nil)
}

func (n *Node) tryRouteWithAssignment(ctx context.Context, id uint64, data []byte, mode routeMode, owned *clusterproposal.Submission, localAssignment *localAssignmentResult) ([]byte, error) {
	readOnly := mode != routeCommand
	v := n.view()
	n.routeMu.RLock()
	targets := []uint64{n.routes[id]}
	n.routeMu.RUnlock()
	if g := n.group(id); g != nil {
		targets = append(targets, g.Leader(), n.cfg.Peer.ID)
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
		client := n.peerClient()
		if (peer != n.cfg.Peer.ID && (addr == "" || client == nil)) || visited[peer] || peer == 0 {
			continue
		}
		visited[peer] = true
		attempt := ctx
		var cancel context.CancelFunc
		if readOnly {
			// A deposed leader can wait for a quorum for most of the caller's
			// deadline. Give other candidates time to answer a retry.
			attempt, cancel = context.WithTimeout(ctx, requestTimeout/4)
		}
		var response reply
		var wire []byte
		var wireErr error
		if peer == n.cfg.Peer.ID {
			switch mode {
			case routeRead:
				localData := data
				if n.assignmentReplicaSlots != nil {
					localData = clusterqueue.InlineAssignmentReplicaRequest(data)
				}
				response = n.executeReadWithAssignment(attempt, id, localData, localAssignment)
			case routeWait:
				response = n.executeWait(attempt, id, data)
			default:
				if owned != nil && n.SupportsOwnedPublishes() {
					response = n.executeProposal(attempt, id, data, owned)
				} else {
					response = n.execute(attempt, id, data)
				}
			}
		} else {
			if readOnly && frame == nil {
				frame = make([]byte, 8, 8+len(data))
				binary.BigEndian.PutUint64(frame, id)
				frame = append(frame, data...)
			}
			switch mode {
			case routeRead:
				wire, wireErr = client.Read(attempt, addr, frame)
			case routeWait:
				wire, wireErr = client.Wait(attempt, addr, frame)
			default:
				wire, wireErr = client.CommandForGroup(attempt, addr, id, data)
			}
		}
		if cancel != nil {
			cancel()
		}
		if peer != n.cfg.Peer.ID {
			if wireErr != nil {
				if mode != routeWait {
					n.routeMu.Lock()
					if n.routes[id] == peer {
						delete(n.routes, id)
					}
					n.routeMu.Unlock()
				}
				if readOnly || errors.Is(wireErr, clusterwire.ErrUnsent) {
					continue
				}
				return nil, fmt.Errorf("%w: %v", ErrUncertain, wireErr)
			}
			maxCode := oversize
			switch mode {
			case routeRead:
				maxCode = invalidRead
			case routeWait:
				maxCode = waitQueueChanged
			}
			if len(wire) < 9 || wire[0] > maxCode {
				if readOnly {
					continue
				}
				return nil, ErrUncertain
			}
			response = reply{wire[0], binary.BigEndian.Uint64(wire[1:9]), wire[9:]}
		}
		switch response.code {
		case committed:
			if mode != routeWait {
				n.routeMu.Lock()
				n.routes[id] = peer
				n.routeMu.Unlock()
			}
			return response.body, nil
		case redirect, unavailable:
			if response.leader != 0 && !visited[response.leader] {
				targets = append([]uint64{response.leader}, targets...)
			}
		case oversize:
			return nil, clusterraft.ErrTooLarge
		case invalidRead:
			return nil, errInvalidRead
		case waitIdle:
			return nil, clusterqueue.ErrEmpty
		case waitQueueMissing:
			return nil, clusterqueue.ErrQueueNotFound
		case waitQueueChanged:
			return nil, clusterqueue.ErrQueueIncarnation
		default:
			if readOnly {
				continue
			}
			return nil, ErrUncertain
		}
	}
	return nil, ErrUnavailable
}

func (n *Node) execute(ctx context.Context, id uint64, data []byte) reply {
	return n.executeProposal(ctx, id, data, nil)
}

func (n *Node) executeProposal(ctx context.Context, id uint64, data []byte, owned *clusterproposal.Submission) reply {
	if n.cfg.ExperimentalRaftJournalDualStream && n.journal != nil && n.journal.Failure() != nil {
		return reply{code: unavailable}
	}
	if !n.cfg.ExperimentalAssignments && clusterqueue.IsAssignmentCommand(data) ||
		!n.cfg.ExperimentalJournalBodyCompression && clusterqueue.IsPackedPublish(data) ||
		!n.cfg.ExperimentalJournalSmallBodyCompression && clusterqueue.IsSmallPackedPublish(data) {
		return reply{body: clusterqueue.InvalidCommandReply()}
	}
	g := n.group(id)
	if g == nil {
		return reply{code: unavailable}
	}
	leader := g.Status().Leader
	if leader != n.cfg.Peer.ID {
		return reply{code: redirect, leader: leader}
	}
	var result []byte
	var err error
	if owned != nil && owned.Available() {
		result, err = g.ProposeOwned(ctx, owned)
	} else {
		// A gathered proposal can later be safely rejected. Only the
		// existing unavailable/redirect route permits reaching this retry;
		// uncertain results leave retryRoute immediately. Preserve its
		// old semantics with an ordinary copy after ownership was consumed.
		result, err = g.Propose(ctx, data)
	}
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
	response, err := n.commandReply(ctx, data)
	if err != nil {
		return nil, err
	}
	return encodeReply(response), nil
}

// CommandParts encodes the routing prefix and leaves the owned command result
// in its existing buffer for the peer server to sign and write.
func (n *Node) CommandParts(ctx context.Context, data []byte) ([]byte, []byte, error) {
	response, err := n.commandReply(ctx, data)
	if err != nil {
		return nil, nil, err
	}
	return appendReplyPrefix(make([]byte, 0, 9), response), response.body, nil
}

func (n *Node) commandReply(ctx context.Context, data []byte) (reply, error) {
	if len(data) < 8 {
		return reply{}, errors.New("invalid shard command")
	}
	id := binary.BigEndian.Uint64(data[:8])
	if !n.validGroup(id) {
		return reply{}, errors.New("invalid shard ID")
	}
	return n.execute(ctx, id, data[8:]), nil
}

func (n *Node) ReadCommand(ctx context.Context, data []byte) ([]byte, error) {
	response, err := n.readReply(ctx, data)
	if err != nil {
		return nil, err
	}
	return encodeReply(response), nil
}

// ReadCommandParts leaves the owned read result in its existing buffer for
// the peer server to sign and write.
func (n *Node) ReadCommandParts(ctx context.Context, data []byte) ([]byte, []byte, error) {
	response, err := n.readReply(ctx, data)
	if err != nil {
		return nil, nil, err
	}
	return appendReplyPrefix(make([]byte, 0, 9), response), response.body, nil
}

func (n *Node) readReply(ctx context.Context, data []byte) (reply, error) {
	if len(data) < 8 {
		return reply{}, errInvalidRead
	}
	id := binary.BigEndian.Uint64(data[:8])
	if id == catalogID || !n.validGroup(id) {
		return reply{}, errInvalidRead
	}
	if err := n.Ready(); err != nil {
		return reply{}, err
	}
	return n.executeRead(ctx, id, data[8:]), nil
}

func (n *Node) executeRead(ctx context.Context, id uint64, data []byte) reply {
	return n.executeReadWithAssignment(ctx, id, data, nil)
}

func (n *Node) executeReadWithAssignment(ctx context.Context, id uint64, data []byte, localAssignment *localAssignmentResult) reply {
	if len(data) > maxEntryBytes-128 {
		return reply{code: oversize}
	}
	assignments := clusterqueue.IsAssignmentRequest(data)
	replicaBodies := clusterqueue.IsAssignmentReplica(data)
	var coverage clusterqueue.AssignmentCoverage
	var request clusterqueue.AssignmentRequest
	if assignments || replicaBodies {
		var err error
		if replicaBodies {
			if n.assignmentReplicaSlots == nil {
				return reply{code: invalidRead}
			}
			request, coverage, err = clusterqueue.DecodeAssignmentReplicaRequest(data)
		} else {
			request, err = clusterqueue.DecodeAssignmentRequest(data)
		}
		if err != nil || !n.cfg.ExperimentalAssignments || groupFor(request.Queue, n.cfg.Shards) != id {
			return reply{code: invalidRead}
		}
	}
	g := n.group(id)
	if g == nil {
		return reply{code: unavailable}
	}
	if leader := g.Leader(); leader != n.cfg.Peer.ID {
		return reply{code: redirect, leader: leader}
	}
	var result []byte
	var err error
	if assignments && localAssignment != nil && n.SupportsOwnedPublishes() {
		result, err = g.ReadCaptured(ctx, func(machine clusterraft.Machine) (func() ([]byte, error), int, error) {
			if query, ok := machine.(interface {
				CaptureLocalAssignmentPage(clusterqueue.AssignmentRequest, bool) (func() (*clusterqueue.BorrowedAssignmentPage, error), int, error)
			}); ok {
				encode, size, err := query.CaptureLocalAssignmentPage(request, g.ProcessReadAllowed())
				if err != nil {
					if assignmentResultError(err) {
						return func() ([]byte, error) { localAssignment.domainErr = err; return nil, nil }, 0, nil
					}
					return nil, 0, err
				}
				return func() ([]byte, error) {
					page, err := encode()
					if err != nil {
						return nil, err
					}
					localAssignment.page = page
					return nil, nil
				}, size, nil
			}
			// A legacy local machine keeps the same portable capture inside
			// this one authorized read, without selecting a second time.
			if query, ok := machine.(interface {
				CaptureAssignmentPage(clusterqueue.AssignmentRequest) (func() ([]byte, error), int, error)
			}); ok {
				return query.CaptureAssignmentPage(request)
			}
			return nil, 0, errInvalidRead
		}, clusterraft.CaptureOffLock)
		if err != nil {
			localAssignment.release()
		}
	} else if assignments || replicaBodies {
		result, err = g.ReadCaptured(ctx, func(machine clusterraft.Machine) (func() ([]byte, error), int, error) {
			if replicaBodies {
				if query, ok := machine.(interface {
					CaptureAssignmentReplica(clusterqueue.AssignmentRequest, clusterqueue.AssignmentCoverage) (func() ([]byte, error), int, error)
				}); ok {
					return query.CaptureAssignmentReplica(request, coverage)
				}
				return nil, 0, errInvalidRead
			}
			query, ok := machine.(interface {
				CaptureAssignmentPage(clusterqueue.AssignmentRequest) (func() ([]byte, error), int, error)
			})
			if !ok {
				return nil, 0, errInvalidRead
			}
			return query.CaptureAssignmentPage(request)
		})
	} else {
		result, err = g.Read(ctx, func(machine clusterraft.Machine) ([]byte, error) {
			query, ok := machine.(interface{ Query([]byte) ([]byte, error) })
			if !ok {
				return nil, errInvalidRead
			}
			body, err := query.Query(data)
			if err != nil {
				return nil, errInvalidRead
			}
			return body, nil
		})
	}
	switch {
	case err == nil:
		return reply{body: result}
	case errors.Is(err, clusterraft.ErrNotLeader), (assignments || replicaBodies) && errors.Is(err, clusterraft.ErrLeadershipLost):
		return reply{code: redirect, leader: g.Leader()}
	case errors.Is(err, errInvalidRead):
		return reply{code: invalidRead}
	default:
		return reply{code: unavailable}
	}
}

func appendReplyPrefix(dst []byte, response reply) []byte {
	dst = append(dst, response.code)
	return binary.BigEndian.AppendUint64(dst, response.leader)
}

func encodeReply(response reply) []byte {
	frame := appendReplyPrefix(make([]byte, 0, 9+len(response.body)), response)
	return append(frame, response.body...)
}

func (n *Node) Membership(_ context.Context, data []byte) ([]byte, error) {
	if len(data) != 0 || !n.enrolled(n.view()) {
		return nil, ErrUnavailable
	}
	return encode(n.view()), nil
}
