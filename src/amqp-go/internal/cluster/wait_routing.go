package cluster

import (
	"context"
	"encoding/binary"
	"errors"
	"unicode/utf8"

	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/internal/clusterraft"
	"github.com/maxpert/amqp-go/internal/clusterwire"
)

const maxNodeWaitRequests = 256

var errInvalidWait = errors.New("invalid shard readiness wait")

type queueReadinessMachine interface {
	QueueReady(string, clusterqueue.QueueID) (bool, error)
}

type assignmentReadinessMachine interface {
	AssignmentReady(clusterqueue.AssignmentRequest) (bool, error)
}

func validWaitQueue(queue string, incarnation clusterqueue.QueueID) bool {
	return incarnation != 0 && len(queue) > 0 && len(queue) <= clusterwire.MaxWaitQueueBytes && utf8.ValidString(queue)
}

// WaitQueueReady parks on the current shard leader's applied state. The next
// replicated claim determines whether the consumer can acquire a message.
// ErrEmpty means the leader completed an idle wait without seeing work.
func (n *Node) WaitQueueReady(ctx context.Context, queue string, incarnation clusterqueue.QueueID) error {
	if !validWaitQueue(queue, incarnation) {
		return errInvalidWait
	}
	data := make([]byte, 8, 8+len(queue))
	binary.BigEndian.PutUint64(data, uint64(incarnation))
	data = append(data, queue...)
	return n.waitReadiness(ctx, queue, data)
}

func (n *Node) WaitAssignmentReady(ctx context.Context, request clusterqueue.AssignmentRequest) error {
	if !n.cfg.ExperimentalAssignments {
		return errInvalidWait
	}
	packet, err := clusterqueue.EncodeAssignmentRequest(request)
	if err != nil {
		return err
	}
	// Legacy waits reserve incarnation zero as invalid. Use it to
	// discriminate the new predicate in the fenced assignment mode.
	data := make([]byte, 8, 8+len(packet))
	return n.waitReadiness(ctx, request.Queue, append(data, packet...))
}

func (n *Node) ReadAssignmentPage(ctx context.Context, request clusterqueue.AssignmentRequest) ([]clusterqueue.Delivery, clusterqueue.AttemptID, error) {
	if !n.cfg.ExperimentalAssignments {
		return nil, request.After, clusterqueue.ErrInvalidCommand
	}
	data, err := clusterqueue.EncodeAssignmentRequest(request)
	if err != nil {
		return nil, request.After, err
	}
	if n.assignmentReplicaSlots != nil {
		return n.readAssignmentReplica(ctx, request, data)
	}
	return n.readAssignmentInline(ctx, request, data)
}

func (n *Node) waitReadiness(ctx context.Context, queue string, data []byte) error {
	if err := n.Ready(); err != nil {
		return err
	}
	count := n.waitCount.Add(1)
	defer n.waitCount.Add(-1)
	if count > maxNodeWaitRequests {
		return clusterraft.ErrBusy
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	stop := context.AfterFunc(n.ctx, cancel)
	defer stop()
	_, err := n.tryRoute(ctx, groupFor(queue, n.cfg.Shards), data, routeWait)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func (n *Node) WaitCommand(ctx context.Context, data []byte) ([]byte, error) {
	if len(data) < 16 || len(data) > 16+clusterqueue.MaxAssignmentRequestBytes {
		return nil, errInvalidWait
	}
	id := binary.BigEndian.Uint64(data[:8])
	if id == catalogID || !n.validGroup(id) {
		return nil, errInvalidWait
	}
	if err := n.Ready(); err != nil {
		return nil, err
	}
	return encodeReply(n.executeWait(ctx, id, data[8:])), nil
}

func (n *Node) executeWait(ctx context.Context, id uint64, data []byte) reply {
	if len(data) < 8 {
		return reply{code: invalidRead}
	}
	var observe func(clusterraft.Machine) (bool, error)
	incarnation := clusterqueue.QueueID(binary.BigEndian.Uint64(data[:8]))
	if incarnation == 0 && n.cfg.ExperimentalAssignments {
		request, err := clusterqueue.DecodeAssignmentRequest(data[8:])
		if err != nil || groupFor(request.Queue, n.cfg.Shards) != id {
			return reply{code: invalidRead}
		}
		observe = func(machine clusterraft.Machine) (bool, error) {
			readiness, ok := machine.(assignmentReadinessMachine)
			if !ok {
				return false, errInvalidWait
			}
			return readiness.AssignmentReady(request)
		}
	} else {
		queue := string(data[8:])
		if !validWaitQueue(queue, incarnation) || groupFor(queue, n.cfg.Shards) != id {
			return reply{code: invalidRead}
		}
		observe = func(machine clusterraft.Machine) (bool, error) {
			readiness, ok := machine.(queueReadinessMachine)
			if !ok {
				return false, errInvalidWait
			}
			return readiness.QueueReady(queue, incarnation)
		}
	}
	group := n.group(id)
	if group == nil {
		return reply{code: unavailable}
	}
	if leader := group.Leader(); leader != n.cfg.Peer.ID {
		return reply{code: redirect, leader: leader}
	}
	// Leave time to sign and return an idle response before the client's
	// per-peer attempt deadline. Ending idle waits also releases admission
	// so other queues can subscribe.
	wait, cancel := context.WithTimeout(ctx, requestTimeout/5)
	defer cancel()
	err := group.WaitApplied(wait, observe)
	switch {
	case err == nil:
		return reply{}
	case errors.Is(err, clusterraft.ErrNotLeader), errors.Is(err, clusterraft.ErrLeadershipLost):
		return reply{code: redirect, leader: group.Leader()}
	case errors.Is(err, clusterqueue.ErrQueueNotFound):
		return reply{code: waitQueueMissing}
	case errors.Is(err, clusterqueue.ErrQueueIncarnation):
		return reply{code: waitQueueChanged}
	case errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil:
		return reply{code: waitIdle}
	case errors.Is(err, errInvalidWait):
		return reply{code: invalidRead}
	default:
		return reply{code: unavailable}
	}
}
