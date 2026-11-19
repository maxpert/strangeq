package cluster

import (
	"context"
	"errors"
	"log"

	"github.com/maxpert/amqp-go/internal/clusterqueue"
	"github.com/maxpert/amqp-go/internal/clusterraft"
	"go.etcd.io/raft/v3"
)

const maxAssignmentReplicaRequests = 16

func (n *Node) readAssignmentReplica(ctx context.Context, request clusterqueue.AssignmentRequest, full []byte) ([]clusterqueue.Delivery, clusterqueue.AttemptID, error) {
	id := groupFor(request.Queue, n.cfg.Shards)
	local := n.group(id)
	if local == nil {
		return n.readAssignmentInline(ctx, request, full)
	}
	term, first, last := local.AssignmentJournalCoverage()
	if term == 0 {
		return n.readAssignmentInline(ctx, request, full)
	}
	select {
	case n.assignmentReplicaSlots <- struct{}{}:
		defer func() { <-n.assignmentReplicaSlots }()
	default:
		return n.readAssignmentInline(ctx, request, full)
	}

	coverage := clusterqueue.AssignmentCoverage{Term: term, First: first, Last: last}
	wire, err := n.Query(ctx, request.Queue, clusterqueue.EncodeAssignmentReplicaRequest(full, coverage))
	if errors.Is(err, errInvalidRead) {
		// An older or disabled assignment peer rejects the optional tag.
		return n.readAssignmentInline(ctx, request, full)
	}
	if err != nil {
		return nil, request.After, err
	}
	if !clusterqueue.IsAssignmentReplica(wire) {
		return clusterqueue.DecodeAssignmentPage(wire, request)
	}
	page, err := clusterqueue.DecodeAssignmentReplicaPage(wire, request, coverage)
	if err != nil {
		return nil, request.After, err
	}

	refs := page.References()
	var spans [clusterraft.MaxAssignmentJournalSpans]clusterraft.AssignmentJournalSpan
	for i, ref := range refs {
		spans[i] = clusterraft.AssignmentJournalSpan{
			Index: ref.Index, Term: ref.Term, Offset: ref.Offset, Length: ref.Length,
		}
	}
	read, release, err := local.TryPinAssignmentJournal(spans[:len(refs)])
	switch {
	case errors.Is(err, raft.ErrCompacted), errors.Is(err, raft.ErrUnavailable), errors.Is(err, clusterraft.ErrBusy):
		// The full retry uses the original incarnation, session and cursor.
		return n.readAssignmentInline(ctx, request, full)
	case err != nil:
		return nil, request.After, err
	}
	if err := ctx.Err(); err != nil {
		release()
		return nil, request.After, err
	}
	deliveries, through, err := page.Materialize(read)
	if err != nil {
		stopped := local.StopOnAssignmentBodyFailure(err)
		release()
		if stopped {
			log.Printf("stopped local shard %d on node %d after assignment body verification failed: %v", id, n.cfg.Peer.ID, err)
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, request.After, ctxErr
		}
		return n.readAssignmentInline(ctx, request, full)
	}
	release()
	if err := ctx.Err(); err != nil {
		return nil, request.After, err
	}
	return deliveries, through, nil
}

func (n *Node) readAssignmentInline(ctx context.Context, request clusterqueue.AssignmentRequest, data []byte) ([]clusterqueue.Delivery, clusterqueue.AttemptID, error) {
	wire, err := n.Query(ctx, request.Queue, data)
	if errors.Is(err, errInvalidRead) {
		return nil, request.After, clusterqueue.ErrInvalidCommand
	}
	if err != nil {
		return nil, request.After, err
	}
	return clusterqueue.DecodeAssignmentPage(wire, request)
}
