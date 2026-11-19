package server

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/maxpert/amqp-go/internal/clusterqueue"
)

type clusterAssignmentRuntime interface {
	SupportsAssignments() bool
	ReadAssignmentPage(context.Context, clusterqueue.AssignmentRequest) ([]clusterqueue.Delivery, clusterqueue.AttemptID, error)
	WaitAssignmentReady(context.Context, clusterqueue.AssignmentRequest) error
}

type bufferedAssignmentPage struct {
	deliveries []clusterqueue.Delivery
	after      clusterqueue.AttemptID
	through    clusterqueue.AttemptID
	wireBytes  int
}

func (p bufferedAssignmentPage) ready(cursor clusterqueue.AttemptID) bool {
	return len(p.deliveries) != 0 && p.after <= cursor && p.deliveries[len(p.deliveries)-1].Attempt > cursor
}

func (p *bufferedAssignmentPage) extend(page bufferedAssignmentPage) bool {
	if p.through != page.after ||
		len(p.deliveries)+len(page.deliveries) > clusterqueue.MaxClaimBatchMessages ||
		p.wireBytes+page.wireBytes > clusterqueue.MaxSettlementAssignmentPageBytes {
		return false
	}
	p.deliveries = append(p.deliveries, page.deliveries...)
	p.through = page.through
	p.wireBytes += page.wireBytes
	return true
}

func (s *clusterSession) assignmentRequest(after clusterqueue.AttemptID) clusterqueue.AssignmentRequest {
	return clusterqueue.AssignmentRequest{
		Queue: s.queue, Incarnation: s.incarnation, Session: s.id, Owner: s.owner, After: after,
		Limit: clusterqueue.MaxClaimBatchMessages, Bytes: clusterqueue.MaxClaimBatchBytes,
	}
}

func (entry *clusterConsumer) nextManualPage(ctx context.Context, cursor clusterqueue.AttemptID) ([]clusterqueue.Delivery, clusterqueue.AttemptID, error) {
	session := entry.session
	if !session.autoAssign {
		result, err := entry.nextManualClaims(ctx)
		return result.Deliveries, cursor, err
	}
	runtime := session.broker.cluster.(clusterAssignmentRuntime)
	session.broker.mu.Lock()
	if page := session.takeBufferedAssignmentPage(); len(page.deliveries) != 0 {
		defer session.broker.mu.Unlock()
		return entry.acceptAssignmentPage(cursor, page)
	}
	readCtx, cancel := context.WithCancel(ctx)
	session.assignmentWait = cancel
	session.broker.mu.Unlock()
	deliveries, next, err := runtime.ReadAssignmentPage(readCtx, session.assignmentRequest(cursor))
	cancel()

	session.broker.mu.Lock()
	defer session.broker.mu.Unlock()
	session.assignmentWait = nil
	if page := session.takeBufferedAssignmentPage(); len(page.deliveries) != 0 {
		clusterqueue.ReleaseDeliveryBodies(deliveries)
		return entry.acceptAssignmentPage(cursor, page)
	}
	if err != nil {
		clusterqueue.ReleaseDeliveryBodies(deliveries)
		return nil, cursor, err
	}
	return entry.acceptAssignmentPage(cursor, bufferedAssignmentPage{deliveries: deliveries, through: next})
}

// The dispatcher alone consumes pages and advances assignmentCursor.
// The caller holds the broker lock for the reservation and cursor update.
func (entry *clusterConsumer) acceptAssignmentPage(cursor clusterqueue.AttemptID, page bufferedAssignmentPage) ([]clusterqueue.Delivery, clusterqueue.AttemptID, error) {
	session := entry.session
	// Only this dispatcher accepts pages. Reserve locally before exposing
	// any item to the existing handoff and per-channel QoS checks.
	if err := session.ctx.Err(); err != nil {
		clusterqueue.ReleaseDeliveryBodies(page.deliveries)
		return nil, cursor, err
	}
	select {
	case <-entry.protocol.Cancel:
		clusterqueue.ReleaseDeliveryBodies(page.deliveries)
		return nil, cursor, context.Canceled
	default:
	}
	session.holds.Add(int32(len(page.deliveries)))
	session.assignmentCursor = page.through
	return page.deliveries, page.through, nil
}

func (entry *clusterConsumer) waitForManualPage(timer *time.Timer, cursor clusterqueue.AttemptID, prior error) bool {
	session := entry.session
	if !session.autoAssign {
		return entry.waitForQueueReady(timer, prior)
	}
	runtime := session.broker.cluster.(clusterAssignmentRuntime)
	ctx, cancel := context.WithTimeout(session.ctx, clusterRPCDeadline)
	defer cancel()
	session.broker.mu.Lock()
	if session.assignmentPage.ready(cursor) {
		session.broker.mu.Unlock()
		return session.ctx.Err() == nil
	}
	session.assignmentWait = cancel
	session.broker.mu.Unlock()
	err := runtime.WaitAssignmentReady(ctx, session.assignmentRequest(cursor))
	session.broker.mu.Lock()
	session.assignmentWait = nil
	buffered := session.assignmentPage.ready(cursor)
	session.broker.mu.Unlock()
	if buffered || err == nil || errors.Is(err, clusterqueue.ErrEmpty) {
		return session.ctx.Err() == nil
	}
	return !assignmentEnded(err) && entry.waitForBatchDispatch(timer, 5*time.Millisecond)
}

func (s *clusterSession) requestAssignmentPage(command *clusterqueue.QueueCommand) {
	if !s.autoAssign || command.Kind != clusterqueue.AckMessage &&
		(command.Kind != clusterqueue.SettleBatch || command.Settlement != clusterqueue.AckMessage) {
		return
	}
	s.broker.mu.Lock()
	defer s.broker.mu.Unlock()
	after := s.assignmentCursor
	if page := s.assignmentPage; page.through > after {
		if len(page.deliveries) >= clusterqueue.MaxClaimBatchMessages || page.wireBytes >= clusterqueue.MaxSettlementAssignmentPageBytes {
			return
		}
		after = page.through
	}
	command.AssignmentAfter = &after
}

func (s *clusterSession) bufferAssignmentPage(requested *clusterqueue.AttemptID, result clusterqueue.QueueResponse) {
	if requested == nil || result.AssignmentAfter == nil || *result.AssignmentAfter < *requested {
		return
	}
	// A page can be stale by the time the settlement reply arrives. Its
	// availability never affects the original settlement outcome.
	after := *result.AssignmentAfter
	deliveries, through, err := clusterqueue.DecodeAssignmentPage(result.AssignmentPage, s.assignmentRequest(after))
	if err != nil || len(deliveries) == 0 || deliveries[len(deliveries)-1].Attempt != through {
		return
	}
	s.broker.mu.Lock()
	defer s.broker.mu.Unlock()
	if s.ctx.Err() != nil || through <= s.assignmentCursor {
		return
	}
	page := bufferedAssignmentPage{deliveries: deliveries, after: after, through: through, wireBytes: len(result.AssignmentPage)}
	if s.assignmentPage.through <= s.assignmentCursor {
		s.assignmentPage = page
	} else if !s.assignmentPage.extend(page) {
		return
	}
	if s.assignmentWait != nil && s.assignmentPage.ready(s.assignmentCursor) {
		s.assignmentWait()
	}
}

func (s *clusterSession) takeBufferedAssignmentPage() bufferedAssignmentPage {
	page := s.assignmentPage
	if page.after > s.assignmentCursor {
		// Ordinary reads fill the gap before the newly assigned page.
		return bufferedAssignmentPage{}
	}
	s.assignmentPage = bufferedAssignmentPage{}
	if !page.ready(s.assignmentCursor) {
		clusterqueue.ReleaseDeliveryBodies(page.deliveries)
		return bufferedAssignmentPage{}
	}
	start := slices.IndexFunc(page.deliveries, func(delivery clusterqueue.Delivery) bool {
		return delivery.Attempt > s.assignmentCursor
	})
	clusterqueue.ReleaseDeliveryBodies(page.deliveries[:start])
	page.deliveries = page.deliveries[start:]
	return page
}

func assignmentEnded(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, clusterqueue.ErrQueueNotFound) ||
		errors.Is(err, clusterqueue.ErrQueueIncarnation) || errors.Is(err, clusterqueue.ErrSessionClosed) ||
		errors.Is(err, clusterqueue.ErrSessionOwner) || errors.Is(err, clusterqueue.ErrUnknownSession) ||
		errors.Is(err, clusterqueue.ErrUnknownAttempt) || errors.Is(err, clusterqueue.ErrInvalidCommand) ||
		errors.Is(err, clusterqueue.ErrIDsExhausted)
}
