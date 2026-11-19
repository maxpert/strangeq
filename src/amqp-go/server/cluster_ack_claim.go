package server

import (
	"context"
	"errors"

	"github.com/maxpert/amqp-go/internal/clusterqueue"
)

// Support is opt-in: the runtime must fence incompatible replicated state
// machines before advertising the combined command.
type clusterAckClaimSupport interface {
	SupportsAckAndClaim() bool
}

func (s *clusterSession) hasRefill() bool {
	s.broker.mu.Lock()
	defer s.broker.mu.Unlock()
	return len(s.refill) != 0
}

// The caller holds the session sequence lock followed by the broker lock.
func (b *ClusterBroker) ackClaimLimitLocked(delivery clusterDelivery, settled int) uint32 {
	support, ok := b.cluster.(clusterAckClaimSupport)
	if !ok || !support.SupportsAckAndClaim() {
		return 0
	}
	if delivery.Session.autoAssign || len(delivery.Session.refill) != 0 {
		return 0
	}
	entry := b.consumers[delivery.Consumer]
	if entry == nil || entry.session != delivery.Session || entry.protocol.NoAck || entry.session.ctx.Err() != nil {
		return 0
	}
	select {
	case <-entry.protocol.Cancel:
		return 0
	default:
	}
	available := int32(entry.credit.Load()) - entry.session.holds.Load() + int32(settled)
	if available <= 0 {
		return 0
	}
	return uint32(min(available, clusterqueue.MaxClaimBatchMessages))
}

func validAckClaimResult(command clusterqueue.QueueCommand, result clusterqueue.QueueResponse, err error) bool {
	if result.Count > uint64(command.ClaimLimit) || result.Count != uint64(len(result.Deliveries)) {
		return false
	}
	requested := uint64(len(command.Attempts))
	switch {
	case err == nil:
		return result.AckClaim && result.Settled == requested
	case errors.Is(err, clusterqueue.ErrUnknownAttempt):
		return result.AckClaim && result.Settled < requested && result.Count == 0
	default:
		return result.Settled == 0 && result.Count == 0
	}
}

func (entry *clusterConsumer) nextManualClaims(ctx context.Context) (clusterqueue.QueueResponse, error) {
	s := entry.session
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ctx.Err(); err != nil {
		return clusterqueue.QueueResponse{}, err
	}
	// Refill was installed under this same sequence lock. Consume it before
	// considering a later claim, so the sole dispatcher preserves claim order.
	s.broker.mu.Lock()
	if len(s.refill) != 0 {
		result := clusterqueue.QueueResponse{Deliveries: s.refill}
		s.refill = nil
		s.broker.mu.Unlock()
		return result, nil
	}
	available := int32(entry.credit.Load()) - s.holds.Load()
	s.broker.mu.Unlock()
	if available <= 0 {
		return clusterqueue.QueueResponse{}, clusterqueue.ErrNoCredit
	}
	command := s.command(clusterqueue.ClaimBatch)
	command.ClaimLimit = uint32(min(available, clusterqueue.MaxClaimBatchMessages))
	command.ClaimBytes = clusterqueue.MaxClaimBatchBytes
	return s.runCommandLocked(ctx, command)
}
