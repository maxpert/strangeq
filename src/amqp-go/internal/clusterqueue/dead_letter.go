package clusterqueue

import (
	"errors"
	"slices"
	"unicode/utf8"
)

// DeadLetterPolicy enables reliable dead-lettering for non-requeued settlements.
type DeadLetterPolicy struct {
	Exchange      string
	RoutingKey    string
	HasRoutingKey bool
}

func (p *DeadLetterPolicy) valid() bool {
	return len(p.Exchange) <= 255 && utf8.ValidString(p.Exchange) &&
		len(p.RoutingKey) <= 255 && utf8.ValidString(p.RoutingKey) && (p.HasRoutingKey || p.RoutingKey == "")
}

func (p *DeadLetterPolicy) reserve(queue string) uint16 {
	if p == nil {
		return 0
	}
	// Reserve snapshot space at admission so rejection can always move the
	// body to the outbox. The destination plan has a separate byte budget.
	// The annotation adds routing fields, up to three copies of the source
	// name (x-death, x-first-death and x-last-death), and scalar properties.
	return uint16(2048 + 3*len(queue) + len(p.Exchange) + len(p.RoutingKey))
}

func sameDeadLetterPolicy(a, b *DeadLetterPolicy) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

const (
	maxDeadLetterPlanBytes = 4 << 20
	maxDeadLetterReserve   = 2048 + 3*1024 + 2*255
)

type DeadLetterUpdate struct {
	ID        uint64
	Plan      []string `json:",omitempty"`
	Completed string   `json:",omitempty"`
	Authorize string   `json:",omitempty"`
}

type DeadLetterTransfer struct {
	ID        uint64
	Source    string
	Policy    DeadLetterPolicy
	Rejected  int64
	Remaining []string `json:",omitempty" cbor:",omitempty"`
}

type pendingDeadLetter struct {
	DeadLetterTransfer
	Message messageState
}

func validDeadLetterNames(names []string) bool {
	if len(names) > maxQueues {
		return false
	}
	for i, name := range names {
		if !validQueueName(name) || i > 0 && names[i-1] >= name {
			return false
		}
	}
	return true
}

func deadLetterNamesBytes(names []string) uint64 {
	var size uint64
	for _, name := range names {
		size += uint64(8 + len(name))
	}
	return size
}

func (m *Machine) stageDeadLetter(q *queueState, message messageState, rejectedAt int64) {
	s := &m.state
	s.LastDeadLetter++
	id := s.LastDeadLetter
	if s.DeadLetters == nil {
		s.DeadLetters = make(map[uint64]pendingDeadLetter)
	}
	s.DeadLetters[id] = pendingDeadLetter{
		DeadLetterTransfer: DeadLetterTransfer{ID: id, Source: q.name, Policy: *q.Spec.DeadLetter, Rejected: rejectedAt},
		Message:            message,
	}
	s.deadLetterIDs = append(s.deadLetterIDs, id)
}

func validBatchSettlementError(code string) bool {
	switch errorCodes[code] {
	case ErrUnknownAttempt, ErrInvalidCommand, ErrIDsExhausted:
		return true
	}
	return false
}

// A reliably routed source reserves one extra local copy at admission. With
// a full shard the first same-shard fanout copy can still reach a consumer.
func deadLetterCopyCredit(size uint64, unused bool) (uint64, uint64) {
	if unused {
		return size, 1
	}
	return 0, 0
}

func (m messageState) deadLetterCopyCredit() (uint64, uint64) {
	size := uint64(m.logicalBodySize()) + uint64(m.DeadLetterReserve)
	return deadLetterCopyCredit(size, m.DeadLetterReserve != 0 && !m.DeadLetterCopyUsed)
}

func (m *Machine) releaseDeadLetterCopyCredit(message messageState) {
	bytes, slots := message.deadLetterCopyCredit()
	m.state.deadLetterCopyBytes -= bytes
	m.state.deadLetterCopySlots -= slots
}

type deadLetterPublishCredit struct {
	copyBytes, copySlots uint64
	bodyBytes, bodySlots uint64
}

func (s *machineState) canMoveWithCredit(usedBytes, usedSlots, size uint64, credit deadLetterPublishCredit) bool {
	if credit.copySlots == 0 && credit.bodySlots == 0 {
		return false
	}

	// A funded local move may reach a consumer while postponing the new
	// target's future copy reservation. Ordinary admission waits for that
	// debt to drain. Actual bodies and slots remain bounded, with at most
	// one message's policy metadata above the normal byte budget.
	return s.bodies+size <= maxBodyBytes+maxDeadLetterReserve+credit.bodyBytes &&
		s.messages+1 <= maxMessages+credit.bodySlots &&
		usedBytes+size <= max(usedBytes, maxBodyBytes)+credit.bodyBytes+credit.copyBytes+maxDeadLetterReserve &&
		usedSlots+1 <= max(usedSlots, maxMessages)+credit.bodySlots+credit.copySlots
}

func (m *Machine) publishDeadLetter(c QueueCommand, q *queueState, origin journalBodyOrigin) QueueResponse {
	if !q.Spec.Durable {
		return failure(ErrInvalidCommand)
	}
	if c.DeadLetterMove == 0 {
		return m.publish(q, c.Body, origin, c.logicalSize)
	}
	entry, ok := m.state.DeadLetters[c.DeadLetterMove]
	if !ok {
		return QueueResponse{}
	}
	if _, pending := slices.BinarySearch(entry.Remaining, q.name); !pending {
		return QueueResponse{}
	}
	copyBytes, copySlots := entry.Message.deadLetterCopyCredit()
	credit := deadLetterPublishCredit{copyBytes: copyBytes, copySlots: copySlots}
	if len(entry.Remaining) == 1 {
		// This commit can replace the final pending copy even at the shard's
		// capacity limit. Once the target is present, release the source.
		credit.bodyBytes = uint64(entry.Message.logicalBodySize()) + uint64(entry.Message.DeadLetterReserve)
		credit.bodySlots = 1
	}
	reply := m.publishWithCredit(q, c.Body, origin, c.logicalSize, credit)
	if reply.Code == "" && reply.storageErr == nil {
		if len(entry.Remaining) > 1 && copySlots != 0 {
			m.releaseDeadLetterCopyCredit(entry.Message)
			entry.Message.DeadLetterCopyUsed = true
			m.state.DeadLetters[c.DeadLetterMove] = entry
		}
		m.updateDeadLetter(DeadLetterUpdate{ID: c.DeadLetterMove, Completed: q.name})
	}
	return reply
}

func (m *Machine) updateDeadLetter(update DeadLetterUpdate) QueueResponse {
	s := &m.state
	entry, exists := s.DeadLetters[update.ID]
	if !exists {
		return QueueResponse{}
	}
	if len(update.Plan) != 0 {
		if len(entry.Remaining) != 0 {
			return QueueResponse{}
		}
		size := deadLetterNamesBytes(update.Plan)
		if s.deadLetterPlanBytes+size > maxDeadLetterPlanBytes {
			return failure(ErrCapacity)
		}
		entry.Remaining = update.Plan
		s.deadLetterPlanBytes += size
	} else {
		index, pending := slices.BinarySearch(entry.Remaining, update.Completed)
		if !pending {
			return QueueResponse{}
		}
		s.deadLetterPlanBytes -= uint64(8 + len(update.Completed))
		if len(entry.Remaining) == 1 {
			m.removeBody(entry.Message)
			delete(s.DeadLetters, update.ID)
			s.compactDeadLetterIDs()
			return QueueResponse{}
		}
		// The captured snapshots and worker reads share immutable slices.
		remaining := make([]string, 0, len(entry.Remaining)-1)
		remaining = append(remaining, entry.Remaining[:index]...)
		entry.Remaining = append(remaining, entry.Remaining[index+1:]...)
	}
	s.DeadLetters[update.ID] = entry
	return QueueResponse{}
}

func (s *machineState) compactDeadLetterIDs() {
	if len(s.DeadLetters) == 0 {
		s.deadLetterIDs = nil
		s.DeadLetters = nil
		return
	}
	if len(s.deadLetterIDs) > 1024 && len(s.deadLetterIDs) > 2*len(s.DeadLetters) {
		index := make([]uint64, 0, len(s.DeadLetters))
		for _, id := range s.deadLetterIDs {
			if _, exists := s.DeadLetters[id]; exists {
				index = append(index, id)
			}
		}
		s.deadLetterIDs = index
	}
}

// HasDeadLetters is an advisory check. A worker must confirm each destination
// against the replicated pending plan before sending a copy.
func (m *Machine) HasDeadLetters() bool { return len(m.state.DeadLetters) != 0 }

func (s *machineState) validateDeadLetters() error {
	if len(s.DeadLetters) == 0 {
		return nil
	}
	s.deadLetterIDs = make([]uint64, 0, len(s.DeadLetters))
	for id, entry := range s.DeadLetters {
		if id == 0 || id > s.LastDeadLetter || entry.ID != id || !validQueueName(entry.Source) ||
			!entry.Policy.valid() || entry.Rejected <= 0 || entry.Message.ID == 0 ||
			entry.Message.DeadLetterReserve != entry.Policy.reserve(entry.Source) ||
			(entry.Message.DeadLetterCopyUsed && len(entry.Remaining) == 0) || !validDeadLetterNames(entry.Remaining) {
			return errors.New("invalid dead letter in snapshot")
		}
		if err := s.accountSnapshotMessage(entry.Message); err != nil {
			return err
		}
		s.deadLetterPlanBytes += deadLetterNamesBytes(entry.Remaining)
		if s.deadLetterPlanBytes > maxDeadLetterPlanBytes {
			return ErrCapacity
		}
		s.deadLetterIDs = append(s.deadLetterIDs, id)
	}
	slices.Sort(s.deadLetterIDs)
	return nil
}
