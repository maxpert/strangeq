package clusterqueue

import (
	"crypto/sha256"
	"encoding/binary"
	"math"
	"slices"
)

const maxRegisteredConsumerCredit = 2000

func assignmentCreditKey(credit uint32) [sha256.Size]byte {
	var data [4]byte
	binary.BigEndian.PutUint32(data[:], credit)
	return sha256.Sum256(data[:])
}

func isPullCommand(kind QueueCommandKind) bool {
	return kind == ClaimMessage || kind == ClaimBatch || kind == AckAndClaim
}

func (q *queueState) addEligibleConsumer(id SessionID) {
	index, exists := slices.BinarySearch(q.pushEligible, id)
	if !exists {
		q.pushEligible = slices.Insert(q.pushEligible, index, id)
	}
}

func (q *queueState) removeEligibleConsumer(id SessionID) {
	index, exists := slices.BinarySearch(q.pushEligible, id)
	if exists {
		q.pushEligible = slices.Delete(q.pushEligible, index, index+1)
	}
}

// assignReady rotates between eligible registered consumers. Flights remain
// the durable assignments. Restore rebuilds the sorted index from credit.
func (q *queueState) assignReady() {
	for len(q.Ready) != 0 && len(q.pushEligible) != 0 {
		next, previousEligible := slices.BinarySearch(q.pushEligible, q.LastAssignedSession)
		if previousEligible {
			next++
		}
		if next == len(q.pushEligible) {
			next = 0
		}
		id := q.pushEligible[next]
		session := q.Sessions[id]
		result := q.claim(session)
		if result.Code != "" {
			return // No new attempt ID can be allocated.
		}
		q.LastAssignedSession = id
		session.AssignedThrough = result.Attempt
		if len(session.Flights) == int(session.RegisteredCredit) {
			q.pushEligible = slices.Delete(q.pushEligible, next, next+1)
		}
	}
}

// ReadAssignments returns existing flights after an exclusive attempt cursor.
// The caller serializes access and keeps captured journal bodies readable.
func (m *Machine) ReadAssignments(request QueueCommand, after AttemptID) ([]Delivery, AttemptID, error) {
	if !validQueueName(request.Queue) || request.Incarnation == 0 || request.ClaimLimit == 0 ||
		request.ClaimLimit > MaxClaimBatchMessages || request.ClaimBytes == 0 || request.ClaimBytes > MaxClaimBatchBytes {
		return nil, after, ErrInvalidCommand
	}
	q, session, err := m.registeredSession(request.Queue, request.Incarnation, request.Session, request.Owner)
	if err != nil {
		return nil, after, err
	}
	if after > session.AssignedThrough {
		return nil, after, ErrUnknownAttempt
	}
	if after == session.AssignedThrough {
		if assignmentsExhausted(q, session) {
			return nil, after, ErrIDsExhausted
		}
		return nil, after, nil
	}

	attempts := make([]AttemptID, 0, len(session.Flights))
	for attempt := range session.Flights {
		if attempt > after {
			attempts = append(attempts, attempt)
		}
	}
	slices.Sort(attempts)
	deliveries := make([]Delivery, 0, min(len(attempts), int(request.ClaimLimit)))
	remaining := int(request.ClaimBytes)
	for _, attempt := range attempts {
		message := session.Flights[attempt]
		size := message.logicalBodySize()
		if len(deliveries) == int(request.ClaimLimit) || len(deliveries) != 0 && size > remaining {
			return deliveries, after, nil
		}
		deliveries = append(deliveries, message.delivery(attempt))
		after = attempt
		remaining -= size
	}
	return deliveries, session.AssignedThrough, nil
}

func (m *Machine) registeredSession(name string, incarnation QueueID, id SessionID, owner string) (*queueState, *sessionState, error) {
	queue := m.state.Queues[name]
	if queue == nil {
		return nil, nil, ErrQueueNotFound
	}
	if queue.ID != incarnation {
		return nil, nil, ErrQueueIncarnation
	}
	session, err := queue.session(id, owner)
	if err != nil {
		return nil, nil, err
	}
	if !session.AutoAssign {
		return nil, nil, ErrInvalidCommand
	}
	return queue, session, nil
}

func assignmentsExhausted(queue *queueState, session *sessionState) bool {
	return queue.LastAttempt == math.MaxUint64 && len(queue.Ready) != 0 &&
		len(session.Flights) < int(session.RegisteredCredit)
}
