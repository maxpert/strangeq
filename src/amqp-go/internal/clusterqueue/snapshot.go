package clusterqueue

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"

	"github.com/fxamacker/cbor/v2"
	"github.com/maxpert/amqp-go/internal/clusterpayload"
)

// Reserve wire space for the Raft envelope and protocol-specific shard state.
const maxSnapshotBytes = 112 << 20
const snapshotVersion byte = 2
const legacySnapshotVersion = 1
const (
	snapshotMessageFraming = 64
	snapshotSessionFraming = 512
	snapshotQueueFraming   = 256
)

var snapshotEncoding = func() cbor.UserBufferEncMode {
	mode, err := cbor.CoreDetEncOptions().UserBufferEncMode()
	if err != nil {
		panic(err)
	}
	return mode
}()

var snapshotDecoding = func() cbor.DecMode {
	mode, err := (cbor.DecOptions{
		DupMapKey:        cbor.DupMapKeyEnforcedAPF,
		IndefLength:      cbor.IndefLengthForbidden,
		TagsMd:           cbor.TagsForbidden,
		MaxArrayElements: maxMessages,
		MaxMapPairs:      maxMessages,
	}).DecMode()
	if err != nil {
		panic(err)
	}
	return mode
}()

type legacySnapshot struct {
	Version uint16
	State   *machineState
}

// CloneForSnapshot copies the mutable containers used by the snapshot encoder.
// The caller serializes capture with apply and restore. Payloads, arguments and
// journal references are shared because their contents remain immutable.
func (m *Machine) CloneForSnapshot() Machine {
	captured := *m
	captured.state.Queues = maps.Clone(m.state.Queues)
	captured.state.DeadLetters = maps.Clone(m.state.DeadLetters)
	for name, original := range captured.state.Queues {
		queue := *original
		queue.Ready = slices.Clone(original.Ready)
		queue.Sessions = maps.Clone(original.Sessions)
		for id, originalSession := range queue.Sessions {
			session := *originalSession
			session.Flights = maps.Clone(originalSession.Flights)
			queue.Sessions[id] = &session
		}
		captured.state.Queues[name] = &queue
	}
	return captured
}

func (m *Machine) Snapshot() ([]byte, error) {
	return m.AppendSnapshot(nil)
}

// AppendSnapshot appends the prefix fragments and versioned queue state to dst.
// Only the encoded queue state counts toward the queue snapshot size limit.
func (m *Machine) AppendSnapshot(dst []byte, prefixes ...[]byte) ([]byte, error) {
	queueStart := len(dst)
	for _, prefix := range prefixes {
		if len(prefix) > math.MaxInt-queueStart {
			return nil, errors.New("queue snapshot prefix exceeds size limit")
		}
		queueStart += len(prefix)
	}
	if m.state.coldBytes != 0 {
		defer m.state.clearJournalBodies()
		if err := m.state.loadJournalBodies(); err != nil {
			return nil, err
		}
	}
	hint := m.state.snapshotCapacity()
	if hint > math.MaxInt-queueStart {
		return nil, errors.New("queue snapshot prefix exceeds size limit")
	}
	if capacity := queueStart + hint; capacity > cap(dst) {
		grown := make([]byte, len(dst), capacity)
		copy(grown, dst)
		dst = grown
	}
	out := bytes.NewBuffer(dst)
	for _, prefix := range prefixes {
		_, _ = out.Write(prefix)
	}
	_ = out.WriteByte(snapshotVersion)
	if err := snapshotEncoding.MarshalToBuffer(&m.state, out); err != nil {
		return nil, err
	}
	if out.Len()-queueStart > maxSnapshotBytes {
		return nil, errors.New("queue snapshot exceeds peer size limit")
	}
	return out.Bytes(), nil
}

// EstimatedSnapshotBytes estimates the encoded live queue image.
func (m *Machine) EstimatedSnapshotBytes() uint64 {
	return m.state.snapshotBaseBytes()
}

func (s *machineState) snapshotBaseBytes() uint64 {
	return 64 + s.bodies - s.packingSavings + s.messages*snapshotMessageFraming + s.sessions*snapshotSessionFraming +
		uint64(len(s.Queues))*snapshotQueueFraming + s.deadLetterPlanBytes
}

// snapshotCapacity estimates encoded bytes and the encoder's temporary space
// for rearranging map entries into deterministic order.
func (s *machineState) snapshotCapacity() int {
	size := int(s.snapshotBaseBytes())
	for name, queue := range s.Queues {
		size += len(name) + len(queue.Spec.Arguments)
	}
	size = min(size, maxSnapshotBytes)
	if len(s.Queues) > 1 {
		// Sorting the outer queue map can temporarily duplicate its content.
		return 2 * size
	}
	for _, queue := range s.Queues {
		if len(queue.Sessions) > 1 || queue.unacked > 1 {
			// A single queue need only sort its sessions and flight maps.
			scratch := int(s.sessions) * snapshotSessionFraming
			for _, session := range queue.Sessions {
				for _, message := range session.Flights {
					scratch += snapshotMessageFraming + message.bodySize()
				}
			}
			return size + min(scratch, maxSnapshotBytes)
		}
	}
	return size
}

// Restore validates the snapshot and reconstructs the resource counters before
// replacing the state. An invalid snapshot leaves the current machine intact.
func (m *Machine) Restore(data []byte) error {
	if len(data) > maxSnapshotBytes {
		return errors.New("queue snapshot exceeds peer size limit")
	}
	var state *machineState
	switch {
	case len(data) > 0 && data[0] == snapshotVersion:
		if err := snapshotDecoding.Unmarshal(data[1:], &state); err != nil {
			return fmt.Errorf("invalid queue snapshot: %w", err)
		}
	case len(data) > 0 && data[0] == '{':
		var legacy legacySnapshot
		if err := json.Unmarshal(data, &legacy); err != nil {
			return fmt.Errorf("invalid queue snapshot: %w", err)
		}
		if legacy.Version == legacySnapshotVersion {
			state = legacy.State
		}
	}
	if state == nil {
		return errors.New("unsupported queue snapshot")
	}
	if err := state.validate(m.assignments); err != nil {
		return err
	}
	m.ReleaseJournalSources()
	m.state = *state
	return nil
}

func (s *machineState) validate(assignments bool) error {
	if len(s.Queues) > maxQueues {
		return ErrCapacity
	}
	ids := make(map[QueueID]bool, len(s.Queues))
	for name, q := range s.Queues {
		if !validQueueName(name) || q == nil || q.ID == 0 || q.ID > s.LastQueue ||
			ids[q.ID] || len(q.Spec.Arguments) > maxArgumentsBytes {
			return errors.New("invalid queue in snapshot")
		}
		ids[q.ID] = true
		q.name = name
		if q.Spec.DeadLetter != nil && (!q.Spec.Durable || !q.Spec.DeadLetter.valid()) {
			return errors.New("invalid dead-letter policy in snapshot")
		}
		if err := s.validateQueue(q, assignments); err != nil {
			return fmt.Errorf("snapshot queue %q: %w", name, err)
		}
	}
	return s.validateDeadLetters()
}

func (s *machineState) validateQueue(q *queueState, assignments bool) error {
	s.sessions += uint64(len(q.Sessions))
	if s.sessions > maxSessions {
		return ErrCapacity
	}
	if q.LastAssignedSession > q.LastSession || !assignments && q.LastAssignedSession != 0 {
		return errors.New("invalid consumer assignment cursor in snapshot")
	}
	messages := make(map[MessageID]bool, min(len(q.Ready), maxMessages))
	attempts := make(map[AttemptID]bool)
	for _, message := range q.Ready {
		if err := s.validateMessage(q, messages, message); err != nil {
			return err
		}
	}
	for id, sess := range q.Sessions {
		if id == 0 || id > q.LastSession || sess == nil || sess.Owner == "" || len(sess.Owner) > 128 ||
			sess.Deadline <= 0 {
			return errors.New("invalid session in snapshot")
		}
		if sess.Consumer {
			if sess.Prefetch == 0 {
				return errors.New("snapshot consumer has no delivery credit")
			}
			q.consumers++
		}
		if sess.AutoAssign {
			if !assignments || !sess.Consumer || sess.Prefetch > maxRegisteredConsumerCredit ||
				sess.RegisteredCredit == 0 || sess.RegisteredCredit > maxRegisteredConsumerCredit ||
				len(sess.Flights) > maxRegisteredConsumerCredit ||
				sess.AssignedThrough > q.LastAttempt || isPullCommand(sess.LastKey.Kind) {
				return errors.New("invalid registered consumer in snapshot")
			}
			if len(sess.Flights) < int(sess.RegisteredCredit) {
				q.pushEligible = append(q.pushEligible, id)
			}
		} else if sess.AssignedThrough != 0 || sess.RegisteredCredit != 0 ||
			uint64(len(sess.Flights)) > uint64(sess.Prefetch) {
			return errors.New("invalid pull consumer in snapshot")
		}
		if err := validateOutcome(q, sess); err != nil {
			return err
		}
		for id, message := range sess.Flights {
			if id == 0 || id > q.LastAttempt || attempts[id] || sess.AutoAssign && id > sess.AssignedThrough {
				return errors.New("invalid delivery attempt in snapshot")
			}
			attempts[id] = true
			if err := s.validateMessage(q, messages, message); err != nil {
				return err
			}
			q.unacked++
		}
	}
	slices.Sort(q.pushEligible)
	return nil
}

func (s *machineState) validateMessage(q *queueState, seen map[MessageID]bool, message messageState) error {
	if message.ID == 0 || message.ID > q.LastMessage || seen[message.ID] || message.DeadLetterReserve != q.Spec.DeadLetter.reserve(q.name) || message.DeadLetterCopyUsed {
		return errors.New("invalid message in snapshot")
	}
	seen[message.ID] = true
	return s.accountSnapshotMessage(message)
}

func (s *machineState) accountSnapshotMessage(message messageState) error {
	if message.bodySize() > maxMessageBytes {
		return errors.New("invalid message size in snapshot")
	}
	if message.LogicalSize != 0 {
		logical, err := clusterpayload.ValidateEnvelope(message.Body)
		if err != nil || logical != message.LogicalSize || logical <= len(message.Body) || logical > maxMessageBytes {
			return errors.New("invalid compressed message in snapshot")
		}
		s.packingSavings += uint64(logical - len(message.Body))
	}
	s.messages++
	s.bodies += uint64(message.logicalBodySize()) + uint64(message.DeadLetterReserve)
	copyBytes, copySlots := message.deadLetterCopyCredit()
	s.deadLetterCopyBytes += copyBytes
	s.deadLetterCopySlots += copySlots
	if s.messages > maxMessages || s.bodies > maxBodyBytes+maxDeadLetterReserve {
		return ErrCapacity
	}
	return nil
}

func validateOutcome(q *queueState, sess *sessionState) error {
	last := sess.Last
	if last.Ready > maxMessages || (last.Ready != 0 && sess.LastKey.Kind != ClaimMessage && sess.LastKey.Kind != ClaimBatch && sess.LastKey.Kind != AckAndClaim) {
		return errors.New("invalid latest ready count in snapshot")
	}
	if sess.LastKey.Kind != ClaimBatch && sess.LastKey.Kind != AckAndClaim && (last.BatchFirst != 0 || last.BatchCount != 0) {
		return errors.New("unexpected latest claim batch in snapshot")
	}
	if sess.LastKey.Kind != AckAndClaim && last.ClaimCode != "" {
		return errors.New("unexpected latest refill result in snapshot")
	}
	if sess.Sequence == 0 {
		if last != (outcome{}) || sess.LastKey != (commandKey{}) {
			return errors.New("invalid empty session history in snapshot")
		}
		return nil
	}
	switch sess.LastKey.Kind {
	case SetAssignmentCredit:
		if !sess.AutoAssign || last != (outcome{}) || sess.LastKey.Attempt != 0 ||
			sess.LastKey.Hash != assignmentCreditKey(sess.RegisteredCredit) {
			return errors.New("invalid registered consumer credit result in snapshot")
		}
		return nil
	case PublishMessage, ClaimMessage, AckMessage, RequeueMessage, DropMessage:
		if last.Settled != 0 {
			return errors.New("unexpected batch settlement result in snapshot")
		}
	case ClaimBatch:
		return validateClaimBatchOutcome(q, sess)
	case AckAndClaim:
		return validateAckAndClaimOutcome(q, sess)
	case SettleBatch:
		if sess.LastKey.Attempt != 0 || last.Attempt != 0 || last.Message != 0 || last.Settled > MaxSettlementBatch {
			return errors.New("invalid batch settlement result in snapshot")
		}
		if last.Code == "" && last.Settled >= 2 || validBatchSettlementError(last.Code) && last.Settled < MaxSettlementBatch {
			return nil
		}
		return errors.New("invalid batch settlement result in snapshot")
	default:
		return errors.New("invalid latest command in snapshot")
	}
	if last.Code != "" {
		if errorCodes[last.Code] == nil || last.Attempt != 0 || last.Message != 0 {
			return errors.New("invalid error outcome in snapshot")
		}
		return nil
	}
	if last.Attempt != 0 {
		if _, exists := sess.Flights[last.Attempt]; !exists || last.Message != 0 || sess.LastKey.Kind != ClaimMessage {
			return errors.New("latest claim is not outstanding in snapshot")
		}
		return nil
	}
	if sess.LastKey.Kind == PublishMessage && last.Message > 0 && last.Message <= q.LastMessage {
		return nil
	}
	if last.Message != 0 || sess.LastKey.Kind == PublishMessage || sess.LastKey.Kind == ClaimMessage {
		return errors.New("invalid latest result in snapshot")
	}
	return nil
}

func validateClaimBatchOutcome(q *queueState, sess *sessionState) error {
	last := sess.Last
	invalid := errors.New("invalid latest claim batch in snapshot")
	if sess.LastKey.Attempt != 0 || last.Attempt != 0 || last.Message != 0 || last.Settled != 0 ||
		last.BatchCount > MaxClaimBatchMessages || last.BatchCount > sess.Prefetch {
		return invalid
	}
	if last.BatchCount == 0 {
		if last.BatchFirst == 0 && (last.Code == errorsToCode[ErrNoCredit] || last.Code == errorsToCode[ErrEmpty] ||
			last.Code == errorsToCode[ErrIDsExhausted]) {
			return nil
		}
		return invalid
	}
	if last.Code != "" || last.BatchFirst == 0 || last.BatchFirst > q.LastAttempt ||
		AttemptID(last.BatchCount-1) > q.LastAttempt-last.BatchFirst {
		return invalid
	}
	size := 0
	for i := range last.BatchCount {
		message, ok := sess.Flights[last.BatchFirst+AttemptID(i)]
		if !ok {
			return invalid
		}
		size += message.logicalBodySize()
		if last.BatchCount > 1 && size > MaxClaimBatchBytes {
			return invalid
		}
	}
	return nil
}
