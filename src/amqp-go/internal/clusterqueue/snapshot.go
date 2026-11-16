package clusterqueue

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/fxamacker/cbor/v2"
)

// Reserve wire space for the Raft envelope and protocol-specific shard state.
const maxSnapshotBytes = 112 << 20
const snapshotVersion byte = 2
const legacySnapshotVersion = 1

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

func (m *Machine) Snapshot() ([]byte, error) {
	// CBOR stores payloads and arguments as raw bytes and needs no string
	// escaping. At capacity, the 48 MiB payload budget plus 4096 queues
	// (name, arguments, and fields), 8192 sessions, and 131072 message
	// records fit under the snapshot cap, including in-flight map keys.
	var out bytes.Buffer
	_ = out.WriteByte(snapshotVersion)
	if err := snapshotEncoding.MarshalToBuffer(&m.state, &out); err != nil {
		return nil, err
	}
	if out.Len() > maxSnapshotBytes {
		return nil, errors.New("queue snapshot exceeds peer size limit")
	}
	return out.Bytes(), nil
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
	if err := state.validate(); err != nil {
		return err
	}
	m.state = *state
	return nil
}

func (s *machineState) validate() error {
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
		if err := s.validateQueue(q); err != nil {
			return fmt.Errorf("snapshot queue %q: %w", name, err)
		}
	}
	return nil
}

func (s *machineState) validateQueue(q *queueState) error {
	s.sessions += uint64(len(q.Sessions))
	if s.sessions > maxSessions {
		return ErrCapacity
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
			sess.Deadline <= 0 || uint64(len(sess.Flights)) > uint64(sess.Prefetch) {
			return errors.New("invalid session in snapshot")
		}
		if sess.Consumer {
			if sess.Prefetch == 0 {
				return errors.New("snapshot consumer has no delivery credit")
			}
			q.consumers++
		}
		if err := validateOutcome(q, sess); err != nil {
			return err
		}
		for id, message := range sess.Flights {
			if id == 0 || id > q.LastAttempt || attempts[id] {
				return errors.New("invalid delivery attempt in snapshot")
			}
			attempts[id] = true
			if err := s.validateMessage(q, messages, message); err != nil {
				return err
			}
			q.unacked++
		}
	}
	return nil
}

func (s *machineState) validateMessage(q *queueState, seen map[MessageID]bool, message messageState) error {
	if message.ID == 0 || message.ID > q.LastMessage || seen[message.ID] || len(message.Body) > maxMessageBytes {
		return errors.New("invalid message in snapshot")
	}
	s.messages++
	s.bodies += uint64(len(message.Body))
	if s.messages > maxMessages || s.bodies > maxBodyBytes {
		return ErrCapacity
	}
	seen[message.ID] = true
	return nil
}

func validateOutcome(q *queueState, sess *sessionState) error {
	last := sess.Last
	if last.Ready > maxMessages || (last.Ready != 0 && sess.LastKey.Kind != ClaimMessage) {
		return errors.New("invalid latest ready count in snapshot")
	}
	if sess.Sequence == 0 {
		if last != (outcome{}) || sess.LastKey != (commandKey{}) {
			return errors.New("invalid empty session history in snapshot")
		}
		return nil
	}
	switch sess.LastKey.Kind {
	case PublishMessage, ClaimMessage, AckMessage, RequeueMessage, DropMessage:
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
