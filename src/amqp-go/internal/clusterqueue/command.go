// Package clusterqueue implements the deterministic queue state in one shard.
package clusterqueue

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
)

type QueueID uint64
type SessionID uint64
type MessageID uint64
type AttemptID uint64
type QueueCommandKind uint8

const (
	DeclareQueue QueueCommandKind = iota + 1
	DescribeQueue
	DeleteQueue
	PurgeQueue
	OpenQueueSession
	RenewQueueSession
	ReleaseQueueSession
	ExpireSessions
	PublishMessage
	ClaimMessage
	AckMessage
	RequeueMessage
	DropMessage
	ListQueues
)

type QueueSpec struct {
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	Arguments  []byte
}

// Sequence orders the commands in a session. A caller resolves an uncertain
// outcome before advancing its sequence. The latest outcome is repeatable.
// A publish without a session is a single command with an ambiguous outcome
// if its reply is lost. It can fence a named binding with Incarnation or use
// zero to select the current queue with Queue at commit time.
type QueueCommand struct {
	Kind        QueueCommandKind
	Queue       string    `json:",omitempty"`
	Incarnation QueueID   `json:",omitempty"`
	Session     SessionID `json:",omitempty"`
	Owner       string    `json:",omitempty"`
	Sequence    uint64    `json:",omitempty"`
	Prefetch    uint32    `json:",omitempty"`
	Consumer    bool      `json:",omitempty"`
	Body        []byte    `json:"-"`
	Attempt     AttemptID `json:",omitempty"`
	// Deadline is absolute Unix nanoseconds. The leader supplies expiry cutoffs.
	Deadline  int64     `json:",omitempty"`
	QueueSpec QueueSpec `json:",omitzero"`
	IfEmpty   bool      `json:",omitempty"`
	IfUnused  bool      `json:",omitempty"`
}

type QueueInfo struct {
	Name           string
	Incarnation    QueueID
	Ready          uint64
	Unacknowledged uint64
	Consumers      uint64
	NextSession    SessionID
	QueueSpec      QueueSpec
}

type Delivery struct {
	MessageID   MessageID
	Attempt     AttemptID
	Body        []byte `json:"-"`
	Redelivered bool
}

type Result struct {
	MessageID MessageID `json:",omitempty"`
	Delivery  Delivery  `json:",omitzero"`
}

type QueueResponse struct {
	Queue  QueueInfo   `json:",omitzero"`
	Result Result      `json:",omitzero"`
	Count  uint64      `json:",omitempty"`
	Queues []QueueInfo `json:",omitempty"`
	Code   string      `json:",omitempty"`
}

var (
	ErrInvalidCommand   = errors.New("invalid queue command")
	ErrInvalidQueueName = errors.New("invalid queue name")
	ErrQueueNotFound    = errors.New("queue not found")
	ErrQueueIncarnation = errors.New("queue incarnation changed")
	ErrQueueMismatch    = errors.New("queue declaration mismatch")
	ErrQueueInUse       = errors.New("queue in use")
	ErrQueueNotEmpty    = errors.New("queue not empty")
	ErrCapacity         = errors.New("queue shard capacity reached")
	ErrSessionOrder     = errors.New("session ID out of order")
	ErrSessionConflict  = errors.New("session ID reused by a different owner or with different settings")
	ErrSessionClosed    = errors.New("session closed")
	ErrUnknownSession   = errors.New("unknown session")
	ErrSessionOwner     = errors.New("wrong session owner")
	ErrInvalidSequence  = errors.New("sequence must start at one")
	ErrStaleSequence    = errors.New("sequence older than most recent command")
	ErrSequenceGap      = errors.New("sequence skips next command")
	ErrSequenceConflict = errors.New("sequence reused with different command")
	ErrIDsExhausted     = errors.New("queue ID space exhausted")
	ErrNoCredit         = errors.New("no delivery credit")
	ErrEmpty            = errors.New("queue empty")
	ErrUnknownAttempt   = errors.New("attempt is not outstanding for this session")
)

var errorCodes = map[string]error{
	"invalid-command": ErrInvalidCommand, "invalid-queue-name": ErrInvalidQueueName,
	"not-found": ErrQueueNotFound, "incarnation": ErrQueueIncarnation,
	"declaration": ErrQueueMismatch, "in-use": ErrQueueInUse, "not-empty": ErrQueueNotEmpty,
	"capacity": ErrCapacity, "session-order": ErrSessionOrder, "session-conflict": ErrSessionConflict,
	"session-closed": ErrSessionClosed, "session-unknown": ErrUnknownSession, "session-owner": ErrSessionOwner,
	"invalid-sequence": ErrInvalidSequence, "sequence-stale": ErrStaleSequence,
	"sequence-gap": ErrSequenceGap, "sequence-conflict": ErrSequenceConflict, "ids-exhausted": ErrIDsExhausted,
	"no-credit": ErrNoCredit, "empty": ErrEmpty, "unknown-attempt": ErrUnknownAttempt,
}

var errorsToCode = func() map[error]string {
	codes := make(map[error]string, len(errorCodes))
	for key, value := range errorCodes {
		codes[value] = key
	}
	return codes
}()

func failure(err error) QueueResponse { return QueueResponse{Code: errorsToCode[err]} }

func (r QueueResponse) Err() error {
	if r.Code == "" {
		return nil
	}
	if err := errorCodes[r.Code]; err != nil {
		return err
	}
	return fmt.Errorf("unknown cluster queue response %q", r.Code)
}

const (
	frameVersion = 1
	framePrefix  = 9
	maxFrame     = 128 << 20
)

// Commands and replies carry raw payload bytes after a typed metadata header.
// This keeps publish and delivery bodies out of the metadata codec.
func encodeFrame(metadata any, body []byte) ([]byte, error) {
	header, err := json.Marshal(metadata)
	if err != nil {
		return nil, err
	}
	size := framePrefix + len(header) + len(body)
	if size > maxFrame {
		return nil, ErrCapacity
	}
	frame := make([]byte, framePrefix, size)
	frame[0] = frameVersion
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(header)))
	binary.BigEndian.PutUint32(frame[5:9], uint32(len(body)))
	frame = append(frame, header...)
	return append(frame, body...), nil
}

func decodeFrame(data []byte, metadata any) ([]byte, error) {
	if len(data) < framePrefix || len(data) > maxFrame || data[0] != frameVersion {
		return nil, ErrInvalidCommand
	}
	header := uint64(binary.BigEndian.Uint32(data[1:5])) + framePrefix
	body := uint64(binary.BigEndian.Uint32(data[5:9]))
	if header+body != uint64(len(data)) {
		return nil, ErrInvalidCommand
	}
	if err := json.Unmarshal(data[framePrefix:header], metadata); err != nil {
		return nil, err
	}
	if body == 0 {
		return nil, nil
	}
	return data[header:], nil
}

func EncodeCommand(c QueueCommand) ([]byte, error) {
	if len(c.Body) > maxMessageBytes || (c.Kind != PublishMessage && len(c.Body) != 0) {
		return nil, ErrInvalidCommand
	}
	return encodeFrame(c, c.Body)
}

// DecodeCommand unpacks a queue operation, borrowing the body from data.
func DecodeCommand(data []byte) (QueueCommand, error) {
	if len(data) > maxCommandBytes {
		return QueueCommand{}, ErrInvalidCommand
	}
	var c QueueCommand
	var err error
	c.Body, err = decodeFrame(data, &c)
	if err != nil || c.Kind != PublishMessage && len(c.Body) != 0 {
		return QueueCommand{}, ErrInvalidCommand
	}
	return c, nil
}

func DecodeResponse(data []byte) (QueueResponse, error) {
	var r QueueResponse
	body, err := decodeFrame(data, &r)
	if err == nil {
		if len(body) > 0 && r.Result.Delivery.Attempt == 0 {
			return QueueResponse{}, ErrInvalidCommand
		}
		r.Result.Delivery.Body = body
	}
	return r, err
}
