// Package clusterqueue implements the deterministic queue state in one shard.
package clusterqueue

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/maxpert/amqp-go/internal/clusterpayload"
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
	SettleBatch
	PublishBatch
	ClaimBatch
	// AckAndClaim acknowledges an ordered prefix, then claims only after all
	// requested ACKs succeed. Both outcomes share one session sequence.
	AckAndClaim
	SetAssignmentCredit
)

const MaxSettlementBatch = 64

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
	Queue       string           `json:",omitempty"`
	Incarnation QueueID          `json:",omitempty"`
	Session     SessionID        `json:",omitempty"`
	Owner       string           `json:",omitempty"`
	Sequence    uint64           `json:",omitempty"`
	Prefetch    uint32           `json:",omitempty"`
	Consumer    bool             `json:",omitempty"`
	AutoAssign  bool             `json:"-"`
	Body        []byte           `json:"-"`
	Attempt     AttemptID        `json:",omitempty"`
	Settlement  QueueCommandKind `json:",omitempty"`
	Attempts    []AttemptID      `json:",omitempty"`
	ClaimLimit  uint32           `json:",omitempty"`
	ClaimBytes  uint32           `json:",omitempty"`
	// Deadline is absolute Unix nanoseconds. The leader supplies expiry cutoffs.
	Deadline      int64     `json:",omitempty"`
	QueueSpec     QueueSpec `json:",omitzero"`
	IfEmpty       bool      `json:",omitempty"`
	IfUnused      bool      `json:",omitempty"`
	packedVersion byte
	logicalSize   int

	// AssignmentAfter requests an optional page of newly assigned flights.
	AssignmentAfter *AttemptID `json:",omitempty"`
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
	Packed      bool `json:",omitempty"`
	cold        *journalBody
}

type Result struct {
	MessageID MessageID `json:",omitempty"`
	Delivery  Delivery  `json:",omitzero"`
}

type QueueResponse struct {
	storageErr    error
	Queue         QueueInfo   `json:",omitzero"`
	Result        Result      `json:",omitzero"`
	Count         uint64      `json:",omitempty"`
	Queues        []QueueInfo `json:",omitempty"`
	Code          string      `json:",omitempty"`
	DeliveryBatch bool        `json:",omitempty"`
	Deliveries    []Delivery  `json:"-"`
	// AckClaim distinguishes a combined result, including partial settlement
	// and empty refill. Count still counts deliveries; Settled counts ACKs.
	AckClaim  bool   `json:",omitempty"`
	Settled   uint64 `json:",omitempty"`
	ClaimCode string `json:",omitempty"`
	// A settlement can include a read-only page of already assigned flights.
	AssignmentAfter      *AttemptID `json:",omitempty"`
	AssignmentPage       []byte     `json:"-"`
	assignmentPageEncode func() ([]byte, error)
	assignmentPageBytes  int
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

// ClaimErr reports the refill outcome independently of the ACK outcome. A
// successful AckAndClaim can settle messages even when refill finds no work.
func (r QueueResponse) ClaimErr() error {
	return (QueueResponse{Code: r.ClaimCode}).Err()
}

const (
	frameVersion              = 1
	assignmentCommandVersion  = 2
	packedPublishVersion      = 3
	smallPackedPublishVersion = 4
	framePrefix               = 9
	maxFrame                  = 128 << 20
)

type assignmentCommandMetadata struct {
	*QueueCommand
	AutoAssign bool `json:",omitempty"`
}

// Commands and replies carry raw payload bytes after a typed metadata header.
// This keeps publish and delivery bodies out of the metadata codec.
func encodeFrame(metadata any, body []byte) ([]byte, error) {
	frame, err := encodeFrameHeader(metadata, len(body))
	if err != nil {
		return nil, err
	}
	return append(frame, body...), nil
}

func encodeFrameHeader(metadata any, bodySize int) ([]byte, error) {
	header, err := json.Marshal(metadata)
	if err != nil {
		return nil, err
	}
	size := framePrefix + len(header) + bodySize
	if size > maxFrame {
		return nil, ErrCapacity
	}
	frame := make([]byte, framePrefix, size)
	frame[0] = frameVersion
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(header)))
	binary.BigEndian.PutUint32(frame[5:9], uint32(bodySize))
	return append(frame, header...), nil
}

func decodeFrame(data []byte, metadata any) ([]byte, error) {
	return decodeFrameVersion(data, metadata, frameVersion)
}

func decodeFrameVersion(data []byte, metadata any, version byte) ([]byte, error) {
	if len(data) < framePrefix || len(data) > maxFrame || data[0] != version {
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
	return encodeCommand(c, nil)
}

func encodeCommand(c QueueCommand, prefix []byte) ([]byte, error) {
	if len(prefix) > maxMessageBytes-len(c.Body) || !validCommandPayload(c) {
		return nil, ErrInvalidCommand
	}
	assignment := c.AutoAssign || c.Kind == SetAssignmentCredit
	var metadata any = &c
	if assignment {
		metadata = assignmentCommandMetadata{QueueCommand: &c, AutoAssign: c.AutoAssign}
	}
	frame, err := encodeFrameHeader(metadata, len(prefix)+len(c.Body))
	if err != nil {
		return nil, err
	}
	if assignment {
		frame[0] = assignmentCommandVersion
	} else if c.packedVersion != 0 {
		frame[0] = c.packedVersion
	}
	frame = append(frame, prefix...)
	return append(frame, c.Body...), nil
}

// DecodeCommand unpacks a queue operation, borrowing the body from data.
func DecodeCommand(data []byte) (QueueCommand, error) {
	if len(data) > maxCommandBytes {
		return QueueCommand{}, ErrInvalidCommand
	}
	var c QueueCommand
	var err error
	if IsAssignmentCommand(data) {
		metadata := assignmentCommandMetadata{QueueCommand: &c}
		c.Body, err = decodeFrameVersion(data, &metadata, assignmentCommandVersion)
		c.AutoAssign = metadata.AutoAssign
		if !c.AutoAssign && c.Kind != SetAssignmentCredit {
			return QueueCommand{}, ErrInvalidCommand
		}
	} else {
		version := byte(frameVersion)
		if IsPackedPublish(data) {
			version = data[0]
			c.packedVersion = version
		}
		c.Body, err = decodeFrameVersion(data, &c, version)
		if c.Kind == SetAssignmentCredit {
			return QueueCommand{}, ErrInvalidCommand
		}
	}
	if err == nil && c.packedVersion != 0 {
		switch c.Kind {
		case PublishMessage:
			if !c.allowsPackedEnvelope(c.Body) {
				return QueueCommand{}, ErrInvalidCommand
			}
			c.logicalSize, err = clusterpayload.ValidateEnvelope(c.Body)
			if c.Session != 0 || c.logicalSize <= len(c.Body) || c.logicalSize > maxMessageBytes {
				err = ErrInvalidCommand
			}
		case PublishBatch:
			// Batch validation checks the decoded size of each record.
		default:
			err = ErrInvalidCommand
		}
	}
	if err != nil || !validCommandPayload(c) {
		return QueueCommand{}, ErrInvalidCommand
	}
	return c, nil
}

func IsAssignmentCommand(data []byte) bool {
	return len(data) != 0 && data[0] == assignmentCommandVersion
}

func IsPackedPublish(data []byte) bool {
	return len(data) != 0 && (data[0] == packedPublishVersion || data[0] == smallPackedPublishVersion)
}

func IsSmallPackedPublish(data []byte) bool {
	return len(data) != 0 && data[0] == smallPackedPublishVersion
}

func (c QueueCommand) allowsPackedEnvelope(body []byte) bool {
	return len(body) != 0 && (body[0] != clusterpayload.SmallPackedEnvelope || c.packedVersion == smallPackedPublishVersion)
}

var ackCommandPrefix = fmt.Appendf(nil, `{"Kind":%d,`, AckMessage)
var settleBatchCommandPrefix = fmt.Appendf(nil, `{"Kind":%d,`, SettleBatch)

// IsSettlementCommand recognizes the normal bodyless encoding for scheduling.
// The replicated state machine still decodes and validates the whole command.
func IsSettlementCommand(data []byte) bool {
	if len(data) < framePrefix || data[0] != frameVersion || binary.BigEndian.Uint32(data[5:9]) != 0 {
		return false
	}
	return bytes.HasPrefix(data[framePrefix:], ackCommandPrefix) ||
		bytes.HasPrefix(data[framePrefix:], settleBatchCommandPrefix)
}

func InvalidCommandReply() []byte {
	frame, _ := encodeQueueReply(failure(ErrInvalidCommand))
	return frame
}

func validCommandPayload(c QueueCommand) bool {
	if c.Kind < DeclareQueue || c.Kind > SetAssignmentCredit {
		return false
	}
	if c.AutoAssign && (c.Kind != OpenQueueSession || !c.Consumer || c.Prefetch == 0 ||
		c.Prefetch > maxRegisteredConsumerCredit) {
		return false
	}
	if c.AssignmentAfter != nil && (c.Kind != AckMessage &&
		(c.Kind != SettleBatch || c.Settlement != AckMessage) || len(c.Body) != 0) {
		return false
	}
	if c.Kind == AckAndClaim {
		return validAckAndClaimCommand(c)
	}
	if c.Kind == ClaimBatch {
		return validClaimBatchCommand(c)
	}
	if c.ClaimLimit != 0 || c.ClaimBytes != 0 {
		return false
	}
	if c.Kind == SetAssignmentCredit {
		return validQueueName(c.Queue) && c.Incarnation != 0 && c.Session != 0 && c.Sequence != 0 &&
			c.Owner != "" && len(c.Owner) <= 128 && c.Prefetch > 0 && c.Prefetch <= maxRegisteredConsumerCredit &&
			len(c.Body) == 0 && c.Attempt == 0 && c.Settlement == 0 && len(c.Attempts) == 0 &&
			!c.Consumer && c.Deadline == 0 && !c.IfEmpty && !c.IfUnused &&
			!c.QueueSpec.Durable && !c.QueueSpec.AutoDelete && !c.QueueSpec.Exclusive && len(c.QueueSpec.Arguments) == 0
	}
	if c.Kind == PublishBatch {
		return validPublishBatchCommand(c)
	}
	if c.Kind != SettleBatch {
		return c.Settlement == 0 && len(c.Attempts) == 0 && (c.Kind == PublishMessage || len(c.Body) == 0)
	}
	if !validQueueName(c.Queue) || c.Incarnation == 0 || c.Session == 0 || c.Sequence == 0 || len(c.Owner) == 0 || len(c.Owner) > 128 ||
		c.Attempt != 0 || len(c.Body) != 0 || len(c.Attempts) < 2 || len(c.Attempts) > MaxSettlementBatch ||
		c.Prefetch != 0 || c.Consumer || c.Deadline != 0 || c.IfEmpty || c.IfUnused ||
		c.QueueSpec.Durable || c.QueueSpec.AutoDelete || c.QueueSpec.Exclusive || len(c.QueueSpec.Arguments) != 0 {
		return false
	}
	switch c.Settlement {
	case AckMessage, DropMessage, RequeueMessage:
	default:
		return false
	}
	for _, attempt := range c.Attempts {
		if attempt == 0 {
			return false
		}
	}
	return true
}

// DecodeResponse returns a zero response if decoding or validation fails.
func DecodeResponse(data []byte) (QueueResponse, error) {
	var r QueueResponse
	body, err := decodeFrame(data, &r)
	if err != nil {
		return QueueResponse{}, err
	}
	if r.AssignmentAfter != nil {
		if r.AckClaim || r.DeliveryBatch || r.Settled != 0 || r.ClaimCode != "" || r.Code != "" ||
			r.Result.Delivery.Attempt != 0 || len(body) == 0 || len(body) > MaxSettlementAssignmentPageBytes {
			return QueueResponse{}, ErrInvalidCommand
		}
		r.AssignmentPage = body
		return r, nil
	}
	if r.AckClaim {
		return decodeAckAndClaimResponse(r, body)
	}
	if r.Settled != 0 || r.ClaimCode != "" {
		return QueueResponse{}, ErrInvalidCommand
	}
	if r.DeliveryBatch {
		return decodeClaimBatchResponse(r, body)
	}
	if len(body) > 0 && r.Result.Delivery.Attempt == 0 {
		return QueueResponse{}, ErrInvalidCommand
	}
	r.Result.Delivery.Body = body
	if r.Result.Delivery.Packed {
		size, valid := r.Result.Delivery.logicalWireSize()
		if !valid || size > maxMessageBytes {
			return QueueResponse{}, ErrInvalidCommand
		}
	}
	return r, nil
}
