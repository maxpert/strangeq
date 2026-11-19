package clusterqueue

import (
	"encoding/binary"
	"slices"

	"github.com/maxpert/amqp-go/internal/clusterpayload"
)

const (
	MaxDeadLetterPublishBatchMessages = 32
	MaxDeadLetterPublishBatchBytes    = 2 << 20
	MaxDeadLetterUpdateBatch          = 64
	deadLetterRecordHeaderBytes       = 12
)

// DeadLetterPublishRecord is one ordered publication to the batch's durable
// destination. Move is the source outbox ID for same-shard transfers.
type DeadLetterPublishRecord struct {
	Move   uint64
	Record PublishRecord
}

func (r DeadLetterPublishRecord) WireBytes() int {
	return deadLetterRecordHeaderBytes + len(r.Record.Prefix) + len(r.Record.Body)
}

// EncodeDeadLetterPublishBatch writes ordered, owned payloads for one target.
// Its reply bitmap identifies the accepted records, including an idempotent
// replay of a local move. An ambiguous proposal outcome may be retried by
// the source at the cost of duplicates.
func EncodeDeadLetterPublishBatch(queue string, incarnation QueueID, records []DeadLetterPublishRecord) ([]byte, error) {
	if !validQueueName(queue) || len(records) == 0 || len(records) > MaxDeadLetterPublishBatchMessages {
		return nil, ErrInvalidCommand
	}
	size := 0
	var version byte
	for _, record := range records {
		if record.WireBytes() > MaxDeadLetterPublishBatchBytes-size {
			return nil, ErrCapacity
		}
		size += record.WireBytes()
		version = max(version, record.Record.packedVersion())
	}
	command := QueueCommand{Kind: PublishDeadLetterBatch, Queue: queue, Incarnation: incarnation}
	frame, err := encodeFrameHeader(command, size)
	if err != nil {
		return nil, err
	}
	if version != 0 {
		frame[0] = version
	}
	logicalBytes := size
	for _, record := range records {
		length := record.WireBytes() - deadLetterRecordHeaderBytes
		frame = binary.BigEndian.AppendUint64(frame, record.Move)
		frame = binary.BigEndian.AppendUint32(frame, uint32(length))
		frame = append(frame, record.Record.Prefix...)
		frame = append(frame, record.Record.Body...)
		if record.Record.Packed {
			logical, err := clusterpayload.EnvelopeSize(frame[len(frame)-length:])
			if err != nil || logical <= length {
				return nil, ErrInvalidCommand
			}
			logicalBytes += logical - length
			if logicalBytes > MaxDeadLetterPublishBatchBytes {
				return nil, ErrCapacity
			}
		}
	}
	return frame, nil
}

// EncodeDeadLetterUpdates applies plan and completion changes independently in
// the requested order. A set bit in the reply acknowledges the corresponding
// request, including an idempotent replay. A clear bit leaves it pending.
func EncodeDeadLetterUpdates(updates []DeadLetterUpdate) ([]byte, error) {
	if len(updates) == 0 || len(updates) > MaxDeadLetterUpdateBatch {
		return nil, ErrInvalidCommand
	}
	if deadLetterUpdateBytes(updates) > maxDeadLetterPlanBytes {
		return nil, ErrCapacity
	}
	return EncodeCommand(QueueCommand{Kind: UpdateDeadLetterBatch, DeadLetterUpdates: &updates})
}

func validDeadLetterUpdate(d *DeadLetterUpdate) bool {
	if d == nil || d.ID == 0 {
		return false
	}
	switch {
	case len(d.Plan) != 0:
		return d.Completed == "" && d.Authorize == "" && validDeadLetterNames(d.Plan)
	case d.Completed != "":
		return d.Authorize == "" && validQueueName(d.Completed)
	default:
		return validQueueName(d.Authorize)
	}
}

func deadLetterUpdateBytes(updates []DeadLetterUpdate) uint64 {
	var size uint64
	for _, update := range updates {
		size += deadLetterNamesBytes(update.Plan) + uint64(len(update.Completed)+len(update.Authorize))
	}
	return size
}

func validDeadLetterUpdateBatch(c QueueCommand) bool {
	if c.Queue != "" || c.Incarnation != 0 || len(c.Body) != 0 || c.DeadLetterUpdates == nil || !onlyDeadLetterMetadata(c) {
		return false
	}
	updates := *c.DeadLetterUpdates
	if len(updates) == 0 || len(updates) > MaxDeadLetterUpdateBatch {
		return false
	}
	for i := range updates {
		if !validDeadLetterUpdate(&updates[i]) {
			return false
		}
	}
	return deadLetterUpdateBytes(updates) <= maxDeadLetterPlanBytes
}

// Both batch operations carry only destination or update metadata. Their frame
// version and raw body are validated separately.
func onlyDeadLetterMetadata(c QueueCommand) bool {
	return !c.AutoAssign && !c.Consumer && c.Session == 0 && c.Sequence == 0 && c.Owner == "" &&
		c.Attempt == 0 && c.Settlement == 0 && len(c.Attempts) == 0 &&
		c.ClaimLimit == 0 && c.ClaimBytes == 0 && c.Prefetch == 0 && c.Deadline == 0 &&
		c.AssignmentAfter == nil && c.RejectedAt == 0 && !c.IfEmpty && !c.IfUnused &&
		c.DeadLetter == nil && c.QueueSpec.DeadLetter == nil && !c.QueueSpec.Durable &&
		!c.QueueSpec.AutoDelete && !c.QueueSpec.Exclusive && len(c.QueueSpec.Arguments) == 0
}

func validateDeadLetterPublishRecords(body []byte, command QueueCommand) error {
	if len(body) == 0 || len(body) > MaxDeadLetterPublishBatchBytes {
		return ErrInvalidCommand
	}
	logicalBytes := 0
	for count := 0; len(body) != 0; count++ {
		if count == MaxDeadLetterPublishBatchMessages || len(body) < deadLetterRecordHeaderBytes {
			return ErrInvalidCommand
		}
		size := binary.BigEndian.Uint32(body[8:12])
		if size > uint32(len(body)-deadLetterRecordHeaderBytes) {
			return ErrInvalidCommand
		}
		end := deadLetterRecordHeaderBytes + int(size)
		if command.packedVersion != 0 {
			record := body[deadLetterRecordHeaderBytes:end]
			if !command.allowsPackedEnvelope(record) {
				return ErrInvalidCommand
			}
			logical, err := clusterpayload.ValidateEnvelope(record)
			if err != nil {
				return ErrInvalidCommand
			}
			logicalBytes += deadLetterRecordHeaderBytes + logical
			if logicalBytes > MaxDeadLetterPublishBatchBytes {
				return ErrCapacity
			}
		}
		body = body[end:]
	}
	return nil
}

func (m *Machine) applyDeadLetterUpdates(updates []DeadLetterUpdate) QueueResponse {
	var reply QueueResponse
	for i, update := range updates {
		if update.Authorize != "" {
			entry := m.state.DeadLetters[update.ID]
			if _, pending := slices.BinarySearch(entry.Remaining, update.Authorize); pending {
				reply.DeadLetterApplied |= uint64(1) << i
			}
			continue
		}
		if result := m.updateDeadLetter(update); result.Code != "" {
			continue
		}
		if len(update.Plan) != 0 {
			// A delayed worker may propose after a new leader makes a
			// different plan or after a destination has been completed.
			// Acknowledge only the exact plan that this worker can use.
			entry := m.state.DeadLetters[update.ID]
			if !slices.Equal(entry.Remaining, update.Plan) {
				continue
			}
		}
		reply.DeadLetterApplied |= uint64(1) << i
	}
	return reply
}

func (m *Machine) applyDeadLetterPublishBatch(q *queueState, command QueueCommand, origin journalBodyOrigin) QueueResponse {
	if !q.Spec.Durable {
		return failure(ErrInvalidCommand)
	}
	var result QueueResponse
	// DecodeCommand validated the full batch before applying the first
	// record. Cold payload offsets point into the original command.
	body := command.Body
	for i := 0; len(body) != 0; i++ {
		end := deadLetterRecordHeaderBytes + int(binary.BigEndian.Uint32(body[8:12]))
		messageOrigin := origin
		messageOrigin.offset += deadLetterRecordHeaderBytes
		record := body[deadLetterRecordHeaderBytes:end]
		logical := 0
		if command.packedVersion != 0 {
			logical, _ = clusterpayload.EnvelopeSize(record)
		}
		published := m.publishDeadLetter(QueueCommand{
			DeadLetterMove: binary.BigEndian.Uint64(body),
			Body:           record, logicalSize: logical,
		}, q, messageOrigin)
		if published.storageErr != nil {
			return published
		}
		// Semantic failures leave that record pending. A later record
		// can still fit or carry independent same-shard move credit.
		if published.Code == "" {
			result.Count++
			result.DeadLetterApplied |= uint64(1) << i
		} else if result.Code == "" {
			result.Code = published.Code
		}
		origin.offset += int64(end)
		body = body[end:]
	}
	return result
}
