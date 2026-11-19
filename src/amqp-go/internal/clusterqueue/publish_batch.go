package clusterqueue

import (
	"encoding/binary"

	"github.com/maxpert/amqp-go/internal/clusterpayload"
)

const (
	MaxPublishBatchMessages = 64
	MaxPublishBatchBytes    = 1 << 20
	publishBatchLengthBytes = 4
)

// PublishRecord holds the two parts of a single message body. The encoder
// copies both into the final command without assembling an intermediate body.
type PublishRecord struct {
	Prefix []byte
	Body   []byte
	Packed bool
}

func (r PublishRecord) packedVersion() byte {
	if !r.Packed {
		return 0
	}
	prefix := r.Prefix
	if len(prefix) == 0 {
		prefix = r.Body
	}
	if len(prefix) != 0 && prefix[0] == clusterpayload.SmallPackedEnvelope {
		return smallPackedPublishVersion
	}
	return packedPublishVersion
}

// EncodePublishCommand builds a sessionless publish for one target.
func EncodePublishCommand(queue string, incarnation QueueID, record PublishRecord) ([]byte, error) {
	command := QueueCommand{Kind: PublishMessage, Queue: queue, Incarnation: incarnation, Body: record.Body, packedVersion: record.packedVersion()}
	return encodeCommand(command, record.Prefix)
}

// EncodePublishBatch builds one ordered queue command. Each message is
// preceded by its 32-bit big-endian length in the raw frame body.
func EncodePublishBatch(queue string, incarnation QueueID, bodies [][]byte) ([]byte, error) {
	if len(bodies) > MaxPublishBatchMessages {
		return nil, ErrInvalidCommand
	}
	var records [MaxPublishBatchMessages]PublishRecord
	for i, body := range bodies {
		records[i].Body = body
	}
	return EncodePublishBatchRecords(queue, incarnation, records[:len(bodies)])
}

// EncodePublishBatchRecords writes the same batch format from split records.
func EncodePublishBatchRecords(queue string, incarnation QueueID, records []PublishRecord) ([]byte, error) {
	if !validQueueName(queue) || len(records) < 2 || len(records) > MaxPublishBatchMessages {
		return nil, ErrInvalidCommand
	}
	size := 0
	var packedVersion byte
	for _, record := range records {
		if len(record.Prefix) > maxMessageBytes-len(record.Body) {
			return nil, ErrInvalidCommand
		}
		length := len(record.Prefix) + len(record.Body)
		if length > MaxPublishBatchBytes-size-publishBatchLengthBytes {
			return nil, ErrCapacity
		}
		size += publishBatchLengthBytes + length
		packedVersion = max(packedVersion, record.packedVersion())
	}
	command := QueueCommand{Kind: PublishBatch, Queue: queue, Incarnation: incarnation}
	frame, err := encodeFrameHeader(command, size)
	if err != nil {
		return nil, err
	}
	if packedVersion != 0 {
		frame[0] = packedVersion
	}
	for _, record := range records {
		frame = binary.BigEndian.AppendUint32(frame, uint32(len(record.Prefix)+len(record.Body)))
		frame = append(frame, record.Prefix...)
		frame = append(frame, record.Body...)
	}
	return frame, nil
}

func validPublishBatchCommand(c QueueCommand) bool {
	if !validQueueName(c.Queue) || len(c.Body) > MaxPublishBatchBytes ||
		c.Session != 0 || c.Owner != "" || c.Sequence != 0 || c.Prefetch != 0 || c.Consumer || c.Deadline != 0 ||
		c.Attempt != 0 || c.Settlement != 0 || len(c.Attempts) != 0 || c.IfEmpty || c.IfUnused ||
		c.QueueSpec.Durable || c.QueueSpec.AutoDelete || c.QueueSpec.Exclusive || len(c.QueueSpec.Arguments) != 0 {
		return false
	}
	count := 0
	logicalBatch := 0
	for body := c.Body; len(body) > 0; count++ {
		if count == MaxPublishBatchMessages || len(body) < publishBatchLengthBytes {
			return false
		}
		size := binary.BigEndian.Uint32(body)
		if size > maxMessageBytes || uint64(size) > uint64(len(body)-publishBatchLengthBytes) {
			return false
		}
		end := publishBatchLengthBytes + int(size)
		if c.packedVersion != 0 {
			record := body[publishBatchLengthBytes:end]
			if !c.allowsPackedEnvelope(record) {
				return false
			}
			logical, err := clusterpayload.ValidateEnvelope(record)
			if err != nil || logical > maxMessageBytes || logical > MaxPublishBatchBytes-logicalBatch-publishBatchLengthBytes {
				return false
			}
			logicalBatch += publishBatchLengthBytes + logical
		}
		body = body[end:]
	}
	return count >= 2
}

// publishBatch walks the already validated command. A rejection leaves its
// message and the following suffix unattempted.
func (m *Machine) publishBatch(q *queueState, body []byte, origin journalBodyOrigin, packed bool) QueueResponse {
	var result QueueResponse
	for len(body) > 0 {
		end := publishBatchLengthBytes + int(binary.BigEndian.Uint32(body))
		messageOrigin := origin
		messageOrigin.offset += publishBatchLengthBytes
		record := body[publishBatchLengthBytes:end]
		logical := 0
		if packed {
			// DecodeCommand already validated each envelope in this batch.
			logical, _ = clusterpayload.EnvelopeSize(record)
		}
		published := m.publish(q, record, messageOrigin, logical)
		if published.storageErr != nil {
			return published
		}
		if published.Code != "" {
			result.Code = published.Code
			return result
		}
		result.Count++
		origin.offset += int64(end)
		body = body[end:]
	}
	return result
}
