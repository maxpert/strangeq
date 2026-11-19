package clusterqueue

import (
	"encoding/binary"

	"github.com/maxpert/amqp-go/internal/clusterpayload"
)

const (
	MaxClaimBatchMessages = 32
	MaxClaimBatchBytes    = 1 << 20
	claimBatchRecordBytes = 21
	deliveryRedelivered   = 1
	deliveryReferenced    = 2
	deliveryPacked        = 4
)

func validClaimBatchCommand(c QueueCommand) bool {
	return validQueueName(c.Queue) && c.Incarnation != 0 && c.Session != 0 && c.Sequence != 0 &&
		len(c.Owner) > 0 && len(c.Owner) <= 128 &&
		c.ClaimLimit > 0 && c.ClaimLimit <= MaxClaimBatchMessages &&
		c.ClaimBytes > 0 && c.ClaimBytes <= MaxClaimBatchBytes &&
		len(c.Body) == 0 && c.Attempt == 0 && c.Settlement == 0 && len(c.Attempts) == 0 &&
		c.Prefetch == 0 && !c.Consumer && c.Deadline == 0 && !c.IfEmpty && !c.IfUnused &&
		!c.QueueSpec.Durable && !c.QueueSpec.AutoDelete && !c.QueueSpec.Exclusive && len(c.QueueSpec.Arguments) == 0
}

func (q *queueState) claimBatch(c QueueCommand, sess *sessionState) outcome {
	first := q.claim(sess)
	if first.Code != "" {
		return outcome{Code: first.Code, Ready: uint64(len(q.Ready))}
	}

	// Allow one oversized first message to use the ordinary single-message
	// budget. The byte limit applies to all additional records.
	batch := outcome{BatchFirst: first.Attempt, BatchCount: 1}
	remainingBytes := int(c.ClaimBytes) - sess.Flights[first.Attempt].logicalBodySize()
	for batch.BatchCount < c.ClaimLimit && len(q.Ready) > 0 && q.Ready[0].logicalBodySize() <= remainingBytes {
		size := q.Ready[0].logicalBodySize()
		if next := q.claim(sess); next.Code != "" {
			break
		}
		batch.BatchCount++
		remainingBytes -= size
	}
	batch.Ready = uint64(len(q.Ready))
	return batch
}

func (sess *sessionState) claimBatchOutcome(last outcome) QueueResponse {
	deliveries := make([]Delivery, last.BatchCount)
	for i := range deliveries {
		attempt := last.BatchFirst + AttemptID(i)
		message := sess.Flights[attempt]
		deliveries[i] = message.delivery(attempt)
	}
	return QueueResponse{
		Queue: QueueInfo{Ready: last.Ready}, Count: uint64(last.BatchCount), DeliveryBatch: true, Deliveries: deliveries,
	}
}

func (r QueueResponse) deliveryBytes() int {
	if !r.DeliveryBatch {
		return r.Result.Delivery.bodySize()
	}
	size := 0
	for _, delivery := range r.Deliveries {
		size += delivery.bodySize()
	}
	return size
}

// A delivery record embeds its identity and payload length before the raw
// body. No per-delivery JSON or separate payload buffer is needed.
func encodeClaimBatchResponse(response QueueResponse) ([]byte, error) {
	size := len(response.Deliveries)*claimBatchRecordBytes + response.deliveryBytes()
	frame, err := encodeFrameHeader(response, size)
	if err != nil {
		return encodeFrame(failure(ErrCapacity), nil)
	}
	for _, delivery := range response.Deliveries {
		frame, err = delivery.appendRecord(frame)
		if err != nil {
			return nil, err
		}
	}
	return frame, nil
}

func (d Delivery) appendRecord(frame []byte) ([]byte, error) {
	frame = binary.BigEndian.AppendUint64(frame, uint64(d.MessageID))
	frame = binary.BigEndian.AppendUint64(frame, uint64(d.Attempt))
	flags := byte(0)
	if d.Redelivered {
		flags |= deliveryRedelivered
	}
	if d.Packed {
		flags |= deliveryPacked
	}
	frame = append(frame, flags)
	frame = binary.BigEndian.AppendUint32(frame, uint32(d.bodySize()))
	return d.appendBody(frame)
}

func decodeClaimBatchResponse(response QueueResponse, body []byte) (QueueResponse, error) {
	legacy := response.Result
	if response.Count == 0 || response.Count > MaxClaimBatchMessages || response.Code != "" || len(response.Queues) != 0 ||
		response.Queue.Ready > maxMessages || legacy.MessageID != 0 || legacy.Delivery.Attempt != 0 ||
		legacy.Delivery.MessageID != 0 || legacy.Delivery.Redelivered || legacy.Delivery.Packed {
		return QueueResponse{}, ErrInvalidCommand
	}
	payloadBytes := len(body) - int(response.Count)*claimBatchRecordBytes
	if payloadBytes < 0 || payloadBytes > maxMessageBytes || response.Count > 1 && payloadBytes > MaxClaimBatchBytes {
		return QueueResponse{}, ErrInvalidCommand
	}

	deliveries := make([]Delivery, response.Count)
	var previous AttemptID
	logicalBytes := 0
	for i := range deliveries {
		if len(body) < claimBatchRecordBytes {
			return QueueResponse{}, ErrInvalidCommand
		}
		id := MessageID(binary.BigEndian.Uint64(body[:8]))
		attempt := AttemptID(binary.BigEndian.Uint64(body[8:16]))
		flags := body[16]
		size := binary.BigEndian.Uint32(body[17:claimBatchRecordBytes])
		if id == 0 || attempt == 0 || (i > 0 && (attempt <= previous || attempt-previous != 1)) ||
			flags & ^byte(deliveryRedelivered|deliveryPacked) != 0 ||
			size > maxMessageBytes || uint64(size) > uint64(len(body)-claimBatchRecordBytes) {
			return QueueResponse{}, ErrInvalidCommand
		}
		end := claimBatchRecordBytes + int(size)
		delivery := Delivery{MessageID: id, Attempt: attempt, Redelivered: flags&deliveryRedelivered != 0, Packed: flags&deliveryPacked != 0}
		if size != 0 {
			delivery.Body = body[claimBatchRecordBytes:end]
		}
		logical, valid := delivery.logicalWireSize()
		logicalBytes += logical
		if !valid || logicalBytes > maxMessageBytes || response.Count > 1 && logicalBytes > MaxClaimBatchBytes {
			return QueueResponse{}, ErrInvalidCommand
		}
		deliveries[i] = delivery
		previous = attempt
		body = body[end:]
	}
	if len(body) != 0 {
		return QueueResponse{}, ErrInvalidCommand
	}
	response.Deliveries = deliveries
	return response, nil
}

func (d Delivery) logicalWireSize() (int, bool) {
	if !d.Packed {
		return len(d.Body), true
	}
	size, err := clusterpayload.EnvelopeSize(d.Body)
	return size, err == nil && size > len(d.Body)
}
