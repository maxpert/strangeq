package clusterqueue

import "encoding/binary"

const assignmentRequestTag = 0xa7
const MaxAssignmentRequestBytes = 8 << 10
const MaxSettlementAssignmentPageBytes = 64 << 10

type AssignmentRequest struct {
	Queue       string
	Incarnation QueueID
	Session     SessionID
	Owner       string
	After       AttemptID
	Limit       uint32
	Bytes       uint32
}

type assignmentPageHeader struct {
	Through AttemptID
	Count   uint32 `json:",omitempty"`
	Code    string `json:",omitempty"`
}

func (r AssignmentRequest) valid() bool {
	return validQueueName(r.Queue) && r.Incarnation != 0 && r.Session != 0 &&
		r.Owner != "" && len(r.Owner) <= 128 && r.Limit != 0 && r.Limit <= MaxClaimBatchMessages &&
		r.Bytes != 0 && r.Bytes <= MaxClaimBatchBytes
}

func IsAssignmentRequest(data []byte) bool {
	return len(data) != 0 && data[0] == assignmentRequestTag
}

func EncodeAssignmentRequest(request AssignmentRequest) ([]byte, error) {
	if !request.valid() {
		return nil, ErrInvalidCommand
	}
	frame, err := encodeFrame(request, nil)
	if err != nil {
		return nil, err
	}
	if len(frame) >= MaxAssignmentRequestBytes {
		return nil, ErrInvalidCommand
	}
	return append([]byte{assignmentRequestTag}, frame...), nil
}

func DecodeAssignmentRequest(data []byte) (AssignmentRequest, error) {
	if !IsAssignmentRequest(data) || len(data) > MaxAssignmentRequestBytes {
		return AssignmentRequest{}, ErrInvalidCommand
	}
	var request AssignmentRequest
	body, err := decodeFrame(data[1:], &request)
	if err != nil || len(body) != 0 || !request.valid() {
		return AssignmentRequest{}, ErrInvalidCommand
	}
	return request, nil
}

// CaptureAssignmentPage captures immutable delivery bodies while the caller
// serializes machine access and holds the journal reader through encoding.
func (m *Machine) CaptureAssignmentPage(request AssignmentRequest) (func() ([]byte, error), int, error) {
	if !request.valid() {
		return nil, 0, ErrInvalidCommand
	}
	deliveries, header := m.captureAssignmentDeliveries(request)
	return captureInlineAssignmentPage(deliveries, header)
}

func (m *Machine) captureSettlementAssignment(c QueueCommand, queue *queueState, after AttemptID, reply *QueueResponse) {
	if len(queue.Ready) == 0 {
		return
	}
	request := AssignmentRequest{
		Queue: c.Queue, Incarnation: c.Incarnation, Session: c.Session, Owner: c.Owner,
		After: after, Limit: MaxClaimBatchMessages, Bytes: MaxSettlementAssignmentPageBytes,
	}
	deliveries, header := m.captureAssignmentDeliveries(request)
	if len(deliveries) == 0 {
		return
	}
	for _, delivery := range deliveries {
		if delivery.cold != nil {
			return
		}
	}
	encode, size, err := captureInlineAssignmentPage(deliveries, header)
	if err == nil && size < MaxSettlementAssignmentPageBytes {
		reply.AssignmentAfter = &after
		reply.assignmentPageEncode = encode
		reply.assignmentPageBytes = size
	}
}

func (m *Machine) captureAssignmentDeliveries(request AssignmentRequest) ([]Delivery, assignmentPageHeader) {
	command := QueueCommand{
		Queue: request.Queue, Incarnation: request.Incarnation, Session: request.Session, Owner: request.Owner,
		ClaimLimit: request.Limit, ClaimBytes: request.Bytes,
	}
	deliveries, through, err := m.ReadAssignments(command, request.After)
	return deliveries, assignmentPageHeader{
		Through: through, Count: uint32(len(deliveries)), Code: errorsToCode[err],
	}
}

func captureInlineAssignmentPage(deliveries []Delivery, header assignmentPageHeader) (func() ([]byte, error), int, error) {
	size := len(deliveries) * claimBatchRecordBytes
	for _, delivery := range deliveries {
		size += delivery.bodySize()
	}
	return func() ([]byte, error) {
		page, encodeErr := encodeFrameHeader(header, size)
		if encodeErr != nil {
			return nil, encodeErr
		}
		for _, delivery := range deliveries {
			page, encodeErr = delivery.appendRecord(page)
			if encodeErr != nil {
				return nil, encodeErr
			}
		}
		return page, nil
	}, size, nil
}

// AssignmentReady observes the session watermark without materializing bodies.
func (m *Machine) AssignmentReady(request AssignmentRequest) (bool, error) {
	if !request.valid() {
		return false, ErrInvalidCommand
	}
	queue, session, err := m.registeredSession(request.Queue, request.Incarnation, request.Session, request.Owner)
	if err != nil {
		return false, err
	}
	if request.After > session.AssignedThrough {
		return false, ErrUnknownAttempt
	}
	return request.After < session.AssignedThrough || assignmentsExhausted(queue, session), nil
}

func DecodeAssignmentPage(data []byte, request AssignmentRequest) ([]Delivery, AttemptID, error) {
	if !request.valid() {
		return nil, request.After, ErrInvalidCommand
	}
	var page assignmentPageHeader
	body, err := decodeFrame(data, &page)
	if err != nil || page.Through < request.After || page.Count > request.Limit ||
		page.Code != "" && (page.Count != 0 || page.Through != request.After) {
		return nil, request.After, ErrInvalidCommand
	}
	payloadBytes := len(body) - int(page.Count)*claimBatchRecordBytes
	if payloadBytes < 0 || payloadBytes > maxMessageBytes || page.Count > 1 && payloadBytes > int(request.Bytes) {
		return nil, request.After, ErrInvalidCommand
	}
	deliveries := make([]Delivery, 0, page.Count)
	previous := request.After
	logicalBytes := 0
	for range page.Count {
		if len(body) < claimBatchRecordBytes {
			return nil, request.After, ErrInvalidCommand
		}
		id := MessageID(binary.BigEndian.Uint64(body[:8]))
		attempt := AttemptID(binary.BigEndian.Uint64(body[8:16]))
		flags := body[16]
		size := binary.BigEndian.Uint32(body[17:claimBatchRecordBytes])
		if id == 0 || attempt <= previous || attempt > page.Through || flags & ^byte(deliveryRedelivered|deliveryPacked) != 0 ||
			uint64(size) > uint64(len(body)-claimBatchRecordBytes) {
			return nil, request.After, ErrInvalidCommand
		}
		end := claimBatchRecordBytes + int(size)
		delivery := Delivery{
			MessageID: id, Attempt: attempt, Redelivered: flags&deliveryRedelivered != 0,
			Packed: flags&deliveryPacked != 0, Body: body[claimBatchRecordBytes:end],
		}
		logical, valid := delivery.logicalWireSize()
		logicalBytes += logical
		if !valid || logicalBytes > maxMessageBytes || page.Count > 1 && logicalBytes > int(request.Bytes) {
			return nil, request.After, ErrInvalidCommand
		}
		deliveries = append(deliveries, delivery)
		body = body[end:]
		previous = attempt
	}
	if len(body) != 0 {
		return nil, request.After, ErrInvalidCommand
	}
	return deliveries, page.Through, (QueueResponse{Code: page.Code}).Err()
}
