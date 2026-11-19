package clusterqueue

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
)

const assignmentReplicaTag = 0xa8
const assignmentReplicaPrefix = 25
const maxAssignmentReferenceBody = 128 << 10
const assignmentReferenceBytes = 24 + sha256.Size

var ErrAssignmentBodyDigest = errors.New("assignment journal body digest mismatch")

type AssignmentCoverage struct {
	Term, First, Last uint64
}

func (c AssignmentCoverage) valid() bool {
	return c.Term != 0 && c.First != 0 && c.First <= c.Last && c.Last-c.First < 1024
}

func (c AssignmentCoverage) contains(index, term uint64) bool {
	return term == c.Term && index >= c.First && index <= c.Last
}

func IsAssignmentReplica(data []byte) bool {
	return len(data) != 0 && data[0] == assignmentReplicaTag
}

func EncodeAssignmentReplicaRequest(request []byte, c AssignmentCoverage) []byte {
	if !c.valid() {
		return request
	}
	data := make([]byte, 1, assignmentReplicaPrefix+len(request))
	data[0] = assignmentReplicaTag
	data = binary.BigEndian.AppendUint64(data, c.Term)
	data = binary.BigEndian.AppendUint64(data, c.First)
	data = binary.BigEndian.AppendUint64(data, c.Last)
	return append(data, request...)
}

// The local attempt always takes the ordinary full-body path.
func InlineAssignmentReplicaRequest(data []byte) []byte {
	if IsAssignmentReplica(data) && len(data) > assignmentReplicaPrefix {
		return data[assignmentReplicaPrefix:]
	}
	return data
}

func DecodeAssignmentReplicaRequest(data []byte) (AssignmentRequest, AssignmentCoverage, error) {
	if !IsAssignmentReplica(data) || len(data) <= assignmentReplicaPrefix {
		return AssignmentRequest{}, AssignmentCoverage{}, ErrInvalidCommand
	}
	coverage := AssignmentCoverage{
		Term:  binary.BigEndian.Uint64(data[1:9]),
		First: binary.BigEndian.Uint64(data[9:17]),
		Last:  binary.BigEndian.Uint64(data[17:25]),
	}
	if !coverage.valid() {
		return AssignmentRequest{}, AssignmentCoverage{}, ErrInvalidCommand
	}
	request, err := DecodeAssignmentRequest(data[assignmentReplicaPrefix:])
	return request, coverage, err
}

type AssignmentReference struct {
	Index, Term         uint64
	Offset, Length, CRC uint32
	digest              [sha256.Size]byte
}

type assignmentJournalEntry interface {
	AssignmentJournalEntry() (index, term uint64, size int64)
}

func (d Delivery) assignmentReference(coverage AssignmentCoverage) (AssignmentReference, bool) {
	if d.Packed || d.cold == nil || d.cold.assignmentDigest == nil {
		return AssignmentReference{}, false
	}
	source, ok := d.cold.source.(assignmentJournalEntry)
	if !ok {
		return AssignmentReference{}, false
	}
	index, term, size := source.AssignmentJournalEntry()
	offset, length := d.cold.offset, int64(d.cold.size)
	if !coverage.contains(index, term) || offset < 0 || offset > math.MaxUint32 ||
		length < minJournalBodyBytes || length > maxAssignmentReferenceBody || offset > size || length > size-offset {
		return AssignmentReference{}, false
	}
	return AssignmentReference{Index: index, Term: term, Offset: uint32(offset), Length: uint32(length),
		CRC: d.cold.checksum, digest: *d.cold.assignmentDigest}, true
}

func (m *Machine) CaptureAssignmentReplica(request AssignmentRequest, coverage AssignmentCoverage) (func() ([]byte, error), int, error) {
	if !request.valid() || !coverage.valid() {
		return nil, 0, ErrInvalidCommand
	}
	deliveries, header := m.captureAssignmentDeliveries(request)
	return captureAssignmentReplica(deliveries, header, coverage)
}

func captureAssignmentReplica(deliveries []Delivery, header assignmentPageHeader, coverage AssignmentCoverage) (func() ([]byte, error), int, error) {
	var refs [MaxClaimBatchMessages]AssignmentReference
	size, omitted := len(deliveries)*claimBatchRecordBytes, 0
	for i, delivery := range deliveries {
		if ref, ok := delivery.assignmentReference(coverage); ok {
			refs[i] = ref
			size += assignmentReferenceBytes
			omitted++
		} else {
			size += delivery.bodySize()
		}
	}
	if omitted == 0 {
		return captureInlineAssignmentPage(deliveries, header)
	}
	return func() ([]byte, error) {
		frame, err := encodeFrameHeader(header, size)
		if err != nil {
			return nil, err
		}
		frame[0] = assignmentReplicaTag
		for i, d := range deliveries {
			ref := refs[i]
			if ref.Length == 0 {
				frame, err = d.appendRecord(frame)
				if err != nil {
					return nil, err
				}
				continue
			}
			frame = binary.BigEndian.AppendUint64(frame, uint64(d.MessageID))
			frame = binary.BigEndian.AppendUint64(frame, uint64(d.Attempt))
			flags := byte(deliveryReferenced)
			if d.Redelivered {
				flags |= deliveryRedelivered
			}
			frame = append(frame, flags)
			frame = binary.BigEndian.AppendUint32(frame, ref.Length)
			frame = binary.BigEndian.AppendUint64(frame, ref.Index)
			frame = binary.BigEndian.AppendUint64(frame, ref.Term)
			frame = binary.BigEndian.AppendUint32(frame, ref.Offset)
			frame = binary.BigEndian.AppendUint32(frame, ref.CRC)
			frame = append(frame, ref.digest[:]...)
		}
		return frame, nil
	}, size, nil
}

type AssignmentReplicaPage struct {
	through      AttemptID
	deliveries   []Delivery
	refs         []AssignmentReference
	refDelivery  []int
	omittedBytes int
}

func (p *AssignmentReplicaPage) References() []AssignmentReference { return p.refs }

// DecodeAssignmentReplicaPage checks a peer-authenticated mixed page. No
// payload IO or delivery occurs until the entire envelope has validated.
func DecodeAssignmentReplicaPage(data []byte, request AssignmentRequest, c AssignmentCoverage) (*AssignmentReplicaPage, error) {
	if !request.valid() || !c.valid() {
		return nil, ErrInvalidCommand
	}
	var header assignmentPageHeader
	body, err := decodeFrameVersion(data, &header, assignmentReplicaTag)
	if err != nil || header.Code != "" || header.Count == 0 || header.Count > request.Limit || header.Through < request.After {
		return nil, ErrInvalidCommand
	}
	page := &AssignmentReplicaPage{
		through: header.Through, deliveries: make([]Delivery, 0, header.Count),
		refs: make([]AssignmentReference, 0, header.Count), refDelivery: make([]int, 0, header.Count),
	}
	previous := request.After
	var payloadBytes uint64
	for range header.Count {
		if len(body) < claimBatchRecordBytes {
			return nil, ErrInvalidCommand
		}
		id := MessageID(binary.BigEndian.Uint64(body[:8]))
		attempt := AttemptID(binary.BigEndian.Uint64(body[8:16]))
		flags := body[16]
		size := binary.BigEndian.Uint32(body[17:claimBatchRecordBytes])
		if id == 0 || attempt <= previous || attempt > header.Through ||
			flags & ^byte(deliveryRedelivered|deliveryReferenced|deliveryPacked) != 0 {
			return nil, ErrInvalidCommand
		}
		delivery := Delivery{MessageID: id, Attempt: attempt, Redelivered: flags&deliveryRedelivered != 0, Packed: flags&deliveryPacked != 0}
		body = body[claimBatchRecordBytes:]
		if flags&deliveryReferenced == 0 {
			if uint64(size) > uint64(len(body)) {
				return nil, ErrInvalidCommand
			}
			delivery.Body, body = body[:size], body[size:]
			logical, valid := delivery.logicalWireSize()
			if !valid {
				return nil, ErrInvalidCommand
			}
			payloadBytes += uint64(logical)
		} else {
			if delivery.Packed || len(body) < assignmentReferenceBytes || size < minJournalBodyBytes || size > maxAssignmentReferenceBody {
				return nil, ErrInvalidCommand
			}
			ref := AssignmentReference{Index: binary.BigEndian.Uint64(body[:8]), Term: binary.BigEndian.Uint64(body[8:16]),
				Offset: binary.BigEndian.Uint32(body[16:20]), Length: size, CRC: binary.BigEndian.Uint32(body[20:24])}
			if !c.contains(ref.Index, ref.Term) || ref.Offset > math.MaxUint32-ref.Length {
				return nil, ErrInvalidCommand
			}
			copy(ref.digest[:], body[24:assignmentReferenceBytes])
			body = body[assignmentReferenceBytes:]
			page.refDelivery = append(page.refDelivery, len(page.deliveries))
			page.refs = append(page.refs, ref)
			page.omittedBytes += int(size)
			payloadBytes += uint64(size)
		}
		if payloadBytes > maxMessageBytes || header.Count > 1 && payloadBytes > uint64(request.Bytes) {
			return nil, ErrInvalidCommand
		}
		page.deliveries = append(page.deliveries, delivery)
		previous = attempt
	}
	if len(body) != 0 || len(page.refs) == 0 {
		return nil, ErrInvalidCommand
	}
	return page, nil
}

// Materialize publishes the entire page atomically with respect to the
// caller's delivery cursor. Inline bodies already pin the authenticated reply.
func (p *AssignmentReplicaPage) Materialize(read func(int, []byte) error) ([]Delivery, AttemptID, error) {
	bodies := make([]byte, p.omittedBytes)
	for i, ref := range p.refs {
		body := bodies[:ref.Length:ref.Length]
		if err := read(i, body); err != nil {
			return nil, 0, err
		}
		if crc32.Checksum(body, journalBodyChecksum) != ref.CRC || sha256.Sum256(body) != ref.digest {
			return nil, 0, ErrAssignmentBodyDigest
		}
		p.deliveries[p.refDelivery[i]].Body = body
		bodies = bodies[ref.Length:]
	}
	return p.deliveries, p.through, nil
}
