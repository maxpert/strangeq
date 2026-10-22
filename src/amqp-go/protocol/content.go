package protocol

import (
	"encoding/binary"
	"fmt"
)

// ContentHeader represents the content header frame
type ContentHeader struct {
	ClassID         uint16
	Weight          uint16
	BodySize        uint64
	PropertyFlags   uint16
	ContentType     string
	ContentEncoding string
	Headers         map[string]interface{}
	DeliveryMode    uint8
	Priority        uint8
	CorrelationID   string
	ReplyTo         string
	Expiration      string
	MessageID       string
	Timestamp       uint64
	Type            string
	UserID          string
	AppID           string
	ClusterID       string
}

// Property flags for AMQP content header
const (
	FlagContentType     = 0x8000
	FlagContentEncoding = 0x4000
	FlagHeaders         = 0x2000
	FlagDeliveryMode    = 0x1000
	FlagPriority        = 0x0800
	FlagCorrelationID   = 0x0400
	FlagReplyTo         = 0x0200
	FlagExpiration      = 0x0100
	FlagMessageID       = 0x0080
	FlagTimestamp       = 0x0040
	FlagType            = 0x0020
	FlagUserID          = 0x0010
	FlagAppID           = 0x0008
	FlagClusterID       = 0x0004
)

// contentClassBasic is the only class StrangeQ serves content for.
const contentClassBasic = 60

// releaseHeaderOnError returns the pooled header and propagates the error.
// Top-level (not a closure) so the hot path gains no allocation.
func releaseHeaderOnError(h *ContentHeader, err error) (*ContentHeader, error) {
	PutContentHeader(h)
	return nil, err
}

// ReadContentHeader reads a content header frame
func ReadContentHeader(frame *Frame) (*ContentHeader, error) {
	if frame.Type != FrameHeader {
		return nil, fmt.Errorf("%w: expected header frame, got type %d", ErrInvalidContentHeader, frame.Type)
	}
	p := frame.Payload
	if len(p) < 14 { // class(2) + weight(2) + body-size(8) + property-flags(2)
		return nil, fmt.Errorf("%w: frame too short (%d bytes, need 14)", ErrInvalidContentHeader, len(p))
	}
	classID := binary.BigEndian.Uint16(p[0:2])
	if classID != contentClassBasic {
		return nil, fmt.Errorf("%w: %w: class-id %d, want %d (basic)", ErrInvalidContentHeader, ErrContentHeaderClassMismatch, classID, contentClassBasic)
	}
	weight := binary.BigEndian.Uint16(p[2:4])
	if weight != 0 {
		return nil, fmt.Errorf("%w: weight %d, must be 0", ErrInvalidContentHeader, weight)
	}
	flags := binary.BigEndian.Uint16(p[12:14])
	if flags&0x0001 != 0 {
		return nil, fmt.Errorf("%w: property-flags continuation bit set (basic has 14 properties)", ErrInvalidContentHeader)
	}
	// NOTE: property-flags bit 1 (0x0002) is unassigned for basic and stays
	// ignored — tolerant, and it carries no data.

	header := GetContentHeader()
	header.ClassID, header.Weight = classID, weight
	header.BodySize = binary.BigEndian.Uint64(p[4:12])
	header.PropertyFlags = flags
	offset := 14

	// Decode properties based on flags
	if header.PropertyFlags&FlagContentType != 0 {
		if offset >= len(p) {
			return releaseHeaderOnError(header, fmt.Errorf("%w: content type field missing", ErrInvalidContentHeader))
		}
		contentType, newOffset, err := decodeShortString(p, offset)
		if err != nil {
			return releaseHeaderOnError(header, fmt.Errorf("%w: failed to decode content type: %v", ErrInvalidContentHeader, err))
		}
		header.ContentType = contentType
		offset = newOffset
	}

	if header.PropertyFlags&FlagContentEncoding != 0 {
		if offset >= len(p) {
			return releaseHeaderOnError(header, fmt.Errorf("%w: content encoding field missing", ErrInvalidContentHeader))
		}
		contentEncoding, newOffset, err := decodeShortString(p, offset)
		if err != nil {
			return releaseHeaderOnError(header, fmt.Errorf("%w: failed to decode content encoding: %v", ErrInvalidContentHeader, err))
		}
		header.ContentEncoding = contentEncoding
		offset = newOffset
	}

	if header.PropertyFlags&FlagHeaders != 0 {
		if offset+4 > len(p) {
			return releaseHeaderOnError(header, fmt.Errorf("%w: headers length field missing", ErrInvalidContentHeader))
		}
		var err error
		header.Headers, offset, err = decodeFieldTable(p, offset)
		if err != nil {
			return releaseHeaderOnError(header, fmt.Errorf("%w: failed to decode headers: %v", ErrInvalidContentHeader, err))
		}
	}

	if header.PropertyFlags&FlagDeliveryMode != 0 {
		if offset >= len(p) {
			return releaseHeaderOnError(header, fmt.Errorf("%w: delivery mode field missing", ErrInvalidContentHeader))
		}
		header.DeliveryMode = p[offset]
		offset++
	}

	if header.PropertyFlags&FlagPriority != 0 {
		if offset >= len(p) {
			return releaseHeaderOnError(header, fmt.Errorf("%w: priority field missing", ErrInvalidContentHeader))
		}
		header.Priority = p[offset]
		offset++
	}

	if header.PropertyFlags&FlagCorrelationID != 0 {
		if offset >= len(p) {
			return releaseHeaderOnError(header, fmt.Errorf("%w: correlation ID field missing", ErrInvalidContentHeader))
		}
		correlationID, newOffset, err := decodeShortString(p, offset)
		if err != nil {
			return releaseHeaderOnError(header, fmt.Errorf("%w: failed to decode correlation ID: %v", ErrInvalidContentHeader, err))
		}
		header.CorrelationID = correlationID
		offset = newOffset
	}

	if header.PropertyFlags&FlagReplyTo != 0 {
		if offset >= len(p) {
			return releaseHeaderOnError(header, fmt.Errorf("%w: reply-to field missing", ErrInvalidContentHeader))
		}
		replyTo, newOffset, err := decodeShortString(p, offset)
		if err != nil {
			return releaseHeaderOnError(header, fmt.Errorf("%w: failed to decode reply-to: %v", ErrInvalidContentHeader, err))
		}
		header.ReplyTo = replyTo
		offset = newOffset
	}

	if header.PropertyFlags&FlagExpiration != 0 {
		if offset >= len(p) {
			return releaseHeaderOnError(header, fmt.Errorf("%w: expiration field missing", ErrInvalidContentHeader))
		}
		expiration, newOffset, err := decodeShortString(p, offset)
		if err != nil {
			return releaseHeaderOnError(header, fmt.Errorf("%w: failed to decode expiration: %v", ErrInvalidContentHeader, err))
		}
		header.Expiration = expiration
		offset = newOffset
	}

	if header.PropertyFlags&FlagMessageID != 0 {
		if offset >= len(p) {
			return releaseHeaderOnError(header, fmt.Errorf("%w: message ID field missing", ErrInvalidContentHeader))
		}
		messageID, newOffset, err := decodeShortString(p, offset)
		if err != nil {
			return releaseHeaderOnError(header, fmt.Errorf("%w: failed to decode message ID: %v", ErrInvalidContentHeader, err))
		}
		header.MessageID = messageID
		offset = newOffset
	}

	if header.PropertyFlags&FlagTimestamp != 0 {
		if offset+8 > len(p) {
			return releaseHeaderOnError(header, fmt.Errorf("%w: timestamp field missing", ErrInvalidContentHeader))
		}
		header.Timestamp = binary.BigEndian.Uint64(p[offset : offset+8])
		offset += 8
	}

	if header.PropertyFlags&FlagType != 0 {
		if offset >= len(p) {
			return releaseHeaderOnError(header, fmt.Errorf("%w: type field missing", ErrInvalidContentHeader))
		}
		msgType, newOffset, err := decodeShortString(p, offset)
		if err != nil {
			return releaseHeaderOnError(header, fmt.Errorf("%w: failed to decode type: %v", ErrInvalidContentHeader, err))
		}
		header.Type = msgType
		offset = newOffset
	}

	if header.PropertyFlags&FlagUserID != 0 {
		if offset >= len(p) {
			return releaseHeaderOnError(header, fmt.Errorf("%w: user ID field missing", ErrInvalidContentHeader))
		}
		userID, newOffset, err := decodeShortString(p, offset)
		if err != nil {
			return releaseHeaderOnError(header, fmt.Errorf("%w: failed to decode user ID: %v", ErrInvalidContentHeader, err))
		}
		header.UserID = userID
		offset = newOffset
	}

	if header.PropertyFlags&FlagAppID != 0 {
		if offset >= len(p) {
			return releaseHeaderOnError(header, fmt.Errorf("%w: app ID field missing", ErrInvalidContentHeader))
		}
		appID, newOffset, err := decodeShortString(p, offset)
		if err != nil {
			return releaseHeaderOnError(header, fmt.Errorf("%w: failed to decode app ID: %v", ErrInvalidContentHeader, err))
		}
		header.AppID = appID
		offset = newOffset
	}

	if header.PropertyFlags&FlagClusterID != 0 {
		if offset >= len(p) {
			return releaseHeaderOnError(header, fmt.Errorf("%w: cluster ID field missing", ErrInvalidContentHeader))
		}
		clusterID, newOffset, err := decodeShortString(p, offset)
		if err != nil {
			return releaseHeaderOnError(header, fmt.Errorf("%w: failed to decode cluster ID: %v", ErrInvalidContentHeader, err))
		}
		header.ClusterID = clusterID
		offset = newOffset
	}

	if offset != len(p) {
		return releaseHeaderOnError(header, fmt.Errorf("%w: %d trailing bytes after properties", ErrInvalidContentHeader, len(p)-offset))
	}

	return header, nil
}

// Serialize encodes the ContentHeader into a byte slice
func (h *ContentHeader) Serialize() ([]byte, error) {
	// Compute exact capacity to guarantee zero slice regrowth.
	// Fixed header: classID(2) + weight(2) + bodySize(8) + propertyFlags(2) = 14 bytes.
	// Each short-string property: 1 (length byte) + len(string), capped at 255.
	// Timestamp: 8 bytes. DeliveryMode/Priority: 1 byte each.
	// Headers field-table: encoded length (computed below).
	size := 14
	for _, s := range []string{
		h.ContentType, h.ContentEncoding, h.CorrelationID,
		h.ReplyTo, h.Expiration, h.MessageID,
		h.Type, h.UserID, h.AppID, h.ClusterID,
	} {
		if len(s) > 255 {
			size += 256
		} else {
			size += 1 + len(s)
		}
	}
	if h.PropertyFlags&FlagDeliveryMode != 0 {
		size += 1
	}
	if h.PropertyFlags&FlagPriority != 0 {
		size += 1
	}
	if h.PropertyFlags&FlagTimestamp != 0 {
		size += 8
	}

	// Pre-encode headers field-table to measure its exact size.
	var headersBytes []byte
	if h.PropertyFlags&FlagHeaders != 0 {
		var err error
		headersBytes, err = encodeFieldTable(h.Headers)
		if err != nil {
			return nil, fmt.Errorf("error encoding headers: %v", err)
		}
		size += len(headersBytes)
	}

	result := make([]byte, 0, size)

	// Fixed header
	result = binary.BigEndian.AppendUint16(result, h.ClassID)
	result = binary.BigEndian.AppendUint16(result, h.Weight)
	result = binary.BigEndian.AppendUint64(result, h.BodySize)
	result = binary.BigEndian.AppendUint16(result, h.PropertyFlags)

	// Serialize properties based on property flags
	if h.PropertyFlags&FlagContentType != 0 {
		result = append(result, encodeShortString(h.ContentType)...)
	}

	if h.PropertyFlags&FlagContentEncoding != 0 {
		result = append(result, encodeShortString(h.ContentEncoding)...)
	}

	if h.PropertyFlags&FlagHeaders != 0 {
		result = append(result, headersBytes...)
	}

	if h.PropertyFlags&FlagDeliveryMode != 0 {
		result = append(result, h.DeliveryMode)
	}

	if h.PropertyFlags&FlagPriority != 0 {
		result = append(result, h.Priority)
	}

	if h.PropertyFlags&FlagCorrelationID != 0 {
		result = append(result, encodeShortString(h.CorrelationID)...)
	}

	if h.PropertyFlags&FlagReplyTo != 0 {
		result = append(result, encodeShortString(h.ReplyTo)...)
	}

	if h.PropertyFlags&FlagExpiration != 0 {
		result = append(result, encodeShortString(h.Expiration)...)
	}

	if h.PropertyFlags&FlagMessageID != 0 {
		result = append(result, encodeShortString(h.MessageID)...)
	}

	if h.PropertyFlags&FlagTimestamp != 0 {
		result = binary.BigEndian.AppendUint64(result, h.Timestamp)
	}

	if h.PropertyFlags&FlagType != 0 {
		result = append(result, encodeShortString(h.Type)...)
	}

	if h.PropertyFlags&FlagUserID != 0 {
		result = append(result, encodeShortString(h.UserID)...)
	}

	if h.PropertyFlags&FlagAppID != 0 {
		result = append(result, encodeShortString(h.AppID)...)
	}

	if h.PropertyFlags&FlagClusterID != 0 {
		result = append(result, encodeShortString(h.ClusterID)...)
	}

	return result, nil
}

// SerializeInto appends the content header payload directly into buf,
// avoiding all intermediate encodeShortString allocations.
func (h *ContentHeader) SerializeInto(buf []byte) ([]byte, error) {
	// Fixed header
	buf = binary.BigEndian.AppendUint16(buf, h.ClassID)
	buf = binary.BigEndian.AppendUint16(buf, h.Weight)
	buf = binary.BigEndian.AppendUint64(buf, h.BodySize)
	buf = binary.BigEndian.AppendUint16(buf, h.PropertyFlags)

	if h.PropertyFlags&FlagContentType != 0 {
		buf = AppendShortString(buf, h.ContentType)
	}
	if h.PropertyFlags&FlagContentEncoding != 0 {
		buf = AppendShortString(buf, h.ContentEncoding)
	}
	if h.PropertyFlags&FlagHeaders != 0 {
		headersBytes, err := encodeFieldTable(h.Headers)
		if err != nil {
			return nil, fmt.Errorf("error encoding headers: %v", err)
		}
		buf = append(buf, headersBytes...)
	}
	if h.PropertyFlags&FlagDeliveryMode != 0 {
		buf = append(buf, h.DeliveryMode)
	}
	if h.PropertyFlags&FlagPriority != 0 {
		buf = append(buf, h.Priority)
	}
	if h.PropertyFlags&FlagCorrelationID != 0 {
		buf = AppendShortString(buf, h.CorrelationID)
	}
	if h.PropertyFlags&FlagReplyTo != 0 {
		buf = AppendShortString(buf, h.ReplyTo)
	}
	if h.PropertyFlags&FlagExpiration != 0 {
		buf = AppendShortString(buf, h.Expiration)
	}
	if h.PropertyFlags&FlagMessageID != 0 {
		buf = AppendShortString(buf, h.MessageID)
	}
	if h.PropertyFlags&FlagTimestamp != 0 {
		buf = binary.BigEndian.AppendUint64(buf, h.Timestamp)
	}
	if h.PropertyFlags&FlagType != 0 {
		buf = AppendShortString(buf, h.Type)
	}
	if h.PropertyFlags&FlagUserID != 0 {
		buf = AppendShortString(buf, h.UserID)
	}
	if h.PropertyFlags&FlagAppID != 0 {
		buf = AppendShortString(buf, h.AppID)
	}
	if h.PropertyFlags&FlagClusterID != 0 {
		buf = AppendShortString(buf, h.ClusterID)
	}
	return buf, nil
}
