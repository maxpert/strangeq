package protocol

import (
	"encoding/binary"
	"fmt"
)

// ContentHeader represents the content header frame
type ContentHeader struct {
	ClassID       uint16
	Weight        uint16
	BodySize      uint64
	PropertyFlags uint16
	ContentType   string
	ContentEncoding string
	Headers       map[string]interface{}
	DeliveryMode  uint8
	Priority      uint8
	CorrelationID string
	ReplyTo       string
	Expiration    string
	MessageID     string
	Timestamp     uint64
	Type          string
	UserID        string
	AppID         string
	ClusterID     string
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

// ReadContentHeader reads a content header frame
func ReadContentHeader(frame *Frame) (*ContentHeader, error) {
	if frame.Type != FrameHeader {
		return nil, fmt.Errorf("expected header frame, got type %d", frame.Type)
	}

	if len(frame.Payload) < 12 { // Minimum: class(2) + weight(2) + body-size(8)
		return nil, fmt.Errorf("content header frame too short")
	}

	offset := 0

	header := &ContentHeader{
		ClassID:  binary.BigEndian.Uint16(frame.Payload[offset : offset+2]),
		Weight:   binary.BigEndian.Uint16(frame.Payload[offset+2 : offset+4]),
		BodySize: binary.BigEndian.Uint64(frame.Payload[offset+4 : offset+12]),
	}
	
	offset += 12

	if offset+2 > len(frame.Payload) {
		return nil, fmt.Errorf("property flags not present")
	}
	
	header.PropertyFlags = binary.BigEndian.Uint16(frame.Payload[offset : offset+2])
	offset += 2

	// Decode properties based on flags
	if header.PropertyFlags&FlagContentType != 0 {
		if offset >= len(frame.Payload) {
			return nil, fmt.Errorf("content type field missing")
		}
		contentType, newOffset, err := decodeShortString(frame.Payload, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to decode content type: %v", err)
		}
		header.ContentType = contentType
		offset = newOffset
	}

	if header.PropertyFlags&FlagContentEncoding != 0 {
		if offset >= len(frame.Payload) {
			return nil, fmt.Errorf("content encoding field missing")
		}
		contentEncoding, newOffset, err := decodeShortString(frame.Payload, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to decode content encoding: %v", err)
		}
		header.ContentEncoding = contentEncoding
		offset = newOffset
	}

	if header.PropertyFlags&FlagHeaders != 0 {
		if offset+4 > len(frame.Payload) {
			return nil, fmt.Errorf("headers length field missing")
		}
		var err error
		header.Headers, offset, err = decodeFieldTable(frame.Payload, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to decode headers: %v", err)
		}
	}

	if header.PropertyFlags&FlagDeliveryMode != 0 {
		if offset >= len(frame.Payload) {
			return nil, fmt.Errorf("delivery mode field missing")
		}
		header.DeliveryMode = frame.Payload[offset]
		offset++
	}

	if header.PropertyFlags&FlagPriority != 0 {
		if offset >= len(frame.Payload) {
			return nil, fmt.Errorf("priority field missing")
		}
		header.Priority = frame.Payload[offset]
		offset++
	}

	if header.PropertyFlags&FlagCorrelationID != 0 {
		if offset >= len(frame.Payload) {
			return nil, fmt.Errorf("correlation ID field missing")
		}
		correlationID, newOffset, err := decodeShortString(frame.Payload, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to decode correlation ID: %v", err)
		}
		header.CorrelationID = correlationID
		offset = newOffset
	}

	if header.PropertyFlags&FlagReplyTo != 0 {
		if offset >= len(frame.Payload) {
			return nil, fmt.Errorf("reply-to field missing")
		}
		replyTo, newOffset, err := decodeShortString(frame.Payload, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to decode reply-to: %v", err)
		}
		header.ReplyTo = replyTo
		offset = newOffset
	}

	if header.PropertyFlags&FlagExpiration != 0 {
		if offset >= len(frame.Payload) {
			return nil, fmt.Errorf("expiration field missing")
		}
		expiration, newOffset, err := decodeShortString(frame.Payload, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to decode expiration: %v", err)
		}
		header.Expiration = expiration
		offset = newOffset
	}

	if header.PropertyFlags&FlagMessageID != 0 {
		if offset >= len(frame.Payload) {
			return nil, fmt.Errorf("message ID field missing")
		}
		messageID, newOffset, err := decodeShortString(frame.Payload, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to decode message ID: %v", err)
		}
		header.MessageID = messageID
		offset = newOffset
	}

	if header.PropertyFlags&FlagTimestamp != 0 {
		if offset+8 > len(frame.Payload) {
			return nil, fmt.Errorf("timestamp field missing")
		}
		header.Timestamp = binary.BigEndian.Uint64(frame.Payload[offset : offset+8])
		offset += 8
	}

	if header.PropertyFlags&FlagType != 0 {
		if offset >= len(frame.Payload) {
			return nil, fmt.Errorf("type field missing")
		}
		msgType, newOffset, err := decodeShortString(frame.Payload, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to decode type: %v", err)
		}
		header.Type = msgType
		offset = newOffset
	}

	if header.PropertyFlags&FlagUserID != 0 {
		if offset >= len(frame.Payload) {
			return nil, fmt.Errorf("user ID field missing")
		}
		userID, newOffset, err := decodeShortString(frame.Payload, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to decode user ID: %v", err)
		}
		header.UserID = userID
		offset = newOffset
	}

	if header.PropertyFlags&FlagAppID != 0 {
		if offset >= len(frame.Payload) {
			return nil, fmt.Errorf("app ID field missing")
		}
		appID, newOffset, err := decodeShortString(frame.Payload, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to decode app ID: %v", err)
		}
		header.AppID = appID
		offset = newOffset
	}

	if header.PropertyFlags&FlagClusterID != 0 {
		if offset >= len(frame.Payload) {
			return nil, fmt.Errorf("cluster ID field missing")
		}
		clusterID, newOffset, err := decodeShortString(frame.Payload, offset)
		if err != nil {
			return nil, fmt.Errorf("failed to decode cluster ID: %v", err)
		}
		header.ClusterID = clusterID
		offset = newOffset
	}

	return header, nil
}

// Serialize encodes the ContentHeader into a byte slice
func (h *ContentHeader) Serialize() ([]byte, error) {
	var result []byte

	// Class ID (uint16)
	classIDBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(classIDBytes, h.ClassID)
	result = append(result, classIDBytes...)

	// Weight (uint16)
	weightBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(weightBytes, h.Weight)
	result = append(result, weightBytes...)

	// Body size (uint64)
	bodySizeBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bodySizeBytes, h.BodySize)
	result = append(result, bodySizeBytes...)

	// Property flags (uint16)
	propertyFlagsBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(propertyFlagsBytes, h.PropertyFlags)
	result = append(result, propertyFlagsBytes...)

	// Serialize properties based on property flags
	if h.PropertyFlags&FlagContentType != 0 {
		contentTypeBytes := encodeShortString(h.ContentType)
		result = append(result, contentTypeBytes...)
	}

	if h.PropertyFlags&FlagContentEncoding != 0 {
		contentEncodingBytes := encodeShortString(h.ContentEncoding)
		result = append(result, contentEncodingBytes...)
	}

	if h.PropertyFlags&FlagHeaders != 0 {
		headersBytes, err := encodeFieldTable(h.Headers)
		if err != nil {
			return nil, fmt.Errorf("error encoding headers: %v", err)
		}
		result = append(result, headersBytes...)
	}

	if h.PropertyFlags&FlagDeliveryMode != 0 {
		deliveryModeBytes := []byte{h.DeliveryMode}
		result = append(result, deliveryModeBytes...)
	}

	if h.PropertyFlags&FlagPriority != 0 {
		priorityBytes := []byte{h.Priority}
		result = append(result, priorityBytes...)
	}

	if h.PropertyFlags&FlagCorrelationID != 0 {
		correlationIDBytes := encodeShortString(h.CorrelationID)
		result = append(result, correlationIDBytes...)
	}

	if h.PropertyFlags&FlagReplyTo != 0 {
		replyToBytes := encodeShortString(h.ReplyTo)
		result = append(result, replyToBytes...)
	}

	if h.PropertyFlags&FlagExpiration != 0 {
		expirationBytes := encodeShortString(h.Expiration)
		result = append(result, expirationBytes...)
	}

	if h.PropertyFlags&FlagMessageID != 0 {
		messageIDBytes := encodeShortString(h.MessageID)
		result = append(result, messageIDBytes...)
	}

	if h.PropertyFlags&FlagTimestamp != 0 {
		timestampBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(timestampBytes, h.Timestamp)
		result = append(result, timestampBytes...)
	}

	if h.PropertyFlags&FlagType != 0 {
		typeBytes := encodeShortString(h.Type)
		result = append(result, typeBytes...)
	}

	if h.PropertyFlags&FlagUserID != 0 {
		userIDBytes := encodeShortString(h.UserID)
		result = append(result, userIDBytes...)
	}

	if h.PropertyFlags&FlagAppID != 0 {
		appIDBytes := encodeShortString(h.AppID)
		result = append(result, appIDBytes...)
	}

	if h.PropertyFlags&FlagClusterID != 0 {
		clusterIDBytes := encodeShortString(h.ClusterID)
		result = append(result, clusterIDBytes...)
	}

	return result, nil
}