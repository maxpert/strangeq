package storage

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/maxpert/amqp-go/protocol"
)

// MessageCodec provides binary serialization/deserialization for AMQP messages
// Wire format is designed for efficiency and minimal allocations
//
// Format:
//   [version:1]
//   [flags:1] - bitmap for optional fields presence
//   [delivery_tag:8]
//   [timestamp:8]
//   [delivery_mode:1]
//   [priority:1]
//   [redelivered:1]
//   [body_len:4][body]
//   [routing_key_len:2][routing_key]
//   [exchange_len:2][exchange]
//   [content_type_len:2][content_type] (if present)
//   [content_encoding_len:2][content_encoding] (if present)
//   [correlation_id_len:2][correlation_id] (if present)
//   [reply_to_len:2][reply_to] (if present)
//   [expiration_len:2][expiration] (if present)
//   [message_id_len:2][message_id] (if present)
//   [type_len:2][type] (if present)
//   [user_id_len:2][user_id] (if present)
//   [app_id_len:2][app_id] (if present)
//   [cluster_id_len:2][cluster_id] (if present)
//   [headers_count:2][key_len:2][key][value_len:4][value]... (if present)
//
// Version 1 (current)

const (
	codecVersion = 1

	// Flag bits for optional fields
	flagContentType     = 1 << 0
	flagContentEncoding = 1 << 1
	flagCorrelationID   = 1 << 2
	flagReplyTo         = 1 << 3
	flagExpiration      = 1 << 4
	flagMessageID       = 1 << 5
	flagType            = 1 << 6
	flagUserID          = 1 << 7
	flagAppID           = 1 << 8
	flagClusterID       = 1 << 9
	flagHeaders         = 1 << 10
)

// MessageCodec handles message serialization
type MessageCodec struct{}

// NewMessageCodec creates a new message codec
func NewMessageCodec() *MessageCodec {
	return &MessageCodec{}
}

// Serialize converts a protocol.Message to binary format
func (c *MessageCodec) Serialize(msg *protocol.Message) ([]byte, error) {
	if msg == nil {
		return nil, fmt.Errorf("cannot serialize nil message")
	}

	// Calculate size and build flags
	flags := uint16(0)
	size := 1 + 2 + 8 + 8 + 1 + 1 + 1 + 4 + len(msg.Body) + 2 + len(msg.RoutingKey) + 2 + len(msg.Exchange)

	if msg.ContentType != "" {
		flags |= flagContentType
		size += 2 + len(msg.ContentType)
	}
	if msg.ContentEncoding != "" {
		flags |= flagContentEncoding
		size += 2 + len(msg.ContentEncoding)
	}
	if msg.CorrelationID != "" {
		flags |= flagCorrelationID
		size += 2 + len(msg.CorrelationID)
	}
	if msg.ReplyTo != "" {
		flags |= flagReplyTo
		size += 2 + len(msg.ReplyTo)
	}
	if msg.Expiration != "" {
		flags |= flagExpiration
		size += 2 + len(msg.Expiration)
	}
	if msg.MessageID != "" {
		flags |= flagMessageID
		size += 2 + len(msg.MessageID)
	}
	if msg.Type != "" {
		flags |= flagType
		size += 2 + len(msg.Type)
	}
	if msg.UserID != "" {
		flags |= flagUserID
		size += 2 + len(msg.UserID)
	}
	if msg.AppID != "" {
		flags |= flagAppID
		size += 2 + len(msg.AppID)
	}
	if msg.ClusterID != "" {
		flags |= flagClusterID
		size += 2 + len(msg.ClusterID)
	}
	if len(msg.Headers) > 0 {
		flags |= flagHeaders
		size += 2 // header count
		for key, value := range msg.Headers {
			size += 2 + len(key) + 4 + len(c.encodeHeaderValue(value))
		}
	}

	// Allocate buffer
	buf := make([]byte, size)
	offset := 0

	// Version
	buf[offset] = codecVersion
	offset++

	// Flags
	binary.BigEndian.PutUint16(buf[offset:], flags)
	offset += 2

	// Fixed fields
	binary.BigEndian.PutUint64(buf[offset:], msg.DeliveryTag)
	offset += 8

	binary.BigEndian.PutUint64(buf[offset:], msg.Timestamp)
	offset += 8

	buf[offset] = msg.DeliveryMode
	offset++

	buf[offset] = msg.Priority
	offset++

	if msg.Redelivered {
		buf[offset] = 1
	} else {
		buf[offset] = 0
	}
	offset++

	// Body (always present)
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(msg.Body)))
	offset += 4
	copy(buf[offset:], msg.Body)
	offset += len(msg.Body)

	// Routing key (always present)
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(msg.RoutingKey)))
	offset += 2
	copy(buf[offset:], msg.RoutingKey)
	offset += len(msg.RoutingKey)

	// Exchange (always present)
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(msg.Exchange)))
	offset += 2
	copy(buf[offset:], msg.Exchange)
	offset += len(msg.Exchange)

	// Optional fields
	if flags&flagContentType != 0 {
		offset += c.writeString16(buf[offset:], msg.ContentType)
	}
	if flags&flagContentEncoding != 0 {
		offset += c.writeString16(buf[offset:], msg.ContentEncoding)
	}
	if flags&flagCorrelationID != 0 {
		offset += c.writeString16(buf[offset:], msg.CorrelationID)
	}
	if flags&flagReplyTo != 0 {
		offset += c.writeString16(buf[offset:], msg.ReplyTo)
	}
	if flags&flagExpiration != 0 {
		offset += c.writeString16(buf[offset:], msg.Expiration)
	}
	if flags&flagMessageID != 0 {
		offset += c.writeString16(buf[offset:], msg.MessageID)
	}
	if flags&flagType != 0 {
		offset += c.writeString16(buf[offset:], msg.Type)
	}
	if flags&flagUserID != 0 {
		offset += c.writeString16(buf[offset:], msg.UserID)
	}
	if flags&flagAppID != 0 {
		offset += c.writeString16(buf[offset:], msg.AppID)
	}
	if flags&flagClusterID != 0 {
		offset += c.writeString16(buf[offset:], msg.ClusterID)
	}

	// Headers
	if flags&flagHeaders != 0 {
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(msg.Headers)))
		offset += 2

		for key, value := range msg.Headers {
			offset += c.writeString16(buf[offset:], key)
			valueBytes := c.encodeHeaderValue(value)
			binary.BigEndian.PutUint32(buf[offset:], uint32(len(valueBytes)))
			offset += 4
			copy(buf[offset:], valueBytes)
			offset += len(valueBytes)
		}
	}

	return buf, nil
}

// Deserialize converts binary data back to protocol.Message
func (c *MessageCodec) Deserialize(data []byte) (*protocol.Message, error) {
	if len(data) < 23 {
		return nil, fmt.Errorf("invalid message data: too short (got %d bytes)", len(data))
	}

	offset := 0

	// Version
	version := data[offset]
	offset++
	if version != codecVersion {
		return nil, fmt.Errorf("unsupported codec version: %d", version)
	}

	// Flags
	flags := binary.BigEndian.Uint16(data[offset:])
	offset += 2

	msg := &protocol.Message{}

	// Fixed fields
	msg.DeliveryTag = binary.BigEndian.Uint64(data[offset:])
	offset += 8

	msg.Timestamp = binary.BigEndian.Uint64(data[offset:])
	offset += 8

	msg.DeliveryMode = data[offset]
	offset++

	msg.Priority = data[offset]
	offset++

	msg.Redelivered = data[offset] == 1
	offset++

	// Body
	if offset+4 > len(data) {
		return nil, fmt.Errorf("invalid message data: body length at offset %d", offset)
	}
	bodyLen := binary.BigEndian.Uint32(data[offset:])
	offset += 4
	if offset+int(bodyLen) > len(data) {
		return nil, fmt.Errorf("invalid message data: body at offset %d (len=%d)", offset, bodyLen)
	}
	msg.Body = make([]byte, bodyLen)
	copy(msg.Body, data[offset:offset+int(bodyLen)])
	offset += int(bodyLen)

	// Routing key
	var err error
	msg.RoutingKey, offset, err = c.readString16(data, offset)
	if err != nil {
		return nil, fmt.Errorf("routing key: %w", err)
	}

	// Exchange
	msg.Exchange, offset, err = c.readString16(data, offset)
	if err != nil {
		return nil, fmt.Errorf("exchange: %w", err)
	}

	// Optional fields
	if flags&flagContentType != 0 {
		msg.ContentType, offset, err = c.readString16(data, offset)
		if err != nil {
			return nil, fmt.Errorf("content_type: %w", err)
		}
	}
	if flags&flagContentEncoding != 0 {
		msg.ContentEncoding, offset, err = c.readString16(data, offset)
		if err != nil {
			return nil, fmt.Errorf("content_encoding: %w", err)
		}
	}
	if flags&flagCorrelationID != 0 {
		msg.CorrelationID, offset, err = c.readString16(data, offset)
		if err != nil {
			return nil, fmt.Errorf("correlation_id: %w", err)
		}
	}
	if flags&flagReplyTo != 0 {
		msg.ReplyTo, offset, err = c.readString16(data, offset)
		if err != nil {
			return nil, fmt.Errorf("reply_to: %w", err)
		}
	}
	if flags&flagExpiration != 0 {
		msg.Expiration, offset, err = c.readString16(data, offset)
		if err != nil {
			return nil, fmt.Errorf("expiration: %w", err)
		}
	}
	if flags&flagMessageID != 0 {
		msg.MessageID, offset, err = c.readString16(data, offset)
		if err != nil {
			return nil, fmt.Errorf("message_id: %w", err)
		}
	}
	if flags&flagType != 0 {
		msg.Type, offset, err = c.readString16(data, offset)
		if err != nil {
			return nil, fmt.Errorf("type: %w", err)
		}
	}
	if flags&flagUserID != 0 {
		msg.UserID, offset, err = c.readString16(data, offset)
		if err != nil {
			return nil, fmt.Errorf("user_id: %w", err)
		}
	}
	if flags&flagAppID != 0 {
		msg.AppID, offset, err = c.readString16(data, offset)
		if err != nil {
			return nil, fmt.Errorf("app_id: %w", err)
		}
	}
	if flags&flagClusterID != 0 {
		msg.ClusterID, offset, err = c.readString16(data, offset)
		if err != nil {
			return nil, fmt.Errorf("cluster_id: %w", err)
		}
	}

	// Headers
	if flags&flagHeaders != 0 {
		if offset+2 > len(data) {
			return nil, fmt.Errorf("invalid message data: headers count")
		}
		headersCount := binary.BigEndian.Uint16(data[offset:])
		offset += 2

		msg.Headers = make(map[string]interface{}, headersCount)
		for i := 0; i < int(headersCount); i++ {
			key, newOffset, err := c.readString16(data, offset)
			if err != nil {
				return nil, fmt.Errorf("header key %d: %w", i, err)
			}
			offset = newOffset

			if offset+4 > len(data) {
				return nil, fmt.Errorf("invalid message data: header value length %d", i)
			}
			valueLen := binary.BigEndian.Uint32(data[offset:])
			offset += 4

			if offset+int(valueLen) > len(data) {
				return nil, fmt.Errorf("invalid message data: header value %d", i)
			}
			valueBytes := data[offset : offset+int(valueLen)]
			offset += int(valueLen)

			msg.Headers[key] = c.decodeHeaderValue(valueBytes)
		}
	}

	return msg, nil
}

// Helper functions

func (c *MessageCodec) writeString16(buf []byte, s string) int {
	binary.BigEndian.PutUint16(buf, uint16(len(s)))
	copy(buf[2:], s)
	return 2 + len(s)
}

func (c *MessageCodec) readString16(data []byte, offset int) (string, int, error) {
	if offset+2 > len(data) {
		return "", offset, fmt.Errorf("offset %d: cannot read length", offset)
	}
	length := binary.BigEndian.Uint16(data[offset:])
	offset += 2

	if offset+int(length) > len(data) {
		return "", offset, fmt.Errorf("offset %d: cannot read string of length %d", offset, length)
	}
	s := string(data[offset : offset+int(length)])
	offset += int(length)

	return s, offset, nil
}

// encodeHeaderValue serializes header values to bytes
// Supports: string, int64, float64, bool, []byte
func (c *MessageCodec) encodeHeaderValue(value interface{}) []byte {
	switch v := value.(type) {
	case string:
		buf := make([]byte, 1+len(v))
		buf[0] = 'S'
		copy(buf[1:], v)
		return buf

	case int:
		buf := make([]byte, 9)
		buf[0] = 'I'
		binary.BigEndian.PutUint64(buf[1:], uint64(v))
		return buf

	case int64:
		buf := make([]byte, 9)
		buf[0] = 'I'
		binary.BigEndian.PutUint64(buf[1:], uint64(v))
		return buf

	case float64:
		buf := make([]byte, 9)
		buf[0] = 'F'
		binary.BigEndian.PutUint64(buf[1:], math.Float64bits(v))
		return buf

	case bool:
		buf := make([]byte, 2)
		buf[0] = 'B'
		if v {
			buf[1] = 1
		} else {
			buf[1] = 0
		}
		return buf

	case []byte:
		buf := make([]byte, 1+len(v))
		buf[0] = 'D'
		copy(buf[1:], v)
		return buf

	default:
		// Fallback: serialize as string via fmt.Sprintf
		s := fmt.Sprintf("%v", v)
		buf := make([]byte, 1+len(s))
		buf[0] = 'S'
		copy(buf[1:], s)
		return buf
	}
}

// decodeHeaderValue deserializes header values from bytes
func (c *MessageCodec) decodeHeaderValue(data []byte) interface{} {
	if len(data) == 0 {
		return ""
	}

	typeTag := data[0]
	value := data[1:]

	switch typeTag {
	case 'S': // String
		return string(value)

	case 'I': // Int64
		if len(value) < 8 {
			return int64(0)
		}
		return int64(binary.BigEndian.Uint64(value))

	case 'F': // Float64
		if len(value) < 8 {
			return float64(0)
		}
		return math.Float64frombits(binary.BigEndian.Uint64(value))

	case 'B': // Bool
		if len(value) < 1 {
			return false
		}
		return value[0] == 1

	case 'D': // []byte
		return value

	default:
		// Unknown type, return as string
		return string(value)
	}
}

// QueueMetadataCodec handles serialization of queue metadata (NOT runtime state)
// Only serializes: Name, Durable, AutoDelete, Exclusive, Arguments
// Does NOT serialize: Channel, Actor (these are runtime-only)
type QueueMetadataCodec struct{}

// NewQueueMetadataCodec creates a new queue metadata codec
func NewQueueMetadataCodec() *QueueMetadataCodec {
	return &QueueMetadataCodec{}
}

// QueueMetadata represents the persistable state of a queue
type QueueMetadata struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	Arguments  map[string]interface{}
}

// Serialize converts QueueMetadata to binary format
//
// Format:
//
//	[version:1]
//	[flags:1] - bitmap for optional fields
//	[name_len:2][name]
//	[durable:1]
//	[auto_delete:1]
//	[exclusive:1]
//	[arguments_count:2][key_len:2][key][value_len:4][value]... (if present)
func (c *QueueMetadataCodec) Serialize(meta *QueueMetadata) ([]byte, error) {
	if meta == nil {
		return nil, fmt.Errorf("cannot serialize nil queue metadata")
	}

	// Calculate size
	flags := uint8(0)
	size := 1 + 1 + 2 + len(meta.Name) + 1 + 1 + 1

	if len(meta.Arguments) > 0 {
		flags |= 0x01 // Arguments present flag
		size += 2     // arguments count
		for key, value := range meta.Arguments {
			size += 2 + len(key) + 4 + len(c.encodeArgValue(value))
		}
	}

	// Allocate buffer
	buf := make([]byte, size)
	offset := 0

	// Version
	buf[offset] = codecVersion
	offset++

	// Flags
	buf[offset] = flags
	offset++

	// Name
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(meta.Name)))
	offset += 2
	copy(buf[offset:], meta.Name)
	offset += len(meta.Name)

	// Durable
	if meta.Durable {
		buf[offset] = 1
	} else {
		buf[offset] = 0
	}
	offset++

	// AutoDelete
	if meta.AutoDelete {
		buf[offset] = 1
	} else {
		buf[offset] = 0
	}
	offset++

	// Exclusive
	if meta.Exclusive {
		buf[offset] = 1
	} else {
		buf[offset] = 0
	}
	offset++

	// Arguments
	if flags&0x01 != 0 {
		binary.BigEndian.PutUint16(buf[offset:], uint16(len(meta.Arguments)))
		offset += 2

		for key, value := range meta.Arguments {
			binary.BigEndian.PutUint16(buf[offset:], uint16(len(key)))
			offset += 2
			copy(buf[offset:], key)
			offset += len(key)

			valueBytes := c.encodeArgValue(value)
			binary.BigEndian.PutUint32(buf[offset:], uint32(len(valueBytes)))
			offset += 4
			copy(buf[offset:], valueBytes)
			offset += len(valueBytes)
		}
	}

	return buf, nil
}

// Deserialize converts binary data back to QueueMetadata
func (c *QueueMetadataCodec) Deserialize(data []byte) (*QueueMetadata, error) {
	if len(data) < 7 {
		return nil, fmt.Errorf("invalid queue metadata: too short (got %d bytes)", len(data))
	}

	offset := 0

	// Version
	version := data[offset]
	offset++
	if version != codecVersion {
		return nil, fmt.Errorf("unsupported codec version: %d", version)
	}

	// Flags
	flags := data[offset]
	offset++

	meta := &QueueMetadata{}

	// Name
	if offset+2 > len(data) {
		return nil, fmt.Errorf("invalid queue metadata: name length")
	}
	nameLen := binary.BigEndian.Uint16(data[offset:])
	offset += 2
	if offset+int(nameLen) > len(data) {
		return nil, fmt.Errorf("invalid queue metadata: name")
	}
	meta.Name = string(data[offset : offset+int(nameLen)])
	offset += int(nameLen)

	// Durable
	if offset >= len(data) {
		return nil, fmt.Errorf("invalid queue metadata: durable flag")
	}
	meta.Durable = data[offset] == 1
	offset++

	// AutoDelete
	if offset >= len(data) {
		return nil, fmt.Errorf("invalid queue metadata: auto_delete flag")
	}
	meta.AutoDelete = data[offset] == 1
	offset++

	// Exclusive
	if offset >= len(data) {
		return nil, fmt.Errorf("invalid queue metadata: exclusive flag")
	}
	meta.Exclusive = data[offset] == 1
	offset++

	// Arguments
	if flags&0x01 != 0 {
		if offset+2 > len(data) {
			return nil, fmt.Errorf("invalid queue metadata: arguments count")
		}
		argCount := binary.BigEndian.Uint16(data[offset:])
		offset += 2

		meta.Arguments = make(map[string]interface{}, argCount)
		for i := 0; i < int(argCount); i++ {
			// Key
			if offset+2 > len(data) {
				return nil, fmt.Errorf("invalid queue metadata: argument key length %d", i)
			}
			keyLen := binary.BigEndian.Uint16(data[offset:])
			offset += 2
			if offset+int(keyLen) > len(data) {
				return nil, fmt.Errorf("invalid queue metadata: argument key %d", i)
			}
			key := string(data[offset : offset+int(keyLen)])
			offset += int(keyLen)

			// Value
			if offset+4 > len(data) {
				return nil, fmt.Errorf("invalid queue metadata: argument value length %d", i)
			}
			valueLen := binary.BigEndian.Uint32(data[offset:])
			offset += 4
			if offset+int(valueLen) > len(data) {
				return nil, fmt.Errorf("invalid queue metadata: argument value %d", i)
			}
			valueBytes := data[offset : offset+int(valueLen)]
			offset += int(valueLen)

			meta.Arguments[key] = c.decodeArgValue(valueBytes)
		}
	}

	return meta, nil
}

// FromQueue extracts metadata from a protocol.Queue (excludes runtime state)
func (c *QueueMetadataCodec) FromQueue(queue *protocol.Queue) *QueueMetadata {
	return &QueueMetadata{
		Name:       queue.Name,
		Durable:    queue.Durable,
		AutoDelete: queue.AutoDelete,
		Exclusive:  queue.Exclusive,
		Arguments:  queue.Arguments,
	}
}

// ToQueue converts QueueMetadata to protocol.Queue (Actor will be nil, must be initialized separately)
func (c *QueueMetadataCodec) ToQueue(meta *QueueMetadata) *protocol.Queue {
	return &protocol.Queue{
		Name:       meta.Name,
		Durable:    meta.Durable,
		AutoDelete: meta.AutoDelete,
		Exclusive:  meta.Exclusive,
		Arguments:  meta.Arguments,
		Channel:    nil, // Runtime-only, must be set by caller
	}
}

// encodeArgValue serializes queue argument values to bytes
func (c *QueueMetadataCodec) encodeArgValue(value interface{}) []byte {
	switch v := value.(type) {
	case string:
		buf := make([]byte, 1+len(v))
		buf[0] = 'S'
		copy(buf[1:], v)
		return buf

	case int:
		buf := make([]byte, 9)
		buf[0] = 'I'
		binary.BigEndian.PutUint64(buf[1:], uint64(v))
		return buf

	case int32:
		buf := make([]byte, 9)
		buf[0] = 'I'
		binary.BigEndian.PutUint64(buf[1:], uint64(v))
		return buf

	case int64:
		buf := make([]byte, 9)
		buf[0] = 'I'
		binary.BigEndian.PutUint64(buf[1:], uint64(v))
		return buf

	case float64:
		buf := make([]byte, 9)
		buf[0] = 'F'
		binary.BigEndian.PutUint64(buf[1:], math.Float64bits(v))
		return buf

	case bool:
		buf := make([]byte, 2)
		buf[0] = 'B'
		if v {
			buf[1] = 1
		} else {
			buf[1] = 0
		}
		return buf

	case []byte:
		buf := make([]byte, 1+len(v))
		buf[0] = 'D'
		copy(buf[1:], v)
		return buf

	default:
		// Fallback: serialize as string via fmt.Sprintf
		s := fmt.Sprintf("%v", v)
		buf := make([]byte, 1+len(s))
		buf[0] = 'S'
		copy(buf[1:], s)
		return buf
	}
}

// decodeArgValue deserializes queue argument values from bytes
func (c *QueueMetadataCodec) decodeArgValue(data []byte) interface{} {
	if len(data) == 0 {
		return ""
	}

	typeTag := data[0]
	value := data[1:]

	switch typeTag {
	case 'S': // String
		return string(value)

	case 'I': // Int64
		if len(value) < 8 {
			return int64(0)
		}
		return int64(binary.BigEndian.Uint64(value))

	case 'F': // Float64
		if len(value) < 8 {
			return float64(0)
		}
		return math.Float64frombits(binary.BigEndian.Uint64(value))

	case 'B': // Bool
		if len(value) < 1 {
			return false
		}
		return value[0] == 1

	case 'D': // []byte
		return value

	default:
		// Unknown type, return as string
		return string(value)
	}
}
