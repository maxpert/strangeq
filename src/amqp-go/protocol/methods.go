package protocol

import (
	"encoding/binary"
	"fmt"
)

// Method IDs for connection class
const (
	ConnectionStart    = 10
	ConnectionStartOK  = 11
	ConnectionSecure   = 20
	ConnectionSecureOK = 21
	ConnectionTune     = 30
	ConnectionTuneOK   = 31
	ConnectionOpen     = 40
	ConnectionOpenOK   = 41
	ConnectionClose    = 50
	ConnectionCloseOK  = 51
)

// Method IDs for channel class
const (
	ChannelOpen    = 10
	ChannelOpenOK  = 11
	ChannelFlow    = 20
	ChannelFlowOK  = 21
	ChannelClose   = 40
	ChannelCloseOK = 41
)

// Method IDs for exchange class
const (
	ExchangeDeclare   = 10 // 40.10 - class ID 40, method ID 10
	ExchangeDeclareOK = 11 // 40.11 - class ID 40, method ID 11
	ExchangeDelete    = 20 // 40.20 - class ID 40, method ID 20
	ExchangeDeleteOK  = 21 // 40.21 - class ID 40, method ID 21
	ExchangeBind      = 30 // 40.30 - class ID 40, method ID 30
	ExchangeBindOK    = 31 // 40.31 - class ID 40, method ID 31
	ExchangeUnbind    = 40 // 40.40 - class ID 40, method ID 40
	ExchangeUnbindOK  = 41 // 40.41 - class ID 40, method ID 41
)

// Method IDs for queue class
const (
	QueueDeclare   = 10 // 50.10 - class ID 50, method ID 10
	QueueDeclareOK = 11 // 50.11 - class ID 50, method ID 11
	QueueBind      = 20 // 50.20 - class ID 50, method ID 20
	QueueBindOK    = 21 // 50.21 - class ID 50, method ID 21
	QueuePurge     = 30 // 50.30 - class ID 50, method ID 30
	QueuePurgeOK   = 31 // 50.31 - class ID 50, method ID 31
	QueueDelete    = 40 // 50.40 - class ID 50, method ID 40
	QueueDeleteOK  = 41 // 50.41 - class ID 50, method ID 41
	QueueUnbind    = 50 // 50.50 - class ID 50, method ID 50
	QueueUnbindOK  = 51 // 50.51 - class ID 50, method ID 51
)

// Method IDs for basic class
const (
	BasicQos          = 10  // 60.10 - class ID 60, method ID 10
	BasicQosOK        = 11  // 60.11 - class ID 60, method ID 11
	BasicConsume      = 20  // 60.20 - class ID 60, method ID 20
	BasicConsumeOK    = 21  // 60.21 - class ID 60, method ID 21
	BasicCancel       = 30  // 60.30 - class ID 60, method ID 30
	BasicCancelOK     = 31  // 60.31 - class ID 60, method ID 31
	BasicPublish      = 40  // 60.40 - class ID 60, method ID 40
	BasicReturn       = 50  // 60.50 - class ID 60, method ID 50
	BasicDeliver      = 60  // 60.60 - class ID 60, method ID 60
	BasicGet          = 70  // 60.70 - class ID 60, method ID 70
	BasicGetOK        = 71  // 60.71 - class ID 60, method ID 71
	BasicGetEmpty     = 72  // 60.72 - class ID 60, method ID 72
	BasicAck          = 80  // 60.80 - class ID 60, method ID 80
	BasicReject       = 90  // 60.90 - class ID 60, method ID 90
	BasicRecoverAsync = 100 // 60.100 - class ID 60, method ID 100
	BasicRecover      = 110 // 60.110 - class ID 60, method ID 110
	BasicRecoverOK    = 111 // 60.111 - class ID 60, method ID 111
	BasicNack         = 120 // 60.120 - class ID 60, method ID 120
)

// Method IDs for tx class
const (
	TxSelect     = 10 // 90.10 - class ID 90, method ID 10
	TxSelectOK   = 11 // 90.11 - class ID 90, method ID 11
	TxCommit     = 20 // 90.20 - class ID 90, method ID 20
	TxCommitOK   = 21 // 90.21 - class ID 90, method ID 21
	TxRollback   = 30 // 90.30 - class ID 90, method ID 30
	TxRollbackOK = 31 // 90.31 - class ID 90, method ID 31
)

// ConnectionStartMethod represents the connection.start method
type ConnectionStartMethod struct {
	VersionMajor     byte
	VersionMinor     byte
	ServerProperties map[string]interface{}
	Mechanisms       []string
	Locales          []string
}

// Serialize encodes the ConnectionStartMethod into a byte slice
func (m *ConnectionStartMethod) Serialize() ([]byte, error) {
	var result []byte

	// Version major and minor (both are 1 byte each)
	result = append(result, m.VersionMajor, m.VersionMinor)

	// Server properties (field table)
	propsBytes, err := encodeFieldTable(m.ServerProperties)
	if err != nil {
		return nil, err
	}
	result = append(result, propsBytes...)

	// Mechanisms (long string)
	// First, join the mechanisms with a space
	// This is a simplification - in practice, mechanisms might be handled differently
	mechanismsStr := "PLAIN" // For now, just support PLAIN mechanism
	mechBytes := encodeLongString(mechanismsStr)
	result = append(result, mechBytes...)

	// Locales (long string)
	// For now, just support "en_US"
	localesStr := "en_US"
	localeBytes := encodeLongString(localesStr)
	result = append(result, localeBytes...)

	return result, nil
}

// ConnectionStartOKMethod represents the connection.start-ok method
type ConnectionStartOKMethod struct {
	ClientProperties map[string]interface{}
	Mechanism        string
	Response         []byte
	Locale           string
}

// Serialize encodes the ConnectionStartOKMethod into a byte slice
func (m *ConnectionStartOKMethod) Serialize() ([]byte, error) {
	var result []byte

	// Client properties (field table)
	propsBytes, err := encodeFieldTable(m.ClientProperties)
	if err != nil {
		return nil, err
	}
	result = append(result, propsBytes...)

	// Mechanism (short string)
	mechBytes := encodeShortString(m.Mechanism)
	result = append(result, mechBytes...)

	// Response (long string)
	respBytes := encodeLongString(string(m.Response))
	result = append(result, respBytes...)

	// Locale (short string)
	localeBytes := encodeShortString(m.Locale)
	result = append(result, localeBytes...)

	return result, nil
}

// ParseConnectionStartOK parses a connection.start-ok method from bytes
func ParseConnectionStartOK(data []byte) (*ConnectionStartOKMethod, error) {
	method := &ConnectionStartOKMethod{}
	offset := 0

	// Parse client properties (field table)
	var err error
	method.ClientProperties, offset, err = decodeFieldTable(data, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to decode client properties: %w", err)
	}

	// Parse mechanism (short string)
	if len(data) < offset+1 {
		return nil, fmt.Errorf("insufficient data for mechanism length")
	}
	mechLen := int(data[offset])
	offset++

	if len(data) < offset+mechLen {
		return nil, fmt.Errorf("insufficient data for mechanism")
	}
	method.Mechanism = string(data[offset : offset+mechLen])
	offset += mechLen

	// Parse response (long string)
	if len(data) < offset+4 {
		return nil, fmt.Errorf("insufficient data for response length")
	}
	respLen := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	if len(data) < offset+int(respLen) {
		return nil, fmt.Errorf("insufficient data for response")
	}
	method.Response = data[offset : offset+int(respLen)]
	offset += int(respLen)

	// Parse locale (short string)
	if len(data) < offset+1 {
		return nil, fmt.Errorf("insufficient data for locale length")
	}
	localeLen := int(data[offset])
	offset++

	if len(data) < offset+localeLen {
		return nil, fmt.Errorf("insufficient data for locale")
	}
	method.Locale = string(data[offset : offset+localeLen])

	return method, nil
}

// ConnectionTuneMethod represents the connection.tune method
type ConnectionTuneMethod struct {
	ChannelMax uint16
	FrameMax   uint32
	Heartbeat  uint16
}

// Serialize encodes the ConnectionTuneMethod into a byte slice
func (m *ConnectionTuneMethod) Serialize() ([]byte, error) {
	var result []byte

	// Channel max (uint16)
	chMaxBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(chMaxBytes, m.ChannelMax)
	result = append(result, chMaxBytes...)

	// Frame max (uint32)
	frameMaxBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(frameMaxBytes, m.FrameMax)
	result = append(result, frameMaxBytes...)

	// Heartbeat (uint16)
	heartbeatBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(heartbeatBytes, m.Heartbeat)
	result = append(result, heartbeatBytes...)

	return result, nil
}

// ConnectionTuneOKMethod represents the connection.tune-ok method
type ConnectionTuneOKMethod struct {
	ChannelMax uint16
	FrameMax   uint32
	Heartbeat  uint16
}

// Serialize encodes the ConnectionTuneOKMethod into a byte slice
func (m *ConnectionTuneOKMethod) Serialize() ([]byte, error) {
	var result []byte

	// Channel max (uint16)
	chMaxBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(chMaxBytes, m.ChannelMax)
	result = append(result, chMaxBytes...)

	// Frame max (uint32)
	frameMaxBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(frameMaxBytes, m.FrameMax)
	result = append(result, frameMaxBytes...)

	// Heartbeat (uint16)
	heartbeatBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(heartbeatBytes, m.Heartbeat)
	result = append(result, heartbeatBytes...)

	return result, nil
}

// ConnectionOpenMethod represents the connection.open method
type ConnectionOpenMethod struct {
	VirtualHost string
	Reserved1   string
	Reserved2   byte
}

// Serialize encodes the ConnectionOpenMethod into a byte slice
func (m *ConnectionOpenMethod) Serialize() ([]byte, error) {
	var result []byte

	// Virtual host (short string)
	vhostBytes := encodeShortString(m.VirtualHost)
	result = append(result, vhostBytes...)

	// Reserved1 (short string)
	res1Bytes := encodeShortString(m.Reserved1)
	result = append(result, res1Bytes...)

	// Reserved2 (bit)
	result = append(result, m.Reserved2)

	return result, nil
}

// Deserialize decodes the ConnectionOpenMethod from a byte slice
func (m *ConnectionOpenMethod) Deserialize(data []byte) error {
	if len(data) < 1 {
		return fmt.Errorf("connection.open method data too short")
	}

	offset := 0

	// Virtual host (short string)
	vhost, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return err
	}
	m.VirtualHost = vhost
	offset = newOffset

	// Reserved1 (short string)
	res1, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return err
	}
	m.Reserved1 = res1
	offset = newOffset

	// Reserved2 (bit)
	if offset < len(data) {
		m.Reserved2 = data[offset]
		offset++
	}

	return nil
}

// ConnectionOpenOKMethod represents the connection.open-ok method
type ConnectionOpenOKMethod struct {
	Reserved1 string
}

// Serialize encodes the ConnectionOpenOKMethod into a byte slice
func (m *ConnectionOpenOKMethod) Serialize() ([]byte, error) {
	var result []byte

	// Reserved1 (short string)
	res1Bytes := encodeShortString(m.Reserved1)
	result = append(result, res1Bytes...)

	return result, nil
}

// ConnectionCloseMethod represents the connection.close method
type ConnectionCloseMethod struct {
	ReplyCode uint16
	ReplyText string
	ClassID   uint16
	MethodID  uint16
}

// Serialize encodes the ConnectionCloseMethod into a byte slice
func (m *ConnectionCloseMethod) Serialize() ([]byte, error) {
	var result []byte

	// Reply code (uint16)
	replyCodeBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(replyCodeBytes, m.ReplyCode)
	result = append(result, replyCodeBytes...)

	// Reply text (short string)
	replyTextBytes := encodeShortString(m.ReplyText)
	result = append(result, replyTextBytes...)

	// Class ID (uint16)
	classIDBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(classIDBytes, m.ClassID)
	result = append(result, classIDBytes...)

	// Method ID (uint16)
	methodIDBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(methodIDBytes, m.MethodID)
	result = append(result, methodIDBytes...)

	return result, nil
}

// ConnectionCloseOKMethod represents the connection.close-ok method
type ConnectionCloseOKMethod struct {
}

// Serialize encodes the ConnectionCloseOKMethod into a byte slice
func (m *ConnectionCloseOKMethod) Serialize() ([]byte, error) {
	// Connection.close-ok has no content
	return []byte{}, nil
}

// ChannelOpenMethod represents the channel.open method
type ChannelOpenMethod struct {
	Reserved1 string
}

// Serialize encodes the ChannelOpenMethod into a byte slice
func (m *ChannelOpenMethod) Serialize() ([]byte, error) {
	var result []byte

	// Reserved1 (short string)
	res1Bytes := encodeShortString(m.Reserved1)
	result = append(result, res1Bytes...)

	return result, nil
}

// ChannelOpenOKMethod represents the channel.open-ok method
type ChannelOpenOKMethod struct {
	Reserved1 string
}

// Serialize encodes the ChannelOpenOKMethod into a byte slice
func (m *ChannelOpenOKMethod) Serialize() ([]byte, error) {
	var result []byte

	// Reserved1 (long string)
	res1Bytes := encodeLongString(m.Reserved1)
	result = append(result, res1Bytes...)

	return result, nil
}

// ChannelFlowMethod represents the channel.flow method
type ChannelFlowMethod struct {
	Active bool
}

// Serialize encodes the ChannelFlowMethod into a byte slice
func (m *ChannelFlowMethod) Serialize() ([]byte, error) {
	var result []byte

	// Active (bit)
	if m.Active {
		result = append(result, 1)
	} else {
		result = append(result, 0)
	}

	return result, nil
}

// ChannelFlowOKMethod represents the channel.flow-ok method
type ChannelFlowOKMethod struct {
	Active bool
}

// Serialize encodes the ChannelFlowOKMethod into a byte slice
func (m *ChannelFlowOKMethod) Serialize() ([]byte, error) {
	var result []byte

	// Active (bit)
	if m.Active {
		result = append(result, 1)
	} else {
		result = append(result, 0)
	}

	return result, nil
}

// ChannelCloseMethod represents the channel.close method
type ChannelCloseMethod struct {
	ReplyCode uint16
	ReplyText string
	ClassID   uint16
	MethodID  uint16
}

// Serialize encodes the ChannelCloseMethod into a byte slice
func (m *ChannelCloseMethod) Serialize() ([]byte, error) {
	var result []byte

	// Reply code (uint16)
	replyCodeBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(replyCodeBytes, m.ReplyCode)
	result = append(result, replyCodeBytes...)

	// Reply text (short string)
	replyTextBytes := encodeShortString(m.ReplyText)
	result = append(result, replyTextBytes...)

	// Class ID (uint16)
	classIDBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(classIDBytes, m.ClassID)
	result = append(result, classIDBytes...)

	// Method ID (uint16)
	methodIDBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(methodIDBytes, m.MethodID)
	result = append(result, methodIDBytes...)

	return result, nil
}

// ChannelCloseOKMethod represents the channel.close-ok method
type ChannelCloseOKMethod struct {
}

// Serialize encodes the ChannelCloseOKMethod into a byte slice
func (m *ChannelCloseOKMethod) Serialize() ([]byte, error) {
	// Channel.close-ok has no content
	return []byte{}, nil
}

// Helper functions for encoding common AMQP types
// Internal helper functions for backward compatibility
func encodeShortString(s string) []byte {
	return EncodeShortString(s)
}

func encodeFieldTable(fields map[string]interface{}) ([]byte, error) {
	return EncodeFieldTable(fields)
}

func EncodeShortString(s string) []byte {
	// Short strings are prefixed with a byte length
	if len(s) > 255 {
		// Truncate if longer than 255 characters
		s = s[:255]
	}
	result := make([]byte, 1+len(s))
	result[0] = byte(len(s))
	copy(result[1:], []byte(s))
	return result
}

func encodeLongString(s string) []byte {
	// Long strings are prefixed with a uint32 length
	result := make([]byte, 4+len(s))
	binary.BigEndian.PutUint32(result[0:4], uint32(len(s)))
	copy(result[4:], []byte(s))
	return result
}

func EncodeFieldTable(table map[string]interface{}) ([]byte, error) {
	var result []byte

	// First, encode the table to binary format
	// For simplicity in this initial implementation, we'll hardcode a basic server properties table
	// In a real implementation, we'd properly serialize the map

	// Calculate the total length of the table first
	tableBytes, err := serializeFieldTable(table)
	if err != nil {
		return nil, err
	}

	// Add the length prefix (uint32)
	result = make([]byte, 4+len(tableBytes))
	binary.BigEndian.PutUint32(result[0:4], uint32(len(tableBytes)))
	copy(result[4:], tableBytes)

	return result, nil
}

// serializeFieldTable serializes a field table
func serializeFieldTable(table map[string]interface{}) ([]byte, error) {
	var result []byte

	for key, value := range table {
		// Add key as short string
		keyBytes := encodeShortString(key)
		result = append(result, keyBytes...)

		// Add value type indicator and value
		// This is a simplified approach - in real implementation we'd handle all AMQP field types
		switch v := value.(type) {
		case string:
			result = append(result, 'S') // String type
			result = append(result, encodeLongString(v)...)
		case int:
			result = append(result, 'I') // Integer type
			intBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(intBytes, uint32(v))
			result = append(result, intBytes...)
		default:
			// For now, just handle string and integer types with a default string conversion
			result = append(result, 'S') // String type
			result = append(result, encodeLongString(fmt.Sprintf("%v", v))...)
		}
	}

	return result, nil
}

// decodeFieldTable deserializes a field table
func decodeFieldTable(data []byte, offset int) (map[string]interface{}, int, error) {
	if offset+4 > len(data) {
		return nil, offset, fmt.Errorf("field table length field missing")
	}

	tableLen := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	if offset+int(tableLen) > len(data) {
		return nil, offset, fmt.Errorf("field table extends beyond data")
	}

	tableEnd := offset + int(tableLen)
	table := make(map[string]interface{})

	for offset < tableEnd {
		// Read key as short string
		if offset >= len(data) {
			return nil, offset, fmt.Errorf("field key missing")
		}
		keyLen := int(data[offset])
		offset++
		if offset+keyLen > len(data) || offset+keyLen > tableEnd {
			return nil, offset, fmt.Errorf("field key extends beyond data")
		}
		key := string(data[offset : offset+keyLen])
		offset += keyLen

		// Read value type indicator
		if offset >= len(data) {
			return nil, offset, fmt.Errorf("field value type missing for key %s", key)
		}
		typeIndicator := data[offset]
		offset++

		// Read value based on type
		var value interface{}
		var err error
		switch typeIndicator {
		case 'S': // String type
			value, offset, err = decodeLongString(data, offset)
			if err != nil {
				return nil, offset, fmt.Errorf("failed to decode string value for key %s: %v", key, err)
			}
		case 'I': // Integer type
			if offset+4 > len(data) {
				return nil, offset, fmt.Errorf("integer value missing for key %s", key)
			}
			value = int(binary.BigEndian.Uint32(data[offset : offset+4]))
			offset += 4
		default:
			// For now, handle as string
			value, offset, err = decodeLongString(data, offset)
			if err != nil {
				return nil, offset, fmt.Errorf("failed to decode value for key %s: %v", key, err)
			}
		}

		table[key] = value
	}

	return table, offset, nil
}

// decodeShortString decodes a short string from data at the given offset
func decodeShortString(data []byte, offset int) (string, int, error) {
	if offset >= len(data) {
		return "", offset, fmt.Errorf("short string length byte missing")
	}

	strLen := int(data[offset])
	offset++

	if offset+strLen > len(data) {
		return "", offset, fmt.Errorf("short string extends beyond data")
	}

	str := string(data[offset : offset+strLen])
	offset += strLen

	return str, offset, nil
}

// decodeLongString decodes a long string from data at the given offset
func decodeLongString(data []byte, offset int) (string, int, error) {
	if offset+4 > len(data) {
		return "", offset, fmt.Errorf("long string length field missing")
	}

	strLen := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	if offset+int(strLen) > len(data) {
		return "", offset, fmt.Errorf("long string extends beyond data")
	}

	str := string(data[offset : offset+int(strLen)])
	offset += int(strLen)

	return str, offset, nil
}

// ExchangeDeclareMethod represents the exchange.declare method
type ExchangeDeclareMethod struct {
	Reserved1  uint16
	Exchange   string
	Type       string
	Passive    bool
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Arguments  map[string]interface{}
}

// Serialize encodes the ExchangeDeclareMethod into a byte slice
func (m *ExchangeDeclareMethod) Serialize() ([]byte, error) {
	var result []byte

	// Reserved1 (uint16)
	reservedBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(reservedBytes, m.Reserved1)
	result = append(result, reservedBytes...)

	// Exchange (short string)
	exchangeBytes := encodeShortString(m.Exchange)
	result = append(result, exchangeBytes...)

	// Type (short string)
	typeBytes := encodeShortString(m.Type)
	result = append(result, typeBytes...)

	// Now we need to pack the flags together:
	// bits: 0-6: unused (set to 0)
	// bit 7: passive
	// bit 8: durable
	// bit 9: auto-delete
	// bit 10: internal
	// bit 11: no-wait
	// bits 12-15: unused (set to 0)
	flags := uint16(0)
	if m.Passive {
		flags |= (1 << 7) // bit 7
	}
	if m.Durable {
		flags |= (1 << 8) // bit 8
	}
	if m.AutoDelete {
		flags |= (1 << 9) // bit 9
	}
	if m.Internal {
		flags |= (1 << 10) // bit 10
	}
	if m.NoWait {
		flags |= (1 << 11) // bit 11
	}

	flagBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(flagBytes, flags)
	result = append(result, flagBytes...)

	// Arguments (field table)
	argsBytes, err := encodeFieldTable(m.Arguments)
	if err != nil {
		return nil, err
	}
	result = append(result, argsBytes...)

	return result, nil
}

// Deserialize decodes the ExchangeDeclareMethod from a byte slice
func (m *ExchangeDeclareMethod) Deserialize(data []byte) error {
	if len(data) < 4 {
		return fmt.Errorf("exchange.declare method data too short")
	}

	offset := 0

	// Reserved1 (uint16)
	m.Reserved1 = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Exchange (short string)
	if offset >= len(data) {
		return fmt.Errorf("exchange field missing")
	}
	exchange, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return fmt.Errorf("failed to decode exchange name: %v", err)
	}
	m.Exchange = exchange
	offset = newOffset

	// Type (short string)
	if offset >= len(data) {
		return fmt.Errorf("type field missing")
	}
	exchangeType, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return fmt.Errorf("failed to decode exchange type: %v", err)
	}
	m.Type = exchangeType
	offset = newOffset

	// Flags (uint16)
	if offset+2 > len(data) {
		return fmt.Errorf("flags field missing")
	}
	flags := binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Extract flags
	m.Passive = (flags & (1 << 7)) != 0
	m.Durable = (flags & (1 << 8)) != 0
	m.AutoDelete = (flags & (1 << 9)) != 0
	m.Internal = (flags & (1 << 10)) != 0
	m.NoWait = (flags & (1 << 11)) != 0

	// Arguments (field table)
	if offset+4 <= len(data) { // Need at least 4 bytes for the field table length
		var err error
		m.Arguments, offset, err = decodeFieldTable(data, offset)
		if err != nil {
			return err
		}
	} else {
		// If not enough data for field table, initialize empty arguments map
		m.Arguments = make(map[string]interface{})
	}

	return nil
}

// ExchangeDeclareOKMethod represents the exchange.declare-ok method
type ExchangeDeclareOKMethod struct {
}

// Serialize encodes the ExchangeDeclareOKMethod into a byte slice
func (m *ExchangeDeclareOKMethod) Serialize() ([]byte, error) {
	// exchange.declare-ok has no content
	return []byte{}, nil
}

// ExchangeDeleteMethod represents the exchange.delete method
type ExchangeDeleteMethod struct {
	Reserved1 uint16
	Exchange  string
	IfUnused  bool
	NoWait    bool
}

// Serialize encodes the ExchangeDeleteMethod into a byte slice
func (m *ExchangeDeleteMethod) Serialize() ([]byte, error) {
	var result []byte

	// Reserved1 (uint16)
	reservedBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(reservedBytes, m.Reserved1)
	result = append(result, reservedBytes...)

	// Exchange (short string)
	exchangeBytes := encodeShortString(m.Exchange)
	result = append(result, exchangeBytes...)

	// Now we need to pack the flags together:
	// bits: 0-10: unused (set to 0)
	// bit 11: if-unused
	// bit 12: no-wait
	// bits 13-15: unused (set to 0)
	flags := uint16(0)
	if m.IfUnused {
		flags |= (1 << 11) // bit 11
	}
	if m.NoWait {
		flags |= (1 << 12) // bit 12
	}

	flagBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(flagBytes, flags)
	result = append(result, flagBytes...)

	return result, nil
}

// Deserialize decodes the ExchangeDeleteMethod from a byte slice
func (m *ExchangeDeleteMethod) Deserialize(data []byte) error {
	if len(data) < 4 { // Need at least reserved(2) + exchange length byte(1)
		return fmt.Errorf("exchange.delete method data too short")
	}

	offset := 0

	// Reserved1 (uint16)
	m.Reserved1 = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Exchange (short string)
	if offset >= len(data) {
		return fmt.Errorf("exchange field missing")
	}
	exchange, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return fmt.Errorf("failed to decode exchange name: %v", err)
	}
	m.Exchange = exchange
	offset = newOffset

	// Flags (uint16)
	if offset+2 > len(data) {
		// If not enough bytes for flags, use defaults (false for both)
		m.IfUnused = false
		m.NoWait = false
		return nil
	}
	flags := binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Extract flags
	m.IfUnused = (flags & (1 << 11)) != 0
	m.NoWait = (flags & (1 << 12)) != 0

	return nil
}

// ExchangeDeleteOKMethod represents the exchange.delete-ok method
type ExchangeDeleteOKMethod struct {
}

// Serialize encodes the ExchangeDeleteOKMethod into a byte slice
func (m *ExchangeDeleteOKMethod) Serialize() ([]byte, error) {
	// exchange.delete-ok has no content
	return []byte{}, nil
}

// ExchangeUnbindOKMethod represents the exchange.unbind-ok method
type ExchangeUnbindOKMethod struct {
}

// Serialize encodes the ExchangeUnbindOKMethod into a byte slice
func (m *ExchangeUnbindOKMethod) Serialize() ([]byte, error) {
	// exchange.unbind-ok has no content
	return []byte{}, nil
}

// QueueDeclareMethod represents the queue.declare method
type QueueDeclareMethod struct {
	Reserved1  uint16
	Queue      string
	Passive    bool
	Durable    bool
	Exclusive  bool
	AutoDelete bool
	NoWait     bool
	Arguments  map[string]interface{}
}

// Serialize encodes the QueueDeclareMethod into a byte slice
func (m *QueueDeclareMethod) Serialize() ([]byte, error) {
	var result []byte

	// Reserved1 (uint16)
	reservedBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(reservedBytes, m.Reserved1)
	result = append(result, reservedBytes...)

	// Queue (short string)
	queueBytes := encodeShortString(m.Queue)
	result = append(result, queueBytes...)

	// Now we need to pack the flags together:
	// bits: 0-6: unused (set to 0)
	// bit 7: passive
	// bit 8: durable
	// bit 9: exclusive
	// bit 10: auto-delete
	// bit 11: no-wait
	// bits 12-15: unused (set to 0)
	flags := uint16(0)
	if m.Passive {
		flags |= (1 << 7) // bit 7
	}
	if m.Durable {
		flags |= (1 << 8) // bit 8
	}
	if m.Exclusive {
		flags |= (1 << 9) // bit 9
	}
	if m.AutoDelete {
		flags |= (1 << 10) // bit 10
	}
	if m.NoWait {
		flags |= (1 << 11) // bit 11
	}

	flagBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(flagBytes, flags)
	result = append(result, flagBytes...)

	// Arguments (field table)
	argsBytes, err := encodeFieldTable(m.Arguments)
	if err != nil {
		return nil, err
	}
	result = append(result, argsBytes...)

	return result, nil
}

// Deserialize decodes the QueueDeclareMethod from a byte slice
func (m *QueueDeclareMethod) Deserialize(data []byte) error {
	if len(data) < 4 {
		return fmt.Errorf("queue.declare method data too short")
	}

	offset := 0

	// Reserved1 (uint16)
	m.Reserved1 = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Queue (short string)
	if offset >= len(data) {
		return fmt.Errorf("queue field missing")
	}
	queue, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return fmt.Errorf("failed to decode queue name: %v", err)
	}
	m.Queue = queue
	offset = newOffset

	// Flags (uint16)
	if offset+2 > len(data) {
		return fmt.Errorf("flags field missing")
	}
	flags := binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Extract flags
	m.Passive = (flags & (1 << 7)) != 0
	m.Durable = (flags & (1 << 8)) != 0
	m.Exclusive = (flags & (1 << 9)) != 0
	m.AutoDelete = (flags & (1 << 10)) != 0
	m.NoWait = (flags & (1 << 11)) != 0

	// Arguments (field table)
	if offset+4 <= len(data) { // Need at least 4 bytes for the field table length
		var err error
		m.Arguments, offset, err = decodeFieldTable(data, offset)
		if err != nil {
			return err
		}
	} else {
		// If not enough data for field table, initialize empty arguments map
		m.Arguments = make(map[string]interface{})
	}

	return nil
}

// QueueDeclareOKMethod represents the queue.declare-ok method
type QueueDeclareOKMethod struct {
	Queue         string
	MessageCount  uint32
	ConsumerCount uint32
}

// Serialize encodes the QueueDeclareOKMethod into a byte slice
func (m *QueueDeclareOKMethod) Serialize() ([]byte, error) {
	var result []byte

	// Queue (short string)
	queueBytes := encodeShortString(m.Queue)
	result = append(result, queueBytes...)

	// Message count (uint32)
	msgCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(msgCountBytes, m.MessageCount)
	result = append(result, msgCountBytes...)

	// Consumer count (uint32)
	consumerCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(consumerCountBytes, m.ConsumerCount)
	result = append(result, consumerCountBytes...)

	return result, nil
}

// QueueBindMethod represents the queue.bind method
type QueueBindMethod struct {
	Reserved1  uint16
	Queue      string
	Exchange   string
	RoutingKey string
	NoWait     bool
	Arguments  map[string]interface{}
}

// Serialize encodes the QueueBindMethod into a byte slice
func (m *QueueBindMethod) Serialize() ([]byte, error) {
	var result []byte

	// Reserved1 (uint16)
	reservedBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(reservedBytes, m.Reserved1)
	result = append(result, reservedBytes...)

	// Queue (short string)
	queueBytes := encodeShortString(m.Queue)
	result = append(result, queueBytes...)

	// Exchange (short string)
	exchangeBytes := encodeShortString(m.Exchange)
	result = append(result, exchangeBytes...)

	// Routing key (short string)
	routingKeyBytes := encodeShortString(m.RoutingKey)
	result = append(result, routingKeyBytes...)

	// NoWait (bit packed into flags)
	// bit 0: no-wait
	// bits 1-15: unused (set to 0)
	flags := uint16(0)
	if m.NoWait {
		flags |= (1 << 0) // bit 0
	}

	flagBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(flagBytes, flags)
	result = append(result, flagBytes...)

	// Arguments (field table)
	argsBytes, err := encodeFieldTable(m.Arguments)
	if err != nil {
		return nil, err
	}
	result = append(result, argsBytes...)

	return result, nil
}

// Deserialize decodes the QueueBindMethod from a byte slice
func (m *QueueBindMethod) Deserialize(data []byte) error {
	if len(data) < 4 {
		return fmt.Errorf("queue.bind method data too short")
	}

	offset := 0

	// Reserved1 (uint16)
	m.Reserved1 = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Queue (short string)
	if offset >= len(data) {
		return fmt.Errorf("queue field missing")
	}
	queue, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return fmt.Errorf("failed to decode queue name: %v", err)
	}
	m.Queue = queue
	offset = newOffset

	// Exchange (short string)
	if offset >= len(data) {
		return fmt.Errorf("exchange field missing")
	}
	exchange, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return fmt.Errorf("failed to decode exchange name: %v", err)
	}
	m.Exchange = exchange
	offset = newOffset

	// Routing key (short string)
	if offset >= len(data) {
		return fmt.Errorf("routing key field missing")
	}
	routingKey, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return fmt.Errorf("failed to decode routing key: %v", err)
	}
	m.RoutingKey = routingKey
	offset = newOffset

	// Flags (uint16)
	if offset+2 > len(data) {
		return fmt.Errorf("flags field missing")
	}
	flags := binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Extract flags
	m.NoWait = (flags & (1 << 0)) != 0

	// Arguments (field table)
	if offset+4 <= len(data) { // Need at least 4 bytes for the field table length
		var err error
		m.Arguments, offset, err = decodeFieldTable(data, offset)
		if err != nil {
			return err
		}
	} else {
		// If not enough data for field table, initialize empty arguments map
		m.Arguments = make(map[string]interface{})
	}

	return nil
}

// QueueBindOKMethod represents the queue.bind-ok method
type QueueBindOKMethod struct {
}

// Serialize encodes the QueueBindOKMethod into a byte slice
func (m *QueueBindOKMethod) Serialize() ([]byte, error) {
	// queue.bind-ok has no content
	return []byte{}, nil
}

// QueueDeleteMethod represents the queue.delete method
type QueueDeleteMethod struct {
	Reserved1 uint16
	Queue     string
	IfUnused  bool
	IfEmpty   bool
	NoWait    bool
}

// Serialize encodes the QueueDeleteMethod into a byte slice
func (m *QueueDeleteMethod) Serialize() ([]byte, error) {
	var result []byte

	// Reserved1 (uint16)
	reservedBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(reservedBytes, m.Reserved1)
	result = append(result, reservedBytes...)

	// Queue (short string)
	queueBytes := encodeShortString(m.Queue)
	result = append(result, queueBytes...)

	// Now we need to pack the flags together:
	// bits: 0-9: unused (set to 0)
	// bit 10: if-unused
	// bit 11: if-empty
	// bit 12: no-wait
	// bits 13-15: unused (set to 0)
	flags := uint16(0)
	if m.IfUnused {
		flags |= (1 << 10) // bit 10
	}
	if m.IfEmpty {
		flags |= (1 << 11) // bit 11
	}
	if m.NoWait {
		flags |= (1 << 12) // bit 12
	}

	flagBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(flagBytes, flags)
	result = append(result, flagBytes...)

	return result, nil
}

// Deserialize decodes the QueueDeleteMethod from a byte slice
func (m *QueueDeleteMethod) Deserialize(data []byte) error {
	if len(data) < 3 { // Need at least reserved(2) + queue length byte(1)
		return fmt.Errorf("queue.delete method data too short")
	}

	offset := 0

	// Reserved1 (uint16)
	m.Reserved1 = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Queue (short string)
	if offset >= len(data) {
		return fmt.Errorf("queue field missing")
	}
	queue, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return fmt.Errorf("failed to decode queue name: %v", err)
	}
	m.Queue = queue
	offset = newOffset

	// Flags (uint16)
	if offset+2 > len(data) {
		// If not enough bytes for flags, use defaults (false for all)
		m.IfUnused = false
		m.IfEmpty = false
		m.NoWait = false
		return nil
	}
	flags := binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Extract flags
	m.IfUnused = (flags & (1 << 10)) != 0
	m.IfEmpty = (flags & (1 << 11)) != 0
	m.NoWait = (flags & (1 << 12)) != 0

	return nil
}

// QueueDeleteOKMethod represents the queue.delete-ok method
type QueueDeleteOKMethod struct {
	MessageCount uint32
}

// Serialize encodes the QueueDeleteOKMethod into a byte slice
func (m *QueueDeleteOKMethod) Serialize() ([]byte, error) {
	var result []byte

	// Message count (uint32)
	msgCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(msgCountBytes, m.MessageCount)
	result = append(result, msgCountBytes...)

	return result, nil
}

// BasicQosMethod represents the basic.qos method
type BasicQosMethod struct {
	PrefetchSize  uint32
	PrefetchCount uint16
	Global        bool
}

// Serialize encodes the BasicQosMethod into a byte slice
func (m *BasicQosMethod) Serialize() ([]byte, error) {
	var result []byte

	// Prefetch size (uint32)
	prefetchSizeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(prefetchSizeBytes, m.PrefetchSize)
	result = append(result, prefetchSizeBytes...)

	// Prefetch count (uint16)
	prefetchCountBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(prefetchCountBytes, m.PrefetchCount)
	result = append(result, prefetchCountBytes...)

	// Global (bit packed)
	var globalByte byte
	if m.Global {
		globalByte = 1
	} else {
		globalByte = 0
	}
	result = append(result, globalByte)

	return result, nil
}

// Deserialize decodes the BasicQosMethod from a byte slice
func (m *BasicQosMethod) Deserialize(data []byte) error {
	if len(data) < 7 { // 4 bytes + 2 bytes + 1 byte
		return fmt.Errorf("basic.qos method data too short")
	}

	offset := 0

	// Prefetch size (uint32)
	m.PrefetchSize = binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	// Prefetch count (uint16)
	m.PrefetchCount = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Global (bit)
	if offset < len(data) {
		m.Global = data[offset] != 0
	}

	return nil
}

// BasicQosOKMethod represents the basic.qos-ok method
type BasicQosOKMethod struct {
}

// Serialize encodes the BasicQosOKMethod into a byte slice
func (m *BasicQosOKMethod) Serialize() ([]byte, error) {
	return []byte{}, nil // basic.qos-ok has no content
}

// BasicPublishMethod represents the basic.publish method
type BasicPublishMethod struct {
	Reserved1  uint16
	Exchange   string
	RoutingKey string
	Mandatory  bool
	Immediate  bool
}

// Serialize encodes the BasicPublishMethod into a byte slice
func (m *BasicPublishMethod) Serialize() ([]byte, error) {
	var result []byte

	// Reserved1 (uint16)
	reservedBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(reservedBytes, m.Reserved1)
	result = append(result, reservedBytes...)

	// Exchange (short string)
	exchangeBytes := encodeShortString(m.Exchange)
	result = append(result, exchangeBytes...)

	// Routing key (short string)
	routingKeyBytes := encodeShortString(m.RoutingKey)
	result = append(result, routingKeyBytes...)

	// Flags (uint16)
	// bit 0: mandatory
	// bit 1: immediate
	// bits 2-15: unused
	flags := uint16(0)
	if m.Mandatory {
		flags |= (1 << 0)
	}
	if m.Immediate {
		flags |= (1 << 1)
	}

	flagBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(flagBytes, flags)
	result = append(result, flagBytes...)

	return result, nil
}

// Deserialize decodes the BasicPublishMethod from a byte slice
func (m *BasicPublishMethod) Deserialize(data []byte) error {
	if len(data) < 4 { // Need at least reserved(2) + exchange len byte(1)
		return fmt.Errorf("basic.publish method data too short")
	}

	offset := 0

	// Reserved1 (uint16)
	m.Reserved1 = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Exchange (short string)
	if offset >= len(data) {
		return fmt.Errorf("exchange field missing")
	}
	exchange, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return fmt.Errorf("failed to decode exchange name: %v", err)
	}
	m.Exchange = exchange
	offset = newOffset

	// Routing key (short string)
	if offset >= len(data) {
		return fmt.Errorf("routing key field missing")
	}
	routingKey, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return fmt.Errorf("failed to decode routing key: %v", err)
	}
	m.RoutingKey = routingKey
	offset = newOffset

	// Flags (uint16)
	if offset+2 > len(data) {
		// If not enough bytes for flags, use defaults (false for both)
		m.Mandatory = false
		m.Immediate = false
		return nil
	}
	flags := binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Extract flags
	m.Mandatory = (flags & (1 << 0)) != 0
	m.Immediate = (flags & (1 << 1)) != 0

	return nil
}

// BasicPublishOKMethod represents the basic.publish-ok method (only used in some modes)
type BasicPublishOKMethod struct {
}

// Serialize encodes the BasicPublishOKMethod into a byte slice
func (m *BasicPublishOKMethod) Serialize() ([]byte, error) {
	return []byte{}, nil // basic.publish-ok has no content (not always sent)
}

// BasicConsumeMethod represents the basic.consume method
type BasicConsumeMethod struct {
	Reserved1   uint16
	Queue       string
	ConsumerTag string
	NoLocal     bool
	NoAck       bool
	Exclusive   bool
	NoWait      bool
	Arguments   map[string]interface{}
}

// Serialize encodes the BasicConsumeMethod into a byte slice
func (m *BasicConsumeMethod) Serialize() ([]byte, error) {
	var result []byte

	// Reserved1 (uint16)
	reservedBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(reservedBytes, m.Reserved1)
	result = append(result, reservedBytes...)

	// Queue (short string)
	queueBytes := encodeShortString(m.Queue)
	result = append(result, queueBytes...)

	// Consumer tag (short string)
	consumerTagBytes := encodeShortString(m.ConsumerTag)
	result = append(result, consumerTagBytes...)

	// Flags (uint16)
	// bit 0: no-local
	// bit 1: no-ack
	// bit 2: exclusive
	// bit 3: no-wait
	// bits 4-15: unused
	flags := uint16(0)
	if m.NoLocal {
		flags |= (1 << 0)
	}
	if m.NoAck {
		flags |= (1 << 1)
	}
	if m.Exclusive {
		flags |= (1 << 2)
	}
	if m.NoWait {
		flags |= (1 << 3)
	}

	flagBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(flagBytes, flags)
	result = append(result, flagBytes...)

	// Arguments (field table)
	argsBytes, err := encodeFieldTable(m.Arguments)
	if err != nil {
		return nil, err
	}
	result = append(result, argsBytes...)

	return result, nil
}

// Deserialize decodes the BasicConsumeMethod from a byte slice
func (m *BasicConsumeMethod) Deserialize(data []byte) error {
	if len(data) < 4 { // Need at least reserved(2) + queue len byte(1)
		return fmt.Errorf("basic.consume method data too short")
	}

	offset := 0

	// Reserved1 (uint16)
	m.Reserved1 = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Queue (short string)
	if offset >= len(data) {
		return fmt.Errorf("queue field missing")
	}
	queue, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return fmt.Errorf("failed to decode queue name: %v", err)
	}
	m.Queue = queue
	offset = newOffset

	// Consumer tag (short string)
	if offset >= len(data) {
		// If no consumer tag provided, generate one
		m.ConsumerTag = fmt.Sprintf("ctag-%s-%d", generateID()[:8], offset)
	} else {
		consumerTag, newOffset, err := decodeShortString(data, offset)
		if err != nil {
			return fmt.Errorf("failed to decode consumer tag: %v", err)
		}
		m.ConsumerTag = consumerTag
		offset = newOffset
	}

	// Flags (uint16)
	if offset+2 > len(data) {
		// If not enough bytes for flags, use defaults
		m.NoLocal = false
		m.NoAck = false
		m.Exclusive = false
		m.NoWait = false
		// Arguments are handled below
	} else {
		flags := binary.BigEndian.Uint16(data[offset : offset+2])
		offset += 2

		// Extract flags
		m.NoLocal = (flags & (1 << 0)) != 0
		m.NoAck = (flags & (1 << 1)) != 0
		m.Exclusive = (flags & (1 << 2)) != 0
		m.NoWait = (flags & (1 << 3)) != 0
	}

	// Arguments (field table)
	if offset+4 <= len(data) { // Need at least 4 bytes for the field table length
		var err error
		m.Arguments, offset, err = decodeFieldTable(data, offset)
		if err != nil {
			return err
		}
	} else {
		// If not enough data for field table, initialize empty arguments map
		m.Arguments = make(map[string]interface{})
	}

	return nil
}

// BasicConsumeOKMethod represents the basic.consume-ok method
type BasicConsumeOKMethod struct {
	ConsumerTag string
}

// Serialize encodes the BasicConsumeOKMethod into a byte slice
func (m *BasicConsumeOKMethod) Serialize() ([]byte, error) {
	var result []byte

	// Consumer tag (short string)
	consumerTagBytes := encodeShortString(m.ConsumerTag)
	result = append(result, consumerTagBytes...)

	return result, nil
}

// BasicCancelMethod represents the basic.cancel method
type BasicCancelMethod struct {
	ConsumerTag string
	NoWait      bool
}

// Serialize encodes the BasicCancelMethod into a byte slice
func (m *BasicCancelMethod) Serialize() ([]byte, error) {
	var result []byte

	// Consumer tag (short string)
	consumerTagBytes := encodeShortString(m.ConsumerTag)
	result = append(result, consumerTagBytes...)

	// NoWait (packed into flags)
	// bit 0: no-wait
	// bits 1-15: unused
	flags := uint16(0)
	if m.NoWait {
		flags |= (1 << 0)
	}

	flagBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(flagBytes, flags)
	result = append(result, flagBytes...)

	return result, nil
}

// Deserialize decodes the BasicCancelMethod from a byte slice
func (m *BasicCancelMethod) Deserialize(data []byte) error {
	if len(data) < 1 { // Need at least consumer tag length byte
		return fmt.Errorf("basic.cancel method data too short")
	}

	offset := 0

	// Consumer tag (short string)
	if offset >= len(data) {
		return fmt.Errorf("consumer tag field missing")
	}
	consumerTag, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return fmt.Errorf("failed to decode consumer tag: %v", err)
	}
	m.ConsumerTag = consumerTag
	offset = newOffset

	// Flags (uint16)
	if offset+2 <= len(data) {
		flags := binary.BigEndian.Uint16(data[offset : offset+2])
		offset += 2

		// Extract flags
		m.NoWait = (flags & (1 << 0)) != 0
	} else {
		// Default to false if flags not provided
		m.NoWait = false
	}

	return nil
}

// BasicCancelOKMethod represents the basic.cancel-ok method
type BasicCancelOKMethod struct {
	ConsumerTag string
}

// Serialize encodes the BasicCancelOKMethod into a byte slice
func (m *BasicCancelOKMethod) Serialize() ([]byte, error) {
	var result []byte

	// Consumer tag (short string)
	consumerTagBytes := encodeShortString(m.ConsumerTag)
	result = append(result, consumerTagBytes...)

	return result, nil
}

// BasicGetMethod represents the basic.get method
type BasicGetMethod struct {
	Reserved1 uint16
	Queue     string
	NoAck     bool
}

// Serialize encodes the BasicGetMethod into a byte slice
func (m *BasicGetMethod) Serialize() ([]byte, error) {
	var result []byte

	// Reserved1 (uint16)
	reservedBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(reservedBytes, m.Reserved1)
	result = append(result, reservedBytes...)

	// Queue (short string)
	queueBytes := encodeShortString(m.Queue)
	result = append(result, queueBytes...)

	// NoAck (packed into flags)
	// bit 0: no-ack
	// bits 1-15: unused
	flags := uint16(0)
	if m.NoAck {
		flags |= (1 << 0)
	}

	flagBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(flagBytes, flags)
	result = append(result, flagBytes...)

	return result, nil
}

// Deserialize decodes the BasicGetMethod from a byte slice
func (m *BasicGetMethod) Deserialize(data []byte) error {
	if len(data) < 3 { // Need at least reserved(2) + queue length byte(1)
		return fmt.Errorf("basic.get method data too short")
	}

	offset := 0

	// Reserved1 (uint16)
	m.Reserved1 = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Queue (short string)
	if offset >= len(data) {
		return fmt.Errorf("queue field missing")
	}
	queue, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return fmt.Errorf("failed to decode queue name: %v", err)
	}
	m.Queue = queue
	offset = newOffset

	// Flags (uint16)
	if offset+2 <= len(data) {
		flags := binary.BigEndian.Uint16(data[offset : offset+2])
		offset += 2

		// Extract flags
		m.NoAck = (flags & (1 << 0)) != 0
	} else {
		// Default to false if flags not provided
		m.NoAck = false
	}

	return nil
}

// BasicGetOKMethod represents the basic.get-ok method
type BasicGetOKMethod struct {
	DeliveryTag  uint64
	Redelivered  bool
	Exchange     string
	RoutingKey   string
	MessageCount uint32
}

// Serialize encodes the BasicGetOKMethod into a byte slice
func (m *BasicGetOKMethod) Serialize() ([]byte, error) {
	var result []byte

	// Delivery tag (long long integer - 64-bit)
	deliveryTagBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(deliveryTagBytes, m.DeliveryTag)
	result = append(result, deliveryTagBytes...)

	// Redelivered (bit packed)
	var redeliveredByte byte
	if m.Redelivered {
		redeliveredByte = 1
	} else {
		redeliveredByte = 0
	}
	result = append(result, redeliveredByte)

	// Exchange (short string)
	exchangeBytes := encodeShortString(m.Exchange)
	result = append(result, exchangeBytes...)

	// Routing key (short string)
	routingKeyBytes := encodeShortString(m.RoutingKey)
	result = append(result, routingKeyBytes...)

	// Message count (long integer - 32-bit)
	messageCountBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(messageCountBytes, m.MessageCount)
	result = append(result, messageCountBytes...)

	return result, nil
}

// BasicGetEmptyMethod represents the basic.get-empty method
type BasicGetEmptyMethod struct {
	Reserved1 string // Should be "amq.empty" or similar
}

// Serialize encodes the BasicGetEmptyMethod into a byte slice
func (m *BasicGetEmptyMethod) Serialize() ([]byte, error) {
	var result []byte

	// Reserved1 (short string)
	reservedBytes := encodeShortString(m.Reserved1)
	result = append(result, reservedBytes...)

	return result, nil
}

// BasicDeliverOKMethod represents the basic.deliver-ok method (no content)
type BasicDeliverOKMethod struct {
}

// Serialize encodes the BasicDeliverOKMethod into a byte slice
func (m *BasicDeliverOKMethod) Serialize() ([]byte, error) {
	// basic.deliver-ok has no content
	return []byte{}, nil
}

// BasicAckMethod represents the basic.ack method
type BasicAckMethod struct {
	DeliveryTag uint64
	Multiple    bool
}

// Serialize encodes the BasicAckMethod into a byte slice
func (m *BasicAckMethod) Serialize() ([]byte, error) {
	var result []byte

	// Delivery tag (long long integer - 64-bit)
	deliveryTagBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(deliveryTagBytes, m.DeliveryTag)
	result = append(result, deliveryTagBytes...)

	// Multiple (packed into flags)
	// bit 0: multiple
	// bits 1-15: unused
	flags := uint16(0)
	if m.Multiple {
		flags |= (1 << 0)
	}

	flagBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(flagBytes, flags)
	result = append(result, flagBytes...)

	return result, nil
}

// Deserialize decodes the BasicAckMethod from a byte slice
func (m *BasicAckMethod) Deserialize(data []byte) error {
	if len(data) < 8 { // Need at least delivery tag (8 bytes)
		return fmt.Errorf("basic.ack method data too short")
	}

	offset := 0

	// Delivery tag (long long integer - 64-bit)
	m.DeliveryTag = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8

	// Flags (uint16)
	if offset+2 <= len(data) {
		flags := binary.BigEndian.Uint16(data[offset : offset+2])
		offset += 2

		// Extract flags
		m.Multiple = (flags & (1 << 0)) != 0
	} else {
		// Default to false if flags not provided
		m.Multiple = false
	}

	return nil
}

// BasicRejectMethod represents the basic.reject method
type BasicRejectMethod struct {
	DeliveryTag uint64
	Requeue     bool
}

// Serialize encodes the BasicRejectMethod into a byte slice
func (m *BasicRejectMethod) Serialize() ([]byte, error) {
	var result []byte

	// Delivery tag (long long integer - 64-bit)
	deliveryTagBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(deliveryTagBytes, m.DeliveryTag)
	result = append(result, deliveryTagBytes...)

	// Requeue (packed into flags)
	// bit 0: requeue
	// bits 1-15: unused
	flags := uint16(0)
	if m.Requeue {
		flags |= (1 << 0)
	}

	flagBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(flagBytes, flags)
	result = append(result, flagBytes...)

	return result, nil
}

// Deserialize decodes the BasicRejectMethod from a byte slice
func (m *BasicRejectMethod) Deserialize(data []byte) error {
	if len(data) < 8 { // Need at least delivery tag (8 bytes)
		return fmt.Errorf("basic.reject method data too short")
	}

	offset := 0

	// Delivery tag (long long integer - 64-bit)
	m.DeliveryTag = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8

	// Flags (uint16)
	if offset+2 <= len(data) {
		flags := binary.BigEndian.Uint16(data[offset : offset+2])
		offset += 2

		// Extract flags
		m.Requeue = (flags & (1 << 0)) != 0
	} else {
		// Default to true if flags not provided (requeue by default)
		m.Requeue = true
	}

	return nil
}

// BasicNackMethod represents the basic.nack method
type BasicNackMethod struct {
	DeliveryTag uint64
	Multiple    bool
	Requeue     bool
}

// Serialize encodes the BasicNackMethod into a byte slice
func (m *BasicNackMethod) Serialize() ([]byte, error) {
	var result []byte

	// Delivery tag (long long integer - 64-bit)
	deliveryTagBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(deliveryTagBytes, m.DeliveryTag)
	result = append(result, deliveryTagBytes...)

	// Flags (uint16)
	// bit 0: multiple
	// bit 1: requeue
	// bits 2-15: unused
	flags := uint16(0)
	if m.Multiple {
		flags |= (1 << 0)
	}
	if m.Requeue {
		flags |= (1 << 1)
	}

	flagBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(flagBytes, flags)
	result = append(result, flagBytes...)

	return result, nil
}

// Deserialize decodes the BasicNackMethod from a byte slice
func (m *BasicNackMethod) Deserialize(data []byte) error {
	if len(data) < 8 { // Need at least delivery tag (8 bytes)
		return fmt.Errorf("basic.nack method data too short")
	}

	offset := 0

	// Delivery tag (long long integer - 64-bit)
	m.DeliveryTag = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8

	// Flags (uint16)
	if offset+2 <= len(data) {
		flags := binary.BigEndian.Uint16(data[offset : offset+2])
		offset += 2

		// Extract flags
		m.Multiple = (flags & (1 << 0)) != 0
		m.Requeue = (flags & (1 << 1)) != 0
	} else {
		// Default values if flags not provided
		m.Multiple = false
		m.Requeue = true // Requeue by default
	}

	return nil
}

// BasicDeliverMethod represents the basic.deliver method
type BasicDeliverMethod struct {
	ConsumerTag string
	DeliveryTag uint64
	Redelivered bool
	Exchange    string
	RoutingKey  string
}

// Serialize encodes the BasicDeliverMethod into a byte slice
func (m *BasicDeliverMethod) Serialize() ([]byte, error) {
	var result []byte

	// Consumer tag (short string)
	consumerTagBytes := encodeShortString(m.ConsumerTag)
	result = append(result, consumerTagBytes...)

	// Delivery tag (long long integer - 64-bit)
	deliveryTagBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(deliveryTagBytes, m.DeliveryTag)
	result = append(result, deliveryTagBytes...)

	// Redelivered (bit field - single byte with bit 0 set if redelivered)
	// AMQP 0.9.1 spec: redelivered is a single bit field, not a 16-bit flags field
	var redeliveredByte byte
	if m.Redelivered {
		redeliveredByte = 1
	} else {
		redeliveredByte = 0
	}
	result = append(result, redeliveredByte)

	// Exchange (short string)
	exchangeBytes := encodeShortString(m.Exchange)
	result = append(result, exchangeBytes...)

	// Routing key (short string)
	routingKeyBytes := encodeShortString(m.RoutingKey)
	result = append(result, routingKeyBytes...)

	return result, nil
}

// Deserialize decodes the BasicDeliverMethod from a byte slice
func (m *BasicDeliverMethod) Deserialize(data []byte) error {
	if len(data) < 1 { // Need at least consumer tag length byte
		return fmt.Errorf("basic.deliver method data too short")
	}

	offset := 0

	// Consumer tag (short string)
	if offset >= len(data) {
		return fmt.Errorf("consumer tag field missing")
	}
	consumerTag, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return fmt.Errorf("failed to decode consumer tag: %v", err)
	}
	m.ConsumerTag = consumerTag
	offset = newOffset

	// Delivery tag (long long integer - 64-bit)
	if offset+8 > len(data) {
		return fmt.Errorf("delivery tag field missing")
	}
	m.DeliveryTag = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8

	// Redelivered (single bit field)
	if offset >= len(data) {
		// If not enough bytes, use default (false for redelivered)
		m.Redelivered = false
	} else {
		m.Redelivered = data[offset] != 0
		offset++
	}

	// Exchange (short string)
	if offset >= len(data) {
		return fmt.Errorf("exchange field missing")
	}
	exchange, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return fmt.Errorf("failed to decode exchange name: %v", err)
	}
	m.Exchange = exchange
	offset = newOffset

	// Routing key (short string)
	if offset >= len(data) {
		return fmt.Errorf("routing key field missing")
	}
	routingKey, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return fmt.Errorf("failed to decode routing key: %v", err)
	}
	m.RoutingKey = routingKey
	offset = newOffset

	return nil
}

// ExchangeUnbindMethod represents the exchange.unbind method
type ExchangeUnbindMethod struct {
	Reserved1   uint16
	Destination string
	Source      string
	RoutingKey  string
	NoWait      bool
	Arguments   map[string]interface{}
}

// Serialize encodes the ExchangeUnbindMethod into a byte slice
func (m *ExchangeUnbindMethod) Serialize() ([]byte, error) {
	var result []byte

	// Reserved1 (uint16)
	reservedBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(reservedBytes, m.Reserved1)
	result = append(result, reservedBytes...)

	// Destination (short string)
	destBytes := encodeShortString(m.Destination)
	result = append(result, destBytes...)

	// Source (short string)
	sourceBytes := encodeShortString(m.Source)
	result = append(result, sourceBytes...)

	// Routing key (short string)
	routingKeyBytes := encodeShortString(m.RoutingKey)
	result = append(result, routingKeyBytes...)

	// Flags (packed into uint16)
	// bit 0: no-wait
	// bits 1-15: unused (set to 0)
	flags := uint16(0)
	if m.NoWait {
		flags |= (1 << 0) // bit 0
	}

	flagBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(flagBytes, flags)
	result = append(result, flagBytes...)

	// Arguments (field table)
	argsBytes, err := encodeFieldTable(m.Arguments)
	if err != nil {
		return nil, err
	}
	result = append(result, argsBytes...)

	return result, nil
}

// Deserialize decodes the ExchangeUnbindMethod from a byte slice
func (m *ExchangeUnbindMethod) Deserialize(data []byte) error {
	if len(data) < 4 { // Need at least reserved(2) + destination length byte(1)
		return fmt.Errorf("exchange.unbind method data too short")
	}

	offset := 0

	// Reserved1 (uint16)
	m.Reserved1 = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Destination (short string)
	if offset >= len(data) {
		return fmt.Errorf("destination field missing")
	}
	dest, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return fmt.Errorf("failed to decode destination: %v", err)
	}
	m.Destination = dest
	offset = newOffset

	// Source (short string)
	if offset >= len(data) {
		return fmt.Errorf("source field missing")
	}
	source, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return fmt.Errorf("failed to decode source: %v", err)
	}
	m.Source = source
	offset = newOffset

	// Routing key (short string)
	if offset >= len(data) {
		return fmt.Errorf("routing key field missing")
	}
	routingKey, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return fmt.Errorf("failed to decode routing key: %v", err)
	}
	m.RoutingKey = routingKey
	offset = newOffset

	// Flags (uint16)
	if offset+2 > len(data) {
		// If not enough bytes for flags, use defaults (false for no-wait)
		m.NoWait = false
		return nil
	}
	flags := binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Extract flags
	m.NoWait = (flags & (1 << 0)) != 0

	// Arguments (field table)
	if offset < len(data) {
		var err error
		m.Arguments, offset, err = decodeFieldTable(data, offset)
		if err != nil {
			return err
		}
	} else {
		// If no more data, initialize empty arguments map
		m.Arguments = make(map[string]interface{})
	}

	return nil
}

// encodeMethodFrame encodes a method into a method frame
func EncodeMethodFrame(classID, methodID uint16, methodData []byte) *Frame {
	// A method frame contains: class ID (2 bytes) + method ID (2 bytes) + method-specific content
	methodFrameContent := make([]byte, 4+len(methodData))
	binary.BigEndian.PutUint16(methodFrameContent[0:2], classID)
	binary.BigEndian.PutUint16(methodFrameContent[2:4], methodID)
	copy(methodFrameContent[4:], methodData)

	return &Frame{
		Type:    FrameMethod,
		Channel: 0, // For connection-level methods
		Size:    uint32(len(methodFrameContent)),
		Payload: methodFrameContent,
	}
}

// EncodeMethodFrameForChannel encodes a method into a method frame for a specific channel
func EncodeMethodFrameForChannel(channelID uint16, classID, methodID uint16, methodData []byte) *Frame {
	// A method frame contains: class ID (2 bytes) + method ID (2 bytes) + method-specific content
	methodFrameContent := make([]byte, 4+len(methodData))
	binary.BigEndian.PutUint16(methodFrameContent[0:2], classID)
	binary.BigEndian.PutUint16(methodFrameContent[2:4], methodID)
	copy(methodFrameContent[4:], methodData)

	return &Frame{
		Type:    FrameMethod,
		Channel: channelID, // For channel-level methods
		Size:    uint32(len(methodFrameContent)),
		Payload: methodFrameContent,
	}
}

// EncodeHeaderFrameForChannel encodes a content header into a header frame for a specific channel
func EncodeHeaderFrameForChannel(channelID uint16, classID uint16, headerData []byte) *Frame {
	// A content header frame contains: class ID (2 bytes) + weight (2 bytes) + body size (8 bytes) +
	// property flags (2 bytes) + properties (variable length based on property flags)

	// For now, we'll create a minimal header frame
	// In a real implementation, you'd have proper header data

	return &Frame{
		Type:    FrameHeader,
		Channel: channelID,
		Size:    uint32(len(headerData)),
		Payload: headerData,
	}
}

// EncodeContentHeaderFrameForChannel properly encodes a content header frame for a specific channel
func EncodeContentHeaderFrameForChannel(channelID uint16, classID, weight uint16, bodySize uint64, propertyFlags uint16, properties []byte) *Frame {
	// A content header frame contains:
	// - Class ID (2 bytes)
	// - Weight (2 bytes)
	// - Body size (8 bytes)
	// - Property flags (2 bytes)
	// - Properties (variable length)

	headerContent := make([]byte, 14+len(properties))

	// Class ID (2 bytes)
	binary.BigEndian.PutUint16(headerContent[0:2], classID)

	// Weight (2 bytes)
	binary.BigEndian.PutUint16(headerContent[2:4], weight)

	// Body size (8 bytes)
	binary.BigEndian.PutUint64(headerContent[4:12], bodySize)

	// Property flags (2 bytes)
	binary.BigEndian.PutUint16(headerContent[12:14], propertyFlags)

	// Properties (variable length)
	copy(headerContent[14:], properties)

	return &Frame{
		Type:    FrameHeader,
		Channel: channelID,
		Size:    uint32(len(headerContent)),
		Payload: headerContent,
	}
}

// EncodeBodyFrameForChannel encodes a content body into a body frame for a specific channel
func EncodeBodyFrameForChannel(channelID uint16, bodyData []byte) *Frame {
	return &Frame{
		Type:    FrameBody,
		Channel: channelID,
		Size:    uint32(len(bodyData)),
		Payload: bodyData,
	}
}
