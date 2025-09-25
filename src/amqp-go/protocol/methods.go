package protocol

import (
	"encoding/binary"
	"fmt"
)

// Method IDs for connection class
const (
	ConnectionStart     = 10
	ConnectionStartOK   = 11
	ConnectionSecure    = 20
	ConnectionSecureOK  = 21
	ConnectionTune      = 30
	ConnectionTuneOK    = 31
	ConnectionOpen      = 40
	ConnectionOpenOK    = 41
	ConnectionClose     = 50
	ConnectionCloseOK   = 51
)

// Method IDs for channel class
const (
	ChannelOpen      = 10
	ChannelOpenOK    = 11
	ChannelFlow      = 20
	ChannelFlowOK    = 21
	ChannelClose     = 40
	ChannelCloseOK   = 41
)

// Method IDs for exchange class
const (
	ExchangeDeclare  = 10  // 40.10 - class ID 40, method ID 10
	ExchangeDeclareOK = 11 // 40.11 - class ID 40, method ID 11
	ExchangeDelete   = 20  // 40.20 - class ID 40, method ID 20
	ExchangeDeleteOK = 21  // 40.21 - class ID 40, method ID 21
	ExchangeBind     = 30  // 40.30 - class ID 40, method ID 30
	ExchangeBindOK   = 31  // 40.31 - class ID 40, method ID 31
	ExchangeUnbind   = 40  // 40.40 - class ID 40, method ID 40
	ExchangeUnbindOK = 41  // 40.41 - class ID 40, method ID 41
)

// Method IDs for queue class
const (
	QueueDeclare     = 10  // 50.10 - class ID 50, method ID 10
	QueueDeclareOK   = 11  // 50.11 - class ID 50, method ID 11
	QueueBind        = 20  // 50.20 - class ID 50, method ID 20
	QueueBindOK      = 21  // 50.21 - class ID 50, method ID 21
	QueuePurge       = 30  // 50.30 - class ID 50, method ID 30
	QueuePurgeOK     = 31  // 50.31 - class ID 50, method ID 31
	QueueDelete      = 40  // 50.40 - class ID 50, method ID 40
	QueueDeleteOK    = 41  // 50.41 - class ID 50, method ID 41
	QueueUnbind      = 50  // 50.50 - class ID 50, method ID 50
	QueueUnbindOK    = 51  // 50.51 - class ID 50, method ID 51
)

// ConnectionStartMethod represents the connection.start method
type ConnectionStartMethod struct {
	VersionMajor    byte
	VersionMinor    byte
	ServerProperties map[string]interface{}
	Mechanisms      []string
	Locales         []string
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
	ReplyCode    uint16
	ReplyText    string
	ClassID      uint16
	MethodID     uint16
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
	ReplyCode    uint16
	ReplyText    string
	ClassID      uint16
	MethodID     uint16
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
func encodeShortString(s string) []byte {
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

func encodeFieldTable(table map[string]interface{}) ([]byte, error) {
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
	Reserved1   uint16
	Exchange    string
	Type        string
	Passive     bool
	Durable     bool
	AutoDelete  bool
	Internal    bool
	NoWait      bool
	Arguments   map[string]interface{}
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
	exchangeLen := int(data[offset])
	offset += 1
	if offset+exchangeLen > len(data) {
		return fmt.Errorf("exchange name extends beyond data")
	}
	m.Exchange = string(data[offset : offset+exchangeLen])
	offset += exchangeLen

	// Type (short string)
	if offset >= len(data) {
		return fmt.Errorf("type field missing")
	}
	typeLen := int(data[offset])
	offset += 1
	if offset+typeLen > len(data) {
		return fmt.Errorf("exchange type extends beyond data")
	}
	m.Type = string(data[offset : offset+typeLen])
	offset += typeLen

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
	if offset < len(data) {
		var err error
		m.Arguments, offset, err = decodeFieldTable(data, offset)
		if err != nil {
			return err
		}
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
	Reserved1   uint16
	Exchange    string
	IfUnused    bool
	NoWait      bool
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
	if len(data) < 4 {
		return fmt.Errorf("exchange.delete method data too short")
	}

	offset := 0

	// Reserved1 (uint16)
	m.Reserved1 = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Exchange (short string)
	exchange, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return err
	}
	m.Exchange = exchange
	offset = newOffset

	// Flags (uint16)
	if offset+2 > len(data) {
		return fmt.Errorf("flags field missing")
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

// QueueDeclareMethod represents the queue.declare method
type QueueDeclareMethod struct {
	Reserved1   uint16
	Queue       string
	Passive     bool
	Durable     bool
	Exclusive   bool
	AutoDelete  bool
	NoWait      bool
	Arguments   map[string]interface{}
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
	queue, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return err
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
	if offset < len(data) {
		var err error
		m.Arguments, offset, err = decodeFieldTable(data, offset)
		if err != nil {
			return err
		}
	}

	return nil
}

// QueueDeclareOKMethod represents the queue.declare-ok method
type QueueDeclareOKMethod struct {
	Queue       string
	MessageCount uint32
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
	Reserved1   uint16
	Queue       string
	Exchange    string
	RoutingKey  string
	NoWait      bool
	Arguments   map[string]interface{}
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
	queue, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return err
	}
	m.Queue = queue
	offset = newOffset

	// Exchange (short string)
	exchange, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return err
	}
	m.Exchange = exchange
	offset = newOffset

	// Routing key (short string)
	routingKey, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return err
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
	if offset < len(data) {
		var err error
		m.Arguments, offset, err = decodeFieldTable(data, offset)
		if err != nil {
			return err
		}
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
	Reserved1   uint16
	Queue       string
	IfUnused    bool
	IfEmpty     bool
	NoWait      bool
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
	if len(data) < 4 {
		return fmt.Errorf("queue.delete method data too short")
	}

	offset := 0

	// Reserved1 (uint16)
	m.Reserved1 = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Queue (short string)
	queue, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return err
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