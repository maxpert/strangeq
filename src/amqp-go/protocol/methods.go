package protocol

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"
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

// Class ID for confirm class
const (
	ConfirmClass = 85
)

// Method IDs for confirm class
const (
	ConfirmSelect   = 10 // 85.10 - class ID 85, method ID 10
	ConfirmSelectOK = 11 // 85.11 - class ID 85, method ID 11
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

	// Mechanisms (long string) — join mechanism names with spaces
	mechanismsStr := strings.Join(m.Mechanisms, " ")
	if mechanismsStr == "" {
		mechanismsStr = "PLAIN"
	}
	mechBytes := encodeLongString(mechanismsStr)
	result = append(result, mechBytes...)

	// Locales (long string)
	// For now, just support "en_US"
	localesStr := "en_US"
	localeBytes := encodeLongString(localesStr)
	result = append(result, localeBytes...)

	return result, nil
}

// ParseConnectionStart parses a connection.start method from bytes
func ParseConnectionStart(data []byte) (*ConnectionStartMethod, error) {
	method := &ConnectionStartMethod{}
	offset := 0

	// Parse version major and minor (1 byte each)
	if len(data) < 2 {
		return nil, fmt.Errorf("insufficient data for version")
	}
	method.VersionMajor = data[0]
	method.VersionMinor = data[1]
	offset = 2

	// Parse server properties (field table)
	var err error
	method.ServerProperties, offset, err = decodeFieldTable(data, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to decode server properties: %w", err)
	}

	// Parse mechanisms (long string)
	if len(data) < offset+4 {
		return nil, fmt.Errorf("insufficient data for mechanisms length")
	}
	mechLen := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	if len(data) < offset+int(mechLen) {
		return nil, fmt.Errorf("insufficient data for mechanisms")
	}
	// Mechanisms are space-separated
	mechanismsStr := string(data[offset : offset+int(mechLen)])
	method.Mechanisms = []string{mechanismsStr}
	offset += int(mechLen)

	// Parse locales (long string)
	if len(data) < offset+4 {
		return nil, fmt.Errorf("insufficient data for locales length")
	}
	localeLen := binary.BigEndian.Uint32(data[offset : offset+4])
	offset += 4

	if len(data) < offset+int(localeLen) {
		return nil, fmt.Errorf("insufficient data for locales")
	}
	// Locales are space-separated
	localesStr := string(data[offset : offset+int(localeLen)])
	method.Locales = []string{localesStr}

	return method, nil
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

func (m *ConnectionTuneMethod) Deserialize(data []byte) error {
	if len(data) < 8 {
		return fmt.Errorf("connection.tune too short: need 8 bytes, got %d", len(data))
	}
	m.ChannelMax = binary.BigEndian.Uint16(data[0:2])
	m.FrameMax = binary.BigEndian.Uint32(data[2:6])
	m.Heartbeat = binary.BigEndian.Uint16(data[6:8])
	return nil
}

// ParseConnectionTune parses a connection.tune method from bytes
func ParseConnectionTune(data []byte) (*ConnectionTuneMethod, error) {
	method := &ConnectionTuneMethod{}

	// Parse channel max (uint16)
	if len(data) < 2 {
		return nil, fmt.Errorf("insufficient data for channel max")
	}
	method.ChannelMax = binary.BigEndian.Uint16(data[0:2])

	// Parse frame max (uint32)
	if len(data) < 6 {
		return nil, fmt.Errorf("insufficient data for frame max")
	}
	method.FrameMax = binary.BigEndian.Uint32(data[2:6])

	// Parse heartbeat (uint16)
	if len(data) < 8 {
		return nil, fmt.Errorf("insufficient data for heartbeat")
	}
	method.Heartbeat = binary.BigEndian.Uint16(data[6:8])

	return method, nil
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

func (m *ConnectionCloseMethod) Deserialize(data []byte) error {
	if len(data) < 2 {
		return fmt.Errorf("connection.close too short for reply code")
	}
	m.ReplyCode = binary.BigEndian.Uint16(data[0:2])
	offset := 2
	replyText, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return fmt.Errorf("failed to decode reply text: %w", err)
	}
	m.ReplyText = replyText
	offset = newOffset
	if offset+4 > len(data) {
		return fmt.Errorf("connection.close too short for class/method IDs")
	}
	m.ClassID = binary.BigEndian.Uint16(data[offset : offset+2])
	m.MethodID = binary.BigEndian.Uint16(data[offset+2 : offset+4])
	return nil
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

// Deserialize decodes the ChannelFlowMethod from a byte slice
func (m *ChannelFlowMethod) Deserialize(data []byte) error {
	if len(data) < 1 {
		return fmt.Errorf("channel.flow method data too short")
	}
	m.Active = data[0] != 0
	return nil
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

// Deserialize decodes the ChannelFlowOKMethod from a byte slice
func (m *ChannelFlowOKMethod) Deserialize(data []byte) error {
	if len(data) < 1 {
		return fmt.Errorf("channel.flow-ok method data too short")
	}
	m.Active = data[0] != 0
	return nil
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

func clampShortString(s string) string {
	if len(s) <= 255 {
		return s
	}
	end := 255
	for end > 0 && !utf8.RuneStart(s[end]) {
		end--
	}
	return s[:end]
}

func EncodeShortString(s string) []byte {
	s = clampShortString(s)
	result := make([]byte, 1+len(s))
	result[0] = byte(len(s))
	copy(result[1:], []byte(s))
	return result
}

func AppendShortString(buf []byte, s string) []byte {
	s = clampShortString(s)
	buf = append(buf, byte(len(s)))
	buf = append(buf, s...)
	return buf
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

type Decimal struct {
	Scale byte
	Value int32
}

func writeFieldValue(value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case bool:
		var b byte
		if v {
			b = 1
		}
		return []byte{'t', b}, nil
	case int8:
		return []byte{'b', byte(v)}, nil
	case uint8:
		return []byte{'B', v}, nil
	case int16:
		buf := make([]byte, 3)
		buf[0] = 's'
		binary.BigEndian.PutUint16(buf[1:3], uint16(v))
		return buf, nil
	case int32:
		buf := make([]byte, 5)
		buf[0] = 'I'
		binary.BigEndian.PutUint32(buf[1:5], uint32(v))
		return buf, nil
	case int:
		buf := make([]byte, 5)
		buf[0] = 'I'
		binary.BigEndian.PutUint32(buf[1:5], uint32(v))
		return buf, nil
	case int64:
		buf := make([]byte, 9)
		buf[0] = 'l'
		binary.BigEndian.PutUint64(buf[1:9], uint64(v))
		return buf, nil
	case float32:
		buf := make([]byte, 5)
		buf[0] = 'f'
		binary.BigEndian.PutUint32(buf[1:5], math.Float32bits(v))
		return buf, nil
	case float64:
		buf := make([]byte, 9)
		buf[0] = 'd'
		binary.BigEndian.PutUint64(buf[1:9], math.Float64bits(v))
		return buf, nil
	case string:
		buf := make([]byte, 5+len(v))
		buf[0] = 'S'
		binary.BigEndian.PutUint32(buf[1:5], uint32(len(v)))
		copy(buf[5:], v)
		return buf, nil
	case []interface{}:
		var elements []byte
		for _, elem := range v {
			elemBytes, err := writeFieldValue(elem)
			if err != nil {
				return nil, err
			}
			elements = append(elements, elemBytes...)
		}
		buf := make([]byte, 5+len(elements))
		buf[0] = 'A'
		binary.BigEndian.PutUint32(buf[1:5], uint32(len(elements)))
		copy(buf[5:], elements)
		return buf, nil
	case time.Time:
		buf := make([]byte, 9)
		buf[0] = 'T'
		binary.BigEndian.PutUint64(buf[1:9], uint64(v.Unix()))
		return buf, nil
	case map[string]interface{}:
		tblBytes, err := serializeFieldTable(v)
		if err != nil {
			return nil, err
		}
		buf := make([]byte, 5+len(tblBytes))
		buf[0] = 'F'
		binary.BigEndian.PutUint32(buf[1:5], uint32(len(tblBytes)))
		copy(buf[5:], tblBytes)
		return buf, nil
	case []byte:
		buf := make([]byte, 5+len(v))
		buf[0] = 'x'
		binary.BigEndian.PutUint32(buf[1:5], uint32(len(v)))
		copy(buf[5:], v)
		return buf, nil
	case nil:
		return []byte{'V'}, nil
	default:
		return nil, fmt.Errorf("unsupported field value type: %T", value)
	}
}

func serializeFieldTable(table map[string]interface{}) ([]byte, error) {
	var result []byte

	for key, value := range table {
		keyBytes := encodeShortString(key)
		result = append(result, keyBytes...)

		valueBytes, err := writeFieldValue(value)
		if err != nil {
			return nil, err
		}
		result = append(result, valueBytes...)
	}

	return result, nil
}

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
		if offset >= tableEnd {
			return nil, offset, fmt.Errorf("field key missing")
		}
		keyLen := int(data[offset])
		offset++
		if offset+keyLen > tableEnd {
			return nil, offset, fmt.Errorf("field key extends beyond table")
		}
		key := string(data[offset : offset+keyLen])
		offset += keyLen

		value, newOffset, err := decodeFieldValue(data, offset, tableEnd)
		if err != nil {
			return nil, newOffset, fmt.Errorf("failed to decode value for key %s: %w", key, err)
		}
		offset = newOffset
		table[key] = value
	}

	return table, offset, nil
}

func decodeFieldValue(data []byte, offset, end int) (interface{}, int, error) {
	if offset >= end {
		return nil, offset, fmt.Errorf("field value type missing")
	}
	typeIndicator := data[offset]
	offset++

	switch typeIndicator {
	case 't':
		if offset+1 > end {
			return nil, offset, fmt.Errorf("bool value extends beyond data")
		}
		return data[offset] != 0, offset + 1, nil
	case 'b':
		if offset+1 > end {
			return nil, offset, fmt.Errorf("int8 value extends beyond data")
		}
		return int8(data[offset]), offset + 1, nil
	case 'B':
		if offset+1 > end {
			return nil, offset, fmt.Errorf("uint8 value extends beyond data")
		}
		return uint8(data[offset]), offset + 1, nil
	case 's':
		if offset+2 > end {
			return nil, offset, fmt.Errorf("int16 value extends beyond data")
		}
		return int16(binary.BigEndian.Uint16(data[offset : offset+2])), offset + 2, nil
	case 'I':
		if offset+4 > end {
			return nil, offset, fmt.Errorf("int32 value extends beyond data")
		}
		return int32(binary.BigEndian.Uint32(data[offset : offset+4])), offset + 4, nil
	case 'l':
		if offset+8 > end {
			return nil, offset, fmt.Errorf("int64 value extends beyond data")
		}
		return int64(binary.BigEndian.Uint64(data[offset : offset+8])), offset + 8, nil
	case 'f':
		if offset+4 > end {
			return nil, offset, fmt.Errorf("float32 value extends beyond data")
		}
		return math.Float32frombits(binary.BigEndian.Uint32(data[offset : offset+4])), offset + 4, nil
	case 'd':
		if offset+8 > end {
			return nil, offset, fmt.Errorf("float64 value extends beyond data")
		}
		return math.Float64frombits(binary.BigEndian.Uint64(data[offset : offset+8])), offset + 8, nil
	case 'D':
		if offset+5 > end {
			return nil, offset, fmt.Errorf("decimal value extends beyond data")
		}
		return Decimal{Scale: data[offset], Value: int32(binary.BigEndian.Uint32(data[offset+1 : offset+5]))}, offset + 5, nil
	case 'T':
		if offset+8 > end {
			return nil, offset, fmt.Errorf("timestamp value extends beyond data")
		}
		return time.Unix(int64(binary.BigEndian.Uint64(data[offset:offset+8])), 0).UTC(), offset + 8, nil
	case 'S':
		if offset+4 > end {
			return nil, offset, fmt.Errorf("string length extends beyond data")
		}
		strLen := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		if int(strLen) > end-offset {
			return nil, offset, fmt.Errorf("string value extends beyond data")
		}
		return string(data[offset : offset+int(strLen)]), offset + int(strLen), nil
	case 'x':
		if offset+4 > end {
			return nil, offset, fmt.Errorf("byte array length extends beyond data")
		}
		arrLen := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		if int(arrLen) > end-offset {
			return nil, offset, fmt.Errorf("byte array value extends beyond data")
		}
		out := make([]byte, arrLen)
		copy(out, data[offset:offset+int(arrLen)])
		return out, offset + int(arrLen), nil
	case 'F':
		if offset+4 > end {
			return nil, offset, fmt.Errorf("nested table length extends beyond data")
		}
		tblLen := binary.BigEndian.Uint32(data[offset : offset+4])
		subEnd := offset + 4 + int(tblLen)
		if subEnd > end {
			return nil, offset, fmt.Errorf("nested table extends beyond data")
		}
		nested, _, err := decodeFieldTable(data[offset:subEnd], 0)
		if err != nil {
			return nil, offset, fmt.Errorf("failed to decode nested table: %w", err)
		}
		return nested, subEnd, nil
	case 'A':
		if offset+4 > end {
			return nil, offset, fmt.Errorf("array length extends beyond data")
		}
		arrLen := binary.BigEndian.Uint32(data[offset : offset+4])
		arrStart := offset + 4
		arrEnd := arrStart + int(arrLen)
		if arrEnd > end {
			return nil, offset, fmt.Errorf("array extends beyond data")
		}
		arr := []interface{}{}
		arrOffset := arrStart
		for arrOffset < arrEnd {
			elem, newOffset, err := decodeFieldValue(data, arrOffset, arrEnd)
			if err != nil {
				return nil, arrOffset, fmt.Errorf("failed to decode array element: %w", err)
			}
			arr = append(arr, elem)
			arrOffset = newOffset
		}
		return arr, arrEnd, nil
	case 'V':
		return nil, offset, nil
	default:
		return nil, offset, fmt.Errorf("unknown field value type: 0x%02X", typeIndicator)
	}
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
	var flags byte
	if m.Passive {
		flags |= (1 << 0)
	}
	if m.Durable {
		flags |= (1 << 1)
	}
	if m.AutoDelete {
		flags |= (1 << 2)
	}
	if m.Internal {
		flags |= (1 << 3)
	}
	if m.NoWait {
		flags |= (1 << 4)
	}
	result = append(result, flags)

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

	// Flags (single byte — AMQP 0.9.1 packs bit fields into bytes)
	if offset >= len(data) {
		return fmt.Errorf("flags field missing")
	}
	flags := data[offset]
	offset++

	// Extract flags
	m.Passive = (flags & (1 << 0)) != 0
	m.Durable = (flags & (1 << 1)) != 0
	m.AutoDelete = (flags & (1 << 2)) != 0
	m.Internal = (flags & (1 << 3)) != 0
	m.NoWait = (flags & (1 << 4)) != 0

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

// ExchangeBindOKMethod represents the exchange.bind-ok method
type ExchangeBindOKMethod struct {
}

// Serialize encodes the ExchangeBindOKMethod into a byte slice
func (m *ExchangeBindOKMethod) Serialize() ([]byte, error) {
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

	var flags byte
	if m.Passive {
		flags |= (1 << 0)
	}
	if m.Durable {
		flags |= (1 << 1)
	}
	if m.Exclusive {
		flags |= (1 << 2)
	}
	if m.AutoDelete {
		flags |= (1 << 3)
	}
	if m.NoWait {
		flags |= (1 << 4)
	}
	result = append(result, flags)

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

	// Flags (single byte — AMQP 0.9.1 packs bit fields into bytes)
	if offset >= len(data) {
		return fmt.Errorf("flags field missing")
	}
	flags := data[offset]
	offset++

	// Extract flags
	m.Passive = (flags & (1 << 0)) != 0
	m.Durable = (flags & (1 << 1)) != 0
	m.Exclusive = (flags & (1 << 2)) != 0
	m.AutoDelete = (flags & (1 << 3)) != 0
	m.NoWait = (flags & (1 << 4)) != 0

	// Arguments (field table)
	if offset+4 <= len(data) {
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

// QueuePurgeMethod represents the queue.purge method
type QueuePurgeMethod struct {
	Reserved1 uint16
	Queue     string
	NoWait    bool
}

// Serialize encodes the QueuePurgeMethod into a byte slice
func (m *QueuePurgeMethod) Serialize() ([]byte, error) {
	var result []byte

	// Reserved1 (uint16)
	reservedBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(reservedBytes, m.Reserved1)
	result = append(result, reservedBytes...)

	// Queue (short string)
	queueBytes := encodeShortString(m.Queue)
	result = append(result, queueBytes...)

	// Flags (uint16) — bit 0: no-wait
	flags := uint16(0)
	if m.NoWait {
		flags |= (1 << 0)
	}
	flagBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(flagBytes, flags)
	result = append(result, flagBytes...)

	return result, nil
}

// Deserialize decodes the QueuePurgeMethod from a byte slice
func (m *QueuePurgeMethod) Deserialize(data []byte) error {
	if len(data) < 3 { // Need at least reserved(2) + queue length byte(1)
		return fmt.Errorf("queue.purge method data too short")
	}

	offset := 0

	// Reserved1 (uint16)
	m.Reserved1 = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Queue (short string)
	queue, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return fmt.Errorf("failed to decode queue name: %v", err)
	}
	m.Queue = queue
	offset = newOffset

	// Flags (uint16)
	if offset+2 > len(data) {
		m.NoWait = false
		return nil
	}
	flags := binary.BigEndian.Uint16(data[offset : offset+2])
	m.NoWait = (flags & (1 << 0)) != 0

	return nil
}

// QueuePurgeOKMethod represents the queue.purge-ok method
type QueuePurgeOKMethod struct {
	MessageCount uint32
}

// Serialize encodes the QueuePurgeOKMethod into a byte slice
func (m *QueuePurgeOKMethod) Serialize() ([]byte, error) {
	result := make([]byte, 4)
	binary.BigEndian.PutUint32(result, m.MessageCount)
	return result, nil
}

// Deserialize decodes the QueuePurgeOKMethod from a byte slice
func (m *QueuePurgeOKMethod) Deserialize(data []byte) error {
	if len(data) < 4 {
		return fmt.Errorf("queue.purge-ok method data too short")
	}
	m.MessageCount = binary.BigEndian.Uint32(data[:4])
	return nil
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

	// Flags (single octet, per AMQP 0.9.1 — the four bits pack into ONE byte,
	// not a uint16; a spec-compliant client sends exactly one octet here).
	// bit 0: no-local
	// bit 1: no-ack
	// bit 2: exclusive
	// bit 3: no-wait
	// bits 4-7: unused
	var flags byte
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
	result = append(result, flags)

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

	// Flags (single octet, per AMQP 0.9.1 — the four bits pack into ONE byte,
	// not a uint16). Reading two bytes here misaligned the arguments table and
	// decoded no-ack from the wrong byte, so a spec-compliant client's no-ack
	// consumer was silently treated as manual-ack (SQ-0 deadlock).
	if offset >= len(data) {
		// If not enough bytes for flags, use defaults
		m.NoLocal = false
		m.NoAck = false
		m.Exclusive = false
		m.NoWait = false
		// Arguments are handled below
	} else {
		flags := data[offset]
		offset++

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

	// NoAck (single octet, per AMQP 0.9.1 — not a uint16)
	// bit 0: no-ack
	// bits 1-7: unused
	var flags byte
	if m.NoAck {
		flags |= (1 << 0)
	}
	result = append(result, flags)

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

	// Flags (single octet, per AMQP 0.9.1 — not a uint16). basic.get carries
	// exactly one trailing flags byte; reading two bytes here made no-ack from
	// a spec-compliant client fall through to the default (false).
	if offset < len(data) {
		flags := data[offset]
		offset++

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

// Deserialize decodes the BasicGetOKMethod from a byte slice
func (m *BasicGetOKMethod) Deserialize(data []byte) error {
	if len(data) < 9 { // delivery_tag(8) + redelivered(1)
		return fmt.Errorf("basic.get-ok method data too short")
	}

	offset := 0

	// Delivery tag (uint64)
	m.DeliveryTag = binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8

	// Redelivered (bit packed in a single byte)
	m.Redelivered = data[offset] != 0
	offset++

	// Exchange (short string)
	exchange, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return fmt.Errorf("failed to decode exchange: %v", err)
	}
	m.Exchange = exchange
	offset = newOffset

	// Routing key (short string)
	routingKey, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return fmt.Errorf("failed to decode routing key: %v", err)
	}
	m.RoutingKey = routingKey
	offset = newOffset

	// Message count (uint32)
	if offset+4 > len(data) {
		return fmt.Errorf("message count field missing")
	}
	m.MessageCount = binary.BigEndian.Uint32(data[offset : offset+4])

	return nil
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

// Serialize encodes the BasicAckMethod into a byte slice.
// AMQP 0.9.1: delivery-tag (longlong) followed by a SINGLE OCTET bit-field
// (bit 0: multiple). A former encoding wrote the bits as a big-endian uint16,
// which a spec client decodes as multiple=false — silently turning batched
// confirm/ack streams into per-message ones on the wire.
func (m *BasicAckMethod) Serialize() ([]byte, error) {
	result := make([]byte, 9)
	binary.BigEndian.PutUint64(result[:8], m.DeliveryTag)
	if m.Multiple {
		result[8] |= 0x01
	}
	return result, nil
}

// Deserialize decodes the BasicAckMethod from a byte slice (spec octet
// bit-field; an 8-byte payload without the flag octet defaults multiple=false).
func (m *BasicAckMethod) Deserialize(data []byte) error {
	if len(data) < 8 { // Need at least delivery tag (8 bytes)
		return fmt.Errorf("basic.ack method data too short")
	}
	m.DeliveryTag = binary.BigEndian.Uint64(data[:8])
	m.Multiple = len(data) >= 9 && data[8]&0x01 != 0
	return nil
}

// BasicRejectMethod represents the basic.reject method
type BasicRejectMethod struct {
	DeliveryTag uint64
	Requeue     bool
}

// Serialize encodes the BasicRejectMethod into a byte slice.
// AMQP 0.9.1: delivery-tag (longlong) + a SINGLE OCTET bit-field (bit 0:
// requeue). See BasicAckMethod.Serialize for why this must not be a uint16.
func (m *BasicRejectMethod) Serialize() ([]byte, error) {
	result := make([]byte, 9)
	binary.BigEndian.PutUint64(result[:8], m.DeliveryTag)
	if m.Requeue {
		result[8] |= 0x01
	}
	return result, nil
}

// Deserialize decodes the BasicRejectMethod from a byte slice (spec octet
// bit-field; an 8-byte payload without the flag octet keeps the requeue=true
// default).
func (m *BasicRejectMethod) Deserialize(data []byte) error {
	if len(data) < 8 { // Need at least delivery tag (8 bytes)
		return fmt.Errorf("basic.reject method data too short")
	}
	m.DeliveryTag = binary.BigEndian.Uint64(data[:8])
	m.Requeue = true // spec default when the flag octet is absent
	if len(data) >= 9 {
		m.Requeue = data[8]&0x01 != 0
	}
	return nil
}

// BasicNackMethod represents the basic.nack method
type BasicNackMethod struct {
	DeliveryTag uint64
	Multiple    bool
	Requeue     bool
}

// Serialize encodes the BasicNackMethod into a byte slice.
// AMQP 0.9.1: delivery-tag (longlong) + a SINGLE OCTET bit-field (bit 0:
// multiple, bit 1: requeue). See BasicAckMethod.Serialize for why this must
// not be a uint16.
func (m *BasicNackMethod) Serialize() ([]byte, error) {
	result := make([]byte, 9)
	binary.BigEndian.PutUint64(result[:8], m.DeliveryTag)
	if m.Multiple {
		result[8] |= 0x01
	}
	if m.Requeue {
		result[8] |= 0x02
	}
	return result, nil
}

// Deserialize decodes the BasicNackMethod from a byte slice (spec octet
// bit-field; an 8-byte payload without the flag octet keeps the requeue=true
// default).
func (m *BasicNackMethod) Deserialize(data []byte) error {
	if len(data) < 8 { // Need at least delivery tag (8 bytes)
		return fmt.Errorf("basic.nack method data too short")
	}
	m.DeliveryTag = binary.BigEndian.Uint64(data[:8])
	m.Multiple = false
	m.Requeue = true // spec default when the flag octet is absent
	if len(data) >= 9 {
		m.Multiple = data[8]&0x01 != 0
		m.Requeue = data[8]&0x02 != 0
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
	// Pre-allocate with enough capacity for all fields to avoid regrowth.
	// consumerTag(1+255) + deliveryTag(8) + redelivered(1) + exchange(1+255) + routingKey(1+255) = 777 max
	result := make([]byte, 0, 777)

	// Consumer tag (short string)
	result = append(result, encodeShortString(m.ConsumerTag)...)

	// Delivery tag (64-bit, appended in-place — no temp allocation)
	result = binary.BigEndian.AppendUint64(result, m.DeliveryTag)

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
	result = append(result, encodeShortString(m.Exchange)...)

	// Routing key (short string)
	result = append(result, encodeShortString(m.RoutingKey)...)

	return result, nil
}

// SerializeInto appends the basic.deliver method payload directly into buf,
// avoiding all intermediate allocations. This is the zero-alloc version
// of Serialize() for hot paths.
func (m *BasicDeliverMethod) SerializeInto(buf []byte) []byte {
	buf = AppendShortString(buf, m.ConsumerTag)
	buf = binary.BigEndian.AppendUint64(buf, m.DeliveryTag)
	if m.Redelivered {
		buf = append(buf, byte(1))
	} else {
		buf = append(buf, byte(0))
	}
	buf = AppendShortString(buf, m.Exchange)
	buf = AppendShortString(buf, m.RoutingKey)
	return buf
}
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

// BasicReturnMethod represents the basic.return method (class 60, method 50)
type BasicReturnMethod struct {
	ReplyCode  uint16
	ReplyText  string
	Exchange   string
	RoutingKey string
}

// Serialize encodes the BasicReturnMethod into a byte slice
func (m *BasicReturnMethod) Serialize() ([]byte, error) {
	result := make([]byte, 0, 2+256+256+256)

	result = binary.BigEndian.AppendUint16(result, m.ReplyCode)
	result = append(result, encodeShortString(m.ReplyText)...)
	result = append(result, encodeShortString(m.Exchange)...)
	result = append(result, encodeShortString(m.RoutingKey)...)

	return result, nil
}

// Deserialize decodes the BasicReturnMethod from a byte slice
func (m *BasicReturnMethod) Deserialize(data []byte) error {
	if len(data) < 2 {
		return fmt.Errorf("basic.return method data too short")
	}

	offset := 0

	m.ReplyCode = binary.BigEndian.Uint16(data[offset : offset+2])
	offset += 2

	if offset >= len(data) {
		return fmt.Errorf("reply-text field missing")
	}
	replyText, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return fmt.Errorf("failed to decode reply-text: %v", err)
	}
	m.ReplyText = replyText
	offset = newOffset

	if offset >= len(data) {
		return fmt.Errorf("exchange field missing")
	}
	exchange, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return fmt.Errorf("failed to decode exchange: %v", err)
	}
	m.Exchange = exchange
	offset = newOffset

	if offset >= len(data) {
		return fmt.Errorf("routing-key field missing")
	}
	routingKey, newOffset, err := decodeShortString(data, offset)
	if err != nil {
		return fmt.Errorf("failed to decode routing-key: %v", err)
	}
	m.RoutingKey = routingKey

	return nil
}

// BasicRecoverMethod represents the basic.recover method (class 60, method 110)
type BasicRecoverMethod struct {
	Requeue bool
}

// Serialize encodes the BasicRecoverMethod into a byte slice
func (m *BasicRecoverMethod) Serialize() ([]byte, error) {
	if m.Requeue {
		return []byte{1}, nil
	}
	return []byte{0}, nil
}

// Deserialize decodes the BasicRecoverMethod from a byte slice
func (m *BasicRecoverMethod) Deserialize(data []byte) error {
	if len(data) < 1 {
		m.Requeue = false
		return nil
	}
	m.Requeue = data[0] != 0
	return nil
}

// BasicRecoverOKMethod represents the basic.recover-ok method (class 60, method 111)
type BasicRecoverOKMethod struct {
}

// Serialize encodes the BasicRecoverOKMethod into a byte slice
func (m *BasicRecoverOKMethod) Serialize() ([]byte, error) {
	return []byte{}, nil
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

// ExchangeBindMethod represents the exchange.bind method
type ExchangeBindMethod struct {
	Reserved1   uint16
	Destination string
	Source      string
	RoutingKey  string
	NoWait      bool
	Arguments   map[string]interface{}
}

// Serialize encodes the ExchangeBindMethod into a byte slice
func (m *ExchangeBindMethod) Serialize() ([]byte, error) {
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
	flags := uint16(0)
	if m.NoWait {
		flags |= (1 << 0)
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

// Deserialize decodes the ExchangeBindMethod from a byte slice
func (m *ExchangeBindMethod) Deserialize(data []byte) error {
	if len(data) < 4 {
		return fmt.Errorf("exchange.bind method data too short")
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

// ConfirmSelectMethod represents the confirm.select method (class 85, method 10)
type ConfirmSelectMethod struct {
	NoWait bool
}

// Serialize encodes the ConfirmSelectMethod into a byte slice
func (m *ConfirmSelectMethod) Serialize() ([]byte, error) {
	var bits byte
	if m.NoWait {
		bits |= 1 << 0
	}
	return []byte{bits}, nil
}

// Deserialize decodes the ConfirmSelectMethod from a byte slice
func (m *ConfirmSelectMethod) Deserialize(data []byte) error {
	if len(data) < 1 {
		m.NoWait = false
		return nil
	}
	m.NoWait = data[0]&(1<<0) != 0
	return nil
}

// ConfirmSelectOKMethod represents the confirm.select-ok method (class 85, method 11)
type ConfirmSelectOKMethod struct {
}

// Serialize encodes the ConfirmSelectOKMethod into a byte slice
func (m *ConfirmSelectOKMethod) Serialize() ([]byte, error) {
	return []byte{}, nil
}

// FragmentBodyIntoFrames fragments a message body into multiple body frames
// respecting the AMQP maxFrameSize constraint. Each frame payload must be
// <= (maxFrameSize - 8) bytes to account for frame header overhead.
//
// AMQP 0.9.1 Specification:
// - Large message bodies MUST be split into multiple body frames
// - Each body frame MUST NOT exceed the negotiated max-frame-size
// - Frame overhead is 8 bytes: 1 (type) + 2 (channel) + 4 (size) + 1 (end marker)
//
// For a 16MB message with 128KB maxFrameSize, this creates ~122 body frames.
// Returns empty slice for zero-length bodies (method + header frames only).
func FragmentBodyIntoFrames(channelID uint16, body []byte, maxFrameSize uint32) []*Frame {
	if len(body) == 0 {
		return nil // No body frames for empty bodies
	}

	// Calculate max payload size per frame (frame size - 8 byte overhead)
	maxBodyPerFrame := int(maxFrameSize) - 8
	if maxBodyPerFrame <= 0 {
		// Should never happen with valid maxFrameSize, but handle defensively
		maxBodyPerFrame = 4096 // Minimum safe frame size
	}

	// Pre-allocate slice with expected capacity
	numFrames := (len(body) + maxBodyPerFrame - 1) / maxBodyPerFrame
	frames := make([]*Frame, 0, numFrames)

	// Fragment body into chunks
	for offset := 0; offset < len(body); offset += maxBodyPerFrame {
		end := offset + maxBodyPerFrame
		if end > len(body) {
			end = len(body)
		}

		chunk := body[offset:end]
		frames = append(frames, EncodeBodyFrameForChannel(channelID, chunk))
	}

	return frames
}
