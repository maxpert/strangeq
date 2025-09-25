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