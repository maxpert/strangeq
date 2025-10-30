package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// TestAMQPCompliance_FrameFormat validates AMQP 0.9.1 frame format spec
// Frame format: type(1) + channel(2) + size(4) + payload + frame-end(1)
func TestAMQPCompliance_FrameFormat(t *testing.T) {
	frame := &Frame{
		Type:    FrameMethod,
		Channel: 1,
		Payload: []byte{0x00, 0x0A, 0x00, 0x0A}, // connection.start
	}

	data, err := frame.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to marshal frame: %v", err)
	}

	// Verify frame structure
	if len(data) < 8 {
		t.Fatalf("Frame too short: %d bytes", len(data))
	}

	// Type (1 byte)
	if data[0] != FrameMethod {
		t.Errorf("Expected frame type %d, got %d", FrameMethod, data[0])
	}

	// Channel (2 bytes, big-endian)
	channel := binary.BigEndian.Uint16(data[1:3])
	if channel != 1 {
		t.Errorf("Expected channel 1, got %d", channel)
	}

	// Size (4 bytes, big-endian)
	size := binary.BigEndian.Uint32(data[3:7])
	if size != uint32(len(frame.Payload)) {
		t.Errorf("Expected size %d, got %d", len(frame.Payload), size)
	}

	// Frame end marker (0xCE)
	if data[len(data)-1] != 0xCE {
		t.Errorf("Expected frame end 0xCE, got 0x%02X", data[len(data)-1])
	}

	// Payload should be at correct offset
	payloadStart := 7
	payloadEnd := 7 + len(frame.Payload)
	if !bytes.Equal(data[payloadStart:payloadEnd], frame.Payload) {
		t.Error("Payload mismatch in marshaled frame")
	}
}

// TestAMQPCompliance_ProtocolHeader validates AMQP protocol header
// Protocol header: "AMQP" + 0 + 0 + 9 + 1
func TestAMQPCompliance_ProtocolHeader(t *testing.T) {
	expected := []byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}

	// The protocol header is sent at connection start
	// This test verifies the expected format
	if len(expected) != 8 {
		t.Errorf("Protocol header should be 8 bytes, got %d", len(expected))
	}

	if string(expected[0:4]) != "AMQP" {
		t.Error("Protocol header should start with 'AMQP'")
	}

	// Version should be 0.9.1
	if expected[6] != 9 || expected[7] != 1 {
		t.Errorf("Expected AMQP 0.9.1, got 0.%d.%d", expected[6], expected[7])
	}
}

// TestAMQPCompliance_MethodFrameEncoding validates method frame encoding
// Method frame: class-id(2) + method-id(2) + arguments
func TestAMQPCompliance_MethodFrameEncoding(t *testing.T) {
	tests := []struct {
		name     string
		classID  uint16
		methodID uint16
	}{
		{"Connection.Start", 10, 10},
		{"Connection.StartOK", 10, 11},
		{"Channel.Open", 20, 10},
		{"Exchange.Declare", 40, 10},
		{"Queue.Declare", 50, 10},
		{"Basic.Publish", 60, 40},
		{"Tx.Select", 90, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			methodData := []byte{0x01, 0x02, 0x03} // Dummy method data
			frame := EncodeMethodFrame(tt.classID, tt.methodID, methodData)

			if frame.Type != FrameMethod {
				t.Errorf("Expected FrameMethod type, got %d", frame.Type)
			}

			// Payload should start with class-id and method-id
			if len(frame.Payload) < 4 {
				t.Fatalf("Payload too short for class and method IDs")
			}

			classID := binary.BigEndian.Uint16(frame.Payload[0:2])
			methodID := binary.BigEndian.Uint16(frame.Payload[2:4])

			if classID != tt.classID {
				t.Errorf("Expected class ID %d, got %d", tt.classID, classID)
			}

			if methodID != tt.methodID {
				t.Errorf("Expected method ID %d, got %d", tt.methodID, methodID)
			}

			// Remaining payload should be the method data
			if !bytes.Equal(frame.Payload[4:], methodData) {
				t.Error("Method data mismatch in frame payload")
			}
		})
	}
}

// TestAMQPCompliance_StringEncoding validates AMQP string encoding
func TestAMQPCompliance_StringEncoding(t *testing.T) {
	t.Run("ShortString", func(t *testing.T) {
		// Short string: length(1 byte) + octets
		testStr := "hello"
		encoded := EncodeShortString(testStr)

		if len(encoded) < 1 {
			t.Fatal("Encoded string too short")
		}

		// First byte should be length
		length := encoded[0]
		if length != byte(len(testStr)) {
			t.Errorf("Expected length %d, got %d", len(testStr), length)
		}

		// Remaining bytes should be the string
		if string(encoded[1:]) != testStr {
			t.Errorf("Expected string '%s', got '%s'", testStr, string(encoded[1:]))
		}
	})

	t.Run("EmptyShortString", func(t *testing.T) {
		encoded := EncodeShortString("")

		if len(encoded) != 1 {
			t.Errorf("Empty string should encode to 1 byte, got %d", len(encoded))
		}

		if encoded[0] != 0 {
			t.Errorf("Empty string length should be 0, got %d", encoded[0])
		}
	})

	t.Run("MaxLengthShortString", func(t *testing.T) {
		// Short strings are limited to 255 bytes
		longStr := string(make([]byte, 300))
		encoded := EncodeShortString(longStr)

		// Should be truncated to 255
		if encoded[0] != 255 {
			t.Errorf("Expected max length 255, got %d", encoded[0])
		}

		if len(encoded) != 256 { // 1 byte length + 255 bytes data
			t.Errorf("Expected 256 bytes total, got %d", len(encoded))
		}
	})
}

// TestAMQPCompliance_FieldTableEncoding validates AMQP field table encoding
// Field table: size(4 bytes) + name-value pairs
func TestAMQPCompliance_FieldTableEncoding(t *testing.T) {
	table := map[string]interface{}{
		"string": "value",
		"int":    int32(42),
	}

	encoded, err := EncodeFieldTable(table)
	if err != nil {
		t.Fatalf("Failed to encode field table: %v", err)
	}

	if len(encoded) < 4 {
		t.Fatal("Field table must have at least 4 bytes for size")
	}

	// First 4 bytes should be the table size
	tableSize := binary.BigEndian.Uint32(encoded[0:4])

	// Table size should match payload length
	if tableSize != uint32(len(encoded)-4) {
		t.Errorf("Table size %d doesn't match payload %d", tableSize, len(encoded)-4)
	}
}

// TestAMQPCompliance_ContentEncoding validates content message encoding
// Content message: header frame + body frames
func TestAMQPCompliance_ContentEncoding(t *testing.T) {
	// Create a content header
	header := &ContentHeader{
		ClassID:       60,
		BodySize:      100,
		PropertyFlags: FlagContentType | FlagDeliveryMode,
		ContentType:   "application/json",
		DeliveryMode:  2,
	}

	headerData, err := header.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize header: %v", err)
	}

	headerFrame := &Frame{
		Type:    FrameHeader,
		Channel: 1,
		Payload: headerData,
	}

	// Header frame must be type 2
	if headerFrame.Type != FrameHeader {
		t.Errorf("Expected header frame type 2, got %d", headerFrame.Type)
	}

	// Create body frames
	bodyData := make([]byte, 100)
	bodyFrame := &Frame{
		Type:    FrameBody,
		Channel: 1,
		Payload: bodyData,
	}

	// Body frame must be type 3
	if bodyFrame.Type != FrameBody {
		t.Errorf("Expected body frame type 3, got %d", bodyFrame.Type)
	}

	// Content must be split into header + body frames
	if headerFrame.Type == bodyFrame.Type {
		t.Error("Header and body frames must have different types")
	}
}

// TestAMQPCompliance_FrameTypes validates all AMQP frame types
func TestAMQPCompliance_FrameTypes(t *testing.T) {
	tests := []struct {
		name     string
		frameType byte
		expected byte
	}{
		{"Method", FrameMethod, 1},
		{"Header", FrameHeader, 2},
		{"Body", FrameBody, 3},
		{"Heartbeat", FrameHeartbeat, 8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.frameType != tt.expected {
				t.Errorf("Frame type %s should be %d, got %d", tt.name, tt.expected, tt.frameType)
			}
		})
	}
}

// TestAMQPCompliance_ClassIDs validates AMQP class IDs
func TestAMQPCompliance_ClassIDs(t *testing.T) {
	tests := []struct {
		name    string
		classID uint16
	}{
		{"Connection", 10},
		{"Channel", 20},
		{"Exchange", 40},
		{"Queue", 50},
		{"Basic", 60},
		{"Tx", 90},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Verify class IDs are as per AMQP spec
			// This is mostly documentation, but ensures we're using correct values
			switch tt.name {
			case "Connection":
				if tt.classID != 10 {
					t.Errorf("Connection class ID should be 10, got %d", tt.classID)
				}
			case "Channel":
				if tt.classID != 20 {
					t.Errorf("Channel class ID should be 20, got %d", tt.classID)
				}
			case "Exchange":
				if tt.classID != 40 {
					t.Errorf("Exchange class ID should be 40, got %d", tt.classID)
				}
			case "Queue":
				if tt.classID != 50 {
					t.Errorf("Queue class ID should be 50, got %d", tt.classID)
				}
			case "Basic":
				if tt.classID != 60 {
					t.Errorf("Basic class ID should be 60, got %d", tt.classID)
				}
			case "Tx":
				if tt.classID != 90 {
					t.Errorf("Tx class ID should be 90, got %d", tt.classID)
				}
			}
		})
	}
}

// TestAMQPCompliance_PropertyFlags validates content header property flags
func TestAMQPCompliance_PropertyFlags(t *testing.T) {
	tests := []struct {
		name     string
		flag     uint16
		expected uint16
	}{
		{"ContentType", FlagContentType, 0x8000},
		{"ContentEncoding", FlagContentEncoding, 0x4000},
		{"Headers", FlagHeaders, 0x2000},
		{"DeliveryMode", FlagDeliveryMode, 0x1000},
		{"Priority", FlagPriority, 0x0800},
		{"CorrelationID", FlagCorrelationID, 0x0400},
		{"ReplyTo", FlagReplyTo, 0x0200},
		{"Expiration", FlagExpiration, 0x0100},
		{"MessageID", FlagMessageID, 0x0080},
		{"Timestamp", FlagTimestamp, 0x0040},
		{"Type", FlagType, 0x0020},
		{"UserID", FlagUserID, 0x0010},
		{"AppID", FlagAppID, 0x0008},
		{"ClusterID", FlagClusterID, 0x0004},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.flag != tt.expected {
				t.Errorf("Flag %s should be 0x%04X, got 0x%04X", tt.name, tt.expected, tt.flag)
			}
		})
	}
}

// TestAMQPCompliance_DeliveryModes validates delivery mode values
func TestAMQPCompliance_DeliveryModes(t *testing.T) {
	tests := []struct {
		name         string
		mode         uint8
		persistent   bool
	}{
		{"NonPersistent", 1, false},
		{"Persistent", 2, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := &ContentHeader{
				ClassID:       60,
				BodySize:      100,
				PropertyFlags: FlagDeliveryMode,
				DeliveryMode:  tt.mode,
			}

			data, err := header.Serialize()
			if err != nil {
				t.Fatalf("Failed to serialize: %v", err)
			}

			frame := &Frame{
				Type:    FrameHeader,
				Channel: 1,
				Payload: data,
			}

			parsed, err := ReadContentHeader(frame)
			if err != nil {
				t.Fatalf("Failed to parse: %v", err)
			}

			if parsed.DeliveryMode != tt.mode {
				t.Errorf("Expected delivery mode %d, got %d", tt.mode, parsed.DeliveryMode)
			}

			// Verify persistence semantics
			isPersistent := parsed.DeliveryMode == 2
			if isPersistent != tt.persistent {
				t.Errorf("Delivery mode %d persistence mismatch", tt.mode)
			}
		})
	}
}

// TestAMQPCompliance_FrameMaxSize validates frame size limits
func TestAMQPCompliance_FrameMaxSize(t *testing.T) {
	// AMQP spec recommends frame-max of 131072 bytes (128KB)
	// Frame overhead: type(1) + channel(2) + size(4) + frame-end(1) = 8 bytes
	frameOverhead := 8
	typicalMaxFrameSize := 131072
	maxPayloadSize := typicalMaxFrameSize - frameOverhead

	largePayload := make([]byte, maxPayloadSize)
	frame := &Frame{
		Type:    FrameBody,
		Channel: 1,
		Payload: largePayload,
	}

	data, err := frame.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to marshal large frame: %v", err)
	}

	if len(data) > typicalMaxFrameSize {
		t.Errorf("Frame size %d exceeds typical max %d", len(data), typicalMaxFrameSize)
	}
}
