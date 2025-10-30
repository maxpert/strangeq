package protocol

import (
	"testing"
	"time"
)

// Test ContentHeader with all 14 properties set
func TestContentHeader_AllProperties(t *testing.T) {
	header := &ContentHeader{
		ClassID:  60, // Basic class
		Weight:   0,
		BodySize: 1024,
		PropertyFlags: FlagContentType | FlagContentEncoding | FlagHeaders |
			FlagDeliveryMode | FlagPriority | FlagCorrelationID | FlagReplyTo |
			FlagExpiration | FlagMessageID | FlagTimestamp | FlagType |
			FlagUserID | FlagAppID | FlagClusterID,
		ContentType:     "application/json",
		ContentEncoding: "utf-8",
		Headers: map[string]interface{}{
			"x-custom": "value",
		},
		DeliveryMode:  2, // Persistent
		Priority:      5,
		CorrelationID: "correlation-123",
		ReplyTo:       "reply-queue",
		Expiration:    "60000",
		MessageID:     "msg-456",
		Timestamp:     uint64(time.Now().Unix()),
		Type:          "order",
		UserID:        "user-789",
		AppID:         "app-001",
		ClusterID:     "cluster-01",
	}

	// Serialize
	data, err := header.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize content header: %v", err)
	}

	// Should have data for all properties
	if len(data) == 0 {
		t.Error("Expected non-empty serialized data")
	}

	// Create frame and read back
	frame := &Frame{
		Type:    FrameHeader,
		Channel: 1,
		Payload: data,
	}

	parsed, err := ReadContentHeader(frame)
	if err != nil {
		t.Fatalf("Failed to parse content header: %v", err)
	}

	// Verify all properties
	if parsed.ClassID != header.ClassID {
		t.Errorf("ClassID mismatch: expected %d, got %d", header.ClassID, parsed.ClassID)
	}
	if parsed.BodySize != header.BodySize {
		t.Errorf("BodySize mismatch: expected %d, got %d", header.BodySize, parsed.BodySize)
	}
	if parsed.ContentType != header.ContentType {
		t.Errorf("ContentType mismatch: expected %s, got %s", header.ContentType, parsed.ContentType)
	}
	if parsed.ContentEncoding != header.ContentEncoding {
		t.Errorf("ContentEncoding mismatch: expected %s, got %s", header.ContentEncoding, parsed.ContentEncoding)
	}
	if parsed.DeliveryMode != header.DeliveryMode {
		t.Errorf("DeliveryMode mismatch: expected %d, got %d", header.DeliveryMode, parsed.DeliveryMode)
	}
	if parsed.Priority != header.Priority {
		t.Errorf("Priority mismatch: expected %d, got %d", header.Priority, parsed.Priority)
	}
	if parsed.CorrelationID != header.CorrelationID {
		t.Errorf("CorrelationID mismatch: expected %s, got %s", header.CorrelationID, parsed.CorrelationID)
	}
	if parsed.ReplyTo != header.ReplyTo {
		t.Errorf("ReplyTo mismatch: expected %s, got %s", header.ReplyTo, parsed.ReplyTo)
	}
	if parsed.Expiration != header.Expiration {
		t.Errorf("Expiration mismatch: expected %s, got %s", header.Expiration, parsed.Expiration)
	}
	if parsed.MessageID != header.MessageID {
		t.Errorf("MessageID mismatch: expected %s, got %s", header.MessageID, parsed.MessageID)
	}
	if parsed.Timestamp != header.Timestamp {
		t.Errorf("Timestamp mismatch: expected %d, got %d", header.Timestamp, parsed.Timestamp)
	}
	if parsed.Type != header.Type {
		t.Errorf("Type mismatch: expected %s, got %s", header.Type, parsed.Type)
	}
	if parsed.UserID != header.UserID {
		t.Errorf("UserID mismatch: expected %s, got %s", header.UserID, parsed.UserID)
	}
	if parsed.AppID != header.AppID {
		t.Errorf("AppID mismatch: expected %s, got %s", header.AppID, parsed.AppID)
	}
	if parsed.ClusterID != header.ClusterID {
		t.Errorf("ClusterID mismatch: expected %s, got %s", header.ClusterID, parsed.ClusterID)
	}
}

// Test ContentHeader with no properties (minimal)
func TestContentHeader_EmptyProperties(t *testing.T) {
	header := &ContentHeader{
		ClassID:  60,
		Weight:   0,
		BodySize: 100,
	}

	data, err := header.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize empty content header: %v", err)
	}

	// Minimum size: class(2) + weight(2) + body-size(8) + flags(2) = 14 bytes
	if len(data) < 14 {
		t.Errorf("Expected at least 14 bytes, got %d", len(data))
	}

	frame := &Frame{
		Type:    FrameHeader,
		Channel: 1,
		Payload: data,
	}

	parsed, err := ReadContentHeader(frame)
	if err != nil {
		t.Fatalf("Failed to parse empty content header: %v", err)
	}

	if parsed.PropertyFlags != 0 {
		t.Errorf("Expected PropertyFlags 0 for empty header, got 0x%04x", parsed.PropertyFlags)
	}
}

// Test individual property flags
func TestContentHeader_PropertyFlags(t *testing.T) {
	tests := []struct {
		name          string
		header        *ContentHeader
		expectedFlag  uint16
		propertyName  string
		propertyValue interface{}
	}{
		{
			name: "ContentType",
			header: &ContentHeader{
				ClassID:       60,
				BodySize:      100,
				PropertyFlags: FlagContentType,
				ContentType:   "text/plain",
			},
			expectedFlag:  FlagContentType,
			propertyName:  "ContentType",
			propertyValue: "text/plain",
		},
		{
			name: "ContentEncoding",
			header: &ContentHeader{
				ClassID:         60,
				BodySize:        100,
				PropertyFlags:   FlagContentEncoding,
				ContentEncoding: "gzip",
			},
			expectedFlag:  FlagContentEncoding,
			propertyName:  "ContentEncoding",
			propertyValue: "gzip",
		},
		{
			name: "DeliveryMode",
			header: &ContentHeader{
				ClassID:       60,
				BodySize:      100,
				PropertyFlags: FlagDeliveryMode,
				DeliveryMode:  2,
			},
			expectedFlag:  FlagDeliveryMode,
			propertyName:  "DeliveryMode",
			propertyValue: uint8(2),
		},
		{
			name: "Priority",
			header: &ContentHeader{
				ClassID:       60,
				BodySize:      100,
				PropertyFlags: FlagPriority,
				Priority:      9,
			},
			expectedFlag:  FlagPriority,
			propertyName:  "Priority",
			propertyValue: uint8(9),
		},
		{
			name: "CorrelationID",
			header: &ContentHeader{
				ClassID:       60,
				BodySize:      100,
				PropertyFlags: FlagCorrelationID,
				CorrelationID: "corr-123",
			},
			expectedFlag:  FlagCorrelationID,
			propertyName:  "CorrelationID",
			propertyValue: "corr-123",
		},
		{
			name: "ReplyTo",
			header: &ContentHeader{
				ClassID:       60,
				BodySize:      100,
				PropertyFlags: FlagReplyTo,
				ReplyTo:       "reply-queue",
			},
			expectedFlag:  FlagReplyTo,
			propertyName:  "ReplyTo",
			propertyValue: "reply-queue",
		},
		{
			name: "Expiration",
			header: &ContentHeader{
				ClassID:       60,
				BodySize:      100,
				PropertyFlags: FlagExpiration,
				Expiration:    "30000",
			},
			expectedFlag:  FlagExpiration,
			propertyName:  "Expiration",
			propertyValue: "30000",
		},
		{
			name: "MessageID",
			header: &ContentHeader{
				ClassID:       60,
				BodySize:      100,
				PropertyFlags: FlagMessageID,
				MessageID:     "msg-789",
			},
			expectedFlag:  FlagMessageID,
			propertyName:  "MessageID",
			propertyValue: "msg-789",
		},
		{
			name: "Timestamp",
			header: &ContentHeader{
				ClassID:       60,
				BodySize:      100,
				PropertyFlags: FlagTimestamp,
				Timestamp:     1609459200,
			},
			expectedFlag:  FlagTimestamp,
			propertyName:  "Timestamp",
			propertyValue: uint64(1609459200),
		},
		{
			name: "Type",
			header: &ContentHeader{
				ClassID:       60,
				BodySize:      100,
				PropertyFlags: FlagType,
				Type:          "order",
			},
			expectedFlag:  FlagType,
			propertyName:  "Type",
			propertyValue: "order",
		},
		{
			name: "UserID",
			header: &ContentHeader{
				ClassID:       60,
				BodySize:      100,
				PropertyFlags: FlagUserID,
				UserID:        "user123",
			},
			expectedFlag:  FlagUserID,
			propertyName:  "UserID",
			propertyValue: "user123",
		},
		{
			name: "AppID",
			header: &ContentHeader{
				ClassID:       60,
				BodySize:      100,
				PropertyFlags: FlagAppID,
				AppID:         "app001",
			},
			expectedFlag:  FlagAppID,
			propertyName:  "AppID",
			propertyValue: "app001",
		},
		{
			name: "ClusterID",
			header: &ContentHeader{
				ClassID:       60,
				BodySize:      100,
				PropertyFlags: FlagClusterID,
				ClusterID:     "cluster1",
			},
			expectedFlag:  FlagClusterID,
			propertyName:  "ClusterID",
			propertyValue: "cluster1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := tt.header.Serialize()
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

			// Check that the expected flag is set
			if parsed.PropertyFlags&tt.expectedFlag == 0 {
				t.Errorf("Expected flag 0x%04x to be set, but PropertyFlags is 0x%04x",
					tt.expectedFlag, parsed.PropertyFlags)
			}
		})
	}
}

// Test Headers field table property
func TestContentHeader_HeadersFieldTable(t *testing.T) {
	header := &ContentHeader{
		ClassID:       60,
		BodySize:      100,
		PropertyFlags: FlagHeaders,
		Headers: map[string]interface{}{
			"x-priority":    int32(5),
			"x-retry-count": int32(3),
			"x-custom":      "value",
		},
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

	if parsed.PropertyFlags&FlagHeaders == 0 {
		t.Error("Expected FlagHeaders to be set")
	}

	if len(parsed.Headers) == 0 {
		t.Error("Expected non-empty headers map")
	}
}

// Test DeliveryMode values (transient vs persistent)
func TestContentHeader_DeliveryModes(t *testing.T) {
	tests := []struct {
		name         string
		deliveryMode uint8
		description  string
	}{
		{"Transient", 1, "Non-persistent message"},
		{"Persistent", 2, "Persistent message"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := &ContentHeader{
				ClassID:       60,
				BodySize:      100,
				PropertyFlags: FlagDeliveryMode,
				DeliveryMode:  tt.deliveryMode,
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

			if parsed.DeliveryMode != tt.deliveryMode {
				t.Errorf("Expected delivery mode %d, got %d", tt.deliveryMode, parsed.DeliveryMode)
			}
		})
	}
}

// Test Priority range (0-9)
func TestContentHeader_PriorityRange(t *testing.T) {
	for priority := uint8(0); priority <= 9; priority++ {
		t.Run("", func(t *testing.T) {
			header := &ContentHeader{
				ClassID:       60,
				BodySize:      100,
				PropertyFlags: FlagPriority,
				Priority:      priority,
			}

			data, err := header.Serialize()
			if err != nil {
				t.Fatalf("Failed to serialize priority %d: %v", priority, err)
			}

			frame := &Frame{
				Type:    FrameHeader,
				Channel: 1,
				Payload: data,
			}

			parsed, err := ReadContentHeader(frame)
			if err != nil {
				t.Fatalf("Failed to parse priority %d: %v", priority, err)
			}

			if parsed.Priority != priority {
				t.Errorf("Priority mismatch: expected %d, got %d", priority, parsed.Priority)
			}
		})
	}
}

// Test ReadContentHeader with wrong frame type
func TestReadContentHeader_WrongFrameType(t *testing.T) {
	frame := &Frame{
		Type:    FrameMethod, // Wrong type
		Channel: 1,
		Payload: []byte{},
	}

	_, err := ReadContentHeader(frame)
	if err == nil {
		t.Error("Expected error for wrong frame type, got nil")
	}
}

// Test ReadContentHeader with truncated data
func TestReadContentHeader_TruncatedData(t *testing.T) {
	frame := &Frame{
		Type:    FrameHeader,
		Channel: 1,
		Payload: []byte{0x00, 0x3C}, // Only 2 bytes, need at least 12
	}

	_, err := ReadContentHeader(frame)
	if err == nil {
		t.Error("Expected error for truncated data, got nil")
	}
}

// Test round-trip with various BodySize values
func TestContentHeader_BodySizeValues(t *testing.T) {
	tests := []struct {
		name     string
		bodySize uint64
	}{
		{"Zero", 0},
		{"Small", 100},
		{"Medium", 65536},
		{"Large", 1048576},
		{"VeryLarge", 1073741824},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := &ContentHeader{
				ClassID:  60,
				BodySize: tt.bodySize,
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

			if parsed.BodySize != tt.bodySize {
				t.Errorf("BodySize mismatch: expected %d, got %d", tt.bodySize, parsed.BodySize)
			}
		})
	}
}
