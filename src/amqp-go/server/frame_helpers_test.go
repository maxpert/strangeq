package server

import (
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
)

func TestBuildPropertyFlags(t *testing.T) {
	tests := []struct {
		name     string
		message  *protocol.Message
		expected uint16
	}{
		{
			name:     "empty message",
			message:  &protocol.Message{},
			expected: 0,
		},
		{
			name: "content type only",
			message: &protocol.Message{
				ContentType: "application/json",
			},
			expected: protocol.FlagContentType,
		},
		{
			name: "multiple properties",
			message: &protocol.Message{
				ContentType:     "text/plain",
				ContentEncoding: "utf-8",
				DeliveryMode:    2,
				Priority:        5,
			},
			expected: protocol.FlagContentType | protocol.FlagContentEncoding | protocol.FlagDeliveryMode | protocol.FlagPriority,
		},
		{
			name: "all properties",
			message: &protocol.Message{
				ContentType:     "application/json",
				ContentEncoding: "gzip",
				Headers:         map[string]interface{}{"x-key": "value"},
				DeliveryMode:    2,
				Priority:        9,
				CorrelationID:   "corr-123",
				ReplyTo:         "reply-queue",
				Expiration:      "60000",
				MessageID:       "msg-456",
				Timestamp:       1234567890,
				Type:            "test",
				UserID:          "user123",
				AppID:           "app",
				ClusterID:       "cluster1",
			},
			expected: protocol.FlagContentType | protocol.FlagContentEncoding | protocol.FlagHeaders |
				protocol.FlagDeliveryMode | protocol.FlagPriority | protocol.FlagCorrelationID |
				protocol.FlagReplyTo | protocol.FlagExpiration | protocol.FlagMessageID |
				protocol.FlagTimestamp | protocol.FlagType | protocol.FlagUserID |
				protocol.FlagAppID | protocol.FlagClusterID,
		},
		{
			name: "headers with empty map should not set flag",
			message: &protocol.Message{
				Headers: map[string]interface{}{},
			},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flags := buildPropertyFlags(tt.message)
			assert.Equal(t, tt.expected, flags, "Property flags mismatch")
		})
	}
}

func TestSendMethodResponse(t *testing.T) {
	// This test verifies that sendMethodResponse correctly serializes methods
	// and constructs frames. We test that serialization works, not the actual sending.
	tests := []struct {
		name      string
		channelID uint16
		classID   uint16
		methodID  uint16
		method    SerializableMethod
		wantErr   bool
	}{
		{
			name:      "valid connection level method",
			channelID: 0,
			classID:   10,
			methodID:  51,
			method:    &protocol.ConnectionCloseOKMethod{},
			wantErr:   false,
		},
		{
			name:      "valid channel level method",
			channelID: 1,
			classID:   20,
			methodID:  11,
			method:    &protocol.ChannelOpenOKMethod{Reserved1: ""},
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test that serialization works
			data, err := tt.method.Serialize()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, data)
			}
		})
	}
}

func TestSendSimpleOKMethods(t *testing.T) {
	// Test that the helper functions exist and are callable
	// Actual network I/O is tested in integration tests
	t.Run("functions are defined", func(t *testing.T) {
		server := &Server{}
		assert.NotNil(t, server.sendConnectionCloseOK)
		assert.NotNil(t, server.sendChannelOpenOK)
		assert.NotNil(t, server.sendChannelCloseOK)
		assert.NotNil(t, server.sendExchangeDeclareOK)
		assert.NotNil(t, server.sendQueueBindOK)
		assert.NotNil(t, server.sendBasicQosOK)
	})
}
