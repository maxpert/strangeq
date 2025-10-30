package protocol

import (
	"bytes"
	"testing"
)

// Helper function to test method serialization
func testMethodSerialization(t *testing.T, name string, classID, methodID uint16, serialize func() ([]byte, error)) {
	t.Helper()

	methodData, err := serialize()
	if err != nil {
		t.Fatalf("%s: Failed to serialize: %v", name, err)
	}

	// Create a method frame with class and method IDs
	frame := EncodeMethodFrame(classID, methodID, methodData)

	// Verify frame payload has class and method IDs at the start
	if len(frame.Payload) < 4 {
		t.Fatalf("%s: Frame payload too small, expected at least 4 bytes, got %d", name, len(frame.Payload))
	}

	// Check class ID
	actualClassID := uint16(frame.Payload[0])<<8 | uint16(frame.Payload[1])
	if actualClassID != classID {
		t.Errorf("%s: Expected class ID %d, got %d", name, classID, actualClassID)
	}

	// Check method ID
	actualMethodID := uint16(frame.Payload[2])<<8 | uint16(frame.Payload[3])
	if actualMethodID != methodID {
		t.Errorf("%s: Expected method ID %d, got %d", name, methodID, actualMethodID)
	}
}

// Test all Connection class methods
func TestConnectionMethodsSerialization(t *testing.T) {
	t.Run("ConnectionStart", func(t *testing.T) {
		method := &ConnectionStartMethod{
			VersionMajor: 0,
			VersionMinor: 9,
			ServerProperties: map[string]interface{}{
				"product": "AMQP-Go",
			},
			Mechanisms: []string{"PLAIN"},
			Locales:    []string{"en_US"},
		}
		testMethodSerialization(t, "ConnectionStart", 10, 10, method.Serialize)
	})

	t.Run("ConnectionStartOK", func(t *testing.T) {
		method := &ConnectionStartOKMethod{
			ClientProperties: map[string]interface{}{
				"product": "TestClient",
			},
			Mechanism: "PLAIN",
			Response:  []byte("\x00guest\x00guest"),
			Locale:    "en_US",
		}
		testMethodSerialization(t, "ConnectionStartOK", 10, 11, method.Serialize)
	})

	t.Run("ConnectionTune", func(t *testing.T) {
		method := &ConnectionTuneMethod{
			ChannelMax: 2047,
			FrameMax:   131072,
			Heartbeat:  60,
		}
		testMethodSerialization(t, "ConnectionTune", 10, 30, method.Serialize)
	})

	t.Run("ConnectionTuneOK", func(t *testing.T) {
		method := &ConnectionTuneOKMethod{
			ChannelMax: 2047,
			FrameMax:   131072,
			Heartbeat:  60,
		}
		testMethodSerialization(t, "ConnectionTuneOK", 10, 31, method.Serialize)
	})

	t.Run("ConnectionOpen", func(t *testing.T) {
		method := &ConnectionOpenMethod{
			VirtualHost: "/",
			Reserved1:   "",
			Reserved2:   0,
		}
		testMethodSerialization(t, "ConnectionOpen", 10, 40, method.Serialize)
	})

	t.Run("ConnectionOpenOK", func(t *testing.T) {
		method := &ConnectionOpenOKMethod{
			Reserved1: "",
		}
		testMethodSerialization(t, "ConnectionOpenOK", 10, 41, method.Serialize)
	})

	t.Run("ConnectionClose", func(t *testing.T) {
		method := &ConnectionCloseMethod{
			ReplyCode: 200,
			ReplyText: "Normal shutdown",
			ClassID:   0,
			MethodID:  0,
		}
		testMethodSerialization(t, "ConnectionClose", 10, 50, method.Serialize)
	})

	t.Run("ConnectionCloseOK", func(t *testing.T) {
		method := &ConnectionCloseOKMethod{}
		testMethodSerialization(t, "ConnectionCloseOK", 10, 51, method.Serialize)
	})
}

// Test all Channel class methods
func TestChannelMethodsSerialization(t *testing.T) {
	t.Run("ChannelOpen", func(t *testing.T) {
		method := &ChannelOpenMethod{
			Reserved1: "",
		}
		testMethodSerialization(t, "ChannelOpen", 20, 10, method.Serialize)
	})

	t.Run("ChannelOpenOK", func(t *testing.T) {
		method := &ChannelOpenOKMethod{
			Reserved1: "",
		}
		testMethodSerialization(t, "ChannelOpenOK", 20, 11, method.Serialize)
	})

	t.Run("ChannelClose", func(t *testing.T) {
		method := &ChannelCloseMethod{
			ReplyCode: 200,
			ReplyText: "Normal shutdown",
			ClassID:   0,
			MethodID:  0,
		}
		testMethodSerialization(t, "ChannelClose", 20, 40, method.Serialize)
	})

	t.Run("ChannelCloseOK", func(t *testing.T) {
		method := &ChannelCloseOKMethod{}
		testMethodSerialization(t, "ChannelCloseOK", 20, 41, method.Serialize)
	})
}

// Test field table encoding with various types
func TestFieldTableEncodingSimplified(t *testing.T) {
	tests := []struct {
		name  string
		table map[string]interface{}
	}{
		{"empty", map[string]interface{}{}},
		{"strings", map[string]interface{}{
			"key1": "value1",
			"key2": "value2",
		}},
		{"numbers", map[string]interface{}{
			"int":  int32(42),
			"long": int64(123456),
		}},
		{"bool", map[string]interface{}{
			"flag": true,
		}},
		{"mixed", map[string]interface{}{
			"string": "test",
			"number": int32(100),
			"bool":   true,
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := EncodeFieldTable(tt.table)
			if err != nil {
				t.Fatalf("Failed to encode: %v", err)
			}

			// Empty tables should have at least the size prefix (4 bytes)
			if len(data) < 4 {
				t.Errorf("Expected at least 4 bytes, got %d", len(data))
			}
		})
	}
}

// Test ParseConnectionStartOK
func TestParseConnectionStartOKSimplified(t *testing.T) {
	method := &ConnectionStartOKMethod{
		ClientProperties: map[string]interface{}{
			"product": "TestClient",
		},
		Mechanism: "PLAIN",
		Response:  []byte("\x00guest\x00guest"),
		Locale:    "en_US",
	}

	methodData, err := method.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize: %v", err)
	}

	frame := EncodeMethodFrame(10, 11, methodData)

	// Skip class ID and method ID (4 bytes) from frame payload
	payload := frame.Payload[4:]

	parsed, err := ParseConnectionStartOK(payload)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	if parsed.Mechanism != "PLAIN" {
		t.Errorf("Expected mechanism 'PLAIN', got '%s'", parsed.Mechanism)
	}

	if parsed.Locale != "en_US" {
		t.Errorf("Expected locale 'en_US', got '%s'", parsed.Locale)
	}

	if !bytes.Equal(parsed.Response, method.Response) {
		t.Errorf("Response mismatch")
	}
}

// Test string encoding
func TestStringEncodingSimplified(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"short", "test"},
		{"medium", "Hello, World!"},
		{"unicode", "Hello 世界 🌍"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := EncodeShortString(tt.input)

			// Should have at least length byte
			if len(data) < 1 {
				t.Errorf("Expected at least 1 byte, got %d", len(data))
			}

			// Length byte should match (truncated to 255 if needed)
			expectedLen := len(tt.input)
			if expectedLen > 255 {
				expectedLen = 255
			}
			if data[0] != byte(expectedLen) {
				t.Errorf("Expected length %d, got %d", expectedLen, data[0])
			}
		})
	}
}

// Test all Exchange class methods
func TestExchangeMethodsSerialization(t *testing.T) {
	t.Run("ExchangeDeclare", func(t *testing.T) {
		method := &ExchangeDeclareMethod{
			Reserved1:  0,
			Exchange:   "test-exchange",
			Type:       "direct",
			Passive:    false,
			Durable:    true,
			AutoDelete: false,
			Internal:   false,
			NoWait:     false,
			Arguments:  map[string]interface{}{},
		}
		testMethodSerialization(t, "ExchangeDeclare", 40, 10, method.Serialize)
	})

	t.Run("ExchangeDeclareOK", func(t *testing.T) {
		method := &ExchangeDeclareOKMethod{}
		testMethodSerialization(t, "ExchangeDeclareOK", 40, 11, method.Serialize)
	})

	t.Run("ExchangeDelete", func(t *testing.T) {
		method := &ExchangeDeleteMethod{
			Reserved1: 0,
			Exchange:  "test-exchange",
			IfUnused:  false,
			NoWait:    false,
		}
		testMethodSerialization(t, "ExchangeDelete", 40, 20, method.Serialize)
	})

	t.Run("ExchangeDeleteOK", func(t *testing.T) {
		method := &ExchangeDeleteOKMethod{}
		testMethodSerialization(t, "ExchangeDeleteOK", 40, 21, method.Serialize)
	})
}

// Test all Queue class methods
func TestQueueMethodsSerialization(t *testing.T) {
	t.Run("QueueDeclare", func(t *testing.T) {
		method := &QueueDeclareMethod{
			Reserved1:  0,
			Queue:      "test-queue",
			Passive:    false,
			Durable:    true,
			Exclusive:  false,
			AutoDelete: false,
			NoWait:     false,
			Arguments:  map[string]interface{}{},
		}
		testMethodSerialization(t, "QueueDeclare", 50, 10, method.Serialize)
	})

	t.Run("QueueDeclareOK", func(t *testing.T) {
		method := &QueueDeclareOKMethod{
			Queue:         "test-queue",
			MessageCount:  0,
			ConsumerCount: 0,
		}
		testMethodSerialization(t, "QueueDeclareOK", 50, 11, method.Serialize)
	})

	t.Run("QueueBind", func(t *testing.T) {
		method := &QueueBindMethod{
			Reserved1:  0,
			Queue:      "test-queue",
			Exchange:   "test-exchange",
			RoutingKey: "test.key",
			NoWait:     false,
			Arguments:  map[string]interface{}{},
		}
		testMethodSerialization(t, "QueueBind", 50, 20, method.Serialize)
	})

	t.Run("QueueBindOK", func(t *testing.T) {
		method := &QueueBindOKMethod{}
		testMethodSerialization(t, "QueueBindOK", 50, 21, method.Serialize)
	})

	t.Run("QueueDelete", func(t *testing.T) {
		method := &QueueDeleteMethod{
			Reserved1: 0,
			Queue:     "test-queue",
			IfUnused:  false,
			IfEmpty:   false,
			NoWait:    false,
		}
		testMethodSerialization(t, "QueueDelete", 50, 40, method.Serialize)
	})

	t.Run("QueueDeleteOK", func(t *testing.T) {
		method := &QueueDeleteOKMethod{
			MessageCount: 5,
		}
		testMethodSerialization(t, "QueueDeleteOK", 50, 41, method.Serialize)
	})
}

// Test all Basic class methods
func TestBasicMethodsSerialization(t *testing.T) {
	t.Run("BasicQos", func(t *testing.T) {
		method := &BasicQosMethod{
			PrefetchSize:  0,
			PrefetchCount: 10,
			Global:        false,
		}
		testMethodSerialization(t, "BasicQos", 60, 10, method.Serialize)
	})

	t.Run("BasicQosOK", func(t *testing.T) {
		method := &BasicQosOKMethod{}
		testMethodSerialization(t, "BasicQosOK", 60, 11, method.Serialize)
	})

	t.Run("BasicConsume", func(t *testing.T) {
		method := &BasicConsumeMethod{
			Reserved1:   0,
			Queue:       "test-queue",
			ConsumerTag: "consumer-1",
			NoLocal:     false,
			NoAck:       false,
			Exclusive:   false,
			NoWait:      false,
			Arguments:   map[string]interface{}{},
		}
		testMethodSerialization(t, "BasicConsume", 60, 20, method.Serialize)
	})

	t.Run("BasicConsumeOK", func(t *testing.T) {
		method := &BasicConsumeOKMethod{
			ConsumerTag: "consumer-1",
		}
		testMethodSerialization(t, "BasicConsumeOK", 60, 21, method.Serialize)
	})

	t.Run("BasicCancel", func(t *testing.T) {
		method := &BasicCancelMethod{
			ConsumerTag: "consumer-1",
			NoWait:      false,
		}
		testMethodSerialization(t, "BasicCancel", 60, 30, method.Serialize)
	})

	t.Run("BasicCancelOK", func(t *testing.T) {
		method := &BasicCancelOKMethod{
			ConsumerTag: "consumer-1",
		}
		testMethodSerialization(t, "BasicCancelOK", 60, 31, method.Serialize)
	})

	t.Run("BasicPublish", func(t *testing.T) {
		method := &BasicPublishMethod{
			Reserved1:  0,
			Exchange:   "test-exchange",
			RoutingKey: "test.key",
			Mandatory:  false,
			Immediate:  false,
		}
		testMethodSerialization(t, "BasicPublish", 60, 40, method.Serialize)
	})

	t.Run("BasicDeliver", func(t *testing.T) {
		method := &BasicDeliverMethod{
			ConsumerTag: "consumer-1",
			DeliveryTag: 1,
			Redelivered: false,
			Exchange:    "test-exchange",
			RoutingKey:  "test.key",
		}
		testMethodSerialization(t, "BasicDeliver", 60, 60, method.Serialize)
	})

	t.Run("BasicGet", func(t *testing.T) {
		method := &BasicGetMethod{
			Reserved1: 0,
			Queue:     "test-queue",
			NoAck:     false,
		}
		testMethodSerialization(t, "BasicGet", 60, 70, method.Serialize)
	})

	t.Run("BasicGetOK", func(t *testing.T) {
		method := &BasicGetOKMethod{
			DeliveryTag:  1,
			Redelivered:  false,
			Exchange:     "test-exchange",
			RoutingKey:   "test.key",
			MessageCount: 10,
		}
		testMethodSerialization(t, "BasicGetOK", 60, 71, method.Serialize)
	})

	t.Run("BasicGetEmpty", func(t *testing.T) {
		method := &BasicGetEmptyMethod{
			Reserved1: "",
		}
		testMethodSerialization(t, "BasicGetEmpty", 60, 72, method.Serialize)
	})

	t.Run("BasicAck", func(t *testing.T) {
		method := &BasicAckMethod{
			DeliveryTag: 1,
			Multiple:    false,
		}
		testMethodSerialization(t, "BasicAck", 60, 80, method.Serialize)
	})

	t.Run("BasicReject", func(t *testing.T) {
		method := &BasicRejectMethod{
			DeliveryTag: 1,
			Requeue:     true,
		}
		testMethodSerialization(t, "BasicReject", 60, 90, method.Serialize)
	})

	t.Run("BasicNack", func(t *testing.T) {
		method := &BasicNackMethod{
			DeliveryTag: 1,
			Multiple:    false,
			Requeue:     true,
		}
		testMethodSerialization(t, "BasicNack", 60, 120, method.Serialize)
	})
}

// Test all Transaction class methods
func TestTransactionMethodsSerialization(t *testing.T) {
	// Tx methods return []byte instead of ([]byte, error), so we need a wrapper
	wrapTxSerialize := func(serialize func() []byte) func() ([]byte, error) {
		return func() ([]byte, error) {
			return serialize(), nil
		}
	}

	t.Run("TxSelect", func(t *testing.T) {
		method := &TxSelectMethod{}
		testMethodSerialization(t, "TxSelect", 90, 10, wrapTxSerialize(method.Serialize))
	})

	t.Run("TxSelectOK", func(t *testing.T) {
		method := &TxSelectOKMethod{}
		testMethodSerialization(t, "TxSelectOK", 90, 11, wrapTxSerialize(method.Serialize))
	})

	t.Run("TxCommit", func(t *testing.T) {
		method := &TxCommitMethod{}
		testMethodSerialization(t, "TxCommit", 90, 20, wrapTxSerialize(method.Serialize))
	})

	t.Run("TxCommitOK", func(t *testing.T) {
		method := &TxCommitOKMethod{}
		testMethodSerialization(t, "TxCommitOK", 90, 21, wrapTxSerialize(method.Serialize))
	})

	t.Run("TxRollback", func(t *testing.T) {
		method := &TxRollbackMethod{}
		testMethodSerialization(t, "TxRollback", 90, 30, wrapTxSerialize(method.Serialize))
	})

	t.Run("TxRollbackOK", func(t *testing.T) {
		method := &TxRollbackOKMethod{}
		testMethodSerialization(t, "TxRollbackOK", 90, 31, wrapTxSerialize(method.Serialize))
	})
}
