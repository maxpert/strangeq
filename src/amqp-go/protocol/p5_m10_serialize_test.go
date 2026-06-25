package protocol

import (
	"encoding/hex"
	"testing"
)

// sink prevents the compiler from dead-store-eliminating Serialize() allocations.
var sink []byte

// TestP5_ContentHeaderSerializeCorrectness verifies that the optimized
// ContentHeader.Serialize() produces byte-for-byte identical output to
// the expected AMQP 0.9.1 wire format.
func TestP5_ContentHeaderSerializeCorrectness(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		header *ContentHeader
	}{
		{
			name: "minimal (no properties)",
			header: &ContentHeader{
				ClassID:       60,
				Weight:        0,
				BodySize:      1024,
				PropertyFlags: 0,
			},
		},
		{
			name: "all properties",
			header: &ContentHeader{
				ClassID:         60,
				Weight:          0,
				BodySize:        4096,
				PropertyFlags:   FlagContentType | FlagContentEncoding | FlagHeaders | FlagDeliveryMode | FlagPriority | FlagCorrelationID | FlagReplyTo | FlagExpiration | FlagMessageID | FlagTimestamp | FlagType | FlagUserID | FlagAppID | FlagClusterID,
				ContentType:     "text/plain",
				ContentEncoding: "utf-8",
				Headers:         map[string]interface{}{"key1": "val1"},
				DeliveryMode:    2,
				Priority:        5,
				CorrelationID:   "corr-123",
				ReplyTo:         "reply-queue",
				Expiration:      "60000",
				MessageID:       "msg-456",
				Timestamp:       1700000000,
				Type:            "test-type",
				UserID:          "guest",
				AppID:           "test-app",
				ClusterID:       "",
			},
		},
		{
			name: "only timestamp",
			header: &ContentHeader{
				ClassID:       60,
				Weight:        0,
				BodySize:      0,
				PropertyFlags: FlagTimestamp,
				Timestamp:     9999999999,
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			data, err := tc.header.Serialize()
			if err != nil {
				t.Fatalf("Serialize() error: %v", err)
			}

			// Round-trip: deserialize and verify ALL fields match
			frame := &Frame{
				Type:    FrameHeader,
				Channel: 1,
				Size:    uint32(len(data)),
				Payload: data,
			}
			parsed, err := ReadContentHeader(frame)
			if err != nil {
				t.Fatalf("ReadContentHeader() error: %v", err)
			}

			if parsed.ClassID != tc.header.ClassID {
				t.Errorf("ClassID: got %d, want %d", parsed.ClassID, tc.header.ClassID)
			}
			if parsed.Weight != tc.header.Weight {
				t.Errorf("Weight: got %d, want %d", parsed.Weight, tc.header.Weight)
			}
			if parsed.BodySize != tc.header.BodySize {
				t.Errorf("BodySize: got %d, want %d", parsed.BodySize, tc.header.BodySize)
			}
			if parsed.PropertyFlags != tc.header.PropertyFlags {
				t.Errorf("PropertyFlags: got %d, want %d", parsed.PropertyFlags, tc.header.PropertyFlags)
			}
			if parsed.ContentType != tc.header.ContentType {
				t.Errorf("ContentType: got %q, want %q", parsed.ContentType, tc.header.ContentType)
			}
			if parsed.ContentEncoding != tc.header.ContentEncoding {
				t.Errorf("ContentEncoding: got %q, want %q", parsed.ContentEncoding, tc.header.ContentEncoding)
			}
			if parsed.DeliveryMode != tc.header.DeliveryMode {
				t.Errorf("DeliveryMode: got %d, want %d", parsed.DeliveryMode, tc.header.DeliveryMode)
			}
			if parsed.Priority != tc.header.Priority {
				t.Errorf("Priority: got %d, want %d", parsed.Priority, tc.header.Priority)
			}
			if parsed.CorrelationID != tc.header.CorrelationID {
				t.Errorf("CorrelationID: got %q, want %q", parsed.CorrelationID, tc.header.CorrelationID)
			}
			if parsed.ReplyTo != tc.header.ReplyTo {
				t.Errorf("ReplyTo: got %q, want %q", parsed.ReplyTo, tc.header.ReplyTo)
			}
			if parsed.Expiration != tc.header.Expiration {
				t.Errorf("Expiration: got %q, want %q", parsed.Expiration, tc.header.Expiration)
			}
			if parsed.MessageID != tc.header.MessageID {
				t.Errorf("MessageID: got %q, want %q", parsed.MessageID, tc.header.MessageID)
			}
			if parsed.Timestamp != tc.header.Timestamp {
				t.Errorf("Timestamp: got %d, want %d", parsed.Timestamp, tc.header.Timestamp)
			}
			if parsed.Type != tc.header.Type {
				t.Errorf("Type: got %q, want %q", parsed.Type, tc.header.Type)
			}
			if parsed.UserID != tc.header.UserID {
				t.Errorf("UserID: got %q, want %q", parsed.UserID, tc.header.UserID)
			}
			if parsed.AppID != tc.header.AppID {
				t.Errorf("AppID: got %q, want %q", parsed.AppID, tc.header.AppID)
			}
			if parsed.ClusterID != tc.header.ClusterID {
				t.Errorf("ClusterID: got %q, want %q", parsed.ClusterID, tc.header.ClusterID)
			}
		})
	}
}

// TestP5_ContentHeaderSerializeGoldenBytes verifies the wire format against
// known-good AMQP 0.9.1 bytes to catch any serialization regression independent
// of the deserializer.
func TestP5_ContentHeaderSerializeGoldenBytes(t *testing.T) {
	t.Parallel()

	// Minimal header: classID=60, weight=0, bodySize=1024, propFlags=0
	// Wire: 003c 0000 0000000000000400 0000
	minimal := &ContentHeader{ClassID: 60, Weight: 0, BodySize: 1024, PropertyFlags: 0}
	data, err := minimal.Serialize()
	if err != nil {
		t.Fatalf("Serialize error: %v", err)
	}
	expected := "003c000000000000000004000000"
	if got := hex.EncodeToString(data); got != expected {
		t.Errorf("minimal golden bytes: got %s, want %s", got, expected)
	}

	// Timestamp-only header: classID=60, weight=0, bodySize=0, propFlags=0x0040, ts=9999999999
	// Wire: 003c 0000 0000000000000000 0040 00000002540be3ff
	tsHeader := &ContentHeader{ClassID: 60, Weight: 0, BodySize: 0, PropertyFlags: FlagTimestamp, Timestamp: 9999999999}
	data, err = tsHeader.Serialize()
	if err != nil {
		t.Fatalf("Serialize error: %v", err)
	}
	expected = "003c00000000000000000000004000000002540be3ff"
	if got := hex.EncodeToString(data); got != expected {
		t.Errorf("timestamp golden bytes: got %s, want %s", got, expected)
	}
}

// TestP5_ContentHeaderSerializeAllocs verifies that Serialize() eliminates
// the temporary []byte allocations for fixed-width fields. With AppendUint*
// and exact pre-allocation, the only allocation should be the result slice.
func TestP5_ContentHeaderSerializeAllocs(t *testing.T) {
	header := &ContentHeader{
		ClassID:       60,
		Weight:        0,
		BodySize:      1024,
		PropertyFlags: 0,
	}

	allocs := testing.AllocsPerRun(100, func() {
		data, _ := header.Serialize()
		sink = data
	})

	// Only allocation should be the result slice.
	if allocs > 1 {
		t.Errorf("ContentHeader.Serialize() with no properties: got %.0f allocs, want ≤1", allocs)
	}
}

// TestP5_ContentHeaderSerializeWithTimestampAllocs verifies that the timestamp
// field also uses AppendUint64 with zero extra allocations.
func TestP5_ContentHeaderSerializeWithTimestampAllocs(t *testing.T) {
	header := &ContentHeader{
		ClassID:       60,
		Weight:        0,
		BodySize:      1024,
		PropertyFlags: FlagTimestamp,
		Timestamp:     1700000000,
	}

	allocs := testing.AllocsPerRun(100, func() {
		data, _ := header.Serialize()
		sink = data
	})

	if allocs > 1 {
		t.Errorf("ContentHeader.Serialize() with timestamp: got %.0f allocs, want ≤1", allocs)
	}
}

// TestM10_BasicDeliverSerializeCorrectness verifies that the optimized
// BasicDeliverMethod.Serialize() produces correct AMQP 0.9.1 wire format.
func TestM10_BasicDeliverSerializeCorrectness(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method *BasicDeliverMethod
	}{
		{
			name: "basic",
			method: &BasicDeliverMethod{
				ConsumerTag: "consumer-1",
				DeliveryTag: 42,
				Redelivered: false,
				Exchange:    "amq.direct",
				RoutingKey:  "routing.key",
			},
		},
		{
			name: "redelivered with long strings",
			method: &BasicDeliverMethod{
				ConsumerTag: "tag-with-moderate-length-name",
				DeliveryTag: 18446744073709551615, // Max uint64
				Redelivered: true,
				Exchange:    "my-custom-exchange-name",
				RoutingKey:  "a.rather.long.routing.key.string.here",
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			data, err := tc.method.Serialize()
			if err != nil {
				t.Fatalf("Serialize() error: %v", err)
			}

			// Round-trip: deserialize and verify all fields match
			parsed := &BasicDeliverMethod{}
			if err := parsed.Deserialize(data); err != nil {
				t.Fatalf("Deserialize() error: %v", err)
			}

			if parsed.ConsumerTag != tc.method.ConsumerTag {
				t.Errorf("ConsumerTag: got %q, want %q", parsed.ConsumerTag, tc.method.ConsumerTag)
			}
			if parsed.DeliveryTag != tc.method.DeliveryTag {
				t.Errorf("DeliveryTag: got %d, want %d", parsed.DeliveryTag, tc.method.DeliveryTag)
			}
			if parsed.Redelivered != tc.method.Redelivered {
				t.Errorf("Redelivered: got %v, want %v", parsed.Redelivered, tc.method.Redelivered)
			}
			if parsed.Exchange != tc.method.Exchange {
				t.Errorf("Exchange: got %q, want %q", parsed.Exchange, tc.method.Exchange)
			}
			if parsed.RoutingKey != tc.method.RoutingKey {
				t.Errorf("RoutingKey: got %q, want %q", parsed.RoutingKey, tc.method.RoutingKey)
			}
		})
	}
}

// TestM10_BasicDeliverSerializeGoldenBytes verifies the wire format against
// known-good AMQP 0.9.1 bytes.
func TestM10_BasicDeliverSerializeGoldenBytes(t *testing.T) {
	t.Parallel()

	// basic.deliver: consumerTag="ctag", deliveryTag=42, redelivered=false,
	// exchange="ex", routingKey="rk"
	// Wire: 04"ctag" 000000000000002a 00 02"ex" 02"rk"
	method := &BasicDeliverMethod{
		ConsumerTag: "ctag",
		DeliveryTag: 42,
		Redelivered: false,
		Exchange:    "ex",
		RoutingKey:  "rk",
	}
	data, err := method.Serialize()
	if err != nil {
		t.Fatalf("Serialize error: %v", err)
	}
	expected := "0463746167000000000000002a0002657802726b"
	if got := hex.EncodeToString(data); got != expected {
		t.Errorf("golden bytes: got %s, want %s", got, expected)
	}
}

// TestM10_BasicDeliverSerializeAllocs verifies that Serialize() eliminates
// the make([]byte, 8) temp allocation for the delivery tag.
func TestM10_BasicDeliverSerializeAllocs(t *testing.T) {
	method := &BasicDeliverMethod{
		ConsumerTag: "ctag",
		DeliveryTag: 99,
		Redelivered: false,
		Exchange:    "ex",
		RoutingKey:  "rk",
	}

	allocs := testing.AllocsPerRun(100, func() {
		data, _ := method.Serialize()
		sink = data
	})

	// With escape analysis, encodeShortString allocs are elided.
	// The only heap allocation should be the pre-sized result slice.
	if allocs > 1 {
		t.Errorf("BasicDeliverMethod.Serialize(): got %.0f allocs, want ≤1", allocs)
	}
}
