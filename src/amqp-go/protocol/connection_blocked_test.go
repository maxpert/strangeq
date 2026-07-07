package protocol

import (
	"testing"
)

// TestConnectionBlockedMethodIDs pins the RabbitMQ connection.blocked/unblocked
// extension method IDs (class 10). These are part of the Wave 2 frozen wire
// contract (SQ-12) and must not change.
func TestConnectionBlockedMethodIDs(t *testing.T) {
	if ConnectionBlocked != 60 {
		t.Errorf("ConnectionBlocked = %d, want 60", ConnectionBlocked)
	}
	if ConnectionUnblocked != 61 {
		t.Errorf("ConnectionUnblocked = %d, want 61", ConnectionUnblocked)
	}
}

// TestConnectionBlockedRoundTrip verifies connection.blocked serializes its
// reason as a single shortstr and deserializes back to the same value.
func TestConnectionBlockedRoundTrip(t *testing.T) {
	cases := []string{"", "low on memory", "low on disk", "low on memory & disk"}
	for _, reason := range cases {
		t.Run(reason, func(t *testing.T) {
			m := &ConnectionBlockedMethod{Reason: reason}
			data, err := m.Serialize()
			if err != nil {
				t.Fatalf("Serialize: %v", err)
			}
			// Shortstr: 1 length byte + payload.
			if len(data) != 1+len(reason) {
				t.Fatalf("serialized length = %d, want %d", len(data), 1+len(reason))
			}
			if int(data[0]) != len(reason) {
				t.Fatalf("length prefix = %d, want %d", data[0], len(reason))
			}

			var got ConnectionBlockedMethod
			if err := got.Deserialize(data); err != nil {
				t.Fatalf("Deserialize: %v", err)
			}
			if got.Reason != reason {
				t.Errorf("Reason = %q, want %q", got.Reason, reason)
			}
		})
	}
}

// TestConnectionBlockedFrameRoundTrip exercises the full method-frame path used
// by the server: EncodeMethodFrame(10, 60, ...) then parse of the payload after
// the 4-byte class/method header.
func TestConnectionBlockedFrameRoundTrip(t *testing.T) {
	m := &ConnectionBlockedMethod{Reason: "low on memory"}
	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	frame := EncodeMethodFrame(10, ConnectionBlocked, data)
	if frame.Type != FrameMethod {
		t.Fatalf("frame type = %d, want FrameMethod", frame.Type)
	}

	var got ConnectionBlockedMethod
	if err := got.Deserialize(frame.Payload[4:]); err != nil {
		t.Fatalf("Deserialize: %v", err)
	}
	if got.Reason != m.Reason {
		t.Errorf("Reason = %q, want %q", got.Reason, m.Reason)
	}
}

// TestConnectionUnblockedRoundTrip verifies connection.unblocked has an empty
// body that serializes to zero bytes and deserializes from any (including
// empty) input without error.
func TestConnectionUnblockedRoundTrip(t *testing.T) {
	m := &ConnectionUnblockedMethod{}
	data, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	if len(data) != 0 {
		t.Fatalf("serialized length = %d, want 0", len(data))
	}

	var got ConnectionUnblockedMethod
	if err := got.Deserialize(data); err != nil {
		t.Fatalf("Deserialize: %v", err)
	}
}
