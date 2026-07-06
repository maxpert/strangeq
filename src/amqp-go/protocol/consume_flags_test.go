package protocol

import "testing"

// SQ-0 root cause (wire layer): the bit-flags field of basic.consume and
// basic.get is a SINGLE octet in AMQP 0.9.1, not a uint16. The previous
// codec read/wrote 2 bytes, so a spec-compliant client (e.g. rabbitmq/
// amqp091-go) that auto-acks had its no-ack bit decoded from the wrong byte
// and always came through as NoAck=false. The broker then treated the
// auto-ack consumer as manual-ack, exhausted the prefetch gate after 2000
// unacked deliveries, and deadlocked. These tests pin the on-wire layout to
// the spec so a real client's no-ack flag survives decoding.

// TestBasicConsumeDeserializeSpecFlagsOctet feeds the exact bytes a
// spec-compliant client sends for `basic.consume queue="q" no-ack=true` with
// an empty arguments table, and asserts the flags decode correctly.
func TestBasicConsumeDeserializeSpecFlagsOctet(t *testing.T) {
	// reserved-1(2)=0 | queue shortstr "q" | consumer-tag shortstr "" |
	// bits octet 0x02 (no-ack = bit 1) | arguments field-table length = 0
	data := []byte{
		0x00, 0x00, // reserved-1
		0x01, 'q', // queue = "q"
		0x00,                   // consumer-tag = ""
		0x02,                   // bits: no-ack set (bit 1)
		0x00, 0x00, 0x00, 0x00, // empty arguments table
	}

	var m BasicConsumeMethod
	if err := m.Deserialize(data); err != nil {
		t.Fatalf("Deserialize: %v", err)
	}
	if m.Queue != "q" {
		t.Errorf("Queue = %q, want %q", m.Queue, "q")
	}
	if !m.NoAck {
		t.Errorf("NoAck = false, want true (flags octet misread)")
	}
	if m.NoLocal || m.Exclusive || m.NoWait {
		t.Errorf("unexpected flags: NoLocal=%v Exclusive=%v NoWait=%v", m.NoLocal, m.Exclusive, m.NoWait)
	}
}

// TestBasicConsumeFlagsRoundTrip verifies each flag survives a
// Serialize→Deserialize round trip through this codec.
func TestBasicConsumeFlagsRoundTrip(t *testing.T) {
	orig := &BasicConsumeMethod{
		Queue:       "test-queue",
		ConsumerTag: "ctag",
		NoLocal:     true,
		NoAck:       true,
		Exclusive:   false,
		NoWait:      true,
		Arguments:   map[string]interface{}{},
	}
	raw, err := orig.Serialize()
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	var got BasicConsumeMethod
	if err := got.Deserialize(raw); err != nil {
		t.Fatalf("Deserialize: %v", err)
	}
	if got.NoLocal != orig.NoLocal || got.NoAck != orig.NoAck ||
		got.Exclusive != orig.Exclusive || got.NoWait != orig.NoWait {
		t.Errorf("flags round-trip mismatch: got %+v want NoLocal=%v NoAck=%v Exclusive=%v NoWait=%v",
			got, orig.NoLocal, orig.NoAck, orig.Exclusive, orig.NoWait)
	}
	if got.Queue != orig.Queue || got.ConsumerTag != orig.ConsumerTag {
		t.Errorf("field mismatch: got queue=%q ctag=%q", got.Queue, got.ConsumerTag)
	}
}

// TestBasicGetDeserializeSpecFlagsOctet feeds the exact bytes a spec client
// sends for `basic.get queue="q" no-ack=true` and asserts no-ack decodes.
func TestBasicGetDeserializeSpecFlagsOctet(t *testing.T) {
	// reserved-1(2)=0 | queue shortstr "q" | bits octet 0x01 (no-ack = bit 0)
	data := []byte{
		0x00, 0x00, // reserved-1
		0x01, 'q', // queue = "q"
		0x01, // bits: no-ack set (bit 0)
	}

	var m BasicGetMethod
	if err := m.Deserialize(data); err != nil {
		t.Fatalf("Deserialize: %v", err)
	}
	if m.Queue != "q" {
		t.Errorf("Queue = %q, want %q", m.Queue, "q")
	}
	if !m.NoAck {
		t.Errorf("NoAck = false, want true (flags octet misread)")
	}
}

// TestBasicGetFlagsRoundTrip verifies no-ack survives a round trip.
func TestBasicGetFlagsRoundTrip(t *testing.T) {
	for _, noAck := range []bool{false, true} {
		orig := &BasicGetMethod{Queue: "q", NoAck: noAck}
		raw, err := orig.Serialize()
		if err != nil {
			t.Fatalf("Serialize: %v", err)
		}
		var got BasicGetMethod
		if err := got.Deserialize(raw); err != nil {
			t.Fatalf("Deserialize: %v", err)
		}
		if got.NoAck != noAck {
			t.Errorf("NoAck round-trip: got %v want %v", got.NoAck, noAck)
		}
	}
}
