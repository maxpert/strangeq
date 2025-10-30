package protocol

import (
	"bytes"
	"testing"
)

// FuzzFrameUnmarshal tests frame unmarshaling with random data
// Run with: go test -fuzz=FuzzFrameUnmarshal -fuzztime=30s
func FuzzFrameUnmarshal(f *testing.F) {
	// Seed with valid frames
	validFrame := &Frame{
		Type:    FrameMethod,
		Channel: 1,
		Payload: []byte{0x00, 0x0A, 0x00, 0x0A},
	}
	validData, _ := validFrame.MarshalBinary()
	f.Add(validData)

	// Seed with empty frame
	f.Add([]byte{})

	// Seed with truncated frame
	f.Add([]byte{0x01, 0x00})

	// Seed with heartbeat frame
	heartbeat := &Frame{
		Type:    FrameHeartbeat,
		Channel: 0,
		Payload: []byte{},
	}
	heartbeatData, _ := heartbeat.MarshalBinary()
	f.Add(heartbeatData)

	f.Fuzz(func(t *testing.T, data []byte) {
		// Fuzzing should not panic
		frame := &Frame{}
		_ = frame.UnmarshalBinary(data)
		// We don't check for errors - just ensure no panic
	})
}

// FuzzReadFrame tests ReadFrame with random input
// Run with: go test -fuzz=FuzzReadFrame -fuzztime=30s
func FuzzReadFrame(f *testing.F) {
	// Seed with valid frame data
	validFrame := &Frame{
		Type:    FrameMethod,
		Channel: 1,
		Payload: []byte{0x00, 0x0A, 0x00, 0x0A},
	}
	validData, _ := validFrame.MarshalBinary()
	f.Add(validData)

	f.Fuzz(func(t *testing.T, data []byte) {
		// Fuzzing should not panic
		reader := bytes.NewReader(data)
		_, _ = ReadFrame(reader)
		// We don't check for errors - just ensure no panic
	})
}

// FuzzFieldTableDecode tests field table decoding with random data
// Run with: go test -fuzz=FuzzFieldTableDecode -fuzztime=30s
func FuzzFieldTableDecode(f *testing.F) {
	// Seed with valid field table
	validTable := map[string]interface{}{
		"string": "value",
		"int":    int32(42),
		"bool":   true,
	}
	validData, _ := EncodeFieldTable(validTable)
	f.Add(validData)

	// Seed with empty table
	emptyTable, _ := EncodeFieldTable(map[string]interface{}{})
	f.Add(emptyTable)

	f.Fuzz(func(t *testing.T, data []byte) {
		// Fuzzing should not panic
		_, _, _ = decodeFieldTable(data, 0)
		// We don't check for errors - just ensure no panic
	})
}

// FuzzContentHeaderRead tests content header parsing with random data
// Run with: go test -fuzz=FuzzContentHeaderRead -fuzztime=30s
func FuzzContentHeaderRead(f *testing.F) {
	// Seed with valid content header
	validHeader := &ContentHeader{
		ClassID:       60,
		BodySize:      100,
		PropertyFlags: FlagContentType | FlagDeliveryMode,
		ContentType:   "text/plain",
		DeliveryMode:  2,
	}
	validData, _ := validHeader.Serialize()
	f.Add(validData)

	// Seed with minimal header
	minimalHeader := &ContentHeader{
		ClassID:  60,
		BodySize: 0,
	}
	minimalData, _ := minimalHeader.Serialize()
	f.Add(minimalData)

	f.Fuzz(func(t *testing.T, data []byte) {
		// Fuzzing should not panic
		frame := &Frame{
			Type:    FrameHeader,
			Channel: 1,
			Payload: data,
		}
		_, _ = ReadContentHeader(frame)
		// We don't check for errors - just ensure no panic
	})
}

// FuzzMethodDeserialization tests method serialization/deserialization
// Run with: go test -fuzz=FuzzMethodDeserialization -fuzztime=30s
func FuzzMethodDeserialization(f *testing.F) {
	// Seed with valid method
	method := &ConnectionStartMethod{
		VersionMajor: 0,
		VersionMinor: 9,
		ServerProperties: map[string]interface{}{
			"product": "AMQP-Go",
		},
		Mechanisms: []string{"PLAIN"},
		Locales:    []string{"en_US"},
	}
	validData, _ := method.Serialize()
	f.Add(validData)

	// Seed with ConnectionStartOK
	startOK := &ConnectionStartOKMethod{
		ClientProperties: map[string]interface{}{
			"product": "client",
		},
		Mechanism: "PLAIN",
		Response:  []byte("\x00guest\x00guest"),
		Locale:    "en_US",
	}
	startOKData, _ := startOK.Serialize()
	f.Add(startOKData)

	f.Fuzz(func(t *testing.T, data []byte) {
		// Fuzzing should not panic
		// Try to parse as ConnectionStartOK (most complex structure)
		_, _ = ParseConnectionStartOK(data)
		// We don't check for errors - just ensure no panic
	})
}

// FuzzEncodeShortString tests short string encoding edge cases
// Run with: go test -fuzz=FuzzEncodeShortString -fuzztime=30s
func FuzzEncodeShortString(f *testing.F) {
	// Seed corpus
	f.Add("")
	f.Add("hello")
	f.Add(string(make([]byte, 255))) // Max short string
	f.Add(string(make([]byte, 500))) // Over max
	f.Add("unicode 世界 🌍")
	f.Add("\x00\x01\x02\x03") // Binary data

	f.Fuzz(func(t *testing.T, input string) {
		// Fuzzing should not panic
		encoded := EncodeShortString(input)

		// Verify basic invariants
		if len(encoded) < 1 {
			t.Errorf("Encoded string should have at least 1 byte for length")
		}

		// Length byte should be <= 255
		if encoded[0] > 255 {
			t.Errorf("Length byte should be <= 255")
		}

		// Total length should be length byte + data
		expectedLen := int(encoded[0]) + 1
		if len(encoded) != expectedLen {
			t.Errorf("Expected total length %d, got %d", expectedLen, len(encoded))
		}
	})
}

// FuzzDecodeShortString tests short string decoding robustness
// Run with: go test -fuzz=FuzzDecodeShortString -fuzztime=30s
func FuzzDecodeShortString(f *testing.F) {
	// Seed with valid encoded strings
	f.Add(EncodeShortString("test"))
	f.Add(EncodeShortString(""))
	f.Add(EncodeShortString("unicode 🎉"))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Fuzzing should not panic
		_, _, _ = decodeShortString(data, 0)
		// We don't check for errors - just ensure no panic
	})
}

// FuzzFrameRoundTrip tests frame marshal/unmarshal round trip
// Run with: go test -fuzz=FuzzFrameRoundTrip -fuzztime=30s
func FuzzFrameRoundTrip(f *testing.F) {
	// Seed with various frame types
	frames := []*Frame{
		{Type: FrameMethod, Channel: 1, Payload: []byte{0x00, 0x0A}},
		{Type: FrameHeader, Channel: 1, Payload: []byte{0x00, 0x3C, 0x00, 0x00}},
		{Type: FrameBody, Channel: 1, Payload: []byte("Hello, World!")},
		{Type: FrameHeartbeat, Channel: 0, Payload: []byte{}},
	}

	for _, frame := range frames {
		f.Add(frame.Type, frame.Channel, frame.Payload)
	}

	f.Fuzz(func(t *testing.T, frameType byte, channel uint16, payload []byte) {
		// Create frame
		frame := &Frame{
			Type:    frameType,
			Channel: channel,
			Payload: payload,
		}

		// Marshal
		data, err := frame.MarshalBinary()
		if err != nil {
			return // Skip invalid inputs
		}

		// Unmarshal
		newFrame := &Frame{}
		err = newFrame.UnmarshalBinary(data)
		if err != nil {
			return // Skip if unmarshal fails
		}

		// Verify round trip preserves data
		if newFrame.Type != frameType {
			t.Errorf("Type mismatch: %d != %d", newFrame.Type, frameType)
		}
		if newFrame.Channel != channel {
			t.Errorf("Channel mismatch: %d != %d", newFrame.Channel, channel)
		}
		if !bytes.Equal(newFrame.Payload, payload) {
			t.Errorf("Payload mismatch")
		}
	})
}

// Note: To run fuzz tests, use commands like:
// go test -fuzz=FuzzFrameUnmarshal -fuzztime=30s
// go test -fuzz=FuzzFieldTableDecode -fuzztime=10s
//
// Fuzz tests run indefinitely by default, so use -fuzztime to limit duration.
// The fuzzer will automatically generate test cases and add them to testdata/
// if they find crashes or interesting inputs.
