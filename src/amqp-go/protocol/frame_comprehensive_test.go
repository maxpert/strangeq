package protocol

import (
	"bytes"
	"io"
	"testing"
)

// Test all frame types
func TestAllFrameTypes(t *testing.T) {
	tests := []struct {
		name     string
		frameType byte
		payload  []byte
		description string
	}{
		{
			name:     "method_frame",
			frameType: FrameMethod,
			payload:  []byte{0x00, 0x0A, 0x00, 0x0A}, // connection.start
			description: "Method frame with connection.start",
		},
		{
			name:     "header_frame",
			frameType: FrameHeader,
			payload:  []byte{0x00, 0x3C, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64}, // basic class, body size 100
			description: "Header frame for basic class",
		},
		{
			name:     "body_frame",
			frameType: FrameBody,
			payload:  []byte("Hello, AMQP!"),
			description: "Body frame with message content",
		},
		{
			name:     "heartbeat_frame",
			frameType: FrameHeartbeat,
			payload:  []byte{},
			description: "Heartbeat frame (empty payload)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := &Frame{
				Type:    tt.frameType,
				Channel: 1,
				Size:    uint32(len(tt.payload)),
				Payload: tt.payload,
			}

			// Marshal
			data, err := frame.MarshalBinary()
			if err != nil {
				t.Fatalf("%s: Failed to marshal: %v", tt.description, err)
			}

			// Verify frame structure: type(1) + channel(2) + size(4) + payload + 0xCE(1)
			expectedSize := 1 + 2 + 4 + len(tt.payload) + 1
			if len(data) != expectedSize {
				t.Errorf("%s: Expected frame size %d, got %d", tt.description, expectedSize, len(data))
			}

			// Verify frame end marker (0xCE)
			if data[len(data)-1] != 0xCE {
				t.Errorf("%s: Expected frame end marker 0xCE, got 0x%02X", tt.description, data[len(data)-1])
			}

			// Unmarshal
			newFrame := &Frame{}
			err = newFrame.UnmarshalBinary(data)
			if err != nil {
				t.Fatalf("%s: Failed to unmarshal: %v", tt.description, err)
			}

			// Verify
			if newFrame.Type != tt.frameType {
				t.Errorf("%s: Type mismatch: expected %d, got %d", tt.description, tt.frameType, newFrame.Type)
			}
			if newFrame.Channel != 1 {
				t.Errorf("%s: Channel mismatch", tt.description)
			}
			if !bytes.Equal(newFrame.Payload, tt.payload) {
				t.Errorf("%s: Payload mismatch", tt.description)
			}
		})
	}
}

// Test frame error handling
func TestFrameErrors(t *testing.T) {
	t.Run("invalid_frame_end", func(t *testing.T) {
		frame := &Frame{
			Type:    FrameMethod,
			Channel: 1,
			Payload: []byte{0x00, 0x0A},
		}
		data, _ := frame.MarshalBinary()

		// Corrupt the frame end marker
		data[len(data)-1] = 0xFF

		newFrame := &Frame{}
		err := newFrame.UnmarshalBinary(data)
		if err == nil {
			t.Error("Expected error for invalid frame end marker, got nil")
		}
	})

	t.Run("truncated_frame", func(t *testing.T) {
		frame := &Frame{
			Type:    FrameMethod,
			Channel: 1,
			Payload: []byte{0x00, 0x0A},
		}
		data, _ := frame.MarshalBinary()

		// Truncate the data
		truncated := data[:len(data)-5]

		newFrame := &Frame{}
		err := newFrame.UnmarshalBinary(truncated)
		if err == nil {
			t.Error("Expected error for truncated frame, got nil")
		}
	})

	t.Run("oversized_payload", func(t *testing.T) {
		// Create a frame with payload larger than max frame size
		largePayload := make([]byte, 200000) // Larger than typical max (131072)
		frame := &Frame{
			Type:    FrameBody,
			Channel: 1,
			Size:    uint32(len(largePayload)),
			Payload: largePayload,
		}

		// This should still work (size limit enforced at protocol level)
		data, err := frame.MarshalBinary()
		if err != nil {
			t.Errorf("Unexpected error for large frame: %v", err)
		}

		if len(data) == 0 {
			t.Error("Expected non-empty marshaled data")
		}
	})

	t.Run("empty_buffer", func(t *testing.T) {
		emptyBuf := bytes.NewReader([]byte{})
		_, err := ReadFrame(emptyBuf)
		if err == nil {
			t.Error("Expected error reading from empty buffer, got nil")
		}
		if err != io.EOF {
			t.Errorf("Expected EOF error, got: %v", err)
		}
	})

	t.Run("partial_header", func(t *testing.T) {
		// Only 3 bytes when we need at least 7
		partialBuf := bytes.NewReader([]byte{0x01, 0x00, 0x01})
		_, err := ReadFrame(partialBuf)
		if err == nil {
			t.Error("Expected error for partial header, got nil")
		}
	})
}

// Test WriteFrame function
func TestWriteFrame(t *testing.T) {
	frame := &Frame{
		Type:    FrameMethod,
		Channel: 1,
		Payload: []byte{0x00, 0x0A, 0x00, 0x0A},
	}

	buf := &bytes.Buffer{}
	err := WriteFrame(buf, frame)
	if err != nil {
		t.Fatalf("WriteFrame failed: %v", err)
	}

	if buf.Len() == 0 {
		t.Error("Expected non-empty buffer after WriteFrame")
	}

	// Verify we can read it back
	readFrame, err := ReadFrame(buf)
	if err != nil {
		t.Fatalf("Failed to read back frame: %v", err)
	}

	if readFrame.Type != frame.Type {
		t.Errorf("Type mismatch after WriteFrame/ReadFrame round trip")
	}
	if !bytes.Equal(readFrame.Payload, frame.Payload) {
		t.Errorf("Payload mismatch after WriteFrame/ReadFrame round trip")
	}
}

// Test heartbeat frame specifically
func TestHeartbeatFrame(t *testing.T) {
	heartbeat := &Frame{
		Type:    FrameHeartbeat,
		Channel: 0, // Heartbeats are always on channel 0
		Size:    0,
		Payload: []byte{},
	}

	// Marshal
	data, err := heartbeat.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to marshal heartbeat: %v", err)
	}

	// Heartbeat frame should be: type(1) + channel(2) + size(4) + frameEnd(1) = 8 bytes
	if len(data) != 8 {
		t.Errorf("Expected heartbeat frame size 8, got %d", len(data))
	}

	// Unmarshal
	newHeartbeat := &Frame{}
	err = newHeartbeat.UnmarshalBinary(data)
	if err != nil {
		t.Fatalf("Failed to unmarshal heartbeat: %v", err)
	}

	if newHeartbeat.Type != FrameHeartbeat {
		t.Errorf("Expected heartbeat type %d, got %d", FrameHeartbeat, newHeartbeat.Type)
	}
	if newHeartbeat.Channel != 0 {
		t.Errorf("Expected channel 0 for heartbeat, got %d", newHeartbeat.Channel)
	}
	if len(newHeartbeat.Payload) != 0 {
		t.Errorf("Expected empty payload for heartbeat, got %d bytes", len(newHeartbeat.Payload))
	}
}

// Test multiple frames in sequence
func TestMultipleFrames(t *testing.T) {
	frames := []*Frame{
		{Type: FrameMethod, Channel: 1, Payload: []byte{0x00, 0x0A, 0x00, 0x0A}},
		{Type: FrameHeader, Channel: 1, Payload: []byte{0x00, 0x3C, 0x00, 0x00}},
		{Type: FrameBody, Channel: 1, Payload: []byte("Test message")},
		{Type: FrameHeartbeat, Channel: 0, Payload: []byte{}},
	}

	buf := &bytes.Buffer{}

	// Write all frames
	for i, frame := range frames {
		err := WriteFrame(buf, frame)
		if err != nil {
			t.Fatalf("Failed to write frame %d: %v", i, err)
		}
	}

	// Read them back
	for i := 0; i < len(frames); i++ {
		frame, err := ReadFrame(buf)
		if err != nil {
			t.Fatalf("Failed to read frame %d: %v", i, err)
		}

		if frame.Type != frames[i].Type {
			t.Errorf("Frame %d type mismatch", i)
		}
		if frame.Channel != frames[i].Channel {
			t.Errorf("Frame %d channel mismatch", i)
		}
		if !bytes.Equal(frame.Payload, frames[i].Payload) {
			t.Errorf("Frame %d payload mismatch", i)
		}
	}
}

// Test frame with maximum allowed channel number
func TestMaxChannelNumber(t *testing.T) {
	maxChannel := uint16(65535)
	frame := &Frame{
		Type:    FrameMethod,
		Channel: maxChannel,
		Payload: []byte{0x00, 0x0A},
	}

	data, err := frame.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to marshal frame with max channel: %v", err)
	}

	newFrame := &Frame{}
	err = newFrame.UnmarshalBinary(data)
	if err != nil {
		t.Fatalf("Failed to unmarshal frame with max channel: %v", err)
	}

	if newFrame.Channel != maxChannel {
		t.Errorf("Expected channel %d, got %d", maxChannel, newFrame.Channel)
	}
}

// Test frame size limits
func TestFrameSizeLimits(t *testing.T) {
	tests := []struct {
		name        string
		payloadSize int
		description string
	}{
		{"min_size", 0, "Minimum payload size (empty)"},
		{"small", 10, "Small payload"},
		{"medium", 1024, "Medium payload (1KB)"},
		{"large", 131072, "Large payload (128KB - typical max)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := make([]byte, tt.payloadSize)
			for i := range payload {
				payload[i] = byte(i % 256)
			}

			frame := &Frame{
				Type:    FrameBody,
				Channel: 1,
				Size:    uint32(tt.payloadSize),
				Payload: payload,
			}

			// Marshal
			data, err := frame.MarshalBinary()
			if err != nil {
				t.Fatalf("%s: Failed to marshal: %v", tt.description, err)
			}

			// Unmarshal
			newFrame := &Frame{}
			err = newFrame.UnmarshalBinary(data)
			if err != nil {
				t.Fatalf("%s: Failed to unmarshal: %v", tt.description, err)
			}

			if !bytes.Equal(newFrame.Payload, payload) {
				t.Errorf("%s: Payload mismatch", tt.description)
			}
		})
	}
}
