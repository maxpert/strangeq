package protocol

import (
	"bytes"
	"testing"
)

func TestFrameMarshalUnmarshal(t *testing.T) {
	originalFrame := &Frame{
		Type:    FrameMethod,
		Channel: 1,
		Size:    4,
		Payload: []byte{0x00, 0x0A, 0x00, 0x0A}, // Example: connection.start method
	}

	// Marshal the frame
	data, err := originalFrame.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to marshal frame: %v", err)
	}

	// Create a new frame and unmarshal from the data
	newFrame := &Frame{}
	err = newFrame.UnmarshalBinary(data)
	if err != nil {
		t.Fatalf("Failed to unmarshal frame: %v", err)
	}

	// Check that the frames match
	if newFrame.Type != originalFrame.Type {
		t.Errorf("Expected type %d, got %d", originalFrame.Type, newFrame.Type)
	}
	if newFrame.Channel != originalFrame.Channel {
		t.Errorf("Expected channel %d, got %d", originalFrame.Channel, newFrame.Channel)
	}
	if !bytes.Equal(newFrame.Payload, originalFrame.Payload) {
		t.Errorf("Expected payload %v, got %v", originalFrame.Payload, newFrame.Payload)
	}
}

func TestReadFrame(t *testing.T) {
	// Create a test frame
	frame := &Frame{
		Type:    FrameMethod,
		Channel: 1,
		Size:    4,
		Payload: []byte{0x00, 0x0A, 0x00, 0x0A}, // connection.start method
	}

	// Marshal the frame to binary
	frameData, err := frame.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to marshal frame: %v", err)
	}

	// Create a buffer to simulate network I/O
	buf := bytes.NewReader(frameData)

	// Read the frame back
	readFrame, err := ReadFrame(buf)
	if err != nil {
		t.Fatalf("Failed to read frame: %v", err)
	}

	// Check that the frames match
	if readFrame.Type != frame.Type {
		t.Errorf("Expected type %d, got %d", frame.Type, readFrame.Type)
	}
	if readFrame.Channel != frame.Channel {
		t.Errorf("Expected channel %d, got %d", frame.Channel, readFrame.Channel)
	}
	if !bytes.Equal(readFrame.Payload, frame.Payload) {
		t.Errorf("Expected payload %v, got %v", frame.Payload, readFrame.Payload)
	}
}