package protocol

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
	"time"
)

func buildHeader(frameType byte, channel uint16, size uint32) []byte {
	header := make([]byte, 7)
	header[0] = frameType
	binary.BigEndian.PutUint16(header[1:3], channel)
	binary.BigEndian.PutUint32(header[3:7], size)
	return header
}

func TestReadFrameOptimizedRejectsOversizedFrame(t *testing.T) {
	header := buildHeader(FrameMethod, 1, 0xFFFFFFF0)
	reader := bytes.NewReader(header)

	done := make(chan struct{})
	var err error
	go func() {
		_, err = ReadFrameOptimizedWithLimit(reader, 1024)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ReadFrameOptimizedWithLimit hung attempting multi-GB allocation")
	}

	if err == nil {
		t.Fatal("Expected error for oversized frame, got nil")
	}
}

func TestReadFrameOptimizedRejectsMaxUint32(t *testing.T) {
	header := buildHeader(FrameMethod, 1, 0xFFFFFFFF)
	reader := bytes.NewReader(header)

	_, err := ReadFrameOptimizedWithLimit(reader, MaxInboundFrameSize)
	if err == nil {
		t.Fatal("Expected error for max uint32 size (overflow case), got nil")
	}
}

func TestReadFrameOptimizedAcceptsAtLimit(t *testing.T) {
	maxSize := uint32(16)
	payload := []byte{0x00, 0x0A, 0x00, 0x0B}
	frame := &Frame{Type: FrameMethod, Channel: 1, Size: uint32(len(payload)), Payload: payload}
	data, err := frame.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}
	binary.BigEndian.PutUint32(data[3:7], maxSize)

	body := make([]byte, maxSize)
	copy(body, payload)
	data = append(data[:7], append(body, FrameEnd)...)

	readFrame, err := ReadFrameOptimizedWithLimit(bytes.NewReader(data), maxSize)
	if err != nil {
		t.Fatalf("Expected success at limit, got error: %v", err)
	}
	if readFrame.Type != FrameMethod {
		t.Errorf("Expected type %d, got %d", FrameMethod, readFrame.Type)
	}
	if readFrame.Channel != 1 {
		t.Errorf("Expected channel 1, got %d", readFrame.Channel)
	}
	if !bytes.Equal(readFrame.Payload[:len(payload)], payload) {
		t.Errorf("Payload mismatch")
	}
}

func TestReadFrameOptimizedSmallFrameWorks(t *testing.T) {
	frame := &Frame{
		Type:    FrameMethod,
		Channel: 5,
		Size:    4,
		Payload: []byte{0x00, 0x0A, 0x00, 0x0A},
	}
	data, err := frame.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	readFrame, err := ReadFrameOptimizedWithLimit(bytes.NewReader(data), 1024)
	if err != nil {
		t.Fatalf("Expected success for small frame, got error: %v", err)
	}
	if readFrame.Type != frame.Type {
		t.Errorf("Expected type %d, got %d", frame.Type, readFrame.Type)
	}
	if readFrame.Channel != frame.Channel {
		t.Errorf("Expected channel %d, got %d", frame.Channel, readFrame.Channel)
	}
	if !bytes.Equal(readFrame.Payload, frame.Payload) {
		t.Errorf("Payload mismatch: expected %v, got %v", frame.Payload, readFrame.Payload)
	}
}

func TestReadFrameOptimizedDefaultWrapperUsesCap(t *testing.T) {
	header := buildHeader(FrameMethod, 1, 0xFFFFFFFF)
	reader := bytes.NewReader(header)

	_, err := ReadFrameOptimized(reader)
	if err == nil {
		t.Fatal("Expected ReadFrameOptimized wrapper to reject oversized frame, got nil")
	}
}

func TestReadFrameRejectsOversizedFrame(t *testing.T) {
	header := buildHeader(FrameMethod, 1, 0xFFFFFFF0)
	reader := bytes.NewReader(header)

	done := make(chan struct{})
	var err error
	go func() {
		_, err = ReadFrame(reader)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ReadFrame hung attempting multi-GB allocation")
	}

	if err == nil {
		t.Fatal("Expected error for oversized frame, got nil")
	}
}

func TestReadFrameWithLimitRejectsMaxUint32(t *testing.T) {
	header := buildHeader(FrameMethod, 1, 0xFFFFFFFF)
	reader := bytes.NewReader(header)

	_, err := ReadFrameWithLimit(reader, MaxInboundFrameSize)
	if err == nil {
		t.Fatal("Expected error for max uint32 size, got nil")
	}
}

func TestReadFrameWithLimitAcceptsAtLimit(t *testing.T) {
	maxSize := uint32(16)
	payload := []byte{0x00, 0x0A, 0x00, 0x0B}
	frame := &Frame{Type: FrameMethod, Channel: 1, Size: uint32(len(payload)), Payload: payload}
	data, err := frame.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}
	binary.BigEndian.PutUint32(data[3:7], maxSize)

	body := make([]byte, maxSize)
	copy(body, payload)
	data = append(data[:7], append(body, FrameEnd)...)

	readFrame, err := ReadFrameWithLimit(bytes.NewReader(data), maxSize)
	if err != nil {
		t.Fatalf("Expected success at limit, got error: %v", err)
	}
	if !bytes.Equal(readFrame.Payload[:len(payload)], payload) {
		t.Errorf("Payload mismatch")
	}
}

func TestReadFrameWithLimitSmallFrameWorks(t *testing.T) {
	frame := &Frame{
		Type:    FrameMethod,
		Channel: 5,
		Size:    4,
		Payload: []byte{0x00, 0x0A, 0x00, 0x0A},
	}
	data, err := frame.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	readFrame, err := ReadFrameWithLimit(bytes.NewReader(data), 1024)
	if err != nil {
		t.Fatalf("Expected success for small frame, got error: %v", err)
	}
	if !bytes.Equal(readFrame.Payload, frame.Payload) {
		t.Errorf("Payload mismatch: expected %v, got %v", frame.Payload, readFrame.Payload)
	}
}

func TestReadFrameSmallFrameWorks(t *testing.T) {
	frame := &Frame{
		Type:    FrameMethod,
		Channel: 5,
		Size:    4,
		Payload: []byte{0x00, 0x0A, 0x00, 0x0A},
	}
	data, err := frame.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	readFrame, err := ReadFrame(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Expected success for small frame, got error: %v", err)
	}
	if !bytes.Equal(readFrame.Payload, frame.Payload) {
		t.Errorf("Payload mismatch: expected %v, got %v", frame.Payload, readFrame.Payload)
	}
}

func TestReadFrameEmptyReturnsEOF(t *testing.T) {
	_, err := ReadFrame(bytes.NewReader([]byte{}))
	if err == nil {
		t.Fatal("Expected error reading from empty buffer, got nil")
	}
	if err != io.EOF {
		t.Errorf("Expected EOF, got %v", err)
	}
}
