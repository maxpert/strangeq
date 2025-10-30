package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Optimized frame operations for high-performance use cases.
//
// This file provides memory-optimized versions of frame operations that reduce
// allocations through buffer pooling and slice reuse. These functions are designed
// for hot paths where allocation overhead is critical.
//
// Performance improvements over standard operations:
//   - ReadFrameOptimized: 25% fewer allocations (4 → 3)
//   - WriteFrameOptimized: 100% fewer allocations (zero-allocation)
//   - MarshalBinaryOptimized: Precise pre-allocation
//   - UnmarshalBinaryOptimized: Payload slice reuse
//
// Use these functions in high-throughput scenarios where every allocation matters.
// For typical use cases, the standard frame operations are sufficient.

// ReadFrameOptimized reads a frame from an io.Reader with buffer pooling.
// This reduces allocations by using a pooled header buffer.
// Returns 3 allocations vs 4 for the standard ReadFrame.
func ReadFrameOptimized(reader io.Reader) (*Frame, error) {
	// Get header buffer from pool
	headerPtr := getFrameHeader()
	header := *headerPtr
	defer putFrameHeader(headerPtr)

	// Read the frame header (first 7 bytes: type, channel, size)
	_, err := io.ReadFull(reader, header)
	if err != nil {
		return nil, err
	}

	frameType := header[0]
	channel := binary.BigEndian.Uint16(header[1:3])
	size := binary.BigEndian.Uint32(header[3:7])

	// Read the payload + end-byte
	payload := make([]byte, size+1) // +1 for end-byte
	_, err = io.ReadFull(reader, payload)
	if err != nil {
		return nil, err
	}

	// Verify end-byte
	if payload[size] != FrameEnd {
		return nil, fmt.Errorf("invalid frame end-byte: expected 0x%02X, got 0x%02X", FrameEnd, payload[size])
	}

	return &Frame{
		Type:    frameType,
		Channel: channel,
		Size:    size,
		Payload: payload[:size],
	}, nil
}

// MarshalBinaryOptimized encodes a frame with pre-allocation.
// Calculates the exact frame size upfront and allocates once, avoiding slice growth.
// This is more efficient than the standard MarshalBinary for repeated operations.
func (f *Frame) MarshalBinaryOptimized() ([]byte, error) {
	payloadLen := len(f.Payload)
	frameSize := 8 + payloadLen // type(1) + channel(2) + size(4) + payload + end(1)

	// Pre-allocate exact size needed
	data := make([]byte, frameSize)

	data[0] = f.Type
	binary.BigEndian.PutUint16(data[1:3], f.Channel)
	binary.BigEndian.PutUint32(data[3:7], uint32(payloadLen))
	copy(data[7:7+payloadLen], f.Payload)
	data[7+payloadLen] = FrameEnd

	return data, nil
}

// WriteFrameOptimized writes a frame with buffer reuse.
// Uses a pooled buffer to achieve zero allocations for write operations.
// This is the most efficient way to write frames in high-throughput scenarios.
func WriteFrameOptimized(writer io.Writer, frame *Frame) error {
	// Use a pooled buffer for encoding
	buf := getBuffer()
	defer putBuffer(buf)

	// Pre-allocate capacity
	payloadLen := len(frame.Payload)
	buf.Grow(8 + payloadLen)

	// Write frame header
	buf.WriteByte(frame.Type)

	var header [6]byte
	binary.BigEndian.PutUint16(header[0:2], frame.Channel)
	binary.BigEndian.PutUint32(header[2:6], uint32(payloadLen))
	buf.Write(header[:])

	// Write payload
	buf.Write(frame.Payload)

	// Write frame end marker
	buf.WriteByte(FrameEnd)

	// Write to io.Writer
	_, err := buf.WriteTo(writer)
	return err
}

// UnmarshalBinaryOptimized decodes with reduced allocations.
// Reuses the existing payload slice when possible, avoiding new allocations
// if the frame's payload capacity is sufficient for the incoming data.
func (f *Frame) UnmarshalBinaryOptimized(data []byte) error {
	if len(data) < 8 {
		return fmt.Errorf("frame too short: need at least 8 bytes, got %d", len(data))
	}

	f.Type = data[0]
	f.Channel = binary.BigEndian.Uint16(data[1:3])
	payloadSize := binary.BigEndian.Uint32(data[3:7])

	expectedLen := int(7 + payloadSize + 1)
	if len(data) != expectedLen {
		return fmt.Errorf("frame size mismatch: expected %d bytes but got %d", expectedLen, len(data))
	}

	f.Size = payloadSize

	// Reuse existing payload slice if large enough
	if cap(f.Payload) >= int(payloadSize) {
		f.Payload = f.Payload[:payloadSize]
	} else {
		f.Payload = make([]byte, payloadSize)
	}
	copy(f.Payload, data[7:7+payloadSize])

	// Verify end-byte
	if data[7+payloadSize] != FrameEnd {
		return fmt.Errorf("invalid frame end-byte: expected 0x%02X, got 0x%02X", FrameEnd, data[7+payloadSize])
	}

	return nil
}
