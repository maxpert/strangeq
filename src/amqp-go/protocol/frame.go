package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Frame types as defined in the AMQP specification
const (
	FrameMethod    = 1
	FrameHeader    = 2
	FrameBody      = 3
	FrameHeartbeat = 8
	FrameEnd       = 0xCE // Frame end marker byte
)

// Frame represents an AMQP frame
type Frame struct {
	Type    byte
	Channel uint16
	Size    uint32
	Payload []byte
}

// MarshalBinary encodes a frame into binary format following AMQP 0.9.1 spec
// Format: (1-byte type) + (2-byte channel) + (4-byte size) + (size-byte payload) + (1-byte end: 0xCE)
func (f *Frame) MarshalBinary() ([]byte, error) {
	frameSize := 1 + 2 + 4 + len(f.Payload) + 1 // type + channel + size + payload + end-byte
	data := make([]byte, frameSize)

	data[0] = f.Type
	binary.BigEndian.PutUint16(data[1:3], f.Channel)
	binary.BigEndian.PutUint32(data[3:7], uint32(len(f.Payload)))
	copy(data[7:], f.Payload)
	data[7+len(f.Payload)] = 0xCE // Frame end-byte: 0xCE

	return data, nil
}

// UnmarshalBinary decodes a frame from binary format
func (f *Frame) UnmarshalBinary(data []byte) error {
	if len(data) < 8 {
		return fmt.Errorf("frame too short")
	}

	f.Type = data[0]
	f.Channel = binary.BigEndian.Uint16(data[1:3])
	payloadSize := binary.BigEndian.Uint32(data[3:7])

	// Check if the payload size matches what we expect in the data
	if len(data) != int(7+payloadSize+1) { // 7 bytes header + payload size + 1 end-byte
		return fmt.Errorf("frame size mismatch: expected %d bytes but got %d", 7+payloadSize+1, len(data))
	}

	if len(data) < int(7+payloadSize+1) {
		return fmt.Errorf("payload too short")
	}

	f.Size = payloadSize
	f.Payload = make([]byte, f.Size)
	copy(f.Payload, data[7:7+f.Size])

	// Verify end-byte
	if data[7+f.Size] != 0xCE {
		return fmt.Errorf("invalid frame end-byte")
	}

	return nil
}

// ReadFrame reads a frame from an io.Reader
func ReadFrame(reader io.Reader) (*Frame, error) {
	// Read the frame header (first 7 bytes: type, channel, size)
	header := make([]byte, 7)
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
	if payload[size] != 0xCE {
		return nil, fmt.Errorf("invalid frame end-byte")
	}

	return &Frame{
		Type:    frameType,
		Channel: channel,
		Size:    size,
		Payload: payload[:size],
	}, nil
}

// WriteFrame writes a frame to an io.Writer with buffer pooling for optimal performance.
// Uses a pooled buffer to achieve zero allocations for write operations.
// This is the most efficient way to write frames in high-throughput scenarios.
func WriteFrame(writer io.Writer, frame *Frame) error {
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

// WriteFrameToConnection writes a frame to a connection with mutex protection for thread-safe writes.
// This should be used when writing to a *Connection to ensure thread-safety.
func WriteFrameToConnection(conn *Connection, frame *Frame) error {
	// Lock for thread-safe socket writes
	conn.WriteMutex.Lock()
	defer conn.WriteMutex.Unlock()

	// Write the frame using the optimized writer
	return WriteFrame(conn.Conn, frame)
}
