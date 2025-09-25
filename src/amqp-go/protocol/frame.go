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

// WriteFrame writes a frame to an io.Writer
func WriteFrame(writer io.Writer, frame *Frame) error {
	// Calculate frame size
	data, err := frame.MarshalBinary()
	if err != nil {
		return err
	}

	// Write the frame
	_, err = writer.Write(data)
	return err
}