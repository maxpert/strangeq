package protocol

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// frameWithPayload builds a complete wire frame (header + n zero payload
// bytes + frame-end) with a method type on channel 1.
func frameWithPayload(n int) []byte {
	out := buildHeader(FrameMethod, 1, uint32(n))
	out = append(out, make([]byte, n)...)
	out = append(out, FrameEnd)
	return out
}

func TestReadFrameOptimizedWithLimit_AcceptsFrameMaxMinus8(t *testing.T) {
	// frame-max bounds the WHOLE frame: the largest legal payload is
	// frame-max - FrameOverhead.
	frame, err := ReadFrameOptimizedWithLimit(bytes.NewReader(frameWithPayload(4088)), 4096)
	if err != nil {
		t.Fatalf("payload 4088 at frame-max 4096: %v", err)
	}
	if len(frame.Payload) != 4088 {
		t.Fatalf("len(Payload) = %d, want 4088", len(frame.Payload))
	}
}

func TestReadFrameOptimizedWithLimit_RejectsFrameMaxMinus7(t *testing.T) {
	_, err := ReadFrameOptimizedWithLimit(bytes.NewReader(frameWithPayload(4089)), 4096)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("payload 4089 at frame-max 4096: err = %v, want ErrFrameTooLarge", err)
	}
}

func TestReadFrameOptimizedWithLimit_RejectsPayloadEqualFrameMax(t *testing.T) {
	_, err := ReadFrameOptimizedWithLimit(bytes.NewReader(frameWithPayload(4096)), 4096)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("payload 4096 at frame-max 4096: err = %v, want ErrFrameTooLarge", err)
	}
}

func TestReadFrameOptimizedWithLimit_RejectsBeforeReadingPayload(t *testing.T) {
	// The size check is the DoS guard: it must fire on the header alone,
	// before any payload read (an EOF-family error would mean the reader
	// tried to pull the payload first).
	headerOnly := buildHeader(FrameMethod, 1, 4089)
	_, err := ReadFrameOptimizedWithLimit(bytes.NewReader(headerOnly), 4096)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		t.Fatalf("err = %v: payload was read before the size check fired", err)
	}
}

func TestReadFrameOptimizedWithLimit_ZeroMeansHardCap(t *testing.T) {
	// maxSize == 0 is "no negotiated limit": apply the MaxInboundFrameSize
	// hard cap rather than rejecting every non-empty frame.
	frame, err := ReadFrameOptimizedWithLimit(bytes.NewReader(frameWithPayload(10)), 0)
	if err != nil {
		t.Fatalf("payload 10 at maxSize 0: %v", err)
	}
	if len(frame.Payload) != 10 {
		t.Fatalf("len(Payload) = %d, want 10", len(frame.Payload))
	}

	headerOnly := buildHeader(FrameMethod, 1, MaxInboundFrameSize)
	_, err = ReadFrameOptimizedWithLimit(bytes.NewReader(headerOnly), 0)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("payload MaxInboundFrameSize at maxSize 0: err = %v, want ErrFrameTooLarge", err)
	}
}

func TestReadFrameOptimizedWithLimit_NoUint32Overflow(t *testing.T) {
	// With uint32 math, size+8 wraps to 0 and the check would pass.
	headerOnly := buildHeader(FrameMethod, 1, 0xFFFFFFF8)
	_, err := ReadFrameOptimizedWithLimit(bytes.NewReader(headerOnly), 0xFFFFFFFF)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
}

func TestReadFrameOptimizedWithLimit_HandshakeCapAcceptsMaxHandshakePayload(t *testing.T) {
	// server.go's handshake cap is 131072+8, so a 131072-byte handshake
	// payload must fit exactly.
	frame, err := ReadFrameOptimizedWithLimit(bytes.NewReader(frameWithPayload(131072)), 131072+8)
	if err != nil {
		t.Fatalf("payload 131072 at handshake cap 131080: %v", err)
	}
	if len(frame.Payload) != 131072 {
		t.Fatalf("len(Payload) = %d, want 131072", len(frame.Payload))
	}
}

func TestReadFrameWithLimit_OverheadParity(t *testing.T) {
	// The non-optimized reader enforces the same boundary.
	frame, err := ReadFrameWithLimit(bytes.NewReader(frameWithPayload(4088)), 4096)
	if err != nil {
		t.Fatalf("payload 4088 at frame-max 4096: %v", err)
	}
	if len(frame.Payload) != 4088 {
		t.Fatalf("len(Payload) = %d, want 4088", len(frame.Payload))
	}

	_, err = ReadFrameWithLimit(bytes.NewReader(frameWithPayload(4089)), 4096)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("payload 4089 at frame-max 4096: err = %v, want ErrFrameTooLarge", err)
	}

	_, err = ReadFrameWithLimit(bytes.NewReader(frameWithPayload(4096)), 4096)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("payload 4096 at frame-max 4096: err = %v, want ErrFrameTooLarge", err)
	}
}
