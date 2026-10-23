package protocol

// WI-6: ReadContentHeader validates class-id, weight, the property-flags
// continuation bit and trailing bytes, wraps every failure in
// ErrInvalidContentHeader, and releases the pooled header on every error path.
// H1/H2 are real amqp091-go v1.10.0 content headers (captured wire bytes).

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

var (
	// class 60, weight 0, body size 5, flags 0x9000 (content-type +
	// delivery-mode), content-type "text/plain", delivery-mode 2.
	H1 = []byte{0x00, 0x3c, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0x05, 0x90, 0x00, 0x0a, 't', 'e', 'x', 't', '/', 'p', 'l', 'a', 'i', 'n', 0x02}
	// class 60, weight 0, body size 2, flags 0x2000 (headers), Headers{k:"v"}.
	H2 = []byte{0x00, 0x3c, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0x02, 0x20, 0x00, 0x00, 0x00, 0x00, 0x08, 0x01, 'k', 'S', 0x00, 0x00, 0x00, 0x01, 'v'}
)

func readHeader(t *testing.T, payload []byte) (*ContentHeader, error) {
	t.Helper()
	return ReadContentHeader(&Frame{Type: FrameHeader, Payload: payload})
}

func TestReadContentHeader_RealClientHeaders(t *testing.T) {
	h, err := readHeader(t, H1)
	if err != nil {
		t.Fatalf("H1: %v", err)
	}
	if h.ContentType != "text/plain" {
		t.Errorf("H1 ContentType = %q, want %q", h.ContentType, "text/plain")
	}
	if h.DeliveryMode != 2 {
		t.Errorf("H1 DeliveryMode = %d, want 2", h.DeliveryMode)
	}
	if h.BodySize != 5 {
		t.Errorf("H1 BodySize = %d, want 5", h.BodySize)
	}
	PutContentHeader(h)

	h, err = readHeader(t, H2)
	if err != nil {
		t.Fatalf("H2: %v", err)
	}
	if h.Headers["k"] != "v" {
		t.Errorf("H2 Headers[k] = %v, want %q", h.Headers["k"], "v")
	}
	if h.BodySize != 2 {
		t.Errorf("H2 BodySize = %d, want 2", h.BodySize)
	}
	PutContentHeader(h)
}

func TestReadContentHeader_RejectsNonBasicClass(t *testing.T) {
	for _, classBytes := range [][2]byte{{0x00, 0x32}, {0x00, 0x00}} {
		p := bytes.Clone(H1)
		p[0], p[1] = classBytes[0], classBytes[1]
		_, err := readHeader(t, p)
		if !errors.Is(err, ErrInvalidContentHeader) {
			t.Errorf("class % x: err = %v, want ErrInvalidContentHeader", classBytes, err)
		}
		if !errors.Is(err, ErrContentHeaderClassMismatch) {
			t.Errorf("class % x: err = %v, want ErrContentHeaderClassMismatch", classBytes, err)
		}
	}
}

func TestReadContentHeader_RejectsNonZeroWeight(t *testing.T) {
	p := bytes.Clone(H1)
	p[2], p[3] = 0x00, 0x01
	_, err := readHeader(t, p)
	if !errors.Is(err, ErrInvalidContentHeader) {
		t.Fatalf("err = %v, want ErrInvalidContentHeader", err)
	}
	if errors.Is(err, ErrContentHeaderClassMismatch) {
		t.Fatalf("err = %v, must NOT wrap ErrContentHeaderClassMismatch", err)
	}
}

func TestReadContentHeader_RejectsContinuationBit(t *testing.T) {
	p := bytes.Clone(H1)
	p[12], p[13] = 0x90, 0x01
	_, err := readHeader(t, p)
	if !errors.Is(err, ErrInvalidContentHeader) {
		t.Fatalf("err = %v, want ErrInvalidContentHeader", err)
	}
}

func TestReadContentHeader_IgnoresUnassignedBit1(t *testing.T) {
	// Bit 1 (0x0002) is unassigned for basic: tolerated, carries no data.
	p := bytes.Clone(H1)
	p[12], p[13] = 0x90, 0x02
	h, err := readHeader(t, p)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if h.ContentType != "text/plain" || h.DeliveryMode != 2 || h.BodySize != 5 {
		t.Errorf("decoded fields diverge from H1: %+v", h)
	}
	PutContentHeader(h)
}

func TestReadContentHeader_RejectsTrailingBytes(t *testing.T) {
	_, err := readHeader(t, append(bytes.Clone(H1), 0x00))
	if !errors.Is(err, ErrInvalidContentHeader) {
		t.Fatalf("err = %v, want ErrInvalidContentHeader", err)
	}
	if !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("err = %q, want a message containing %q", err.Error(), "trailing")
	}
}

func TestReadContentHeader_AllErrorsWrapSentinel(t *testing.T) {
	h2Overrun := bytes.Clone(H2)
	h2Overrun[17] = 0x09 // headers table claims 9 bytes, only 8 present

	h1MissingDeliveryMode := bytes.Clone(H1[:25]) // flags 0x9000, no delivery-mode octet

	cases := []struct {
		name  string
		frame *Frame
	}{
		{"wrong frame type", &Frame{Type: FrameMethod, Payload: H1}},
		{"payload of 13 bytes", &Frame{Type: FrameHeader, Payload: H1[:13]}},
		{"content-type overrun", &Frame{Type: FrameHeader, Payload: H1[:20]}},
		{"delivery-mode missing", &Frame{Type: FrameHeader, Payload: h1MissingDeliveryMode}},
		{"headers table overrun", &Frame{Type: FrameHeader, Payload: h2Overrun}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, err := ReadContentHeader(tc.frame)
			if err == nil {
				PutContentHeader(h)
				t.Fatal("err = nil, want an error")
			}
			if !errors.Is(err, ErrInvalidContentHeader) {
				t.Fatalf("err = %v, want it to wrap ErrInvalidContentHeader", err)
			}
			if h != nil {
				t.Fatalf("header = %v, want nil on error (pooled header must be released)", h)
			}
		})
	}
}

func TestReadContentHeader_FullFrame(t *testing.T) {
	wire := cat(
		[]byte{0x02, 0x00, 0x01, 0x00, 0x00, 0x00, 0x1a}, // type=header, channel 1, size 26
		H1,
		[]byte{0xCE},
	)
	frame, err := ReadFrameOptimizedWithLimit(bytes.NewReader(wire), 131072)
	if err != nil {
		t.Fatalf("ReadFrameOptimizedWithLimit: %v", err)
	}
	h, err := ReadContentHeader(frame)
	if err != nil {
		t.Fatalf("ReadContentHeader: %v", err)
	}
	if h.ContentType != "text/plain" || h.DeliveryMode != 2 {
		t.Errorf("decoded fields wrong: %+v", h)
	}
	PutContentHeader(h)
}

func TestReadContentHeader_HappyPathAllocs(t *testing.T) {
	allocs := testing.AllocsPerRun(200, func() {
		h, _ := ReadContentHeader(&Frame{Type: FrameHeader, Payload: H1})
		PutContentHeader(h)
	})
	// baseline measured at 07c391d
	const baseline = 1
	if allocs > baseline {
		t.Fatalf("happy-path allocs/run = %v, want <= %d", allocs, baseline)
	}
}
