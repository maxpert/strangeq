package protocol

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestEncodeShortString_RuneSafeTruncation(t *testing.T) {
	s := strings.Repeat("é", 128)
	if len(s) != 256 {
		t.Fatalf("test setup: expected 256 bytes, got %d", len(s))
	}

	encoded := EncodeShortString(s)
	if encoded[0] > 255 {
		t.Fatalf("expected length byte <= 255, got %d", encoded[0])
	}
	if !utf8.Valid(encoded[1:]) {
		t.Fatalf("truncated string is not valid UTF-8: %v", encoded[1:])
	}
	if int(encoded[0]) != len(encoded)-1 {
		t.Fatalf("length byte %d does not match payload %d", encoded[0], len(encoded)-1)
	}
}

func TestAppendShortString_RuneSafeTruncation(t *testing.T) {
	s := strings.Repeat("é", 128)
	if len(s) != 256 {
		t.Fatalf("test setup: expected 256 bytes, got %d", len(s))
	}

	encoded := AppendShortString(nil, s)
	if encoded[0] > 255 {
		t.Fatalf("expected length byte <= 255, got %d", encoded[0])
	}
	if !utf8.Valid(encoded[1:]) {
		t.Fatalf("truncated string is not valid UTF-8: %v", encoded[1:])
	}
	if int(encoded[0]) != len(encoded)-1 {
		t.Fatalf("length byte %d does not match payload %d", encoded[0], len(encoded)-1)
	}
}

func TestEncodeShortString_NormalLength(t *testing.T) {
	s := "hello world"
	encoded := EncodeShortString(s)
	if int(encoded[0]) != len(s) {
		t.Fatalf("expected length %d, got %d", len(s), encoded[0])
	}
	if string(encoded[1:]) != s {
		t.Fatalf("expected %q, got %q", s, string(encoded[1:]))
	}
}

func TestEncodeShortString_Exact255Bytes(t *testing.T) {
	s := strings.Repeat("a", 255)
	encoded := EncodeShortString(s)
	if encoded[0] != 255 {
		t.Fatalf("expected length 255, got %d", encoded[0])
	}
	if string(encoded[1:]) != s {
		t.Fatalf("expected unchanged 255-byte string")
	}
}
