package server

import (
	"encoding/binary"
	"testing"
)

// AMQP 0.9.1 encodes the trailing bit arguments of basic.ack, basic.reject and
// basic.nack as a SINGLE OCTET bit-field, so the method payload is exactly
// 9 bytes: delivery-tag (longlong) + 1 flag octet. These are the payloads a
// spec-compliant client (e.g. rabbitmq/amqp091-go) actually sends.
//
// A previous decoder read the flags as a big-endian uint16 (payload[8:10]),
// which can never match a 9-byte spec payload: basic.ack multiple was ALWAYS
// false (collapsing every batched multi-ack into a single ack — the
// BenchmarkVersus_MultiAck1000 wedge), and basic.reject/basic.nack requeue was
// ALWAYS the default true (a client rejecting a poison message with
// requeue=false got it redelivered forever). These tests pin the octet
// decoding against exactly-9-byte spec payloads.

func specAckPayload(tag uint64, flags byte) []byte {
	p := make([]byte, 9)
	binary.BigEndian.PutUint64(p[:8], tag)
	p[8] = flags
	return p
}

func TestParseBasicAckArgs_SpecOctetFlags(t *testing.T) {
	cases := []struct {
		name         string
		payload      []byte
		wantTag      uint64
		wantMultiple bool
		wantErr      bool
	}{
		{"multiple set", specAckPayload(1000, 0x01), 1000, true, false},
		{"multiple clear", specAckPayload(7, 0x00), 7, false, false},
		{"legacy 8-byte payload defaults false", specAckPayload(42, 0)[:8], 42, false, false},
		{"short payload errors", []byte{1, 2, 3}, 0, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tag, multiple, err := parseBasicAckArgs(tc.payload)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got tag=%d multiple=%v", tag, multiple)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tag != tc.wantTag || multiple != tc.wantMultiple {
				t.Fatalf("got tag=%d multiple=%v, want tag=%d multiple=%v",
					tag, multiple, tc.wantTag, tc.wantMultiple)
			}
		})
	}
}

func TestParseBasicRejectArgs_SpecOctetFlags(t *testing.T) {
	cases := []struct {
		name        string
		payload     []byte
		wantTag     uint64
		wantRequeue bool
		wantErr     bool
	}{
		{"requeue set", specAckPayload(5, 0x01), 5, true, false},
		{"requeue clear (poison discard)", specAckPayload(5, 0x00), 5, false, false},
		{"legacy 8-byte payload defaults requeue", specAckPayload(9, 0)[:8], 9, true, false},
		{"short payload errors", []byte{}, 0, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tag, requeue, err := parseBasicRejectArgs(tc.payload)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got tag=%d requeue=%v", tag, requeue)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tag != tc.wantTag || requeue != tc.wantRequeue {
				t.Fatalf("got tag=%d requeue=%v, want tag=%d requeue=%v",
					tag, requeue, tc.wantTag, tc.wantRequeue)
			}
		})
	}
}

func TestParseBasicNackArgs_SpecOctetFlags(t *testing.T) {
	cases := []struct {
		name         string
		payload      []byte
		wantTag      uint64
		wantMultiple bool
		wantRequeue  bool
		wantErr      bool
	}{
		{"multiple+requeue", specAckPayload(2000, 0x03), 2000, true, true, false},
		{"multiple only", specAckPayload(2000, 0x01), 2000, true, false, false},
		{"requeue only", specAckPayload(3, 0x02), 3, false, true, false},
		{"neither", specAckPayload(3, 0x00), 3, false, false, false},
		{"legacy 8-byte payload defaults requeue", specAckPayload(4, 0)[:8], 4, false, true, false},
		{"short payload errors", []byte{0}, 0, false, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tag, multiple, requeue, err := parseBasicNackArgs(tc.payload)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got tag=%d", tag)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tag != tc.wantTag || multiple != tc.wantMultiple || requeue != tc.wantRequeue {
				t.Fatalf("got tag=%d multiple=%v requeue=%v, want tag=%d multiple=%v requeue=%v",
					tag, multiple, requeue, tc.wantTag, tc.wantMultiple, tc.wantRequeue)
			}
		})
	}
}
