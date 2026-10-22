package protocol

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

// TestSerialize_RejectsOverlongShortString: the methods a CLIENT sends must
// fail Serialize with ErrShortStringTooLong (wrapped, naming the field) for a
// shortstr over 255 bytes. Silent truncation would produce a frame naming a
// DIFFERENT entity than the caller asked for.
func TestSerialize_RejectsOverlongShortString(t *testing.T) {
	cases := []struct {
		name  string
		build func(s string) interface{ Serialize() ([]byte, error) }
	}{
		{"queue.declare queue", func(s string) interface{ Serialize() ([]byte, error) } {
			return &QueueDeclareMethod{Queue: s}
		}},
		{"exchange.declare exchange", func(s string) interface{ Serialize() ([]byte, error) } {
			return &ExchangeDeclareMethod{Exchange: s, Type: "direct"}
		}},
		{"exchange.declare type", func(s string) interface{ Serialize() ([]byte, error) } {
			return &ExchangeDeclareMethod{Exchange: "e", Type: s}
		}},
		{"queue.bind queue", func(s string) interface{ Serialize() ([]byte, error) } {
			return &QueueBindMethod{Queue: s, Exchange: "e", RoutingKey: "r"}
		}},
		{"queue.bind exchange", func(s string) interface{ Serialize() ([]byte, error) } {
			return &QueueBindMethod{Queue: "q", Exchange: s, RoutingKey: "r"}
		}},
		{"queue.bind routing-key", func(s string) interface{ Serialize() ([]byte, error) } {
			return &QueueBindMethod{Queue: "q", Exchange: "e", RoutingKey: s}
		}},
		{"queue.unbind queue", func(s string) interface{ Serialize() ([]byte, error) } {
			return &QueueUnbindMethod{Queue: s, Exchange: "e", RoutingKey: "r"}
		}},
		{"queue.unbind exchange", func(s string) interface{ Serialize() ([]byte, error) } {
			return &QueueUnbindMethod{Queue: "q", Exchange: s, RoutingKey: "r"}
		}},
		{"queue.unbind routing-key", func(s string) interface{ Serialize() ([]byte, error) } {
			return &QueueUnbindMethod{Queue: "q", Exchange: "e", RoutingKey: s}
		}},
		{"exchange.bind destination", func(s string) interface{ Serialize() ([]byte, error) } {
			return &ExchangeBindMethod{Destination: s, Source: "s", RoutingKey: "r"}
		}},
		{"exchange.bind source", func(s string) interface{ Serialize() ([]byte, error) } {
			return &ExchangeBindMethod{Destination: "d", Source: s, RoutingKey: "r"}
		}},
		{"exchange.bind routing-key", func(s string) interface{ Serialize() ([]byte, error) } {
			return &ExchangeBindMethod{Destination: "d", Source: "s", RoutingKey: s}
		}},
		{"exchange.unbind destination", func(s string) interface{ Serialize() ([]byte, error) } {
			return &ExchangeUnbindMethod{Destination: s, Source: "s", RoutingKey: "r"}
		}},
		{"exchange.unbind source", func(s string) interface{ Serialize() ([]byte, error) } {
			return &ExchangeUnbindMethod{Destination: "d", Source: s, RoutingKey: "r"}
		}},
		{"exchange.unbind routing-key", func(s string) interface{ Serialize() ([]byte, error) } {
			return &ExchangeUnbindMethod{Destination: "d", Source: "s", RoutingKey: s}
		}},
		{"queue.delete queue", func(s string) interface{ Serialize() ([]byte, error) } {
			return &QueueDeleteMethod{Queue: s}
		}},
		{"queue.purge queue", func(s string) interface{ Serialize() ([]byte, error) } {
			return &QueuePurgeMethod{Queue: s}
		}},
		{"exchange.delete exchange", func(s string) interface{ Serialize() ([]byte, error) } {
			return &ExchangeDeleteMethod{Exchange: s}
		}},
		{"basic.consume queue", func(s string) interface{ Serialize() ([]byte, error) } {
			return &BasicConsumeMethod{Queue: s, ConsumerTag: "c"}
		}},
		{"basic.consume consumer-tag", func(s string) interface{ Serialize() ([]byte, error) } {
			return &BasicConsumeMethod{Queue: "q", ConsumerTag: s}
		}},
		{"basic.cancel consumer-tag", func(s string) interface{ Serialize() ([]byte, error) } {
			return &BasicCancelMethod{ConsumerTag: s}
		}},
		{"basic.publish exchange", func(s string) interface{ Serialize() ([]byte, error) } {
			return &BasicPublishMethod{Exchange: s, RoutingKey: "r"}
		}},
		{"basic.publish routing-key", func(s string) interface{ Serialize() ([]byte, error) } {
			return &BasicPublishMethod{Exchange: "e", RoutingKey: s}
		}},
		{"basic.get queue", func(s string) interface{ Serialize() ([]byte, error) } {
			return &BasicGetMethod{Queue: s}
		}},
	}

	// fieldLabel is the field name as checkShortStr reports it (last word of
	// the test name).
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fieldLabel := tc.name[strings.LastIndex(tc.name, " ")+1:]

			data, err := tc.build(strings.Repeat("a", 256)).Serialize()
			if !errors.Is(err, ErrShortStringTooLong) {
				t.Fatalf("Serialize(256 bytes) err = %v, want ErrShortStringTooLong", err)
			}
			if data != nil {
				t.Fatalf("Serialize(256 bytes) data = %v, want nil", data)
			}
			if !strings.Contains(err.Error(), fieldLabel) {
				t.Fatalf("error %q does not name the field %q", err.Error(), fieldLabel)
			}

			s255 := strings.Repeat("a", 255)
			data, err = tc.build(s255).Serialize()
			if err != nil {
				t.Fatalf("Serialize(255 bytes): %v", err)
			}
			needle := append([]byte{0xFF}, []byte(s255)...)
			if !bytes.Contains(data, needle) {
				t.Fatalf("Serialize(255 bytes) does not contain 0xFF + 255 'a'")
			}
		})
	}
}

// TestSerialize_ServerAndPropertyPathsStillClamp: server-originated methods,
// hot paths and content-header properties keep the historical clamping
// behaviour (tolerant clients rely on it).
func TestSerialize_ServerAndPropertyPathsStillClamp(t *testing.T) {
	long := strings.Repeat("a", 300)

	t.Run("content-header reply-to", func(t *testing.T) {
		h := &ContentHeader{ClassID: 60, PropertyFlags: FlagReplyTo, ReplyTo: long}
		data, err := h.Serialize()
		if err != nil {
			t.Fatalf("Serialize: %v", err)
		}
		frame := &Frame{Type: FrameHeader, Payload: data}
		decoded, err := ReadContentHeader(frame)
		if err != nil {
			t.Fatalf("ReadContentHeader: %v", err)
		}
		if len(decoded.ReplyTo) != 255 {
			t.Fatalf("len(ReplyTo) = %d, want 255 (clamped)", len(decoded.ReplyTo))
		}
		PutContentHeader(decoded)
	})

	t.Run("channel.close reply-text", func(t *testing.T) {
		data, err := (&ChannelCloseMethod{ReplyCode: 320, ReplyText: long, ClassID: 50, MethodID: 20}).Serialize()
		if err != nil {
			t.Fatalf("Serialize: %v", err)
		}
		// layout: reply-code(2) + reply-text len(1) + text
		if data[2] != 255 {
			t.Fatalf("reply-text length octet = %d, want 255 (clamped)", data[2])
		}
	})

	t.Run("basic.deliver Serialize", func(t *testing.T) {
		data, err := (&BasicDeliverMethod{ConsumerTag: long, Exchange: "e", RoutingKey: "r"}).Serialize()
		if err != nil {
			t.Fatalf("Serialize: %v", err)
		}
		if data[0] != 255 {
			t.Fatalf("consumer-tag length octet = %d, want 255 (clamped)", data[0])
		}
	})

	t.Run("basic.deliver SerializeInto", func(t *testing.T) {
		data := (&BasicDeliverMethod{ConsumerTag: long, Exchange: "e", RoutingKey: "r"}).SerializeInto(nil)
		if data[0] != 255 {
			t.Fatalf("consumer-tag length octet = %d, want 255 (clamped)", data[0])
		}
	})

	t.Run("queue.declare-ok queue", func(t *testing.T) {
		data, err := (&QueueDeclareOKMethod{Queue: long}).Serialize()
		if err != nil {
			t.Fatalf("Serialize: %v", err)
		}
		if data[0] != 255 {
			t.Fatalf("queue length octet = %d, want 255 (clamped)", data[0])
		}
	})
}
