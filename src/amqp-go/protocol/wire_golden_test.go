package protocol

// Golden vectors are the METHOD ARGUMENT bytes captured from
// github.com/rabbitmq/amqp091-go v1.10.0 and cross-checked against the
// AMQP 0-9-1 field order (amqp0-9-1.xml). NEVER regenerate them from this
// package's Serialize.

import (
	"bytes"
	"reflect"
	"testing"
)

var xMatchAllTable = []byte{
	0x00, 0x00, 0x00, 0x10, // table length 16
	0x07, 'x', '-', 'm', 'a', 't', 'c', 'h', // key "x-match"
	'S', 0x00, 0x00, 0x00, 0x03, 'a', 'l', 'l', // long-string "all"
}
var emptyTable = []byte{0x00, 0x00, 0x00, 0x00}
var xMatchAll = map[string]interface{}{"x-match": "all"}

func cat(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

type wireMethod interface {
	Serialize() ([]byte, error)
	Deserialize([]byte) error
}

type goldenCase struct {
	name    string
	wire    []byte     // hand-written real-client bytes
	want    wireMethod // fully-populated expected value (Arguments: non-nil map)
	newZero func() wireMethod
}

// runGolden asserts, per case: (1) Deserialize(wire) == want; (2)
// Serialize(want) is byte-identical to wire; (3) Deserialize(Serialize(want))
// == want.
func runGolden(t *testing.T, cases []goldenCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.newZero()
			if err := got.Deserialize(tc.wire); err != nil {
				t.Fatalf("Deserialize(golden): %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("Deserialize(golden) =\n %#v\nwant\n %#v", got, tc.want)
			}
			enc, err := tc.want.Serialize()
			if err != nil {
				t.Fatalf("Serialize: %v", err)
			}
			if !bytes.Equal(enc, tc.wire) {
				t.Fatalf("Serialize =\n % x\nwant\n % x", enc, tc.wire)
			}
			rt := tc.newZero()
			if err := rt.Deserialize(enc); err != nil {
				t.Fatalf("round-trip Deserialize: %v", err)
			}
			if !reflect.DeepEqual(rt, tc.want) {
				t.Fatalf("round-trip =\n %#v\nwant\n %#v", rt, tc.want)
			}
		})
	}
}

var qbPrefix = []byte{0x00, 0x00, 0x01, 'q', 0x02, 'e', 'x', 0x02, 'r', 'k'}                // reserved, "q", "ex", "rk"
var ebPrefix = []byte{0x00, 0x00, 0x03, 'd', 's', 't', 0x03, 's', 'r', 'c', 0x02, 'r', 'k'} // reserved, "dst", "src", "rk"
func emptyArgs() map[string]interface{}                                                     { return map[string]interface{}{} }

func TestGolden_QueueBind(t *testing.T) {
	runGolden(t, []goldenCase{
		{"empty-args", cat(qbPrefix, []byte{0x00}, emptyTable),
			&QueueBindMethod{Queue: "q", Exchange: "ex", RoutingKey: "rk", Arguments: emptyArgs()},
			func() wireMethod { return &QueueBindMethod{} }},
		{"x-match-args", cat(qbPrefix, []byte{0x00}, xMatchAllTable),
			&QueueBindMethod{Queue: "q", Exchange: "ex", RoutingKey: "rk", Arguments: xMatchAll},
			func() wireMethod { return &QueueBindMethod{} }},
		{"no-wait", cat(qbPrefix, []byte{0x01}, emptyTable),
			&QueueBindMethod{Queue: "q", Exchange: "ex", RoutingKey: "rk", NoWait: true, Arguments: emptyArgs()},
			func() wireMethod { return &QueueBindMethod{} }},
	})
}

func TestGolden_QueueDelete(t *testing.T) {
	mk := func(flags byte, want *QueueDeleteMethod) goldenCase {
		return goldenCase{
			wire:    []byte{0x00, 0x00, 0x01, 'q', flags},
			want:    want,
			newZero: func() wireMethod { return &QueueDeleteMethod{} },
		}
	}
	cases := []goldenCase{
		mk(0x00, &QueueDeleteMethod{Queue: "q"}),
		mk(0x01, &QueueDeleteMethod{Queue: "q", IfUnused: true}),
		mk(0x02, &QueueDeleteMethod{Queue: "q", IfEmpty: true}),
		mk(0x03, &QueueDeleteMethod{Queue: "q", IfUnused: true, IfEmpty: true}),
		mk(0x04, &QueueDeleteMethod{Queue: "q", NoWait: true}),
		mk(0x07, &QueueDeleteMethod{Queue: "q", IfUnused: true, IfEmpty: true, NoWait: true}),
	}
	cases[0].name = "none"
	cases[1].name = "if-unused"
	cases[2].name = "if-empty"
	cases[3].name = "if-unused+if-empty"
	cases[4].name = "no-wait"
	cases[5].name = "all"
	runGolden(t, cases)
}

func TestGolden_QueuePurge(t *testing.T) {
	runGolden(t, []goldenCase{
		{"wait", []byte{0x00, 0x00, 0x01, 'q', 0x00},
			&QueuePurgeMethod{Queue: "q"},
			func() wireMethod { return &QueuePurgeMethod{} }},
		{"no-wait", []byte{0x00, 0x00, 0x01, 'q', 0x01},
			&QueuePurgeMethod{Queue: "q", NoWait: true},
			func() wireMethod { return &QueuePurgeMethod{} }},
	})
}

func TestGolden_ExchangeDelete(t *testing.T) {
	mk := func(flags byte, want *ExchangeDeleteMethod) goldenCase {
		return goldenCase{
			wire:    []byte{0x00, 0x00, 0x02, 'e', 'x', flags},
			want:    want,
			newZero: func() wireMethod { return &ExchangeDeleteMethod{} },
		}
	}
	cases := []goldenCase{
		mk(0x00, &ExchangeDeleteMethod{Exchange: "ex"}),
		mk(0x01, &ExchangeDeleteMethod{Exchange: "ex", IfUnused: true}),
		mk(0x02, &ExchangeDeleteMethod{Exchange: "ex", NoWait: true}),
		mk(0x03, &ExchangeDeleteMethod{Exchange: "ex", IfUnused: true, NoWait: true}), // synthetic, from the §3.1 bit order
	}
	cases[0].name = "none"
	cases[1].name = "if-unused"
	cases[2].name = "no-wait"
	cases[3].name = "both"
	runGolden(t, cases)
}

func TestGolden_ExchangeBind(t *testing.T) {
	runGolden(t, []goldenCase{
		{"empty-args", cat(ebPrefix, []byte{0x00}, emptyTable),
			&ExchangeBindMethod{Destination: "dst", Source: "src", RoutingKey: "rk", Arguments: emptyArgs()},
			func() wireMethod { return &ExchangeBindMethod{} }},
		{"x-match-args", cat(ebPrefix, []byte{0x00}, xMatchAllTable),
			&ExchangeBindMethod{Destination: "dst", Source: "src", RoutingKey: "rk", Arguments: xMatchAll},
			func() wireMethod { return &ExchangeBindMethod{} }},
		{"no-wait", cat(ebPrefix, []byte{0x01}, emptyTable),
			&ExchangeBindMethod{Destination: "dst", Source: "src", RoutingKey: "rk", NoWait: true, Arguments: emptyArgs()},
			func() wireMethod { return &ExchangeBindMethod{} }},
	})
}

func TestGolden_ExchangeUnbind(t *testing.T) {
	runGolden(t, []goldenCase{
		{"empty-args", cat(ebPrefix, []byte{0x00}, emptyTable),
			&ExchangeUnbindMethod{Destination: "dst", Source: "src", RoutingKey: "rk", Arguments: emptyArgs()},
			func() wireMethod { return &ExchangeUnbindMethod{} }},
		{"x-match-args", cat(ebPrefix, []byte{0x00}, xMatchAllTable),
			&ExchangeUnbindMethod{Destination: "dst", Source: "src", RoutingKey: "rk", Arguments: xMatchAll},
			func() wireMethod { return &ExchangeUnbindMethod{} }},
		{"no-wait", cat(ebPrefix, []byte{0x01}, emptyTable),
			&ExchangeUnbindMethod{Destination: "dst", Source: "src", RoutingKey: "rk", NoWait: true, Arguments: emptyArgs()},
			func() wireMethod { return &ExchangeUnbindMethod{} }},
	})
}

func TestGolden_QueueUnbind(t *testing.T) {
	// queue.unbind (50.50) has NO no-wait bit: the arguments table follows
	// the routing key directly.
	runGolden(t, []goldenCase{
		{"empty-args", cat(qbPrefix, emptyTable),
			&QueueUnbindMethod{Queue: "q", Exchange: "ex", RoutingKey: "rk", Arguments: emptyArgs()},
			func() wireMethod { return &QueueUnbindMethod{} }},
		{"x-match-args", cat(qbPrefix, xMatchAllTable),
			&QueueUnbindMethod{Queue: "q", Exchange: "ex", RoutingKey: "rk", Arguments: xMatchAll},
			func() wireMethod { return &QueueUnbindMethod{} }},
	})
}

func TestQueueUnbind_HasNoNoWaitOctet(t *testing.T) {
	enc, err := (&QueueUnbindMethod{Queue: "q", Exchange: "ex", RoutingKey: "rk", Arguments: emptyArgs()}).Serialize()
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	if len(enc) != len(qbPrefix)+4 {
		t.Fatalf("Serialize length = %d, want %d (prefix %d + 4-byte empty table); a no-wait octet would make it %d",
			len(enc), len(qbPrefix)+4, len(qbPrefix), len(qbPrefix)+5)
	}
}

func TestGolden_BasicCancel(t *testing.T) {
	runGolden(t, []goldenCase{
		{"wait", []byte{0x04, 'c', 't', 'a', 'g', 0x00},
			&BasicCancelMethod{ConsumerTag: "ctag"},
			func() wireMethod { return &BasicCancelMethod{} }},
		{"no-wait", []byte{0x02, 'c', '2', 0x01},
			&BasicCancelMethod{ConsumerTag: "c2", NoWait: true},
			func() wireMethod { return &BasicCancelMethod{} }},
	})
}

// Regression: queue.declare already packs its five bits into one octet.
func TestGolden_QueueDeclare(t *testing.T) {
	runGolden(t, []goldenCase{
		{"durable", []byte{0x00, 0x00, 0x01, 'q', 0x02, 0x00, 0x00, 0x00, 0x00},
			&QueueDeclareMethod{Queue: "q", Durable: true, Arguments: emptyArgs()},
			func() wireMethod { return &QueueDeclareMethod{} }},
		{"args", cat([]byte{0x00, 0x00, 0x01, 'q', 0x00}, xMatchAllTable),
			&QueueDeclareMethod{Queue: "q", Arguments: xMatchAll},
			func() wireMethod { return &QueueDeclareMethod{} }},
	})
}

// Regression: basic.consume already packs its four bits into one octet.
func TestGolden_BasicConsume(t *testing.T) {
	runGolden(t, []goldenCase{
		{"no-ack", []byte{0x00, 0x00, 0x01, 'q', 0x04, 'c', 't', 'a', 'g', 0x02, 0x00, 0x00, 0x00, 0x00},
			&BasicConsumeMethod{Queue: "q", ConsumerTag: "ctag", NoAck: true, Arguments: emptyArgs()},
			func() wireMethod { return &BasicConsumeMethod{} }},
		{"exclusive-no-wait", []byte{0x00, 0x00, 0x01, 'q', 0x02, 'c', '2', 0x0c, 0x00, 0x00, 0x00, 0x00},
			&BasicConsumeMethod{Queue: "q", ConsumerTag: "c2", Exclusive: true, NoWait: true, Arguments: emptyArgs()},
			func() wireMethod { return &BasicConsumeMethod{} }},
	})
}

func TestGolden_FullFrame_QueueBindWithArgs(t *testing.T) {
	methodArgs := cat(qbPrefix, []byte{0x00}, xMatchAllTable)  // 31 bytes
	payload := cat([]byte{0x00, 0x32, 0x00, 0x14}, methodArgs) // class 50, method 20
	wire := cat(
		[]byte{0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x23}, // type=method, channel 1, size 35
		payload,
		[]byte{0xCE},
	)

	frame, err := ReadFrameOptimizedWithLimit(bytes.NewReader(wire), 131072)
	if err != nil {
		t.Fatalf("ReadFrameOptimizedWithLimit: %v", err)
	}
	if frame.Type != FrameMethod {
		t.Fatalf("frame type = %d, want %d (method)", frame.Type, FrameMethod)
	}
	if frame.Channel != 1 {
		t.Fatalf("frame channel = %d, want 1", frame.Channel)
	}
	if got := frame.Payload[0:4]; !bytes.Equal(got, []byte{0x00, 0x32, 0x00, 0x14}) {
		t.Fatalf("class/method bytes = % x, want 00 32 00 14", got)
	}
	m := &QueueBindMethod{}
	if err := m.Deserialize(frame.Payload[4:]); err != nil {
		t.Fatalf("QueueBindMethod.Deserialize: %v", err)
	}
	if !reflect.DeepEqual(m.Arguments, xMatchAll) {
		t.Fatalf("Arguments = %#v, want %#v", m.Arguments, xMatchAll)
	}
}

func TestWireDecode_MalformedTables(t *testing.T) {
	cases := []struct {
		name    string
		wire    []byte
		newZero func() wireMethod
	}{
		{"queue.bind table stub", cat(qbPrefix, []byte{0x00}, []byte{0x00, 0x00}),
			func() wireMethod { return &QueueBindMethod{} }},
		{"queue.bind table length overruns", cat(qbPrefix, []byte{0x00}, []byte{0x00, 0x00, 0x00, 0x10}),
			func() wireMethod { return &QueueBindMethod{} }},
		{"exchange.bind table stub", cat(ebPrefix, []byte{0x00}, []byte{0x00}),
			func() wireMethod { return &ExchangeBindMethod{} }},
		{"exchange.unbind table stub", cat(ebPrefix, []byte{0x00}, []byte{0x00}),
			func() wireMethod { return &ExchangeUnbindMethod{} }},
		{"queue.unbind table stub", cat(qbPrefix, []byte{0x00, 0x00, 0x00}),
			func() wireMethod { return &QueueUnbindMethod{} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := tc.newZero()
			if err := m.Deserialize(tc.wire); err == nil {
				t.Fatalf("Deserialize(% x) = nil error, want malformed-table error", tc.wire)
			}
		})
	}
}

func TestWireDecode_AbsentTrailingFieldsTolerated(t *testing.T) {
	// A method with NO flag octet and NO table decodes with all flags false
	// and a non-nil empty arguments map (matching the tolerance of the
	// basic.publish/basic.get decoders).
	t.Run("queue.bind", func(t *testing.T) {
		m := &QueueBindMethod{}
		if err := m.Deserialize(qbPrefix); err != nil {
			t.Fatalf("Deserialize: %v", err)
		}
		if m.NoWait {
			t.Fatal("NoWait = true, want false")
		}
		if m.Arguments == nil || len(m.Arguments) != 0 {
			t.Fatalf("Arguments = %#v, want non-nil empty map", m.Arguments)
		}
	})
	t.Run("exchange.bind", func(t *testing.T) {
		m := &ExchangeBindMethod{}
		if err := m.Deserialize(ebPrefix); err != nil {
			t.Fatalf("Deserialize: %v", err)
		}
		if m.NoWait {
			t.Fatal("NoWait = true, want false")
		}
		if m.Arguments == nil || len(m.Arguments) != 0 {
			t.Fatalf("Arguments = %#v, want non-nil empty map", m.Arguments)
		}
	})
	t.Run("queue.delete", func(t *testing.T) {
		m := &QueueDeleteMethod{}
		if err := m.Deserialize([]byte{0x00, 0x00, 0x01, 'q'}); err != nil {
			t.Fatalf("Deserialize: %v", err)
		}
		if m.IfUnused || m.IfEmpty || m.NoWait {
			t.Fatalf("flags = (%v,%v,%v), want all false", m.IfUnused, m.IfEmpty, m.NoWait)
		}
	})
	t.Run("queue.purge", func(t *testing.T) {
		m := &QueuePurgeMethod{}
		if err := m.Deserialize([]byte{0x00, 0x00, 0x01, 'q'}); err != nil {
			t.Fatalf("Deserialize: %v", err)
		}
		if m.NoWait {
			t.Fatal("NoWait = true, want false")
		}
	})
	t.Run("basic.cancel", func(t *testing.T) {
		m := &BasicCancelMethod{}
		if err := m.Deserialize([]byte{0x01, 'c'}); err != nil {
			t.Fatalf("Deserialize: %v", err)
		}
		if m.ConsumerTag != "c" {
			t.Fatalf("ConsumerTag = %q, want %q", m.ConsumerTag, "c")
		}
		if m.NoWait {
			t.Fatal("NoWait = true, want false")
		}
	})
}

// TestBasicCancel_ServerInitiatedWire pins the outbound frame that
// server/frame_helpers.go sendBasicCancel emits: no stray trailing byte.
func TestBasicCancel_ServerInitiatedWire(t *testing.T) {
	enc, err := (&BasicCancelMethod{ConsumerTag: "c2", NoWait: true}).Serialize()
	if err != nil {
		t.Fatalf("Serialize: %v", err)
	}
	want := []byte{0x02, 'c', '2', 0x01}
	if !bytes.Equal(enc, want) {
		t.Fatalf("Serialize = % x, want % x", enc, want)
	}
}

// TestMethodIDs_MatchAMQP091 pins the protocol constants to the AMQP 0-9-1
// method ids (amqp0-9-1.xml; amqp091-go v1.10.0 spec091.go).
func TestMethodIDs_MatchAMQP091(t *testing.T) {
	cases := []struct {
		name string
		got  int
		want int
	}{
		{"ExchangeBind", ExchangeBind, 30},
		{"ExchangeBindOK", ExchangeBindOK, 31},
		{"ExchangeUnbind", ExchangeUnbind, 40},
		{"ExchangeUnbindOK", ExchangeUnbindOK, 51},
		{"QueueBind", QueueBind, 20},
		{"QueueBindOK", QueueBindOK, 21},
		{"QueuePurge", QueuePurge, 30},
		{"QueuePurgeOK", QueuePurgeOK, 31},
		{"QueueDelete", QueueDelete, 40},
		{"QueueDeleteOK", QueueDeleteOK, 41},
		{"QueueUnbind", QueueUnbind, 50},
		{"QueueUnbindOK", QueueUnbindOK, 51},
		{"BasicCancel", BasicCancel, 30},
		{"BasicCancelOK", BasicCancelOK, 31},
	}
	for _, tc := range cases {
		if tc.got != tc.want {
			t.Errorf("%s = %d, want %d (AMQP 0-9-1)", tc.name, tc.got, tc.want)
		}
	}
}
