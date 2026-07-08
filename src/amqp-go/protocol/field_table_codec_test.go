package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

func TestFieldTableRoundTrip(t *testing.T) {
	original := map[string]interface{}{
		"b":      true,
		"i8":     int8(-3),
		"u8":     byte(200),
		"i16":    int16(-1000),
		"i32":    int32(70000),
		"i64":    int64(1) << 40,
		"f32":    float32(1.5),
		"f64":    float64(2.5),
		"ts":     time.Unix(1700000000, 0),
		"s":      "hello",
		"nested": map[string]interface{}{"x": true},
		"arr":    []interface{}{int32(1), "a", true},
		"bytes":  []byte{1, 2, 3},
		"nil":    nil,
	}

	encoded, err := EncodeFieldTable(original)
	if err != nil {
		t.Fatalf("EncodeFieldTable failed: %v", err)
	}

	decoded, _, err := decodeFieldTable(encoded, 0)
	if err != nil {
		t.Fatalf("decodeFieldTable failed: %v", err)
	}

	if v, ok := decoded["b"]; !ok || v != true {
		t.Errorf("b: expected true, got %v (%T)", v, v)
	}
	if v, ok := decoded["i8"]; !ok {
		t.Errorf("i8: missing")
	} else if v.(int8) != int8(-3) {
		t.Errorf("i8: expected -3, got %v (%T)", v, v)
	}
	if v, ok := decoded["u8"]; !ok {
		t.Errorf("u8: missing")
	} else if v.(uint8) != byte(200) {
		t.Errorf("u8: expected 200, got %v (%T)", v, v)
	}
	if v, ok := decoded["i16"]; !ok {
		t.Errorf("i16: missing")
	} else if v.(int16) != int16(-1000) {
		t.Errorf("i16: expected -1000, got %v (%T)", v, v)
	}
	if v, ok := decoded["i32"]; !ok {
		t.Errorf("i32: missing")
	} else if v.(int32) != int32(70000) {
		t.Errorf("i32: expected 70000, got %v (%T)", v, v)
	}
	if v, ok := decoded["i64"]; !ok {
		t.Errorf("i64: missing")
	} else if v.(int64) != int64(1)<<40 {
		t.Errorf("i64: expected %d, got %v (%T)", int64(1)<<40, v, v)
	}
	if v, ok := decoded["f32"]; !ok {
		t.Errorf("f32: missing")
	} else if v.(float32) != float32(1.5) {
		t.Errorf("f32: expected 1.5, got %v (%T)", v, v)
	}
	if v, ok := decoded["f64"]; !ok {
		t.Errorf("f64: missing")
	} else if v.(float64) != float64(2.5) {
		t.Errorf("f64: expected 2.5, got %v (%T)", v, v)
	}
	if v, ok := decoded["ts"]; !ok {
		t.Errorf("ts: missing")
	} else if ts, ok := v.(time.Time); !ok {
		t.Errorf("ts: expected time.Time, got %T", v)
	} else if ts.Unix() != int64(1700000000) {
		t.Errorf("ts: expected unix 1700000000, got %d", ts.Unix())
	}
	if v, ok := decoded["s"]; !ok || v != "hello" {
		t.Errorf("s: expected hello, got %v (%T)", v, v)
	}
	if v, ok := decoded["nested"]; !ok {
		t.Errorf("nested: missing")
	} else if nested, ok := v.(map[string]interface{}); !ok {
		t.Errorf("nested: expected map, got %T", v)
	} else if nested["x"] != true {
		t.Errorf("nested.x: expected true, got %v (%T)", nested["x"], nested["x"])
	}
	if v, ok := decoded["bytes"]; !ok {
		t.Errorf("bytes: missing")
	} else if !bytes.Equal(v.([]byte), []byte{1, 2, 3}) {
		t.Errorf("bytes: expected [1 2 3], got %v (%T)", v, v)
	}
	if v, ok := decoded["nil"]; !ok {
		t.Errorf("nil: missing")
	} else if v != nil {
		t.Errorf("nil: expected nil, got %v (%T)", v, v)
	}
	if v, ok := decoded["arr"]; !ok {
		t.Errorf("arr: missing")
	} else if arr, ok := v.([]interface{}); !ok {
		t.Errorf("arr: expected []interface{}, got %T", v)
	} else {
		if len(arr) != 3 {
			t.Fatalf("arr: expected 3 elements, got %d", len(arr))
		}
		if arr[0].(int32) != int32(1) {
			t.Errorf("arr[0]: expected int32(1), got %v (%T)", arr[0], arr[0])
		}
		if arr[1] != "a" {
			t.Errorf("arr[1]: expected a, got %v (%T)", arr[1], arr[1])
		}
		if arr[2] != true {
			t.Errorf("arr[2]: expected true, got %v (%T)", arr[2], arr[2])
		}
	}
}

func TestFieldTableBoolDoesNotDesync(t *testing.T) {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, uint32(28))
	buf.WriteByte(7)
	buf.WriteString("durable")
	buf.WriteByte('t')
	buf.WriteByte(1)
	buf.WriteByte(7)
	buf.WriteString("product")
	buf.WriteByte('S')
	binary.Write(&buf, binary.BigEndian, uint32(5))
	buf.WriteString("hello")

	data := buf.Bytes()
	decoded, offset, err := decodeFieldTable(data, 0)
	if err != nil {
		t.Fatalf("decodeFieldTable failed: %v", err)
	}

	if decoded["durable"] != true {
		t.Errorf("durable: expected true, got %v (%T)", decoded["durable"], decoded["durable"])
	}
	if decoded["product"] != "hello" {
		t.Errorf("product: expected hello, got %v (%T)", decoded["product"], decoded["product"])
	}
	if offset != len(data) {
		t.Errorf("offset: expected %d, got %d", len(data), offset)
	}
}

func TestFieldTableUnknownMarkerReturnsError(t *testing.T) {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, uint32(4))
	buf.WriteByte(1)
	buf.WriteByte('k')
	buf.WriteByte('Z')
	buf.WriteByte(0x00)

	_, _, err := decodeFieldTable(buf.Bytes(), 0)
	if err == nil {
		t.Fatal("Expected error for unknown marker, got nil")
	}
}

func TestFieldTableTruncationReturnsError(t *testing.T) {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, uint32(7))
	buf.WriteByte(1)
	buf.WriteByte('k')
	buf.WriteByte('S')
	binary.Write(&buf, binary.BigEndian, uint32(0xFFFFFFF0))

	_, _, err := decodeFieldTable(buf.Bytes(), 0)
	if err == nil {
		t.Fatal("Expected error for truncated string, got nil")
	}
}

func TestNegativeIntRoundTrip(t *testing.T) {
	original := map[string]interface{}{"val": int32(-42)}
	encoded, err := EncodeFieldTable(original)
	if err != nil {
		t.Fatalf("EncodeFieldTable failed: %v", err)
	}

	decoded, _, err := decodeFieldTable(encoded, 0)
	if err != nil {
		t.Fatalf("decodeFieldTable failed: %v", err)
	}

	v, ok := decoded["val"]
	if !ok {
		t.Fatal("val: missing")
	}
	i32, ok := v.(int32)
	if !ok {
		t.Fatalf("val: expected int32, got %T (%v)", v, v)
	}
	if i32 != int32(-42) {
		t.Errorf("val: expected -42, got %d", i32)
	}
}

func TestFieldTableUnsupportedTypeReturnsError(t *testing.T) {
	table := map[string]interface{}{"x": struct{ A int }{A: 1}}
	_, err := EncodeFieldTable(table)
	if err == nil {
		t.Fatal("Expected error for unsupported type, got nil")
	}
}

// TestDecodeFieldTablePublicXDeath verifies the public DecodeFieldTable wrapper
// (used by the durable storage layer, W2) reconstructs an x-death field array
// with its field types intact: int64 count, timestamp time, and a field-array
// of longstr routing-keys. These are the exact types RabbitMQ's dead-letter
// x-death header carries, and losing any of them on recovery corrupts x-death.
func TestDecodeFieldTablePublicXDeath(t *testing.T) {
	deathTime := time.Unix(1720000000, 0).UTC()
	original := map[string]interface{}{
		"x-death": []interface{}{
			map[string]interface{}{
				"count":        int64(3),
				"reason":       "expired",
				"queue":        "src-q",
				"time":         deathTime,
				"exchange":     "dlx",
				"routing-keys": []interface{}{"rk-a", "rk-b"},
			},
		},
		"x-attempt": int64(7),
	}

	encoded, err := EncodeFieldTable(original)
	if err != nil {
		t.Fatalf("EncodeFieldTable failed: %v", err)
	}

	decoded, err := DecodeFieldTable(encoded)
	if err != nil {
		t.Fatalf("DecodeFieldTable failed: %v", err)
	}

	if v, ok := decoded["x-attempt"].(int64); !ok || v != 7 {
		t.Errorf("x-attempt: expected int64 7, got %v (%T)", decoded["x-attempt"], decoded["x-attempt"])
	}

	arr, ok := decoded["x-death"].([]interface{})
	if !ok || len(arr) != 1 {
		t.Fatalf("x-death: expected 1-element field array, got %v (%T)", decoded["x-death"], decoded["x-death"])
	}
	entry, ok := arr[0].(map[string]interface{})
	if !ok {
		t.Fatalf("x-death[0]: expected field table, got %T", arr[0])
	}
	if c, ok := entry["count"].(int64); !ok || c != 3 {
		t.Errorf("count: expected int64 3, got %v (%T)", entry["count"], entry["count"])
	}
	tm, ok := entry["time"].(time.Time)
	if !ok {
		t.Fatalf("time: expected time.Time, got %T", entry["time"])
	}
	if tm.Unix() != deathTime.Unix() {
		t.Errorf("time: expected unix %d, got %d", deathTime.Unix(), tm.Unix())
	}
	rks, ok := entry["routing-keys"].([]interface{})
	if !ok || len(rks) != 2 {
		t.Fatalf("routing-keys: expected 2-element field array, got %v (%T)", entry["routing-keys"], entry["routing-keys"])
	}
	if rks[0] != "rk-a" || rks[1] != "rk-b" {
		t.Errorf("routing-keys: expected [rk-a rk-b], got %v", rks)
	}
}

// TestDecodeFieldTableEmpty verifies DecodeFieldTable returns an empty (non-nil)
// map for the empty table EncodeFieldTable(nil) produces.
func TestDecodeFieldTableEmpty(t *testing.T) {
	encoded, err := EncodeFieldTable(nil)
	if err != nil {
		t.Fatalf("EncodeFieldTable(nil) failed: %v", err)
	}
	decoded, err := DecodeFieldTable(encoded)
	if err != nil {
		t.Fatalf("DecodeFieldTable failed: %v", err)
	}
	if len(decoded) != 0 {
		t.Errorf("expected empty table, got %v", decoded)
	}
}

// TestDecimalRoundTrip pins the AMQP decimal-value 'D' encode path. Before it
// existed, DecodeFieldTable could PRODUCE a Decimal (from an inbound header) but
// writeFieldValue had no encode case, so persisting such a header errored — and
// appendMessageExtensions turned that error into an EMPTY table, silently
// dropping EVERY header (including x-death) on that message. This asserts the
// Decimal itself round-trips AND that sibling headers survive alongside it.
func TestDecimalRoundTrip(t *testing.T) {
	original := map[string]interface{}{
		"price": Decimal{Scale: 2, Value: 12345}, // 123.45
		"neg":   Decimal{Scale: 4, Value: -67890},
		"note":  "kept",
		"count": int64(7),
		"x-death-ish": []interface{}{
			map[string]interface{}{"queue": "q1", "count": int64(1)},
		},
	}

	encoded, err := EncodeFieldTable(original)
	if err != nil {
		t.Fatalf("EncodeFieldTable failed (Decimal must be encodable): %v", err)
	}
	decoded, err := DecodeFieldTable(encoded)
	if err != nil {
		t.Fatalf("DecodeFieldTable failed: %v", err)
	}

	if d, ok := decoded["price"].(Decimal); !ok || d.Scale != 2 || d.Value != 12345 {
		t.Fatalf("price Decimal did not round-trip: got %#v", decoded["price"])
	}
	if d, ok := decoded["neg"].(Decimal); !ok || d.Scale != 4 || d.Value != -67890 {
		t.Fatalf("neg Decimal did not round-trip: got %#v", decoded["neg"])
	}
	// The regression guard: sibling headers must NOT be lost because a Decimal
	// shares the table.
	if decoded["note"] != "kept" {
		t.Fatalf("sibling string header lost: got %#v", decoded["note"])
	}
	if decoded["count"] != int64(7) {
		t.Fatalf("sibling int64 header lost: got %#v", decoded["count"])
	}
	if _, ok := decoded["x-death-ish"].([]interface{}); !ok {
		t.Fatalf("sibling array header lost: got %#v", decoded["x-death-ish"])
	}
}
