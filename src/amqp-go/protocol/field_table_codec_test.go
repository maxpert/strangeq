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
