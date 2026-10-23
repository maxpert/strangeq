package protocol

import (
	"reflect"
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"
)

func TestFieldTablesEqual(t *testing.T) {
	cases := []struct {
		name string
		a, b map[string]interface{}
		want bool
	}{
		{"nil-vs-empty", nil, map[string]interface{}{}, true},
		{"both-nil", nil, nil, true},
		{"same-string", map[string]interface{}{"x-match": "all"}, map[string]interface{}{"x-match": "all"}, true},
		{"different-string", map[string]interface{}{"k": "v"}, map[string]interface{}{"k": "w"}, false},
		{"extra-key", map[string]interface{}{"k": "v"}, map[string]interface{}{"k": "v", "j": "v"}, false},
		{"missing-key-same-len", map[string]interface{}{"k": "v"}, map[string]interface{}{"j": "v"}, false},
		{"int32-vs-uint64", map[string]interface{}{"n": int32(5)}, map[string]interface{}{"n": uint64(5)}, true},
		{"int32-neg-vs-int64", map[string]interface{}{"n": int32(-1)}, map[string]interface{}{"n": int64(-1)}, true},
		{"int-vs-different-int", map[string]interface{}{"n": int32(5)}, map[string]interface{}{"n": int64(6)}, false},
		{"bytes-vs-string", map[string]interface{}{"s": []byte("a")}, map[string]interface{}{"s": "a"}, true},
		{"nested-map-kinds",
			map[string]interface{}{"t": map[string]interface{}{"a": int8(1)}},
			map[string]interface{}{"t": map[interface{}]interface{}{"a": uint64(1)}}, true},
		{"array",
			map[string]interface{}{"a": []interface{}{int16(1), "x"}},
			map[string]interface{}{"a": []interface{}{uint64(1), "x"}}, true},
		{"array-order-matters",
			map[string]interface{}{"a": []interface{}{"x", "y"}},
			map[string]interface{}{"a": []interface{}{"y", "x"}}, false},
		{"time-vs-unix",
			map[string]interface{}{"ts": time.Unix(1700000000, 0)},
			map[string]interface{}{"ts": uint64(1700000000)}, true},
		{"float32-vs-float64", map[string]interface{}{"f": float32(1.5)}, map[string]interface{}{"f": float64(1.5)}, true},
		{"bool-vs-int", map[string]interface{}{"b": true}, map[string]interface{}{"b": int64(1)}, false},
		{"void", map[string]interface{}{"v": nil}, map[string]interface{}{"v": nil}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := FieldTablesEqual(tc.a, tc.b); got != tc.want {
				t.Fatalf("FieldTablesEqual(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}

func TestFieldTablesEqual_SurvivesCBORRoundTrip(t *testing.T) {
	type rec struct {
		Arguments map[string]interface{}
	}
	orig := map[string]interface{}{
		"x-match": "all",
		"n":       int32(7),
		"neg":     int32(-3),
		"t":       map[string]interface{}{"a": "b"},
		"arr":     []interface{}{int8(1), "z"},
		"bin":     []byte{1, 2},
	}

	enc, err := cbor.Marshal(rec{Arguments: orig})
	if err != nil {
		t.Fatalf("cbor.Marshal: %v", err)
	}
	var decoded rec
	if err := cbor.Unmarshal(enc, &decoded); err != nil {
		t.Fatalf("cbor.Unmarshal: %v", err)
	}

	// Premise check: the round-trip really does drift the Go types, so the
	// FieldTablesEqual assertion below is exercised.
	if reflect.DeepEqual(orig, decoded.Arguments) {
		t.Fatalf("premise broken: CBOR round-trip preserved Go types exactly; test would not exercise type drift")
	}
	if !FieldTablesEqual(orig, decoded.Arguments) {
		t.Fatalf("FieldTablesEqual(orig, cbor-round-trip) = false\n orig: %#v\n decoded: %#v", orig, decoded.Arguments)
	}
}
