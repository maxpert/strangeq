package broker

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHeaders_NumericWidth_Parity (T10.1): RabbitMQ headers matching compares
// untyped Erlang terms, so integer WIDTHS compare by value (int8 binding ==
// int32 header), while integer and float are different classes. Pre-fix
// mechanism: reflect.DeepEqual made an int8 binding never match an int32
// header, and a CBOR-recovered uint64/int64 binding never matched after
// restart.
func TestHeaders_NumericWidth_Parity(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	cases := []struct {
		name    string
		binding interface{}
		header  interface{}
		want    bool
	}{
		{"int8 vs int32", int8(5), int32(5), true},
		{"int32 vs int64", int32(5), int64(5), true},
		{"int64 vs uint64", int64(5), uint64(5), true},
		{"int vs int16", int(7), int16(7), true},
		{"uint32 vs int8", uint32(9), int8(9), true},
		{"int vs float64 equal value", int(5), float64(5), false},
		{"int64 vs float64 equal value", int64(5), float64(5), false},
		{"float32 vs float64", float32(1.5), float64(1.5), true},
		{"negative int widths", int8(-3), int64(-3), true},
		{"bool strict", true, true, true},
		{"bool vs int", true, int64(1), false},
		{"string vs bytes by bytes", "abc", []byte("abc"), true},
		{"bytes vs string by bytes", []byte("abc"), "abc", true},
		{"string mismatch", "abc", "abd", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bind := map[string]interface{}{"k": tc.binding}
			hdr := map[string]interface{}{"k": tc.header}
			assert.Equal(t, tc.want, b.matchHeaders(bind, hdr),
				"binding %v (%T) vs header %v (%T)", tc.binding, tc.binding, tc.header, tc.header)
		})
	}
}

// TestHeaders_VoidAbsent (T10.3, D5): a void (nil) binding value is a
// PRESENCE check — it matches a header present with any value (including a
// void value), and does not match when the key is absent.
func TestHeaders_VoidAbsent(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	bind := map[string]interface{}{"k": nil}
	assert.True(t, b.matchHeaders(bind, map[string]interface{}{"k": "anything"}), "void binding matches any value")
	assert.True(t, b.matchHeaders(bind, map[string]interface{}{"k": int32(42)}), "void binding matches typed value")
	assert.True(t, b.matchHeaders(bind, map[string]interface{}{"k": nil}), "void binding matches void header")
	assert.False(t, b.matchHeaders(bind, map[string]interface{}{"other": 1}), "void binding requires presence")
	assert.False(t, b.matchHeaders(bind, nil), "void binding requires presence (nil headers)")

	// Inverted: a TYPED binding never matches a void/absent header.
	typed := map[string]interface{}{"k": "v"}
	assert.False(t, b.matchHeaders(typed, map[string]interface{}{"k": nil}), "typed binding vs void header")
	assert.False(t, b.matchHeaders(typed, nil), "typed binding vs nil headers")
}

// TestHeaders_XMatchOnly_AllAny (T10.5, R9): match_all([]) = true,
// match_any([]) = false. A nil or empty binding is mode "all" with zero keys,
// so it matches EVERY message.
func TestHeaders_XMatchOnly_AllAny(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	hdr := map[string]interface{}{"h1": "v1"}
	assert.True(t, b.matchHeaders(map[string]interface{}{"x-match": "all"}, hdr), "{x-match:all} matches anything")
	assert.False(t, b.matchHeaders(map[string]interface{}{"x-match": "any"}, hdr), "{x-match:any} matches nothing")
	assert.True(t, b.matchHeaders(map[string]interface{}{}, hdr), "empty binding matches everything")
	assert.True(t, b.matchHeaders(nil, hdr), "nil binding matches everything")
	assert.True(t, b.matchHeaders(nil, nil), "nil binding matches nil headers")
	assert.False(t, b.matchHeaders(map[string]interface{}{"x-match": "any"}, nil), "{x-match:any} matches nothing, nil headers")
	assert.True(t, b.matchHeaders(map[string]interface{}{"x-match": "all-with-x"}, hdr), "{x-match:all-with-x} zero keys matches")
	assert.False(t, b.matchHeaders(map[string]interface{}{"x-match": "any-with-x"}, hdr), "{x-match:any-with-x} zero keys never matches")
}

// TestHeaders_XPrefixedBindingKeysIgnored_AndWithX (T10.6, R9): x-* binding
// keys are skipped EXCEPT under *-with-x modes.
func TestHeaders_XPrefixedBindingKeysIgnored_AndWithX(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	hdr := map[string]interface{}{"h1": "v1"}
	// x-foo skipped under all/any: matches a message without x-foo.
	assert.True(t, b.matchHeaders(map[string]interface{}{"x-match": "all", "x-foo": 1, "h1": "v1"}, hdr))
	assert.True(t, b.matchHeaders(map[string]interface{}{"x-match": "any", "x-foo": 1, "h1": "v1"}, hdr))
	// Under all-with-x, x-foo is a real requirement.
	assert.False(t, b.matchHeaders(map[string]interface{}{"x-match": "all-with-x", "x-foo": 1, "h1": "v1"}, hdr))
	assert.True(t, b.matchHeaders(map[string]interface{}{"x-match": "all-with-x", "x-foo": 1, "h1": "v1"},
		map[string]interface{}{"h1": "v1", "x-foo": 1}))
	// Under any-with-x, x-foo alone can satisfy.
	assert.True(t, b.matchHeaders(map[string]interface{}{"x-match": "any-with-x", "x-foo": 1, "h1": "nope"},
		map[string]interface{}{"h1": "v1", "x-foo": 1}))
	assert.False(t, b.matchHeaders(map[string]interface{}{"x-match": "any-with-x", "x-foo": 1, "h1": "nope"}, hdr))
}

// TestBindQueue_HeadersInvalidXMatch_Rejected (T10.4 broker twin): bind-time
// validation rejects an unknown or non-longstr x-match with
// ErrInvalidBindingArgument, and no binding is stored (R9 validate_binding).
func TestBindQueue_HeadersInvalidXMatch_Rejected(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, b.DeclareExchange("hx", "headers", false, false, false, nil))
	mustDeclareQueue(t, b, "hq")

	for _, args := range []map[string]interface{}{
		{"x-match": "both"},        // unknown value
		{"x-match": int32(1)},      // non-longstr
		{"x-match": ""},            // empty string is not a valid mode
		{"x-match": []byte("all")}, // binary, not longstr
	} {
		err := b.BindQueue("hq", "hx", "", args)
		require.Error(t, err, "args %v", args)
		assert.True(t, errors.Is(err, ErrInvalidBindingArgument), "args %v: %v", args, err)
	}

	// Valid modes bind cleanly.
	for _, m := range []string{"all", "any", "all-with-x", "any-with-x"} {
		assert.NoError(t, b.BindQueue("hq", "hx", "rk-"+m, map[string]interface{}{"x-match": m}))
	}
	// Absent x-match is fine too.
	assert.NoError(t, b.BindQueue("hq", "hx", "rk-none", map[string]interface{}{"h": "v"}))

	// Validation ran BEFORE StoreBinding: only the valid bindings above exist.
	bindings, err := b.storage.GetExchangeBindings("hx")
	require.NoError(t, err)
	assert.Len(t, bindings, 5)

	// A non-headers exchange never validates x-match.
	require.NoError(t, b.DeclareExchange("dx", "direct", false, false, false, nil))
	mustDeclareQueue(t, b, "dq")
	assert.NoError(t, b.BindQueue("dq", "dx", "k", map[string]interface{}{"x-match": "bogus"}))
}

// TestBindExchange_HeadersInvalidXMatch_Rejected: same validation when the
// SOURCE of an e2e binding is a headers exchange.
func TestBindExchange_HeadersInvalidXMatch_Rejected(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	require.NoError(t, b.DeclareExchange("hx-src", "headers", false, false, false, nil))
	require.NoError(t, b.DeclareExchange("dx-dst", "direct", false, false, false, nil))

	err := b.BindExchange("dx-dst", "hx-src", "", map[string]interface{}{"x-match": "bogus"})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidBindingArgument), "%v", err)

	bindings, berr := b.storage.GetExchangeBindingsFrom("hx-src")
	require.NoError(t, berr)
	assert.Empty(t, bindings, "no binding may be stored for an invalid x-match")

	assert.NoError(t, b.BindExchange("dx-dst", "hx-src", "", map[string]interface{}{"x-match": "any"}))
}

// TestFieldEquiv_ArgEquivalent exercises the shared §2.4 classifier directly
// (R7 type_class semantics: same class AND equal value; integer widths are
// one class, float/double one class; tables/arrays/Decimal fall back to
// reflect.DeepEqual, which must be reflexive for recovery's self-redeclare).
func TestFieldEquiv_ArgEquivalent(t *testing.T) {
	// Integer widths are one class.
	assert.True(t, argEquivalent(int8(5), int64(5)))
	assert.True(t, argEquivalent(uint64(5), int32(5)))
	assert.True(t, argEquivalent(int(5), uint16(5)))
	assert.True(t, argEquivalent(int64(-5), int8(-5)))
	// int vs float: different classes even at equal value (R7).
	assert.False(t, argEquivalent(int64(5), float64(5)))
	assert.False(t, argEquivalent(float32(5), int32(5)))
	// Float/double are one class.
	assert.True(t, argEquivalent(float32(1.5), float64(1.5)))
	// string vs []byte are DIFFERENT classes for args (longstr vs binary, R7).
	assert.False(t, argEquivalent("abc", []byte("abc")))
	assert.True(t, argEquivalent("abc", "abc"))
	assert.True(t, argEquivalent([]byte("abc"), []byte("abc")))
	// bool strict.
	assert.True(t, argEquivalent(true, true))
	assert.False(t, argEquivalent(true, false))
	// nil/void.
	assert.True(t, argEquivalent(nil, nil))
	assert.False(t, argEquivalent(nil, int64(0)))
	// time.Time.
	now := time.Unix(1700000000, 0)
	assert.True(t, argEquivalent(now, now))
	assert.False(t, argEquivalent(now, time.Unix(1700000001, 0)))
	// Tables/arrays via DeepEqual (reflexive for CBOR-decoded self-comparison).
	assert.True(t, argEquivalent(
		map[interface{}]interface{}{"k": uint64(1)},
		map[interface{}]interface{}{"k": uint64(1)}))
	assert.True(t, argEquivalent([]interface{}{"a", uint64(2)}, []interface{}{"a", uint64(2)}))
	assert.False(t, argEquivalent([]interface{}{"a"}, []interface{}{"b"}))
	// uint64 above MaxInt64: only equal to itself (fcUintBig class).
	big := uint64(1) << 63
	assert.True(t, argEquivalent(big, big))
	assert.False(t, argEquivalent(big, int64(-1)))
}

// TestFieldEquiv_HeaderValueMatches: headers =:= semantics (R9) — like
// argEquivalent except string and []byte compare equal BY BYTES, and
// time.Time matches only same Unix second.
func TestFieldEquiv_HeaderValueMatches(t *testing.T) {
	assert.True(t, headerValueMatches("abc", []byte("abc")))
	assert.True(t, headerValueMatches([]byte("abc"), "abc"))
	assert.True(t, headerValueMatches(int8(5), int64(5)))
	assert.False(t, headerValueMatches(int64(5), float64(5)))
	assert.True(t, headerValueMatches(float32(1.5), float64(1.5)))
	assert.True(t, headerValueMatches(nil, nil))
	assert.False(t, headerValueMatches("v", nil))
	assert.True(t, headerValueMatches(time.Unix(1700000000, 500), time.Unix(1700000000, 900)))
	assert.False(t, headerValueMatches(time.Unix(1700000000, 0), time.Unix(1700000001, 0)))
}
