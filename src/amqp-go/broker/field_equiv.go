package broker

import (
	"reflect"
	"time"
)

// AMQP field-value equivalence (WS-D §2.4), shared by the redeclare
// equivalence checks (§2) and the headers exchange matcher (§10).
//
// RabbitMQ type_class semantics (R7, rabbit_misc.erl assert_args_equivalence1):
// values are equivalent iff they are in the same TYPE CLASS and compare equal.
// Every integer width is one class (byte..long, signed and unsigned — decoded
// AMQP integers plus the uint64/int64 that CBOR-decoded metadata produces
// after a restart); float and double are one class; longstr (string) and
// binary ([]byte) are DIFFERENT classes. Headers matching (R9,
// rabbit_exchange_type_headers.erl) uses the same classes except that a
// longstr binding compares equal to binary header bytes (untyped Erlang
// binaries).
//
// Both entry points are allocation-free for scalars: a type switch, no maps
// and no reflection on the scalar path.

// maxInt64 avoids a math import on this hot path.
const maxInt64 = int64(^uint64(0) >> 1)

type fieldClass uint8

const (
	fcOther fieldClass = iota
	fcInt
	fcUintBig // uint64 above MaxInt64: only equal to itself
	fcFloat
	fcString
	fcBytes
	fcBool
	fcVoid
	fcTime
)

// classify canonicalises a decoded AMQP field value. Every integer width maps
// to fcInt with an int64; uint64 > MaxInt64 maps to fcUintBig (it cannot be
// represented as int64, and RabbitMQ never produces it from the wire); every
// float maps to fcFloat as float64.
func classify(v interface{}) (c fieldClass, i int64, u uint64, f float64) {
	switch n := v.(type) {
	case nil:
		return fcVoid, 0, 0, 0
	case int8:
		return fcInt, int64(n), 0, 0
	case uint8:
		return fcInt, int64(n), 0, 0
	case int16:
		return fcInt, int64(n), 0, 0
	case uint16:
		return fcInt, int64(n), 0, 0
	case int32:
		return fcInt, int64(n), 0, 0
	case uint32:
		return fcInt, int64(n), 0, 0
	case int64:
		return fcInt, n, 0, 0
	case int:
		return fcInt, int64(n), 0, 0
	case uint64:
		if n <= uint64(maxInt64) {
			return fcInt, int64(n), 0, 0
		}
		return fcUintBig, 0, n, 0
	case uint:
		if uint64(n) <= uint64(maxInt64) {
			return fcInt, int64(n), 0, 0
		}
		return fcUintBig, 0, uint64(n), 0
	case float32:
		return fcFloat, 0, 0, float64(n)
	case float64:
		return fcFloat, 0, 0, n
	case string:
		return fcString, 0, 0, 0
	case []byte:
		return fcBytes, 0, 0, 0
	case bool:
		return fcBool, 0, 0, 0
	case time.Time:
		return fcTime, 0, 0, 0
	default:
		return fcOther, 0, 0, 0
	}
}

// argEquivalent implements RabbitMQ redeclare-argument equivalence (R7):
// same type class AND equal value. string vs []byte are different classes
// (longstr vs binary). Tables, arrays, Decimal and other composite values
// fall back to reflect.DeepEqual, which is reflexive so recovery's
// self-redeclare against CBOR-decoded metadata always passes (§2.5).
func argEquivalent(a, b interface{}) bool {
	ca, ia, ua, fa := classify(a)
	cb, ib, ub, fb := classify(b)
	if ca != cb {
		return false
	}
	switch ca {
	case fcVoid:
		return true
	case fcInt:
		return ia == ib
	case fcUintBig:
		return ua == ub
	case fcFloat:
		return fa == fb
	case fcString:
		return a.(string) == b.(string)
	case fcBytes:
		return string(a.([]byte)) == string(b.([]byte))
	case fcBool:
		return a.(bool) == b.(bool)
	case fcTime:
		return a.(time.Time).Equal(b.(time.Time))
	default:
		return reflect.DeepEqual(a, b)
	}
}

// headerValueMatches implements RabbitMQ headers-exchange =:= semantics (R9):
// like argEquivalent except string and []byte compare equal BY BYTES (Erlang
// binaries are untyped), and time.Time matches only a time.Time with the same
// Unix second (RabbitMQ timestamps have second resolution).
func headerValueMatches(binding, header interface{}) bool {
	ca, ia, ua, fa := classify(binding)
	cb, ib, ub, fb := classify(header)
	// The one cross-class case: longstr binding vs binary header (and vice
	// versa) compare by bytes.
	if (ca == fcString && cb == fcBytes) || (ca == fcBytes && cb == fcString) {
		return asBytes(binding) == asBytes(header)
	}
	if ca != cb {
		return false
	}
	switch ca {
	case fcVoid:
		return true
	case fcInt:
		return ia == ib
	case fcUintBig:
		return ua == ub
	case fcFloat:
		return fa == fb
	case fcString:
		return binding.(string) == header.(string)
	case fcBytes:
		return string(binding.([]byte)) == string(header.([]byte))
	case fcBool:
		return binding.(bool) == header.(bool)
	case fcTime:
		return binding.(time.Time).Unix() == header.(time.Time).Unix()
	default:
		return reflect.DeepEqual(binding, header)
	}
}

func asBytes(v interface{}) string {
	switch s := v.(type) {
	case string:
		return s
	case []byte:
		return string(s)
	default:
		return ""
	}
}
