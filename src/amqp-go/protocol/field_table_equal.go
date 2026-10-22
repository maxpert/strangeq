package protocol

import (
	"fmt"
	"math"
	"reflect"
	"time"
)

// FieldTablesEqual reports whether two AMQP field tables are semantically
// equal: same key set, values equal after normalisation. It tolerates the type
// drift a table undergoes through the wire codec (int8/16/32/64, uint8 'B') and
// through CBOR persistence (ints -> uint64/int64, nested tables ->
// map[interface{}]interface{}, time.Time -> unix seconds, Decimal -> map).
// A nil table equals an empty table.
func FieldTablesEqual(a, b map[string]interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		bv, ok := b[k]
		if !ok {
			return false
		}
		if !reflect.DeepEqual(normalizeFieldValue(av), normalizeFieldValue(bv)) {
			return false
		}
	}
	return true
}

func normalizeFieldValue(v interface{}) interface{} {
	switch x := v.(type) {
	case nil:
		return nil
	case bool:
		return x
	case int:
		return int64(x)
	case int8:
		return int64(x)
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	case int64:
		return x
	case uint:
		return normalizeUint(uint64(x))
	case uint8:
		return int64(x)
	case uint16:
		return int64(x)
	case uint32:
		return int64(x)
	case uint64:
		return normalizeUint(x)
	case float32:
		return float64(x)
	case float64:
		return x
	case string:
		return x
	case []byte:
		return string(x) // 'S' long-string and 'x' byte-array compare by content
	case time.Time:
		return x.Unix()
	case Decimal:
		return map[string]interface{}{"Scale": int64(x.Scale), "Value": int64(x.Value)}
	case map[string]interface{}:
		out := make(map[string]interface{}, len(x))
		for k, e := range x {
			out[k] = normalizeFieldValue(e)
		}
		return out
	case map[interface{}]interface{}:
		out := make(map[string]interface{}, len(x))
		for k, e := range x {
			out[fmt.Sprint(k)] = normalizeFieldValue(e)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(x))
		for i, e := range x {
			out[i] = normalizeFieldValue(e)
		}
		return out
	default:
		return v
	}
}

func normalizeUint(u uint64) interface{} {
	if u <= math.MaxInt64 {
		return int64(u)
	}
	return u
}
