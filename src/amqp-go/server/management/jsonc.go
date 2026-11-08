package management

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// orderedObject is a JSON object whose keys are emitted in insertion order.
// It exists so a `columns=` projection preserves the item's DECLARATION order
// (FR-CONV-046) instead of the alphabetical order a Go map would impose.
type orderedObject []orderedField

type orderedField struct {
	key   string
	value any
}

func (o orderedObject) MarshalJSON() ([]byte, error) {
	var b bytes.Buffer
	b.WriteByte('{')
	for i, f := range o {
		if i > 0 {
			b.WriteByte(',')
		}
		k, err := marshalCompact(f.key)
		if err != nil {
			return nil, err
		}
		b.Write(k)
		b.WriteByte(':')
		v, err := marshalCompact(f.value)
		if err != nil {
			return nil, err
		}
		b.Write(v)
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}

// jsonName returns the JSON name for a struct field: the json tag if present,
// else the field name. An anonymous embedded struct without an explicit tag is
// inlined by encoding/json, so it has no name of its own and returns "".
func jsonName(f reflect.StructField) string {
	tag := f.Tag.Get("json")
	if tag == "" {
		if f.Anonymous && f.Type.Kind() == reflect.Struct {
			return ""
		}
		return f.Name
	}
	name := strings.Split(tag, ",")[0]
	if name == "" || name == "-" {
		return ""
	}
	return name
}

// embeddedStruct reports whether f is an anonymous struct to be inlined.
func embeddedStruct(f reflect.StructField) bool {
	return f.Anonymous && f.Type.Kind() == reflect.Struct && f.Tag.Get("json") == ""
}

// fieldRef names a (possibly promoted) struct field and the index path to it.
type fieldRef struct {
	name  string
	index []int
}

// flatFields lists the JSON-visible fields of t in emission order, expanding
// anonymous embedded structs at their declaration position (which is exactly
// what encoding/json does).
func flatFields(t reflect.Type, prefix []int, out *[]fieldRef) {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		idx := append(append([]int(nil), prefix...), i)
		if embeddedStruct(f) {
			flatFields(f.Type, idx, out)
			continue
		}
		name := jsonName(f)
		if name == "" {
			continue
		}
		*out = append(*out, fieldRef{name: name, index: idx})
	}
}

func structFields(t reflect.Type) []fieldRef {
	var out []fieldRef
	flatFields(t, nil, &out)
	return out
}

// projectValue rebuilds v as an ordered object containing only the fields named
// in cols. cols may use dotted paths (a.b) to project into a nested struct or
// map. An empty cols returns v unchanged.
func projectValue(v any, cols []string) (any, error) {
	if len(cols) == 0 {
		return v, nil
	}
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil, nil
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return nil, fmt.Errorf("columns projection requires an object, got %s", rv.Kind())
	}

	// Group the requested columns by their head segment so nested projections
	// are handled once per field.
	heads := make(map[string][]string)
	order := make([]string, 0, len(cols))
	for _, c := range cols {
		head, tail := splitHead(c)
		if _, seen := heads[head]; !seen {
			order = append(order, head)
		}
		heads[head] = append(heads[head], tail)
	}

	out := make(orderedObject, 0, len(order))
	for _, f := range structFields(rv.Type()) {
		tails, ok := heads[f.name]
		if !ok {
			continue
		}
		whole := false
		var sub []string
		for _, t := range tails {
			if t == "" {
				whole = true
			} else {
				sub = append(sub, t)
			}
		}
		fv := rv.FieldByIndex(f.index)
		if whole {
			out = append(out, orderedField{key: f.name, value: fv.Interface()})
			continue
		}
		pv, err := projectValue(fv.Interface(), sub)
		if err != nil {
			return nil, err
		}
		out = append(out, orderedField{key: f.name, value: pv})
	}
	return out, nil
}

func splitHead(col string) (string, string) {
	if i := strings.Index(col, "."); i >= 0 {
		return col[:i], col[i+1:]
	}
	return col, ""
}

// resolvePath returns the value at a dotted path inside v. Struct fields are
// matched by JSON name (case-insensitive fallback); map keys are matched
// exactly. It reports false for an unknown path so callers can return 400.
func resolvePath(v any, path string) (any, bool) {
	cur := reflect.ValueOf(v)
	for _, seg := range strings.Split(path, ".") {
		for cur.Kind() == reflect.Pointer {
			if cur.IsNil() {
				return nil, false
			}
			cur = cur.Elem()
		}
		switch cur.Kind() {
		case reflect.Struct:
			found := false
			for _, fr := range structFields(cur.Type()) {
				if fr.name == seg || strings.EqualFold(fr.name, seg) {
					cur = cur.FieldByIndex(fr.index)
					found = true
					break
				}
			}
			if !found {
				return nil, false
			}
		case reflect.Map:
			if cur.Type().Key().Kind() != reflect.String {
				return nil, false
			}
			mv := cur.MapIndex(reflect.ValueOf(seg))
			if !mv.IsValid() {
				return nil, false
			}
			cur = mv
		default:
			return nil, false
		}
	}
	if !cur.IsValid() {
		return nil, false
	}
	return cur.Interface(), true
}

// compareValues orders two comparable JSON scalars. Numbers compare
// numerically, strings lexically case-insensitively then case-sensitively,
// bools false<true. Mixed or unsupported types fall back to string form.
func compareValues(a, b any) int {
	an, aok := toFloat(a)
	bn, bok := toFloat(b)
	if aok && bok {
		switch {
		case an < bn:
			return -1
		case an > bn:
			return 1
		default:
			return 0
		}
	}
	as, aok := a.(string)
	bs, bok := b.(string)
	if aok && bok {
		la, lb := strings.ToLower(as), strings.ToLower(bs)
		if la != lb {
			return strings.Compare(la, lb)
		}
		return strings.Compare(as, bs)
	}
	ab, aok := a.(bool)
	bb, bok := b.(bool)
	if aok && bok {
		if ab == bb {
			return 0
		}
		if !ab {
			return -1
		}
		return 1
	}
	return strings.Compare(fmt.Sprint(a), fmt.Sprint(b))
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	}
	return 0, false
}

// etagFor returns the strong ETag for a response body: a quoted lowercase hex
// SHA-256 of the exact bytes (FR-CONV-064).
func etagFor(body []byte) string {
	sum := sha256.Sum256(body)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

// fieldTableJSON maps an AMQP field-table value onto JSON per FR-CONV-047:
// booleans stay booleans, numeric widths collapse to JSON numbers, strings stay
// strings (non-UTF-8 longs base64), timestamps become integer seconds, bytes
// become base64, and nested tables/arrays recurse. Map keys are left for
// encoding/json to sort byte-wise on emit.
func fieldTableJSON(v any) any {
	switch t := v.(type) {
	case nil:
		return nil
	case bool:
		return t
	case string:
		if utf8.ValidString(t) {
			return t
		}
		return base64String([]byte(t))
	case []byte:
		return base64String(t)
	case time.Time:
		return t.Unix()
	case int, int8, int16, int32, int64:
		return t
	case uint, uint8, uint16, uint32, uint64:
		return t
	case float32:
		return float64(t)
	case float64:
		return t
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = fieldTableJSON(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = fieldTableJSON(val)
		}
		return out
	default:
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Map:
			out := make(map[string]any, rv.Len())
			iter := rv.MapRange()
			for iter.Next() {
				out[fmt.Sprint(iter.Key().Interface())] = fieldTableJSON(iter.Value().Interface())
			}
			return out
		case reflect.Slice, reflect.Array:
			out := make([]any, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				out[i] = fieldTableJSON(rv.Index(i).Interface())
			}
			return out
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return rv.Int()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return rv.Uint()
		case reflect.Float32, reflect.Float64:
			return rv.Float()
		case reflect.Bool:
			return rv.Bool()
		case reflect.String:
			return rv.String()
		}
		return fmt.Sprint(v)
	}
}

// intToString renders an int64 as an unsigned decimal; the JSON encoder would
// do it, but header values need a plain string.
func intToString(n int64) string { return strconv.FormatInt(n, 10) }

var _ = json.Marshal
