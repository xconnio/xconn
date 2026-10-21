// Package valuebridge transfers Go values into foreign linear memory. It implements
// no WAMP message rules or network codecs; those belong to the Rust engine.
package valuebridge

import (
	"encoding"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"strings"
	"unicode/utf8"
)

const maxBytes = 64 * 1024 * 1024

func Encode(value any) ([]byte, error) { return appendValue(nil, value, 0) }

func length(out []byte, size int) []byte {
	if size < 0 || size > math.MaxUint32 {
		panic("foreign value length exceeds u32")
	}

	return binary.LittleEndian.AppendUint32(out, uint32(size))
}

func appendValue(out []byte, value any, depth int) ([]byte, error) {
	if depth > 64 {
		return nil, fmt.Errorf("value nesting exceeds 64")
	}

	switch v := value.(type) {
	case nil:
		out = append(out, 0)
	case bool:
		if v {
			out = append(out, 2)
		} else {
			out = append(out, 1)
		}
	case int:
		out = binary.LittleEndian.AppendUint64(append(out, 3), signedBits(int64(v)))
	case int8:
		out = binary.LittleEndian.AppendUint64(append(out, 3), signedBits(int64(v)))
	case int16:
		out = binary.LittleEndian.AppendUint64(append(out, 3), signedBits(int64(v)))
	case int32:
		out = binary.LittleEndian.AppendUint64(append(out, 3), signedBits(int64(v)))
	case int64:
		out = binary.LittleEndian.AppendUint64(append(out, 3), signedBits(v))
	case uint:
		out = binary.LittleEndian.AppendUint64(append(out, 4), uint64(v))
	case uint8:
		out = binary.LittleEndian.AppendUint64(append(out, 4), uint64(v))
	case uint16:
		out = binary.LittleEndian.AppendUint64(append(out, 4), uint64(v))
	case uint32:
		out = binary.LittleEndian.AppendUint64(append(out, 4), uint64(v))
	case uint64:
		out = binary.LittleEndian.AppendUint64(append(out, 4), v)
	case float32:
		return appendValue(out, float64(v), depth)
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return nil, fmt.Errorf("non-finite number")
		}

		out = binary.LittleEndian.AppendUint64(append(out, 5), math.Float64bits(v))
	case string:
		if !utf8.ValidString(v) {
			return nil, fmt.Errorf("invalid UTF-8 string")
		}

		out = append(length(append(out, 6), len(v)), v...)
	case []string:
		if v == nil {
			out = append(out, 0)
			break
		}

		out = length(append(out, 8), len(v))

		for _, item := range v {
			var err error
			out, err = appendValue(out, item, depth+1)

			if err != nil {
				return nil, err
			}
		}
	case []byte:
		if v == nil {
			out = append(out, 0)
		} else {
			out = append(length(append(out, 7), len(v)), v...)
		}
	case []any:
		if v == nil {
			out = append(out, 0)
			break
		}

		out = length(append(out, 8), len(v))

		for _, item := range v {
			var err error
			out, err = appendValue(out, item, depth+1)

			if err != nil {
				return nil, err
			}
		}
	case map[string]any:
		if v == nil {
			out = append(out, 0)
			break
		}

		out = length(append(out, 9), len(v))

		for key, item := range v {
			if !utf8.ValidString(key) {
				return nil, fmt.Errorf("invalid UTF-8 key")
			}

			out = append(length(out, len(key)), key...)
			var err error
			out, err = appendValue(out, item, depth+1)

			if err != nil {
				return nil, err
			}
		}
	default:
		converted, err := hostValue(reflect.ValueOf(value), depth)
		if err != nil {
			return nil, err
		}

		return appendValue(out, converted, depth)
	}

	if len(out) > maxBytes {
		return nil, fmt.Errorf("value transfer exceeds 64 MiB")
	}

	return out, nil
}

type reader struct {
	data   []byte
	offset int
}

func (r *reader) take(n int) ([]byte, error) {
	if n < 0 || n > len(r.data)-r.offset {
		return nil, fmt.Errorf("truncated value transfer")
	}

	v := r.data[r.offset : r.offset+n]
	r.offset += n
	return v, nil
}

func (r *reader) length() (int, error) {
	b, err := r.take(4)
	if err != nil {
		return 0, err
	}

	n := int(binary.LittleEndian.Uint32(b))
	if n > maxBytes || n > len(r.data)-r.offset {
		return 0, fmt.Errorf("invalid value length")
	}

	return n, nil
}

func (r *reader) text() (string, error) {
	n, err := r.length()
	if err != nil {
		return "", err
	}

	b, err := r.take(n)
	if err != nil {
		return "", err
	}

	if !utf8.Valid(b) {
		return "", fmt.Errorf("invalid UTF-8 string")
	}

	return string(b), nil
}

func (r *reader) value(depth int) (any, error) {
	if depth > 64 {
		return nil, fmt.Errorf("value nesting exceeds 64")
	}

	tag, err := r.take(1)
	if err != nil {
		return nil, err
	}

	switch tag[0] {
	case 0:
		return nil, nil
	case 1:
		return false, nil
	case 2:
		return true, nil
	case 3, 4, 5:
		b, err := r.take(8)
		if err != nil {
			return nil, err
		}

		v := binary.LittleEndian.Uint64(b)
		switch tag[0] {
		case 3:
			return int64(v), nil //nolint:gosec // Decode the ABI's two's-complement signed bits.
		case 4:
			return v, nil
		default:
			f := math.Float64frombits(v)
			if math.IsNaN(f) || math.IsInf(f, 0) {
				return nil, fmt.Errorf("non-finite number")
			}

			return f, nil
		}
	case 6:
		return r.text()
	case 7:
		n, err := r.length()
		if err != nil {
			return nil, err
		}

		b, err := r.take(n)
		if err != nil {
			return nil, err
		}

		return append([]byte{}, b...), nil
	case 8:
		n, err := r.length()
		if err != nil {
			return nil, err
		}

		out := make([]any, n)

		for i := range out {
			out[i], err = r.value(depth + 1)

			if err != nil {
				return nil, err
			}
		}

		return out, nil
	case 9:
		n, err := r.length()
		if err != nil {
			return nil, err
		}

		out := make(map[string]any, n)

		for i := 0; i < n; i++ {
			key, err := r.text()
			if err != nil {
				return nil, err
			}

			if _, ok := out[key]; ok {
				return nil, fmt.Errorf("duplicate dictionary key")
			}

			value, err := r.value(depth + 1)
			if err != nil {
				return nil, err
			}

			out[key] = value
		}

		return out, nil
	default:
		return nil, fmt.Errorf("unknown value tag")
	}
}

func Decode(data []byte) (any, error) {
	if len(data) > maxBytes {
		return nil, fmt.Errorf("value transfer exceeds 64 MiB")
	}

	r := reader{data: data}
	value, err := r.value(0)
	if err != nil {
		return nil, err
	}

	if r.offset != len(data) {
		return nil, fmt.Errorf("trailing value transfer data")
	}

	return value, nil
}

// Reflection adapts Go application types to the existing value ABI; Rust still encodes the wire.
func hostValue(v reflect.Value, depth int) (any, error) {
	if depth > 64 {
		return nil, fmt.Errorf("value nesting exceeds 64")
	}

	if !v.IsValid() {
		return nil, nil
	}

	if (v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface) && v.IsNil() {
		return nil, nil
	}

	if v.CanInterface() {
		if text, ok := v.Interface().(encoding.TextMarshaler); ok {
			b, e := text.MarshalText()
			return string(b), e
		}
	}

	switch v.Kind() {
	case reflect.Bool:
		return v.Bool(), nil
	case reflect.String:
		return v.String(), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint(), nil
	case reflect.Float32, reflect.Float64:
		return v.Float(), nil
	case reflect.Pointer, reflect.Interface:
		return hostValue(v.Elem(), depth+1)
	case reflect.Slice, reflect.Array:
		if v.Kind() == reflect.Slice && v.IsNil() {
			return nil, nil
		}

		if v.Type().Elem().Kind() == reflect.Uint8 {
			b := make([]byte, v.Len())

			for i := range b {
				b[i] = byte(v.Index(i).Uint())
			}

			return b, nil
		}

		out := make([]any, v.Len())

		for i := range out {
			out[i] = v.Index(i).Interface()
		}

		return out, nil
	case reflect.Map:
		if v.IsNil() {
			return nil, nil
		}

		if v.Type().Key().Kind() != reflect.String {
			return nil, fmt.Errorf("map keys must be strings")
		}

		out := make(map[string]any, v.Len())

		for it := v.MapRange(); it.Next(); {
			out[it.Key().String()] = it.Value().Interface()
		}

		return out, nil
	case reflect.Struct:
		out := map[string]any{}

		for i := 0; i < v.NumField(); i++ {
			field := v.Type().Field(i)
			if field.PkgPath != "" {
				continue
			}

			parts := strings.Split(field.Tag.Get("json"), ",")
			name := parts[0]
			if name == "-" {
				continue
			}

			item := v.Field(i)
			omit := false

			for _, option := range parts[1:] {
				if option == "omitempty" {
					switch item.Kind() {
					case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
						omit = item.Len() == 0
					case reflect.Struct:
					default:
						omit = item.IsZero()
					}
				}
			}

			if omit {
				continue
			}

			if name == "" && field.Anonymous {
				embedded, e := hostValue(item, depth+1)
				if e != nil {
					return nil, e
				}

				if fields, ok := embedded.(map[string]any); ok {
					for k, v := range fields {
						out[k] = v
					}

					continue
				}
			}

			if name == "" {
				name = field.Name
			}

			out[name] = item.Interface()
		}

		return out, nil
	default:
		return nil, fmt.Errorf("unsupported Go value %s", v.Type())
	}
}

func signedBits(value int64) uint64 {
	return uint64(value) //nolint:gosec // Preserve two's-complement bits; this is not a numeric range conversion.
}
