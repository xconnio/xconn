// Package bridge adapts language values and resource handles to the Rust ABI.
package bridge

import (
	"fmt"
	"math"

	"github.com/xconnio/xconn-core/bindings/go/internal/native"
)

func Call(method string, args ...any) (any, error) {
	if args == nil {
		args = []any{}
	}

	return native.Call(5, 0, map[string]any{"method": "protocol_call", "name": method, "args": args})
}

func Must(method string, args ...any) any {
	value, err := Call(method, args...)
	if err != nil {
		panic(err)
	}

	return value
}

func Uint(value any) uint64 {
	switch v := value.(type) {
	case int64:
		if v < 0 {
			return 0
		}

		return uint64(v)
	case uint64:
		return v
	case uint32:
		return uint64(v)
	case int:
		if v < 0 {
			return 0
		}

		return uint64(v)
	case float64:
		if v < 0 || v >= math.Exp2(64) {
			return 0
		}

		return uint64(v)
	}

	return 0
}

func String(value any) string {
	out, _ := value.(string)
	return out
}

func Dict(value any) map[string]any {
	out, _ := value.(map[string]any)
	return out
}

func Slice(value any) []any {
	out, _ := value.([]any)
	return out
}

func Bytes(value any) []byte {
	out, _ := value.([]byte)
	return out
}

func Strings(value any) []string {
	values := Slice(value)
	out := make([]string, len(values))

	for i, v := range values {
		out[i] = String(v)
	}

	return out
}

func Bool(value any) bool {
	out, _ := value.(bool)
	return out
}

func Parse(code uint64, wire []any) (map[string]any, error) {
	value, err := Call("parse", code, wire)
	if err != nil {
		return nil, err
	}

	out, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid Rust field result")
	}

	return out, nil
}

func Marshal(code uint64, fields map[string]any) []any { return Slice(Must("marshal", code, fields)) }

func Int(value any) int {
	unsigned := Uint(value)
	if unsigned > uint64(math.MaxInt) {
		panic("native integer exceeds int range")
	}

	return int(unsigned)
}
