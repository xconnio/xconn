package xconn_test

import (
	"encoding/json"
	"math"
	"os"
	"reflect"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/xconnio/xconn-core/bindings/go"
)

type payloadCase struct {
	Name   string
	Tag    string
	Value  any
	Accept []string
}

func payloadValue(t *testing.T, entry payloadCase) any {
	t.Helper()
	switch entry.Tag {
	case "int":
		number, err := strconv.ParseInt(entry.Value.(string), 10, 64)
		require.NoError(t, err)
		return number
	case "uint":
		number, err := strconv.ParseUint(entry.Value.(string), 10, 64)
		require.NoError(t, err)
		return number
	case "bytes":
		result := make([]byte, len(entry.Value.([]any)))
		for i, value := range entry.Value.([]any) {
			result[i] = byte(value.(float64))
		}
		return result
	case "special":
		return map[string]float64{"nan": math.NaN(), "infinity": math.Inf(1), "-infinity": math.Inf(-1)}[entry.Value.(string)]
	default:
		return entry.Value
	}
}

func TestSharedPayloadMatrix(t *testing.T) {
	data, err := os.ReadFile("../../tests/payload/cases.json")
	require.NoError(t, err)
	var cases []payloadCase
	require.NoError(t, json.Unmarshal(data, &cases))
	kinds := map[string]string{
		"string": "String",
		"bool":   "Bool",
		"int":    "Int64",
		"uint":   "UInt64",
		"float":  "Float64",
		"bytes":  "Bytes",
		"list":   "List",
		"dict":   "Dict",
	}
	fallback := map[string]any{
		"string": "fallback",
		"bool":   false,
		"int":    int64(7),
		"uint":   uint64(7),
		"float":  float64(7),
		"bytes":  []byte{},
		"list":   xconn.List{},
		"dict":   xconn.Dict{},
	}
	for _, entry := range cases {
		t.Run(entry.Name, func(t *testing.T) {
			value := payloadValue(t, entry)
			args, kwargs := []any{value}, map[string]any{"value": value}
			payloads := []any{
				xconn.NewInvocation(args, kwargs, nil),
				xconn.NewEvent(args, kwargs, nil),
				&xconn.Result{Args: args, Kwargs: kwargs},
			}
			for _, payload := range payloads {
				for kind, suffix := range kinds {
					for _, scope := range []string{"Arg", "Kwarg"} {
						accepted := false
						for _, candidate := range entry.Accept {
							if candidate == kind {
								accepted = true
							}
						}
						key, absent := any(0), any(-1)
						if scope == "Kwarg" {
							key, absent = "value", "absent"
						}
						method := reflect.ValueOf(payload).MethodByName(scope + suffix)
						result := method.Call([]reflect.Value{reflect.ValueOf(key)})
						if accepted {
							require.True(t, result[1].IsNil(), "%T.%s%s", payload, scope, suffix)
							actual := result[0].Interface()
							switch parsed := actual.(type) {
							case xconn.List:
								actual = parsed.Raw()
							case xconn.Dict:
								actual = parsed.Raw()
							}
							if kind == "float" {
								source := reflect.ValueOf(value)
								if source.Kind() == reflect.Int64 {
									require.Equal(t, float64(source.Int()), actual)
								} else if source.Kind() == reflect.Uint64 {
									require.Equal(t, float64(source.Uint()), actual)
								} else {
									require.EqualValues(t, value, actual)
								}
							} else {
								require.EqualValues(t, value, actual)
							}
						} else {
							require.False(t, result[1].IsNil(), "%T.%s%s", payload, scope, suffix)
						}
						defaulted := reflect.ValueOf(payload).MethodByName(scope + suffix + "Or")
						got := defaulted.Call([]reflect.Value{reflect.ValueOf(key), reflect.ValueOf(fallback[kind])})[0]
						if accepted {
							require.Equal(t, result[0].Interface(), got.Interface())
						} else {
							require.Equal(t, fallback[kind], got.Interface())
						}
						missing := method.Call([]reflect.Value{reflect.ValueOf(absent)})
						var parseError *xconn.PayloadError
						require.ErrorAs(t, missing[1].Interface().(error), &parseError)
						require.Equal(t, "missing", parseError.Kind)
						require.Equal(
							t,
							fallback[kind],
							defaulted.Call([]reflect.Value{reflect.ValueOf(absent), reflect.ValueOf(fallback[kind])})[0].Interface(),
						)
					}
				}
			}
		})
	}
}
