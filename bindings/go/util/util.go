package util

import (
	"fmt"
	"math"
	"reflect"
)

func AsInt(i any) (int, bool) {
	const (
		intMin = int64(math.MinInt)
		intMax = int64(math.MaxInt)
	)

	switch v := i.(type) {
	case int:
		return v, true
	case int8:
		return int(v), true
	case int16:
		return int(v), true
	case int32:
		return int(v), true
	case int64:
		if v >= intMin && v <= intMax {
			return int(v), true
		}
	case uint:
		if uint64(v) <= uint64(intMax) {
			return int(v), true // #nosec
		}
	case uint8, uint16, uint32:
		return int(reflect.ValueOf(v).Uint()), true // #nosec
	case uint64:
		if v <= uint64(intMax) {
			return int(v), true
		}
	case float32:
		if v >= float32(intMin) && v <= float32(intMax) {
			return int(v), true
		}
	case float64:
		if v >= float64(intMin) && v <= float64(intMax) {
			return int(v), true
		}
	}

	return 0, false
}

func AsUInt64(i any) (uint64, bool) {
	switch v := i.(type) {
	case int64:
		return uint64(v), true // #nosec
	case uint64:
		return v, true // #nosec
	case uint8:
		return uint64(v), true
	case int:
		return uint64(v), true // #nosec
	case int8:
		return uint64(v), true // #nosec
	case int32:
		return uint64(v), true // #nosec
	case uint:
		return uint64(v), true // #nosec
	case uint16:
		return uint64(v), true
	case uint32:
		return uint64(v), true
	case float64:
		return uint64(v), true
	case float32:
		return uint64(v), true
	}

	return 0, false
}

func AsFloat64(v any) (float64, bool) {
	switch v := v.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint64:
		return float64(v), true
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int32:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	}

	return 0.0, false
}

func AsBool(i any) (bool, bool) {
	boolean, ok := i.(bool)
	return boolean, ok
}

func ToBool(i any) bool {
	boolean, _ := i.(bool)
	return boolean
}

func AsString(i any) (string, bool) {
	str, ok := i.(string)
	return str, ok
}

func ToString(i any) string {
	str, _ := i.(string)
	return str
}

func AnysToStrings(input []any) ([]string, error) {
	result := make([]string, 0, len(input))

	for _, item := range input {
		str, ok := item.(string)
		if !ok {
			return nil, fmt.Errorf("element %v is not a string", item)
		}

		result = append(result, str)
	}

	return result, nil
}
