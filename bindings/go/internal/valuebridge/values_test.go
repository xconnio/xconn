package valuebridge_test

import (
	"math"
	"reflect"
	"testing"

	"github.com/xconnio/xconn-core/bindings/go/internal/valuebridge"
)

func TestValuesAndMalformedTransfers(t *testing.T) {
	input := []any{
		nil,
		false,
		true,
		int64(-1),
		uint64(math.MaxUint64),
		1.5,
		"hello",
		[]byte{0, 255},
		map[string]any{"key": []any{int64(1)}},
	}
	encoded, err := valuebridge.Encode(input)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := valuebridge.Decode(encoded)
	if err != nil || !reflect.DeepEqual(input, decoded) {
		t.Fatalf("%#v %v", decoded, err)
	}
	for _, data := range [][]byte{nil, {255}, {8, 255, 255, 255, 255}, {6, 1, 0, 0, 0, 255}, append(encoded, 0)} {
		if _, err := valuebridge.Decode(data); err == nil {
			t.Fatalf("accepted malformed %x", data)
		}
	}
	cyclic := map[string]any{}
	cyclic["self"] = cyclic
	if _, err := valuebridge.Encode(cyclic); err == nil {
		t.Fatal("accepted cycle")
	}
}

func FuzzDecode(f *testing.F) {
	for _, seed := range [][]byte{{0}, {8, 0, 0, 0, 0}, {9, 0, 0, 0, 0}, {255}} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 4096 {
			return
		}
		value, err := valuebridge.Decode(data)
		if err == nil {
			if _, err := valuebridge.Encode(value); err != nil {
				t.Fatal(err)
			}
		}
	})
}

func TestNamedGoApplicationValues(t *testing.T) {
	type duration int64
	type IDs []uint64
	type Byte uint8
	type Bytes []Byte
	type data struct {
		Duration duration `json:"duration"`
		IDs      IDs      `json:"ids"`
		Hidden   string   `json:"-"`
		Empty    string   `json:"empty,omitempty"`
	}
	input := data{Duration: 123, IDs: IDs{1, 2}, Hidden: "secret"}
	encoded, e := valuebridge.Encode(input)
	if e != nil {
		t.Fatal(e)
	}
	got, e := valuebridge.Decode(encoded)
	if e != nil {
		t.Fatal(e)
	}
	want := map[string]any{"duration": int64(123), "ids": []any{uint64(1), uint64(2)}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%#v", got)
	}
	b, e := valuebridge.Encode(Bytes{1, 2})
	if e != nil {
		t.Fatal(e)
	}
	decoded, e := valuebridge.Decode(b)
	if e != nil || !reflect.DeepEqual(decoded, []byte{1, 2}) {
		t.Fatal(decoded, e)
	}
	var pointer *int
	b, e = valuebridge.Encode(pointer)
	if e != nil {
		t.Fatal(e)
	}
	decoded, e = valuebridge.Decode(b)
	if e != nil || decoded != nil {
		t.Fatal(decoded, e)
	}
	type node struct{ Next *node }
	cycle := &node{}
	cycle.Next = cycle
	if _, e = valuebridge.Encode(cycle); e == nil {
		t.Fatal("pointer cycle accepted")
	}
}
