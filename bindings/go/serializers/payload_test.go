package serializers_test

import (
	"bytes"
	"testing"

	"github.com/xconnio/xconn-core/bindings/go/serializers"
)

func TestPayloads(t *testing.T) {
	for _, id := range []uint64{1, 2, 3} {
		for _, args := range [][]any{nil, {"hello", int64(42)}} {
			b, e := serializers.SerializePayload(id, args, map[string]any{"key": "value"})
			if e != nil {
				t.Fatal(e)
			}
			a, k, e := serializers.DeserializePayload(id, b)
			if e != nil || len(a) != len(args) || k["key"] != "value" {
				t.Fatal(a, k, e)
			}
		}
		b, e := serializers.SerializePayload(id, nil, nil)
		if e != nil || b != nil {
			t.Fatal(b, e)
		}
	}
	b, e := serializers.SerializePayload(0, []any{[]byte("opaque")}, nil)
	if e != nil || !bytes.Equal(b, []byte("opaque")) {
		t.Fatal(b, e)
	}
	if _, e := serializers.SerializePayload(0, []any{"text"}, nil); e == nil {
		t.Fatal("accepted text")
	}
	for _, raw := range []string{`[1]`, `[[],[]]`, `[[],{},{}]`} {
		if _, _, e := serializers.DeserializePayload(1, []byte(raw)); e == nil {
			t.Fatal("accepted", raw)
		}
	}
}

func TestFixedCBORPair(t *testing.T) {
	b, e := serializers.EncodeCBOR([]any{"value"}, map[string]any{})
	if e != nil {
		t.Fatal(e)
	}
	a, k, e := serializers.DecodeCBOR(b)
	if e != nil || len(a) != 1 || a[0] != "value" || k == nil {
		t.Fatal(a, k, e)
	}
	if _, _, e := serializers.DecodeCBOR([]byte{0x80}); e == nil {
		t.Fatal("accepted missing pair")
	}
}
