package serializers

import "github.com/xconnio/xconn-core/bindings/go/internal/bridge"

func SerializePayload(codec uint64, args []any, kwargs map[string]any) ([]byte, error) {
	value, err := bridge.Call("payload_encode", codec, args, kwargs)
	if err != nil {
		return nil, err
	}

	return bridge.Bytes(value), nil
}

func DeserializePayload(codec uint64, data []byte) ([]any, map[string]any, error) {
	value, err := bridge.Call("payload_decode", codec, data)
	if err != nil {
		return nil, nil, err
	}

	fields := bridge.Slice(value)
	return bridge.Slice(fields[0]), bridge.Dict(fields[1]), nil
}

func JSONEncodePayload(args []any, kwargs map[string]any) ([]byte, error) {
	return SerializePayload(1, args, kwargs)
}

func JSONDecodePayload(data []byte) ([]any, map[string]any, error) {
	return DeserializePayload(1, data)
}

func MsgPackEncodePayload(args []any, kwargs map[string]any) ([]byte, error) {
	return SerializePayload(2, args, kwargs)
}

func MsgPackDecodePayload(data []byte) ([]any, map[string]any, error) {
	return DeserializePayload(2, data)
}

func CBOREncodePayload(args []any, kwargs map[string]any) ([]byte, error) {
	return SerializePayload(3, args, kwargs)
}

func CBORDecodePayload(data []byte) ([]any, map[string]any, error) {
	return DeserializePayload(3, data)
}

// EncodeCBOR retains the legacy fixed two-field envelope.
func EncodeCBOR(args []any, kwargs map[string]any) ([]byte, error) {
	v, e := bridge.Call("cbor_pair_encode", args, kwargs)
	return bridge.Bytes(v), e
}

func DecodeCBOR(data []byte) ([]any, map[string]any, error) {
	v, e := bridge.Call("cbor_pair_decode", data)
	if e != nil {
		return nil, nil, e
	}

	f := bridge.Slice(v)
	return bridge.Slice(f[0]), bridge.Dict(f[1]), nil
}
