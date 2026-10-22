package xconn

import (
	"github.com/xconnio/xconn-core/bindings/go/serializers"
	"github.com/xconnio/xconn-core/bindings/go/transports"
)

const (
	JSONSerializerID    SerializerID = 1
	MsgPackSerializerID SerializerID = 2
	CBORSerializerID    SerializerID = 3
)

//nolint:gochecknoglobals // Retains the established serializer-spec API.
var (
	JSONSerializerSpec    = NewSerializerSpec("wamp.2.json", &serializers.JSONSerializer{}, JSONSerializerID)
	MsgPackSerializerSpec = NewSerializerSpec("wamp.2.msgpack", &serializers.MsgPackSerializer{}, MsgPackSerializerID)
	CBORSerializerSpec    = NewSerializerSpec("wamp.2.cbor", &serializers.CBORSerializer{}, CBORSerializerID)
)

type SerializerID transports.Serializer

type SerializerSpec interface {
	SubProtocol() string
	Serializer() serializers.Serializer
	SerializerID() SerializerID
}

type serializerSpec struct {
	subProtocol  string
	serializer   serializers.Serializer
	serializerID SerializerID
}

func (w *serializerSpec) SubProtocol() string {
	return w.subProtocol
}

func (w *serializerSpec) Serializer() serializers.Serializer {
	return w.serializer
}

func (w *serializerSpec) SerializerID() SerializerID {
	return w.serializerID
}

func NewSerializerSpec(subProtocol string, serializer serializers.Serializer,
	serializerID SerializerID) SerializerSpec {
	return &serializerSpec{
		subProtocol:  subProtocol,
		serializer:   serializer,
		serializerID: serializerID,
	}
}

const (
	codecJSON                = "json"
	codecCBOR                = "cbor"
	codecMsgPack             = "msgpack"
	JSONWebsocketProtocol    = "wamp.2.json"
	CBORWebsocketProtocol    = "wamp.2.cbor"
	MsgPackWebsocketProtocol = "wamp.2.msgpack"
)
