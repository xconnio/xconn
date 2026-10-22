package xconn

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"sync"

	"github.com/xconnio/xconn-core/bindings/go/messages"
	"github.com/xconnio/xconn-core/bindings/go/protocol"
	"github.com/xconnio/xconn-core/bindings/go/serializers"
)

type BaseSession interface {
	ID() uint64
	Realm() string
	AuthID() string
	AuthRole() string
	AuthMethod() string
	AuthExtra() map[string]any
	Serializer() serializers.Serializer
	NetConn() net.Conn
	Read() ([]byte, error)
	Write([]byte) error
	TryWrite([]byte) (bool, error)
	ReadMessage() (messages.Message, error)
	WriteMessage(messages.Message) error
	TryWriteMessage(messages.Message) (bool, error)
	Close() error
}

type nativeBase struct {
	details    *SessionDetails
	handle     uint64
	serializer serializers.Serializer
	once       sync.Once
}

func (b *nativeBase) ID() uint64 { return b.details.ID() }

func (b *nativeBase) Realm() string { return b.details.Realm() }

func (b *nativeBase) AuthID() string { return b.details.AuthID() }

func (b *nativeBase) AuthRole() string { return b.details.AuthRole() }

func (b *nativeBase) AuthMethod() string { return b.details.AuthMethod() }

func (b *nativeBase) AuthExtra() map[string]any { return b.details.AuthExtra() }

func (b *nativeBase) Serializer() serializers.Serializer { return b.serializer }

func (b *nativeBase) NetConn() net.Conn { return nil }

func (b *nativeBase) Read() ([]byte, error) {
	value, err := invoke(context.Background(), "base_read", map[string]any{"handle": b.handle})
	if err != nil {
		return nil, err
	}

	return value.([]byte), nil
}

func (b *nativeBase) Write(bytes []byte) error {
	_, err := invoke(context.Background(), "base_write", map[string]any{"handle": b.handle, "bytes": bytes})
	return err
}

func (b *nativeBase) TryWrite(bytes []byte) (bool, error) {
	value, err := invoke(context.Background(), "base_try_write", map[string]any{"handle": b.handle, "bytes": bytes})
	if err != nil {
		return false, err
	}

	return value == true, nil
}

func (b *nativeBase) ReadMessage() (messages.Message, error) {
	bytes, err := b.Read()
	if err != nil {
		return nil, err
	}

	return b.serializer.Deserialize(bytes)
}

func (b *nativeBase) WriteMessage(message messages.Message) error {
	bytes, err := b.serializer.Serialize(message)
	if err != nil {
		return err
	}

	return b.Write(bytes)
}

func (b *nativeBase) TryWriteMessage(message messages.Message) (bool, error) {
	bytes, err := b.serializer.Serialize(message)
	if err != nil {
		return false, err
	}

	return b.TryWrite(bytes)
}

func (b *nativeBase) Close() error {
	var err error
	b.once.Do(func() {
		_, err = invoke(context.Background(), "base_close", map[string]any{"handle": b.handle})
		closeHandle(b.handle)
	})
	return err
}

func serializerIdentifier(serializer serializers.Serializer) (string, error) {
	switch serializer.(type) {
	case *serializers.JSONSerializer:
		return codecJSON, nil
	case *serializers.MsgPackSerializer:
		return codecMsgPack, nil
	case *serializers.CBORSerializer:
		return codecCBOR, nil
	default:
		return "", fmt.Errorf("local base sessions require a supported serializer")
	}
}

func ConnectInMemoryBase(
	router *Router,
	realm, authID, authRole string,
	serializer serializers.Serializer,
	outQueueSize int,
) (BaseSession, error) {
	name, err := serializerIdentifier(serializer)
	if err != nil {
		return nil, err
	}

	value, err := invoke(
		context.Background(),
		"connect_base",
		map[string]any{
			"handle":         router.handle,
			"realm":          realm,
			"authid":         authID,
			"authrole":       authRole,
			"serializer":     name,
			"out_queue_size": outQueueSize,
		},
	)

	if err != nil {
		return nil, err
	}

	fields := nativeFields(value)
	base := &nativeBase{
		handle:     nativeUint64(fields["handle"]),
		serializer: serializer,
		details:    NewSessionDetails(nativeUint64(fields["session_id"]), realm, authID, authRole, "", nil),
	}

	runtime.AddCleanup(base, closeHandle, base.handle)
	return base, nil
}

func NewSession(base BaseSession, serializer serializers.Serializer) (*Session, error) {
	if native, ok := base.(*nativeBase); ok {
		fields, err := invoke(context.Background(), "session_new", map[string]any{"handle": native.handle})
		if err != nil {
			return nil, err
		}

		session := newSession(context.Background(), nativeFields(fields))
		session.details = NewSessionDetails(
			base.ID(),
			base.Realm(),
			base.AuthID(),
			base.AuthRole(),
			base.AuthMethod(),
			base.AuthExtra(),
		)
		session.base = native
		return session, nil
	}

	var closeOnce sync.Once
	closeBase := func() { closeOnce.Do(func() { _ = base.Close() }) }
	callbacks, err := newForeignCallbacks(context.Background(), func(method string, value any) (any, error) {
		switch method {
		case "peer_read":
			return base.Read()
		case "peer_write":
			return nil, base.Write(value.([]byte))
		case "peer_close":
			closeBase()
			return nil, nil
		default:
			return serializerCallback(serializer, method, value)
		}
	})

	if err != nil {
		return nil, err
	}

	name, err := serializerIdentifier(serializer)
	var codec any = name

	if err != nil {
		codec = map[string]any{"name": codecJSON, "callback": callbacks.handle, "static": serializer.Static()}
	}

	fields, err := invoke(context.Background(), "session_from_peer", map[string]any{
		"session_id": base.ID(), "realm": base.Realm(), "authid": base.AuthID(), "authrole": base.AuthRole(),
		"authmethod": base.AuthMethod(), "authextra": base.AuthExtra(), "serializer": codec, "callback": callbacks.handle,
	})

	if err != nil {
		closeBase()
		callbacks.close()
		return nil, err
	}

	session := newSession(context.Background(), nativeFields(fields), callbacks)
	session.closeBase = closeBase
	return session, nil
}

func ConnectInMemory(router *Router, realm string) (*Session, error) {
	base, err := ConnectInMemoryBase(
		router,
		realm,
		fmt.Sprintf("%012x", protocol.GenerateID()),
		"trusted",
		&serializers.MsgPackSerializer{},
		16,
	)

	if err != nil {
		return nil, err
	}

	return NewSession(base, base.Serializer())
}
