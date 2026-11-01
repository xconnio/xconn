package xconn

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/xconnio/xconn-core/bindings/go/auth"
	"github.com/xconnio/xconn-core/bindings/go/messages"
	"github.com/xconnio/xconn-core/bindings/go/serializers"
)

type Authenticator struct {
	Method string
	ID     string
	Secret string
}

type Client struct {
	// RoutedOnly disables automatic direct-path discovery for this connection.
	RoutedOnly bool

	// Direct optionally overrides direct authentication, ICE servers and retries.
	// By default, direct connections reuse this client's authenticator.
	Direct *DirectConfig

	Authenticator     auth.ClientAuthenticator
	Serializer        string
	SerializerSpec    SerializerSpec
	NetDial           func(context.Context, string, string) (net.Conn, error)
	DialTimeout       time.Duration
	KeepAliveInterval time.Duration
	KeepAliveTimeout  time.Duration
	OutQueueSize      int
}

func (c *Client) fields() map[string]any {
	serializer := c.Serializer
	if serializer == "" {
		serializer = codecJSON
	}

	if c.SerializerSpec != nil {
		switch c.SerializerSpec.SerializerID() {
		case JSONSerializerID:
			serializer = codecJSON
		case MsgPackSerializerID:
			serializer = codecMsgPack
		case CBORSerializerID:
			serializer = codecCBOR
		}
	}

	return map[string]any{"serializer": serializer}

}

func (c *Client) Connect(ctx context.Context, uri, realm string) (*Session, error) {
	return c.connect(ctx, "connect", map[string]any{"uri": uri, "realm": realm})
}

func (c *Client) connect(ctx context.Context, method string, fields map[string]any) (*Session, error) {
	uri := stringField(fields, "uri")
	if err := c.validate(); err != nil {
		return nil, err
	}

	if c.DialTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.DialTimeout)
		defer cancel()
	}

	f := c.fields()
	if method == "connect" && c.Serializer == "" && c.SerializerSpec == nil && !strings.HasPrefix(uri, "ws") {
		f["serializer"] = codecCBOR
	}

	callbacks, usesCustomCodec, err := c.configureCallbacks(ctx, f)
	if err != nil {
		return nil, err
	}
	keepCallbacks := false
	defer func() {
		if callbacks != nil && !keepCallbacks {
			callbacks.close()
		}
	}()

	if c.KeepAliveInterval > 0 {
		f["ping_interval"] = c.KeepAliveInterval.Seconds()

		if c.KeepAliveTimeout > 0 {
			f["ping_timeout"] = c.KeepAliveTimeout.Seconds()
		}
	}

	for key, value := range fields {
		f[key] = value
	}

	result, err := invoke(ctx, method, f)
	if err != nil {
		if callbacks != nil {
			if failure := callbacks.failure(); failure != nil {
				return nil, failure
			}
		}

		return nil, err
	}

	var session *Session
	if usesCustomCodec {
		keepCallbacks = true
		session = newSession(ctx, nativeFields(result), callbacks)
	} else {
		session = newSession(ctx, nativeFields(result))
	}
	// Explicit WebRTC sessions keep their existing transport-specific ownership.
	if c.RoutedOnly || (method != "connect" && method != "quic_session" && method != "webtransport_session") {
		return session, nil
	}
	options := DirectConfig{Authenticator: c.Authenticator, Serializer: c.SerializerSpec}
	if c.Direct != nil {
		options = *c.Direct
		if options.Authenticator == nil {
			options.Authenticator = c.Authenticator
		}
		if options.Serializer == nil {
			options.Serializer = c.SerializerSpec
		}
	}
	// Connect's context bounds dialing; the manager belongs to the returned session.
	session.peer, err = session.startPeer(context.WithoutCancel(ctx), &options)
	if err != nil {
		_ = session.Close()
		return nil, err
	}
	return session, nil
}

func (a Authenticator) AuthMethod() string {
	if a.Method == "" {
		return "anonymous"
	}

	return a.Method
}

func (a Authenticator) AuthID() string { return a.ID }

func (a Authenticator) AuthExtra() map[string]any {
	if a.AuthMethod() == string(auth.CryptoSign) {
		authenticator, err := auth.NewCryptoSignAuthenticator(a.ID, a.Secret, nil)
		if err == nil {
			return authenticator.AuthExtra()
		}
	}

	return nil
}

func (a Authenticator) Authenticate(challenge messages.Challenge) (*messages.Authenticate, error) {
	var authenticator auth.ClientAuthenticator
	switch a.AuthMethod() {
	case "ticket":
		authenticator = auth.NewTicketAuthenticator(a.ID, a.Secret, nil)
	case "wampcra":
		authenticator = auth.NewWAMPCRAAuthenticator(a.ID, a.Secret, nil)
	case string(auth.CryptoSign):
		var err error
		authenticator, err = auth.NewCryptoSignAuthenticator(a.ID, a.Secret, nil)

		if err != nil {
			return nil, err
		}
	default:
		authenticator = auth.NewAnonymousAuthenticator(a.ID, nil)
	}

	return authenticator.Authenticate(challenge)
}

func ConnectAnonymous(ctx context.Context, uri, realm string) (*Session, error) {
	return (&Client{}).Connect(ctx, uri, realm)
}

func ConnectTicket(ctx context.Context, uri, realm, authid, ticket string) (*Session, error) {
	return (&Client{Authenticator: auth.NewTicketAuthenticator(authid, ticket, nil)}).Connect(ctx, uri, realm)
}

func ConnectCRA(ctx context.Context, uri, realm, authid, secret string) (*Session, error) {
	return (&Client{Authenticator: auth.NewWAMPCRAAuthenticator(authid, secret, nil)}).Connect(ctx, uri, realm)
}

func ConnectCryptosign(ctx context.Context, uri, realm, authid, key string) (*Session, error) {
	authenticator, err := auth.NewCryptoSignAuthenticator(authid, key, nil)
	if err != nil {
		return nil, err
	}

	return (&Client{Authenticator: authenticator}).Connect(ctx, uri, realm)
}

func serializerCallback(serializer serializers.Serializer, method string, value any) (any, error) {
	switch method {
	case "serialize":
		fields, ok := value.([]any)
		if !ok {
			return nil, fmt.Errorf("native serializer fields must be an array")
		}

		message, err := messages.NewForWire(fields)
		if err != nil {
			return nil, err
		}

		if err = message.Parse(fields); err != nil {
			return nil, err
		}

		return serializer.Serialize(message)
	case "deserialize":
		data, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("native serializer payload must be bytes")
		}

		message, err := serializer.Deserialize(data)
		if err != nil {
			return nil, err
		}

		return message.Marshal(), nil
	default:
		return nil, unexpectedCallback(method)
	}
}

func (c *Client) validate() error {
	if c.NetDial != nil {
		return fmt.Errorf("custom NetDial is not exposed by the native binding yet")
	}

	if c.OutQueueSize != 0 {
		return fmt.Errorf("network OutQueueSize is not exposed by the native binding yet")
	}

	if c.SerializerSpec != nil {
		switch c.SerializerSpec.SubProtocol() {
		case JSONWebsocketProtocol, CBORWebsocketProtocol, MsgPackWebsocketProtocol:
		default:
			return fmt.Errorf("custom serializer subprotocol is not exposed by the native binding yet")
		}

		switch c.SerializerSpec.SerializerID() {
		case JSONSerializerID, CBORSerializerID, MsgPackSerializerID:
		default:
			return fmt.Errorf("custom serializer ID is not exposed by the native binding yet")
		}
	}

	return nil
}

// configureCallbacks adapts host authentication and custom serializers only.
// Protocol discovery, signaling and transport scheduling stay in Rust.
func (c *Client) configureCallbacks(ctx context.Context, f map[string]any) (*foreignCallbacks, bool, error) {
	var callbacks *foreignCallbacks
	var customSerializer serializers.Serializer

	if c.SerializerSpec != nil {
		switch serializer := c.SerializerSpec.Serializer().(type) {
		case *serializers.JSONSerializer, *serializers.MsgPackSerializer, *serializers.CBORSerializer:
		default:
			customSerializer = serializer
		}
	}

	if c.Authenticator != nil || customSerializer != nil {
		var err error
		callbacks, err = newForeignCallbacks(ctx, func(method string, value any) (any, error) {
			switch method {
			case "authenticate":
				fields := nativeFields(value)
				challenge := messages.NewChallenge(stringField(fields, "method"), nativeFields(fields["extra"]))
				response, err := c.Authenticator.Authenticate(*challenge)
				if err != nil {
					return nil, err
				}

				return map[string]any{"signature": response.Signature(), "extra": response.Extra()}, nil
			case "serialize", "deserialize":
				return serializerCallback(customSerializer, method, value)
			default:
				return nil, unexpectedCallback(method)
			}
		})

		if err != nil {
			return nil, false, err
		}

		if c.Authenticator != nil {
			f["auth"] = map[string]any{
				"method":   c.Authenticator.AuthMethod(),
				"authid":   c.Authenticator.AuthID(),
				"extra":    c.Authenticator.AuthExtra(),
				"callback": callbacks.handle,
			}
		}

		if customSerializer != nil {
			f["serializer"] = map[string]any{
				"name":     f["serializer"],
				"callback": callbacks.handle,
				"static":   customSerializer.Static(),
			}
		}
	}

	return callbacks, customSerializer != nil, nil
}
