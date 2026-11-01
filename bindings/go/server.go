package xconn

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/xconnio/xconn-core/bindings/go/auth"
	"github.com/xconnio/xconn-core/bindings/go/messages"
)

type Network string

const (
	NetworkUnix Network = "unix"
	NetworkTCP  Network = "tcp"
)

type ListenerType int

const (
	ListenerWebSocket ListenerType = iota
	ListenerRawSocket
	ListenerUniversalTCP
	ServerOutQueueSizeDefault = 8
)

type ServerConfig struct {
	KeepAliveInterval time.Duration
	KeepAliveTimeout  time.Duration
	OutQueueSize      int
}

type Server struct {
	router        *Router
	authenticator auth.ServerAuthenticator
	config        *ServerConfig
	mu            sync.Mutex
	listeners     []*Listener
}

func NewServer(router *Router, authenticator auth.ServerAuthenticator, config *ServerConfig) *Server {
	return &Server{router: router, authenticator: authenticator, config: config}
}

func (s *Server) RegisterSpec(spec SerializerSpec) error {
	if spec == nil {
		return fmt.Errorf("serializer spec is required")
	}

	switch spec.SerializerID() {
	case JSONSerializerID, MsgPackSerializerID, CBORSerializerID:
		return fmt.Errorf("spec for %s is already registered", spec.SubProtocol())
	default:
		return fmt.Errorf("custom server serializer specs are not exposed by the binding yet")
	}
}

func (s *Server) ListenAndServeWebSocket(network Network, address string) (*Listener, error) {
	return s.listen(network, address, "websocket", nil)
}

func (s *Server) ListenAndServeRawSocket(network Network, address string) (*Listener, error) {
	return s.listen(network, address, "rawsocket", nil)
}

// ListenAndServeYamux serves WAMP over yamux on a Unix or TCP connection.
func (s *Server) ListenAndServeYamux(network Network, address string) (*Listener, error) {
	return s.listen(network, address, "yamux", nil)
}

// ListenAndServeYamuxTLS serves encrypted yamux over TCP using a server certificate.
func (s *Server) ListenAndServeYamuxTLS(address string, certificate tls.Certificate) (*Listener, error) {
	return s.listen(NetworkTCP, address, "yamux", &certificate)
}

func (s *Server) listen(network Network, address, transport string, certificate *tls.Certificate) (*Listener, error) {

	if network != NetworkTCP && network != NetworkUnix {
		return nil, fmt.Errorf("unsupported listener network %s", network)
	}

	fields := map[string]any{
		"handle":    s.router.handle,
		"address":   address,
		"transport": transport,
		"network":   string(network),
	}

	if certificate != nil {
		key, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
		if err != nil {
			return nil, err
		}
		chain := make([]any, len(certificate.Certificate))
		for index, der := range certificate.Certificate {
			chain[index] = der
		}
		fields["certificates"], fields["private_key"] = chain, key
	}

	if err := serverConfiguration(fields, s.config); err != nil {
		return nil, err
	}

	var callbacks *foreignCallbacks

	if s.authenticator != nil {
		var err error
		callbacks, err = newForeignCallbacks(context.Background(), s.authenticate)

		if err != nil {
			return nil, err
		}

		methods := s.authenticator.Methods()
		names := make([]string, len(methods))

		for i, method := range methods {
			names[i] = string(method)
		}

		fields["server_auth"] = map[string]any{"callback": callbacks.handle, "methods": names}
	}

	value, err := invoke(context.Background(), "listen", fields)
	if err != nil {
		if callbacks != nil {
			callbacks.close()
		}

		return nil, err
	}

	response := nativeFields(value)
	listener := &Listener{handle: nativeUint64(response["handle"]), Address: stringField(response, "address")}
	if network == NetworkUnix {
		listener.addr = &net.UnixAddr{Name: listener.Address, Net: "unix"}
	}

	if callbacks != nil {
		listener.onClose = callbacks.close
	}

	s.mu.Lock()
	s.listeners = append(s.listeners, listener)
	s.mu.Unlock()
	s.router.mu.Lock()

	if s.router.closed {
		s.router.mu.Unlock()
		_ = listener.Close()
		return nil, fmt.Errorf("router closed")
	}

	s.router.listeners = append(s.router.listeners, listener)
	s.router.mu.Unlock()
	return listener, nil
}

func (s *Server) authenticate(method string, value any) (any, error) {
	if method != "server_authenticate" {
		return nil, unexpectedCallback(method)
	}

	fields := nativeFields(value)
	name := stringField(fields, "method")
	hello := messages.NewHello(
		stringField(fields, "realm"),
		stringField(fields, "authid"),
		nativeFields(fields["extra"]),
		nil,
		[]string{name},
	)
	var request auth.Request
	switch name {
	case "ticket":
		request = auth.NewTicketRequest(hello, stringField(fields, "ticket"))
	case "cryptosign":
		request = auth.NewCryptoSignRequest(hello, stringField(fields, "public_key"))
	default:
		request = auth.NewRequest(hello, auth.Method(name))
	}

	response, err := s.authenticator.Authenticate(request)
	if err != nil {
		return nil, err
	}

	fields = map[string]any{
		"authid":   response.AuthID(),
		"authrole": response.AuthRole(),
		"ttl":      response.TTL().Seconds(),
	}

	if cra, ok := response.(*auth.CRAResponse); ok {
		fields["secret"], fields["salt"] = cra.Secret(), cra.Salt()
		fields["iterations"], fields["keylen"] = cra.Iterations(), cra.KeyLen()
	}

	return fields, nil
}

func (s *Server) Close() error {
	s.mu.Lock()
	listeners := s.listeners
	s.listeners = nil
	s.mu.Unlock()

	for _, listener := range listeners {
		if err := listener.Close(); err != nil {
			return err
		}
	}

	return nil
}

func NewListener(closer io.Closer, addr net.Addr) *Listener {
	return &Listener{closer: closer, addr: addr, Address: addr.String()}
}

func (l *Listener) Addr() net.Addr {
	if l.addr != nil {
		return l.addr
	}

	address, _ := net.ResolveTCPAddr("tcp", l.Address)
	return address
}

func (l *Listener) Port() int {
	if address, ok := l.Addr().(*net.TCPAddr); ok {
		return address.Port
	}

	return 0
}

func serverConfiguration(fields map[string]any, config *ServerConfig) error {
	if config == nil {
		return nil
	}
	if config.OutQueueSize != 0 {
		return fmt.Errorf("server OutQueueSize is not exposed by the native binding yet")
	}
	if config.KeepAliveInterval < 0 || config.KeepAliveTimeout < 0 {
		return fmt.Errorf("keepalive durations must not be negative")
	}
	if config.KeepAliveInterval == 0 && config.KeepAliveTimeout != 0 {
		return fmt.Errorf("keepalive timeout requires an interval")
	}
	if config.KeepAliveInterval > 0 {
		fields["ping_interval"] = config.KeepAliveInterval.Seconds()
		if config.KeepAliveTimeout > 0 {
			fields["ping_timeout"] = config.KeepAliveTimeout.Seconds()
		}
	}
	return nil
}
