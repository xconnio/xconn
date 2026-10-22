package xconn

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/url"
	"sync"
)

// WebTransportConnection owns a reliable HTTP/3 connection and its WAMP sessions.
// Close terminates every session and raw stream on the connection.
type WebTransportConnection struct {
	localAddr  net.Addr
	remoteAddr net.Addr
	handle     uint64
	once       sync.Once
}

// WebTransportListener accepts authenticated raw streams alongside router-owned WAMP sessions.
type WebTransportListener struct {
	handle      uint64
	callbacks   *foreignCallbacks
	Address     string
	Certificate []byte
	once        sync.Once
}

// DialWebTransport verifies TLS before exposing the connection to application code.
// A nil config uses the platform's certificate authorities.
func DialWebTransport(
	ctx context.Context,
	address string,
	config *tls.Config,
) (*WebTransportConnection, error) {
	target, err := url.Parse(address)
	if err != nil {
		return nil, err
	}
	if target.Scheme != "https" || target.Hostname() == "" {
		return nil, fmt.Errorf("WebTransport requires an https URL")
	}
	if config == nil {
		config = &tls.Config{MinVersion: tls.VersionTLS13}
	}
	if config.MinVersion > tls.VersionTLS13 ||
		(config.MaxVersion != 0 && config.MaxVersion < tls.VersionTLS13) {
		return nil, fmt.Errorf("WebTransport requires TLS 1.3")
	}
	if len(config.Certificates) != 0 || config.GetClientCertificate != nil {
		return nil, fmt.Errorf("WebTransport TLS client certificates are not supported")
	}
	value, err := invoke(ctx, "webtransport_dial", map[string]any{"url": address})
	if err != nil {
		return nil, err
	}
	fields := nativeFields(value)
	local, _ := net.ResolveUDPAddr("udp", stringField(fields, "local_address"))
	remote, _ := net.ResolveUDPAddr("udp", stringField(fields, "remote_address"))
	connection := &WebTransportConnection{handle: nativeUint64(fields["handle"]), localAddr: local, remoteAddr: remote}
	serverName := target.Hostname()
	if config.ServerName != "" {
		serverName = config.ServerName
	}
	state, err := verifyQUICCertificates(fields["certificates"].([]any), serverName, config)
	state.NegotiatedProtocol = "h3"
	if err == nil && config.VerifyConnection != nil {
		err = config.VerifyConnection(state)
	}
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	return connection, nil
}

func (c *WebTransportConnection) OpenSession(
	ctx context.Context,
	realm string,
	client *Client,
) (*Session, error) {
	if client == nil {
		client = &Client{}
	}
	return client.connect(
		ctx,
		"webtransport_session",
		map[string]any{"handle": c.handle, "realm": realm},
	)
}

func (c *WebTransportConnection) OpenStream(ctx context.Context) (*QUICStream, error) {
	return c.stream(ctx, "webtransport_open")
}

func (c *WebTransportConnection) AcceptStream(ctx context.Context) (*QUICStream, error) {
	return c.stream(ctx, "webtransport_accept_remote")
}

func (c *WebTransportConnection) stream(ctx context.Context, method string) (*QUICStream, error) {
	value, err := invoke(ctx, method, map[string]any{"handle": c.handle})
	if err != nil {
		return nil, err
	}
	return &QUICStream{handle: nativeUint64(nativeFields(value)["handle"])}, nil
}

func (c *WebTransportConnection) Wait(ctx context.Context) error {
	_, err := invoke(ctx, "webtransport_wait", map[string]any{"handle": c.handle})
	return err
}

func (c *WebTransportConnection) Close() error {
	c.once.Do(func() { closeHandle(c.handle) })
	return nil
}

// ListenWebTransport serves one HTTP/3 CONNECT path using the supplied certificate.
func (s *Server) ListenWebTransport(
	ctx context.Context,
	address, path string,
	certificate tls.Certificate,
) (*WebTransportListener, error) {

	key, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
	if err != nil {
		return nil, err
	}
	chain := make([]any, len(certificate.Certificate))
	for i, der := range certificate.Certificate {
		chain[i] = der
	}
	fields := map[string]any{
		"handle":       s.router.handle,
		"address":      address,
		"path":         path,
		"certificates": chain,
		"private_key":  key,
	}
	if err := serverConfiguration(fields, s.config); err != nil {
		return nil, err
	}

	var callbacks *foreignCallbacks
	if s.authenticator != nil {
		callbacks, err = newForeignCallbacks(ctx, s.authenticate)
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
	value, err := invoke(ctx, "webtransport_listen", fields)
	if err != nil {
		if callbacks != nil {
			callbacks.close()
		}
		return nil, err
	}
	fields = nativeFields(value)
	listener := &WebTransportListener{handle: nativeUint64(fields["handle"]), callbacks: callbacks,
		Address: stringField(fields, "address"), Certificate: fields["certificate"].([]byte)}
	s.router.mu.Lock()
	if s.router.closed {
		s.router.mu.Unlock()
		_ = listener.Close()
		return nil, fmt.Errorf("router closed")
	}
	s.router.listeners = append(s.router.listeners, &Listener{closer: listener})
	s.router.mu.Unlock()
	s.mu.Lock()
	s.listeners = append(s.listeners, &Listener{closer: listener})
	s.mu.Unlock()
	return listener, nil
}

func (l *WebTransportListener) AcceptStream(
	ctx context.Context,
) (*QUICStream, *SessionDetails, error) {
	value, err := invoke(ctx, "webtransport_accept", map[string]any{"handle": l.handle})
	if err != nil {
		return nil, nil, err
	}
	fields := nativeFields(value)
	details := NewSessionDetails(
		nativeUint64(fields["session_id"]),
		stringField(fields, "realm"),
		stringField(
			fields,
			"authid",
		),
		stringField(fields, "authrole"),
		stringField(fields, "authmethod"),
		nativeFields(fields["authextra"]),
	)
	return &QUICStream{handle: nativeUint64(fields["handle"])}, details, nil
}

func (l *WebTransportListener) Close() error {
	l.once.Do(func() {
		closeHandle(l.handle)
		if l.callbacks != nil {
			l.callbacks.close()
		}
	})
	return nil
}
