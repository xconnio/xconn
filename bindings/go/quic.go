package xconn

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

// QUICConnection owns a multiplexed connection. Close closes all its sessions and streams.
type QUICConnection struct {
	closed     atomic.Bool
	localAddr  net.Addr
	remoteAddr net.Addr
	handle     uint64
	once       sync.Once
}

// QUICListener accepts authenticated raw streams alongside router-owned WAMP sessions.
// Listeners may use generated or application-supplied certificates.
type QUICListener struct {
	callbacks   *foreignCallbacks
	handle      uint64
	Address     string
	Certificate []byte
	once        sync.Once
}

// ListenQUIC starts a QUIC listener with an ephemeral localhost certificate.
func (r *Router) ListenQUIC(ctx context.Context, address string) (*QUICListener, error) {
	return r.listenQUIC(ctx, map[string]any{"handle": r.handle, "address": address}, nil)
}

// ListenQUICWithCertificate serves a supplied certificate chain and private key.
// TLS verification is client-side; WAMP authenticates application identities.
func (r *Router) ListenQUICWithCertificate(
	ctx context.Context,
	address string,
	certificate tls.Certificate,
) (*QUICListener, error) {
	key, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
	if err != nil {
		return nil, err
	}
	chain := make([]any, len(certificate.Certificate))
	for i, der := range certificate.Certificate {
		chain[i] = der
	}
	return r.listenQUIC(
		ctx,
		map[string]any{"handle": r.handle, "address": address, "certificates": chain, "private_key": key},
		nil,
	)
}

func (r *Router) listenQUIC(
	ctx context.Context,
	fields map[string]any,
	callbacks *foreignCallbacks,
) (*QUICListener, error) {
	value, err := invoke(ctx, "quic_listen", fields)
	if err != nil {
		return nil, err
	}
	fields = nativeFields(value)
	listener := &QUICListener{
		callbacks:   callbacks,
		handle:      nativeUint64(fields["handle"]),
		Address:     stringField(fields, "address"),
		Certificate: fields["certificate"].([]byte),
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		_ = listener.Close()
		return nil, fmt.Errorf("router closed")
	}
	r.listeners = append(r.listeners, &Listener{closer: listener})
	r.mu.Unlock()
	return listener, nil
}

// DialQUIC verifies the server name against the supplied DER trust certificate.
// Address may contain a numeric IP or DNS hostname.
func DialQUIC(ctx context.Context, address, serverName string, certificate []byte) (*QUICConnection, error) {
	value, err := invoke(
		ctx,
		"quic_dial",
		map[string]any{"address": address, "server_name": serverName, "certificate": certificate},
	)
	if err != nil {
		return nil, err
	}
	return newQUICConnection(nativeFields(value)), nil
}

// OpenSession authenticates a WAMP session over an independently owned QUIC connection.
func (c *QUICConnection) OpenSession(ctx context.Context, realm string, client *Client) (*Session, error) {
	if client == nil {
		client = &Client{}
	}
	return client.connect(ctx, "quic_session", map[string]any{"handle": c.handle, "realm": realm})
}

func (c *QUICConnection) Close() error {
	c.once.Do(func() { c.closed.Store(true); closeHandle(c.handle) })
	return nil
}

func (l *QUICListener) Close() error {
	l.once.Do(func() {
		closeHandle(l.handle)
		if l.callbacks != nil {
			l.callbacks.close()
		}
	})
	return nil
}

// QUICStream supports concurrent reading and writing. CloseWrite sends EOF in one direction.
// Context methods allow blocked operations to be cancelled without closing sibling streams.
type QUICStream struct {
	handle uint64
	once   sync.Once
}

func (c *QUICConnection) OpenStream(ctx context.Context) (*QUICStream, error) {
	value, err := invoke(ctx, "quic_open", map[string]any{"handle": c.handle})
	if err != nil {
		return nil, err
	}
	return &QUICStream{handle: nativeUint64(nativeFields(value)["handle"])}, nil
}

// AcceptStream returns the identity established by WAMP authentication on the connection.
func (l *QUICListener) AcceptStream(ctx context.Context) (*QUICStream, *SessionDetails, error) {
	value, err := invoke(ctx, "quic_accept", map[string]any{"handle": l.handle})
	if err != nil {
		return nil, nil, err
	}
	fields := nativeFields(value)
	details := NewSessionDetails(
		nativeUint64(fields["session_id"]),
		stringField(fields, "realm"),
		stringField(fields, "authid"),
		stringField(fields, "authrole"),
		stringField(fields, "authmethod"),
		nativeFields(fields["authextra"]),
	)
	return &QUICStream{handle: nativeUint64(fields["handle"])}, details, nil
}

func (s *QUICStream) Read(data []byte) (int, error) { return s.ReadContext(context.Background(), data) }

func (s *QUICStream) ReadContext(ctx context.Context, data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	size := min(len(data), 1024*1024)
	value, err := invoke(ctx, "quic_read", map[string]any{"handle": s.handle, "size": size})
	if err != nil {
		return 0, err
	}
	bytes := value.([]byte)
	if len(bytes) == 0 {
		return 0, io.EOF
	}
	return copy(data, bytes), nil
}

func (s *QUICStream) Write(data []byte) (int, error) {
	return s.WriteContext(context.Background(), data)
}

func (s *QUICStream) WriteContext(ctx context.Context, data []byte) (int, error) {
	written := 0
	for len(data) > 0 {
		size := min(len(data), 1024*1024)
		value, err := invoke(ctx, "quic_write", map[string]any{"handle": s.handle, "data": data[:size]})
		if err != nil {
			return written, err
		}
		nativeCount := nativeUint64(value)
		if nativeCount > 1024*1024 {
			return written, fmt.Errorf("invalid QUIC write count")
		}
		count := int(nativeCount)
		if count > size {
			return written, fmt.Errorf("invalid QUIC write count")
		}
		if count == 0 {
			return written, io.ErrNoProgress
		}
		written += count
		data = data[count:]
	}
	return written, nil
}

func (s *QUICStream) CloseWrite() error { return s.CloseWriteContext(context.Background()) }

func (s *QUICStream) CloseWriteContext(ctx context.Context) error {
	_, err := invoke(ctx, "quic_finish", map[string]any{"handle": s.handle})
	return err
}

func (s *QUICStream) Close() error {
	s.once.Do(func() { closeHandle(s.handle) })
	return nil
}

// AcceptConnection returns the connection and its first authenticated WAMP identity.
// Callers own the returned connection handle and must close it on shutdown.
func (l *QUICListener) AcceptConnection(ctx context.Context) (*QUICConnection, *SessionDetails, error) {
	value, err := invoke(ctx, "quic_accept_connection", map[string]any{"handle": l.handle})
	if err != nil {
		return nil, nil, err
	}
	fields := nativeFields(value)
	details := NewSessionDetails(
		nativeUint64(fields["session_id"]),
		stringField(fields, "realm"),
		stringField(fields, "authid"),
		stringField(fields, "authrole"),
		stringField(fields, "authmethod"),
		nativeFields(fields["authextra"]),
	)
	return newQUICConnection(fields), details, nil
}

// AcceptStream accepts a raw stream opened by the remote endpoint.
// It consumes unclassified streams; use it on the dialing side, where no listener demultiplexes WAMP.
func (c *QUICConnection) AcceptStream(ctx context.Context) (*QUICStream, error) {
	value, err := invoke(ctx, "quic_accept_remote", map[string]any{"handle": c.handle})
	if err != nil {
		return nil, err
	}
	return &QUICStream{handle: nativeUint64(nativeFields(value)["handle"])}, nil
}

// Wait blocks until the connection closes or ctx is canceled.
func (c *QUICConnection) Wait(ctx context.Context) error {
	if c.closed.Load() {
		return nil
	}
	_, err := invoke(ctx, "quic_wait", map[string]any{"handle": c.handle})
	if c.closed.Load() {
		return nil
	}
	return err
}

// ListenQUIC serves QUIC with this server's WAMP authentication policy.
// A nil certificate selects the generated localhost development certificate.
func (s *Server) ListenQUIC(ctx context.Context, address string, certificate *tls.Certificate) (*QUICListener, error) {

	fields := map[string]any{"handle": s.router.handle, "address": address}
	if err := serverConfiguration(fields, s.config); err != nil {
		return nil, err
	}

	if certificate != nil {
		key, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
		if err != nil {
			return nil, err
		}
		chain := make([]any, len(certificate.Certificate))
		for i, der := range certificate.Certificate {
			chain[i] = der
		}
		fields["certificates"], fields["private_key"] = chain, key
	}
	var callbacks *foreignCallbacks
	if s.authenticator != nil {
		var err error
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
	listener, err := s.router.listenQUIC(ctx, fields, callbacks)
	if err != nil {
		if callbacks != nil {
			callbacks.close()
		}
		return nil, err
	}
	s.mu.Lock()
	s.listeners = append(s.listeners, &Listener{closer: listener})
	s.mu.Unlock()
	return listener, nil
}

// QUICDialOptions configures TLS trust for a numeric or DNS QUIC address.
// Nil RootCertificates uses the native platform trust store.
type QUICDialOptions struct {
	ServerName       string
	RootCertificates [][]byte
}

func DialQUICWithOptions(ctx context.Context, address string, options QUICDialOptions) (*QUICConnection, error) {
	if options.ServerName == "" {
		name, _, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		options.ServerName = name
	}
	fields := map[string]any{"address": address, "server_name": options.ServerName}
	if options.RootCertificates != nil {
		roots := make([]any, len(options.RootCertificates))
		for i, certificate := range options.RootCertificates {
			roots[i] = certificate
		}
		fields["roots"] = roots
	}
	value, err := invoke(ctx, "quic_dial", fields)
	if err != nil {
		return nil, err
	}
	return newQUICConnection(nativeFields(value)), nil
}

// DialQUICWithTLS preserves Go certificate pools and verification callbacks.
// Rust owns QUIC and checks handshake signatures; Go verifies the returned chain
// before this method returns a connection or any WAMP application data is sent.
func DialQUICWithTLS(ctx context.Context, address string, config *tls.Config) (*QUICConnection, error) {
	serverName, _, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	if config == nil {
		return DialQUICWithOptions(ctx, address, QUICDialOptions{ServerName: serverName})
	}
	if config.MinVersion > tls.VersionTLS13 || (config.MaxVersion != 0 && config.MaxVersion < tls.VersionTLS13) {
		return nil, fmt.Errorf("QUIC requires TLS 1.3")
	}
	if len(config.Certificates) != 0 || config.GetClientCertificate != nil {
		return nil, fmt.Errorf("QUIC TLS client certificates are not supported")
	}
	if config.ServerName != "" {
		serverName = config.ServerName
	}
	value, err := invoke(ctx, "quic_dial", map[string]any{
		"address": address, "server_name": serverName, "host_verification": true,
	})
	if err != nil {
		return nil, err
	}
	fields := nativeFields(value)
	connection := newQUICConnection(fields)
	certificates, err := verifyQUICCertificates(fields["certificates"].([]any), serverName, config)
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	if config.VerifyConnection != nil {
		if err = config.VerifyConnection(certificates); err != nil {
			_ = connection.Close()
			return nil, err
		}
	}
	return connection, nil
}

func verifyQUICCertificates(chain []any, serverName string, config *tls.Config) (tls.ConnectionState, error) {
	state := tls.ConnectionState{
		Version:            tls.VersionTLS13,
		HandshakeComplete:  true,
		ServerName:         serverName,
		NegotiatedProtocol: "wamp.2.quic",
	}
	raw := make([][]byte, len(chain))
	for i, value := range chain {
		raw[i] = value.([]byte)
		certificate, err := x509.ParseCertificate(raw[i])
		if err != nil {
			return state, err
		}
		state.PeerCertificates = append(state.PeerCertificates, certificate)
	}
	if len(raw) == 0 {
		return state, fmt.Errorf("missing peer certificate chain")
	}
	if !config.InsecureSkipVerify {
		intermediates := x509.NewCertPool()
		for _, certificate := range state.PeerCertificates[1:] {
			intermediates.AddCert(certificate)
		}
		options := x509.VerifyOptions{DNSName: serverName, Roots: config.RootCAs, Intermediates: intermediates}
		if config.Time != nil {
			options.CurrentTime = config.Time()
		}
		verified, err := state.PeerCertificates[0].Verify(options)
		if err != nil {
			return state, err
		}
		state.VerifiedChains = verified
	}
	if config.VerifyPeerCertificate != nil {
		if err := config.VerifyPeerCertificate(raw, state.VerifiedChains); err != nil {
			return state, err
		}
	}
	return state, nil
}

func newQUICConnection(fields map[string]any) *QUICConnection {
	local, _ := net.ResolveUDPAddr("udp", stringField(fields, "local_address"))
	remote, _ := net.ResolveUDPAddr("udp", stringField(fields, "remote_address"))
	return &QUICConnection{handle: nativeUint64(fields["handle"]), localAddr: local, remoteAddr: remote}
}
