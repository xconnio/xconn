package xconn

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/xconnio/xconn-core/bindings/go/auth"
)

// MultiplexedSession opens raw streams alongside an established WAMP session.
type MultiplexedSession interface {
	OpenStream() (net.Conn, error)
}

// QUICDialerConfig configures a WAMP session on a Rust-owned QUIC connection.
type QUICDialerConfig struct {
	SerializerSpec  SerializerSpec
	Authenticator   auth.ClientAuthenticator
	TLSConfig       *tls.Config
	DialTimeout     time.Duration
	OutQueueSize    int
	KeepAlivePeriod time.Duration
}

// QUICConn preserves the convenience API around an independently owned native connection.
type QUICConn struct{ *QUICConnection }

type QUICSession struct {
	*Session
	connection *QUICConn
}

// ConnectQUIC connects and joins one WAMP realm. Close the session and its connection separately.
func ConnectQUIC(ctx context.Context, address, realm string, config *QUICDialerConfig) (*QUICSession, error) {
	if config == nil {
		config = &QUICDialerConfig{}
	}

	if config.OutQueueSize != 0 || (config.KeepAlivePeriod != 0 && config.KeepAlivePeriod != 15*time.Second) {
		return nil, fmt.Errorf("custom QUIC queue and keepalive configuration is not exposed")
	}

	if config.DialTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, config.DialTimeout)
		defer cancel()
	}

	native, err := DialQUICWithTLS(ctx, address, config.TLSConfig)
	if err != nil {
		return nil, err
	}

	connection := &QUICConn{QUICConnection: native}
	session, err := openQUICSession(ctx, connection, realm, config)
	if err != nil {
		_ = connection.Close()
		return nil, err
	}

	return session, nil
}

func openQUICSession(
	ctx context.Context,
	connection *QUICConn,
	realm string,
	config *QUICDialerConfig,
) (*QUICSession, error) {
	if config == nil {
		config = &QUICDialerConfig{}
	}

	spec := config.SerializerSpec
	if spec == nil {
		spec = CBORSerializerSpec
	}

	session, err := connection.OpenSession(
		ctx,
		realm,
		&Client{Authenticator: config.Authenticator, SerializerSpec: spec, OutQueueSize: config.OutQueueSize},
	)
	if err != nil {
		return nil, err
	}

	return &QUICSession{Session: session, connection: connection}, nil
}

func (s *QUICSession) Connection() *QUICConn { return s.connection }

func (s *QUICSession) OpenSession(ctx context.Context, realm string, config *QUICDialerConfig) (*QUICSession, error) {
	return openQUICSession(ctx, s.connection, realm, config)
}

func (s *QUICSession) OpenStream() (net.Conn, error) { return s.connection.OpenStream() }

func (s *QUICSession) AcceptStream() (net.Conn, error) {
	return s.connection.AcceptRawStream(context.Background())
}

func (c *QUICConn) OpenStream() (net.Conn, error) { return c.OpenRawStream(context.Background()) }

func (c *QUICConn) OpenRawStream(ctx context.Context) (net.Conn, error) {
	stream, err := c.QUICConnection.OpenStream(ctx)
	if err != nil {
		return nil, err
	}

	return &nativeStreamConn{QUICStream: stream, local: c.localAddr, remote: c.remoteAddr}, nil
}

func (c *QUICConn) AcceptRawStream(ctx context.Context) (net.Conn, error) {
	stream, err := c.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}

	return &nativeStreamConn{QUICStream: stream, local: c.localAddr, remote: c.remoteAddr}, nil
}

type nativeStreamConn struct {
	*QUICStream
	local  net.Addr
	remote net.Addr
}

func (s *nativeStreamConn) LocalAddr() net.Addr { return s.local }

func (s *nativeStreamConn) RemoteAddr() net.Addr { return s.remote }

func (s *nativeStreamConn) Read(data []byte) (int, error) {
	n, err := s.QUICStream.Read(data)
	return n, streamDeadlineError(err)
}

func (s *nativeStreamConn) Write(data []byte) (int, error) {
	n, err := s.QUICStream.Write(data)
	return n, streamDeadlineError(err)
}

func streamDeadlineError(err error) error {
	if err != nil && strings.Contains(err.Error(), "stream deadline exceeded") {
		return os.ErrDeadlineExceeded
	}

	return err
}

func (s *nativeStreamConn) SetDeadline(deadline time.Time) error {
	return s.deadlines(deadline, true, true)
}

func (s *nativeStreamConn) SetReadDeadline(deadline time.Time) error {
	return s.deadlines(deadline, true, false)
}

func (s *nativeStreamConn) SetWriteDeadline(deadline time.Time) error {
	return s.deadlines(deadline, false, true)
}

func (s *nativeStreamConn) deadlines(deadline time.Time, read, write bool) error {
	var value any
	if !deadline.IsZero() {
		value = deadline.UnixMilli()
	}

	fields := map[string]any{"handle": s.handle}
	if read {
		fields["read"] = value
	}

	if write {
		fields["write"] = value
	}

	_, err := invoke(context.Background(), "quic_deadlines", fields)
	return err
}

// WebTransportDialerConfig joins a realm over an HTTP/3 connection.
type WebTransportDialerConfig struct {
	SerializerSpec  SerializerSpec
	Authenticator   auth.ClientAuthenticator
	TLSClientConfig *tls.Config
	OutQueueSize    int
}

type WebTransportConn struct{ *WebTransportConnection }
type WebTransportSession struct {
	*Session
	connection *WebTransportConn
}

func ConnectWebTransport(
	ctx context.Context,
	address, realm string,
	config *WebTransportDialerConfig,
) (*WebTransportSession, error) {
	if config == nil {
		config = &WebTransportDialerConfig{}
	}

	connection, err := DialWebTransport(ctx, address, config.TLSClientConfig)
	if err != nil {
		return nil, err
	}

	spec := config.SerializerSpec
	if spec == nil {
		spec = CBORSerializerSpec
	}

	session, err := connection.OpenSession(
		ctx,
		realm,
		&Client{Authenticator: config.Authenticator, SerializerSpec: spec, OutQueueSize: config.OutQueueSize},
	)
	if err != nil {
		_ = connection.Close()
		return nil, err
	}

	return &WebTransportSession{Session: session, connection: &WebTransportConn{connection}}, nil
}

func (s *WebTransportSession) Connection() *WebTransportConn { return s.connection }

func (s *WebTransportSession) OpenStream() (net.Conn, error) {
	stream, err := s.connection.OpenStream(context.Background())
	if err != nil {
		return nil, err
	}

	return &nativeStreamConn{QUICStream: stream, local: s.connection.localAddr, remote: s.connection.remoteAddr}, nil
}

func (c *WebTransportConn) CloseWithError(code uint64, reason string) error {
	if code != 0 || reason != "" {
		return errors.New("custom WebTransport close codes are not exposed")
	}

	return c.Close()
}
