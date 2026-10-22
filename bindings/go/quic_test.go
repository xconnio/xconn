package xconn_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	xconn "github.com/xconnio/xconn-core/bindings/go"
)

func TestQUICSessionsAndAuthenticatedStreams(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	router, err := xconn.NewRouter(nil)
	require.NoError(t, err)
	t.Cleanup(router.Close)
	require.NoError(t, router.AddRealm("quic", &xconn.RealmConfig{Roles: []xconn.RealmRole{{
		Name: "anonymous",
		Permissions: []xconn.Permission{{
			URI: "test.", MatchPolicy: "prefix", AllowCall: true, AllowRegister: true,
		}},
	}}}))
	listener, err := router.ListenQUIC(ctx, "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })
	_, err = xconn.DialQUIC(ctx, listener.Address, "wrong.example", listener.Certificate)
	require.Error(t, err, "TLS must reject a mismatched server name")
	connection, err := xconn.DialQUIC(ctx, listener.Address, "localhost", listener.Certificate)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connection.Close()) })
	session, err := connection.OpenSession(ctx, "quic", nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, session.Close()) })
	accepted, identity, err := listener.AcceptConnection(ctx)
	require.NoError(t, err)
	require.Equal(t, session.ID(), identity.ID())
	t.Cleanup(func() { _ = session.Close(); require.NoError(t, accepted.Close()) })
	reverse, err := accepted.OpenStream(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reverse.Close()) })
	_, err = reverse.WriteContext(ctx, []byte("reverse"))
	require.NoError(t, err)
	require.NoError(t, reverse.CloseWrite())
	remote, err := connection.AcceptStream(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, remote.Close()) })
	received, err := io.ReadAll(remote)
	require.NoError(t, err)
	require.Equal(t, "reverse", string(received))

	require.NoError(
		t,
		session.Register("test.echo", func(_ context.Context, invocation *xconn.Invocation) *xconn.Result {
			return xconn.NewResult(invocation.Args()...)
		}).ExecuteContext(ctx).Err,
	)
	response := session.Call("test.echo").Arg("hello").ExecuteContext(ctx)
	require.NoError(t, response.Err)
	require.Equal(t, "hello", response.ArgStringOr(0, ""))

	stream, err := connection.OpenStream(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stream.Close()) })
	_, err = stream.WriteContext(ctx, []byte("request"))
	require.NoError(t, err)
	require.NoError(t, stream.CloseWriteContext(ctx))
	incoming, streamIdentity, err := listener.AcceptStream(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, incoming.Close()) })
	require.Equal(t, session.ID(), streamIdentity.ID())
	require.Equal(t, "quic", streamIdentity.Realm())
	body, err := io.ReadAll(incoming)
	require.NoError(t, err)
	require.Equal(t, "request", string(body))
	_, err = incoming.WriteContext(ctx, []byte("response"))
	require.NoError(t, err)
	require.NoError(t, incoming.CloseWriteContext(ctx))
	body, err = io.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, "response", string(body))

	blocked, err := connection.OpenStream(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, blocked.Close()) })
	readContext, cancelRead := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancelRead()
	_, err = blocked.ReadContext(readContext, make([]byte, 1))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	// Stream cleanup must not close sibling WAMP sessions.
	require.NoError(t, blocked.Close())
	require.NoError(t, session.Call("test.echo").ExecuteContext(ctx).Err)
	require.NoError(t, session.Close())
	require.NoError(t, connection.Close())
	require.NoError(t, accepted.Wait(ctx))
}

func TestQUICSuppliedCertificate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), DNSNames: []string{"edge.example"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	router, err := xconn.NewRouter(nil)
	require.NoError(t, err)
	defer router.Close()
	listener, err := router.ListenQUICWithCertificate(ctx, "127.0.0.1:0", tls.Certificate{
		Certificate: [][]byte{der}, PrivateKey: key,
	})
	require.NoError(t, err)
	defer listener.Close()
	require.Equal(t, der, listener.Certificate)
	connection, err := xconn.DialQUIC(ctx, listener.Address, "edge.example", der)
	require.NoError(t, err)
	require.NoError(t, connection.Close())
	require.NoError(t, connection.Wait(ctx))
	wrongKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	_, err = router.ListenQUICWithCertificate(ctx, "127.0.0.1:0", tls.Certificate{
		Certificate: [][]byte{der}, PrivateKey: wrongKey,
	})
	require.Error(t, err, "a mismatched certificate and private key must be rejected")
}

func TestQUICTLSCallbacksAndDNS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	router, err := xconn.NewRouter(nil)
	require.NoError(t, err)
	defer router.Close()
	listener, err := router.ListenQUIC(ctx, "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	certificate, err := x509.ParseCertificate(listener.Certificate)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	verified := false
	config := &tls.Config{
		MinVersion: tls.VersionTLS13, ServerName: "localhost", RootCAs: roots,
		VerifyConnection: func(state tls.ConnectionState) error {
			verified = state.HandshakeComplete && len(state.VerifiedChains) == 1 && state.ServerName == "localhost"
			return nil
		},
	}
	_, port, err := net.SplitHostPort(listener.Address)
	require.NoError(t, err)
	connection, err := xconn.DialQUICWithTLS(ctx, net.JoinHostPort("localhost", port), config)
	require.NoError(t, err)
	require.True(t, verified)
	require.NoError(t, connection.Close())
	config.VerifyConnection = func(tls.ConnectionState) error { return errors.New("pin rejected") }
	_, err = xconn.DialQUICWithTLS(ctx, listener.Address, config)
	require.ErrorContains(t, err, "pin rejected")
	config.VerifyConnection = nil
	config.ServerName = "wrong.example"
	_, err = xconn.DialQUICWithTLS(ctx, listener.Address, config)
	require.Error(t, err)
}

func TestQUICIdleConnectionRemainsAlive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	router, err := xconn.NewRouter(nil)
	require.NoError(t, err)
	defer router.Close()
	require.NoError(t, router.AddRealm("idle", nil))
	listener, err := router.ListenQUIC(ctx, "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	connection, err := xconn.DialQUIC(ctx, listener.Address, "localhost", listener.Certificate)
	require.NoError(t, err)
	defer connection.Close()
	session, err := connection.OpenSession(ctx, "idle", nil)
	require.NoError(t, err)
	defer session.Close()
	// Exceed QUIC's default 30-second idle timeout without sending WAMP traffic.
	idle, stop := context.WithTimeout(ctx, 35*time.Second)
	defer stop()
	require.ErrorIs(t, connection.Wait(idle), context.DeadlineExceeded)
	require.True(t, session.Connected())
}
