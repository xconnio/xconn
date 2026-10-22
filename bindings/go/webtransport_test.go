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

func TestWebTransportSessionsStreamsAndTLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{SerialNumber: big.NewInt(1), DNSNames: []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: time.Now().Add(-time.Minute),
		NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	router, err := xconn.NewRouter(nil)
	require.NoError(t, err)
	defer router.Close()
	require.NoError(
		t,
		router.AddRealm(
			"realm",
			&xconn.RealmConfig{
				Roles: []xconn.RealmRole{
					{
						Name: "anonymous",
						Permissions: []xconn.Permission{
							{URI: "", MatchPolicy: "prefix", AllowCall: true, AllowRegister: true},
						},
					},
				},
			},
		),
	)
	server := xconn.NewServer(router, nil, nil)
	defer server.Close()
	listener, err := server.ListenWebTransport(
		ctx,
		"127.0.0.1:0",
		"/wamp",
		tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key},
	)
	require.NoError(t, err)
	defer listener.Close()
	address := "https://" + listener.Address + "/wamp"
	_, err = xconn.DialWebTransport(ctx, address, nil)
	require.Error(t, err, "untrusted certificates must be rejected")
	config := &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots}
	config.VerifyConnection = func(tls.ConnectionState) error { return errors.New("pin rejected") }
	_, err = xconn.DialWebTransport(ctx, address, config)
	require.ErrorContains(t, err, "pin rejected")
	verified := false
	config.VerifyConnection = func(state tls.ConnectionState) error {
		verified = state.NegotiatedProtocol == "h3" && len(state.VerifiedChains) == 1
		return nil
	}
	connection, err := xconn.DialWebTransport(ctx, address, config)
	require.NoError(t, err)
	defer connection.Close()
	require.True(t, verified)
	session, err := connection.OpenSession(ctx, "realm", nil)
	require.NoError(t, err)
	defer session.Close()
	second, err := connection.OpenSession(
		ctx,
		"realm",
		&xconn.Client{SerializerSpec: xconn.CBORSerializerSpec},
	)
	require.NoError(t, err)
	defer second.Close()
	require.NoError(
		t,
		second.Register("echo", func(_ context.Context, inv *xconn.Invocation) *xconn.Result {
			return xconn.NewResult(inv.Args()...)
		}).ExecuteContext(ctx).Err,
	)
	require.Equal(
		t,
		"hello",
		session.Call("echo").Arg("hello").ExecuteContext(ctx).ArgStringOr(0, ""),
	)
	outgoing, err := connection.OpenStream(ctx)
	require.NoError(t, err)
	defer outgoing.Close()
	_, err = outgoing.WriteContext(ctx, []byte("request"))
	require.NoError(t, err)
	require.NoError(t, outgoing.CloseWriteContext(ctx))
	incoming, identity, err := listener.AcceptStream(ctx)
	require.NoError(t, err)
	defer incoming.Close()
	require.Equal(t, session.ID(), identity.ID())
	data, err := io.ReadAll(incoming)
	require.NoError(t, err)
	require.Equal(t, "request", string(data))
	_, err = incoming.WriteContext(ctx, []byte("response"))
	require.NoError(t, err)
	require.NoError(t, incoming.CloseWriteContext(ctx))
	data, err = io.ReadAll(outgoing)
	require.NoError(t, err)
	require.Equal(t, "response", string(data))
	require.NoError(t, session.Close())
	require.NoError(t, second.Call("echo").ExecuteContext(ctx).Err)
	wrongPath, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	_, err = xconn.DialWebTransport(wrongPath, "https://"+listener.Address+"/other", config)
	require.Error(t, err)
}
