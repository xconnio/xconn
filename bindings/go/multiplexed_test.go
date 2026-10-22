package xconn_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	xconn "github.com/xconnio/xconn-core/bindings/go"
)

func TestMultiplexedSessionDeadlines(t *testing.T) {
	for _, transport := range []string{"quic", "webtransport"} {
		t.Run(transport, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			router, err := xconn.NewRouter(nil)
			require.NoError(t, err)
			defer router.Close()
			require.NoError(t, router.AddRealm("realm", nil))
			server := xconn.NewServer(router, nil, nil)
			defer server.Close()
			identity, err := xconn.GenerateSelfSignedTLSConfig()
			require.NoError(t, err)
			parsed, err := x509.ParseCertificate(identity.Certificates[0].Certificate[0])
			require.NoError(t, err)
			roots := x509.NewCertPool()
			roots.AddCert(parsed)
			trust := &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots}
			var session xconn.MultiplexedSession
			var accept func() (*xconn.QUICStream, *xconn.SessionDetails, error)
			if transport == "quic" {
				listener, err := server.ListenQUIC(ctx, "127.0.0.1:0", &identity.Certificates[0])
				require.NoError(t, err)
				connected, err := xconn.ConnectQUIC(
					ctx,
					listener.Address,
					"realm",
					&xconn.QUICDialerConfig{TLSConfig: trust},
				)
				require.NoError(t, err)
				defer connected.Connection().Close()
				defer connected.Close()
				second, err := connected.OpenSession(ctx, "realm", nil)
				require.NoError(t, err)
				defer second.Close()
				require.NoError(t, second.Close())
				require.True(t, connected.Connected())
				session = connected
				accept = func() (*xconn.QUICStream, *xconn.SessionDetails, error) { return listener.AcceptStream(ctx) }
			} else {
				listener, err := server.ListenWebTransport(ctx, "127.0.0.1:0", "/wamp", identity.Certificates[0])
				require.NoError(t, err)
				connected, err := xconn.ConnectWebTransport(
					ctx, "https://"+listener.Address+"/wamp", "realm",
					&xconn.WebTransportDialerConfig{TLSClientConfig: trust},
				)
				require.NoError(t, err)
				defer connected.Connection().Close()
				defer connected.Close()
				session = connected
				accept = func() (*xconn.QUICStream, *xconn.SessionDetails, error) { return listener.AcceptStream(ctx) }
			}
			stream, err := session.OpenStream()
			require.NoError(t, err)
			defer stream.Close()
			require.NotNil(t, stream.LocalAddr())
			require.NotNil(t, stream.RemoteAddr())
			_, err = stream.Write([]byte("a"))
			require.NoError(t, err)
			remote, _, err := accept()
			require.NoError(t, err)
			defer remote.Close()
			_, err = io.ReadFull(remote, make([]byte, 1))
			require.NoError(t, err)

			// Shortening the deadline interrupts an already pending read.
			finished := make(chan error, 1)
			go func() { _, err := stream.Read(make([]byte, 1)); finished <- err }()
			require.NoError(t, stream.SetReadDeadline(time.Now().Add(20*time.Millisecond)))
			select {
			case err := <-finished:
				require.ErrorIs(t, err, os.ErrDeadlineExceeded)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			// Clearing the deadline permits fresh I/O on the same stream.
			require.NoError(t, stream.SetReadDeadline(time.Time{}))
			_, err = remote.WriteContext(ctx, []byte("b"))
			require.NoError(t, err)
			received := make([]byte, 1)
			_, err = io.ReadFull(stream, received)
			require.NoError(t, err)
			require.Equal(t, []byte("b"), received)

			// Extending a pending deadline must retain the read and its bytes.
			require.NoError(t, stream.SetReadDeadline(time.Now().Add(100*time.Millisecond)))
			go func() { _, err := io.ReadFull(stream, received); finished <- err }()
			require.NoError(t, stream.SetReadDeadline(time.Now().Add(time.Second)))
			_, err = remote.WriteContext(ctx, []byte("c"))
			require.NoError(t, err)
			select {
			case err := <-finished:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			require.Equal(t, []byte("c"), received)
			require.NoError(t, stream.SetWriteDeadline(time.Now().Add(-time.Second)))
			_, err = stream.Write([]byte("expired"))
			require.ErrorIs(t, err, os.ErrDeadlineExceeded)
		})
	}
}
