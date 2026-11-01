package xconn_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	api "github.com/xconnio/xconn-core/bindings/go"
)

const (
	yamuxMemoryTransport = "yamux-memory"
	windowsOS            = "windows"
)

type streamFile struct {
	*bytes.Reader
	closed atomic.Int32
}

func (f *streamFile) Close() error {
	f.closed.Add(1)
	return nil
}

func rpcStreamSessions(t *testing.T, transport string) (context.Context, *api.Session, *api.Session) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	router, err := api.NewRouter(nil)
	require.NoError(t, err)
	t.Cleanup(router.Close)
	require.NoError(t, router.AddRealm("streams", &api.RealmConfig{Roles: []api.RealmRole{{
		Name: "anonymous", Permissions: []api.Permission{{
			URI: "app.", MatchPolicy: "prefix", AllowCall: true, AllowRegister: true,
		}},
	}}}))
	if transport == "yamux" || transport == "yamux-unix" || transport == yamuxMemoryTransport {
		network, address := api.NetworkTCP, "127.0.0.1:0"
		if transport == "yamux-unix" || (transport == yamuxMemoryTransport && runtime.GOOS != windowsOS) {
			if runtime.GOOS == windowsOS {
				t.Skip("Unix sockets are unavailable")
			}
			network, address = api.NetworkUnix, filepath.Join(t.TempDir(), "app.sock")
		}
		server := api.NewServer(router, nil, nil)
		t.Cleanup(func() { _ = server.Close() })
		listener, listenErr := server.ListenAndServeYamux(network, address)
		require.NoError(t, listenErr)
		uri := "yamux://" + listener.Address
		if network == api.NetworkUnix {
			uri = "unix+yamux://" + listener.Address
		}
		connect := func() *api.Session {
			session, connectErr := api.ConnectAnonymous(ctx, uri, "streams")
			require.NoError(t, connectErr)
			t.Cleanup(func() { _ = session.Close() })
			return session
		}
		backend := connect()
		if transport == yamuxMemoryTransport {
			target, connectErr := api.ConnectInMemory(router, "streams")
			require.NoError(t, connectErr)
			t.Cleanup(func() { _ = target.Close() })
			return ctx, backend, target
		}
		return ctx, backend, connect()
	}
	listener, err := router.ListenQUIC(ctx, "127.0.0.1:0")
	require.NoError(t, err)
	connect := func() *api.Session {
		connection, dialErr := api.DialQUIC(ctx, listener.Address, "localhost", listener.Certificate)
		require.NoError(t, dialErr)
		t.Cleanup(func() { _ = connection.Close() })
		session, joinErr := connection.OpenSession(ctx, "streams", nil)
		require.NoError(t, joinErr)
		t.Cleanup(func() { _ = session.Close() })
		return session
	}
	backend := connect()
	if transport == "webtransport" {
		tlsConfig, _, tlsErr := api.GenerateWebTransportTLSConfig()
		require.NoError(t, tlsErr)
		server := api.NewServer(router, nil, nil)
		t.Cleanup(func() { _ = server.Close() })
		web, listenErr := server.ListenWebTransport(ctx, "127.0.0.1:0", "/wamp", tlsConfig.Certificates[0])
		require.NoError(t, listenErr)
		certificate, parseErr := x509.ParseCertificate(web.Certificate)
		require.NoError(t, parseErr)
		roots := x509.NewCertPool()
		roots.AddCert(certificate)
		connection, dialErr := api.DialWebTransport(ctx, "https://"+web.Address+"/wamp", &tls.Config{
			RootCAs: roots, MinVersion: tls.VersionTLS13,
		})
		require.NoError(t, dialErr)
		t.Cleanup(func() { _ = connection.Close() })
		caller, joinErr := connection.OpenSession(ctx, "streams", nil)
		require.NoError(t, joinErr)
		t.Cleanup(func() { _ = caller.Close() })
		return ctx, backend, caller
	}
	return ctx, backend, connect()
}

func TestRPCStreamUploadUsesTheOrdinaryArgumentIndex(t *testing.T) {
	ctx, backend, caller := rpcStreamSessions(t, "quic")
	payload := bytes.Repeat([]byte("upload"), 64*1024)
	source := &streamFile{Reader: bytes.NewReader(payload)}
	require.NoError(t, backend.Register("app.upload", func(_ context.Context, inv *api.Invocation) *api.Result {
		name, err := inv.ArgString(0)
		if err != nil {
			return api.NewResultError("app.bad_name", err.Error())
		}
		stream, err := inv.ArgStream(1)
		if err != nil {
			return api.NewResultError("app.bad_stream", err.Error())
		}
		defer func() { _ = stream.Close() }()
		body, err := io.ReadAll(stream)
		if err != nil {
			return api.NewResultError("app.read_failed", err.Error())
		}
		return api.NewResult(name, bytes.Equal(body, payload))
	}).ExecuteContext(ctx).Err)
	result := caller.Call("app.upload").Arg("report.pdf").Arg(api.ReadStream(source)).ExecuteContext(ctx)
	require.NoError(t, result.Err)
	require.Equal(t, "report.pdf", result.ArgStringOr(0, ""))
	require.Equal(t, true, result.Args[1])
	require.EqualValues(t, 1, source.closed.Load())
}

func TestRPCStreamDownloadSurvivesTheFinalResult(t *testing.T) {
	ctx, backend, caller := rpcStreamSessions(t, "quic")
	payload := bytes.Repeat([]byte("download"), 64*1024)
	source := &streamFile{Reader: bytes.NewReader(payload)}
	require.NoError(t, backend.Register("app.download", func(context.Context, *api.Invocation) *api.Result {
		return api.NewResult("report.pdf", api.ReadStream(source))
	}).ExecuteContext(ctx).Err)
	result := caller.Call("app.download").ExecuteContext(ctx)
	require.NoError(t, result.Err)
	require.Equal(t, "report.pdf", result.ArgStringOr(0, ""))
	_, err := result.ArgStream(0)
	require.Error(t, err, "an ordinary argument must not be interpreted as a stream")
	stream, err := result.ArgStream(1)
	require.NoError(t, err)
	defer func() { _ = stream.Close() }()
	_, err = result.ArgStream(1)
	require.Error(t, err, "a stream endpoint can be claimed only once")
	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, payload, body)
	require.NoError(t, stream.Close())
	require.Eventually(t, func() bool { return source.closed.Load() == 1 }, time.Second, time.Millisecond)
}

func TestRPCStreamShellIsBidirectional(t *testing.T) {
	ctx, backend, caller := rpcStreamSessions(t, "quic")
	finished := make(chan struct{})
	handlerContext := make(chan context.Context, 1)
	require.NoError(t, backend.Register("app.shell", func(lifetime context.Context, _ *api.Invocation) *api.Result {
		handlerContext <- lifetime
		endpoint, process := net.Pipe()
		stop := context.AfterFunc(lifetime, func() { _ = process.Close() })
		go func() {
			defer stop()
			defer close(finished)
			defer func() { _ = process.Close() }()
			input := make([]byte, 4)
			if _, err := io.ReadFull(process, input); err == nil && string(input) == "ping" {
				_, _ = process.Write([]byte("pong"))
			}
		}()
		return api.NewResult(api.DuplexStream(endpoint))
	}).ExecuteContext(ctx).Err)
	result := caller.Call("app.shell").ExecuteContext(ctx)
	require.NoError(t, result.Err)
	lifetime := <-handlerContext
	require.NoError(t, lifetime.Err(), "the shell context must survive the RPC reply")
	stream, err := result.ArgStream(0)
	require.NoError(t, err)
	defer func() { _ = stream.Close() }()
	_, err = stream.Write([]byte("ping"))
	require.NoError(t, err)
	output := make([]byte, 4)
	_, err = io.ReadFull(stream, output)
	require.NoError(t, err)
	require.Equal(t, "pong", string(output))
	select {
	case <-finished:
	case <-ctx.Done():
		t.Fatal("shell endpoint did not finish")
	}
	require.NoError(t, stream.Close())
	select {
	case <-lifetime.Done():
	case <-ctx.Done():
		t.Fatal("shell handler context survived stream closure")
	}
}

func TestRPCStreamRejectedCallClosesTheTransferredSource(t *testing.T) {
	ctx, _, caller := rpcStreamSessions(t, "quic")
	for procedure, expected := range map[string]string{
		"app.missing":    "wamp.error.no_such_procedure",
		"private.upload": "wamp.error.authorization_failed",
	} {
		source := &streamFile{Reader: bytes.NewReader([]byte("upload"))}
		result := caller.Call(procedure).Arg(api.ReadStream(source)).ExecuteContext(ctx)
		require.ErrorContains(t, result.Err, expected)
		require.EqualValues(t, 1, source.closed.Load())
	}
}

type blockingStreamFile struct {
	started bool
	closed  chan struct{}
	closes  atomic.Int32
}

func (f *blockingStreamFile) Read(buffer []byte) (int, error) {
	if !f.started {
		f.started = true
		buffer[0] = 'x'
		return 1, nil
	}
	<-f.closed
	return 0, io.ErrClosedPipe
}

func (f *blockingStreamFile) Close() error {
	if f.closes.Add(1) == 1 {
		close(f.closed)
	}
	return nil
}

func TestRPCStreamDownloadCancellationClosesTheSource(t *testing.T) {
	ctx, backend, caller := rpcStreamSessions(t, "quic")
	source := &blockingStreamFile{closed: make(chan struct{})}
	require.NoError(t, backend.Register("app.download", func(context.Context, *api.Invocation) *api.Result {
		return api.NewResult(api.ReadStream(source))
	}).ExecuteContext(ctx).Err)
	lifetime, cancel := context.WithCancel(ctx)
	defer cancel()
	result := caller.Call("app.download").ExecuteContext(lifetime)
	require.NoError(t, result.Err)
	stream, err := result.ArgStream(0)
	require.NoError(t, err)
	defer func() { _ = stream.Close() }()
	_, err = io.ReadFull(stream, make([]byte, 1))
	require.NoError(t, err, "wait until the transfer is actually paired")
	cancel()
	_, err = stream.Read(make([]byte, 1))
	require.Error(t, err)
	require.Eventually(t, func() bool { return source.closes.Load() == 1 }, time.Second, time.Millisecond)
}

func TestRPCStreamsCrossWebTransportAndQUIC(t *testing.T) {
	ctx, backend, caller := rpcStreamSessions(t, "webtransport")
	payload := bytes.Repeat([]byte("cross-transport"), 64*1024)
	require.NoError(t, backend.Register("app.echo", func(_ context.Context, inv *api.Invocation) *api.Result {
		stream, err := inv.ArgStream(0)
		if err != nil {
			return api.NewResultError("app.bad_stream", err.Error())
		}
		defer func() { _ = stream.Close() }()
		body, err := io.ReadAll(stream)
		if err != nil {
			return api.NewResultError("app.read_failed", err.Error())
		}
		return api.NewResult(api.ReadStream(io.NopCloser(bytes.NewReader(body))))
	}).ExecuteContext(ctx).Err)
	result := caller.Call("app.echo").Arg(api.ReadStream(io.NopCloser(bytes.NewReader(payload)))).ExecuteContext(ctx)
	require.NoError(t, result.Err)
	stream, err := result.ArgStream(0)
	require.NoError(t, err)
	defer func() { _ = stream.Close() }()
	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, payload, body)
}

func TestRPCStreamUnsupportedCarrierKeepsOrdinaryRPCWorking(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	router, err := api.NewRouter(nil)
	require.NoError(t, err)
	t.Cleanup(router.Close)
	require.NoError(t, router.AddRealm("plain", &api.RealmConfig{Roles: []api.RealmRole{{
		Name: "anonymous", Permissions: []api.Permission{{
			URI: "app.", MatchPolicy: "prefix", AllowCall: true, AllowRegister: true,
		}},
	}}}))
	listener, err := router.Listen(ctx, "127.0.0.1:0", "rawsocket")
	require.NoError(t, err)
	session, err := api.ConnectAnonymous(ctx, "rs://"+listener.Address, "plain")
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	require.NoError(t, session.Register("app.echo", func(_ context.Context, inv *api.Invocation) *api.Result {
		return api.NewResult(inv.Args()...)
	}).ExecuteContext(ctx).Err)
	source := &streamFile{Reader: bytes.NewReader([]byte("data"))}
	result := session.Call("app.echo").Arg(api.ReadStream(source)).ExecuteContext(ctx)
	require.ErrorContains(t, result.Err, "transport does not support RPC streams")
	require.EqualValues(t, 1, source.closed.Load())
	result = session.Call("app.echo").Arg("ordinary").ExecuteContext(ctx)
	require.NoError(t, result.Err)
	require.Equal(t, "ordinary", result.ArgStringOr(0, ""))
}

func TestRPCStreamsOverYamux(t *testing.T) {
	for _, transport := range []string{"yamux", "yamux-unix"} {
		t.Run(transport, func(t *testing.T) {
			ctx, backend, caller := rpcStreamSessions(t, transport)
			payload := bytes.Repeat([]byte("yamux"), 512*1024)
			require.NoError(t, backend.Register("app.echo", func(_ context.Context, inv *api.Invocation) *api.Result {
				stream, err := inv.ArgStream(1)
				if err != nil {
					return api.NewResultError("app.bad_stream", err.Error())
				}
				defer func() { _ = stream.Close() }()
				body, err := io.ReadAll(stream)
				if err != nil {
					return api.NewResultError("app.read_failed", err.Error())
				}
				return api.NewResult(api.ReadStream(io.NopCloser(bytes.NewReader(body))))
			}).ExecuteContext(ctx).Err)
			require.NoError(t, backend.Register("app.ping", func(context.Context, *api.Invocation) *api.Result {
				return api.NewResult("pong")
			}).ExecuteContext(ctx).Err)
			result := caller.Call("app.echo").Arg("name").
				Arg(api.ReadStream(io.NopCloser(bytes.NewReader(payload)))).ExecuteContext(ctx)
			require.NoError(t, result.Err)
			stream, err := result.ArgStream(0)
			require.NoError(t, err)
			defer func() { _ = stream.Close() }()
			first := make([]byte, 1)
			_, err = io.ReadFull(stream, first)
			require.NoError(t, err)
			// A paused file reader must not block the WAMP control stream.
			pingContext, stop := context.WithTimeout(ctx, 2*time.Second)
			defer stop()
			ping := caller.Call("app.ping").ExecuteContext(pingContext)
			require.NoError(t, ping.Err)
			require.Equal(t, "pong", ping.ArgStringOr(0, ""))
			body, err := io.ReadAll(stream)
			require.NoError(t, err)
			require.Equal(t, payload, append(first, body...))
		})
	}
}

func TestRPCStreamConnectionDeadlineCanBeReset(t *testing.T) {
	for _, transport := range []string{"quic", "yamux", yamuxMemoryTransport} {
		t.Run(transport, func(t *testing.T) {
			ctx, backend, caller := rpcStreamSessions(t, transport)
			endpoint, process := net.Pipe()
			t.Cleanup(func() { _ = process.Close() })
			require.NoError(t, backend.Register("app.deadline", func(context.Context, *api.Invocation) *api.Result {
				return api.NewResult(api.DuplexStream(endpoint))
			}).ExecuteContext(ctx).Err)
			result := caller.Call("app.deadline").ExecuteContext(ctx)
			require.NoError(t, result.Err)
			stream, err := result.ArgStream(0)
			require.NoError(t, err)
			defer func() { _ = stream.Close() }()
			connection, ok := stream.(net.Conn)
			require.True(t, ok, "RPC streams must provide connection deadlines")
			require.NotNil(t, connection.LocalAddr())
			require.NotNil(t, connection.RemoteAddr())
			require.NoError(t, connection.SetReadDeadline(time.Now().Add(20*time.Millisecond)))
			_, err = connection.Read(make([]byte, 1))
			var timeout net.Error
			require.ErrorAs(t, err, &timeout)
			require.True(t, timeout.Timeout())
			require.NoError(t, connection.SetReadDeadline(time.Time{}))
			written := make(chan error, 1)
			go func() {
				_, writeErr := process.Write([]byte("x"))
				written <- writeErr
			}()
			data := make([]byte, 1)
			_, err = io.ReadFull(connection, data)
			require.NoError(t, err)
			require.Equal(t, []byte("x"), data)
			require.NoError(t, <-written)
		})
	}
}

func TestRPCStreamsPreserveMultiplePositionalStreams(t *testing.T) {
	for _, transport := range []string{"quic", "webtransport", "yamux", yamuxMemoryTransport} {
		t.Run(transport, func(t *testing.T) {
			ctx, backend, caller := rpcStreamSessions(t, transport)
			payloads := [][]byte{bytes.Repeat([]byte{0, 255}, 128*1024), bytes.Repeat([]byte("second"), 128*1024)}
			require.NoError(t, backend.Register("app.multiple", func(_ context.Context, inv *api.Invocation) *api.Result {
				for position, index := range []int{0, 2} {
					stream, err := inv.ArgStream(index)
					if err != nil {
						return api.NewResultError("app.bad_stream", err.Error())
					}
					body, readErr := io.ReadAll(stream)
					_ = stream.Close()
					if readErr != nil || !bytes.Equal(body, payloads[position]) {
						return api.NewResultError("app.bad_payload")
					}
				}
				return api.NewResult(api.ReadStream(io.NopCloser(bytes.NewReader(payloads[0]))),
					inv.ArgStringOr(1, ""), api.ReadStream(io.NopCloser(bytes.NewReader(payloads[1]))))
			}).ExecuteContext(ctx).Err)
			result := caller.Call("app.multiple").Arg(api.ReadStream(io.NopCloser(bytes.NewReader(payloads[0])))).
				Arg("metadata").Arg(api.ReadStream(io.NopCloser(bytes.NewReader(payloads[1])))).ExecuteContext(ctx)
			require.NoError(t, result.Err)
			require.Equal(t, "metadata", result.ArgStringOr(1, ""))
			for position, index := range []int{0, 2} {
				stream, err := result.ArgStream(index)
				require.NoError(t, err)
				body, readErr := io.ReadAll(stream)
				require.NoError(t, readErr)
				require.NoError(t, stream.Close())
				require.Equal(t, payloads[position], body)
			}
		})
	}
}

func TestRPCStreamsRemainBoundToThePrimaryQUICSession(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	router, err := api.NewRouter(nil)
	require.NoError(t, err)
	t.Cleanup(router.Close)
	require.NoError(t, router.AddRealm("streams", &api.RealmConfig{Roles: []api.RealmRole{{
		Name: "anonymous", Permissions: []api.Permission{{
			URI: "app.", MatchPolicy: "prefix", AllowCall: true, AllowRegister: true,
		}},
	}}}))
	listener, err := router.ListenQUIC(ctx, "127.0.0.1:0")
	require.NoError(t, err)
	connection, err := api.DialQUIC(ctx, listener.Address, "localhost", listener.Certificate)
	require.NoError(t, err)
	t.Cleanup(func() { _ = connection.Close() })
	primary, err := connection.OpenSession(ctx, "streams", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = primary.Close() })
	secondary, err := connection.OpenSession(ctx, "streams", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = secondary.Close() })
	require.NoError(t, primary.Register("app.upload", func(_ context.Context, inv *api.Invocation) *api.Result {
		stream, openErr := inv.ArgStream(0)
		if openErr != nil {
			return api.NewResultError("app.bad_stream", openErr.Error())
		}
		defer func() { _ = stream.Close() }()
		body, readErr := io.ReadAll(stream)
		if readErr != nil {
			return api.NewResultError("app.read_failed", readErr.Error())
		}
		return api.NewResult(string(body))
	}).ExecuteContext(ctx).Err)
	source := &streamFile{Reader: bytes.NewReader([]byte("input"))}
	result := secondary.Call("app.upload").Arg(api.ReadStream(source)).ExecuteContext(ctx)
	require.ErrorContains(t, result.Err, "transport does not support RPC streams")
	require.EqualValues(t, 1, source.closed.Load())
	caller, err := api.ConnectInMemory(router, "streams")
	require.NoError(t, err)
	t.Cleanup(func() { _ = caller.Close() })
	result = caller.Call("app.upload").
		Arg(api.ReadStream(io.NopCloser(bytes.NewBufferString("input")))).ExecuteContext(ctx)
	require.NoError(t, result.Err)
	require.Equal(t, "input", result.ArgStringOr(0, ""))
}
