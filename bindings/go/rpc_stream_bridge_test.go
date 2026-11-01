package xconn_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	api "github.com/xconnio/xconn-core/bindings/go"
	"github.com/xconnio/xconn-core/bindings/go/serializers"
)

type streamBridgeFixture struct {
	ctx     context.Context
	backend *api.Session
	caller  *api.Session
	source  *api.Session
	target  *api.Session
	bridge  *api.Bridge
}

func newStreamBridgeFixture(t *testing.T) streamBridgeFixture {
	t.Helper()
	ctx, source, caller := rpcStreamSessions(t, "quic")
	_, backend, target := rpcStreamSessions(t, yamuxMemoryTransport)
	bridge, err := source.Bridge(ctx, api.BridgeConfig{
		Target: target,
		Routes: []api.BridgeRoute{{Procedure: "app.", Options: api.Options{"match": "prefix"}}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bridge.Close() })
	return streamBridgeFixture{ctx: ctx, backend: backend, caller: caller, source: source, target: target, bridge: bridge}
}

// Three routers exercise reference replacement at both bridge boundaries.
func newChainedStreamBridgeFixture(t *testing.T) streamBridgeFixture {
	t.Helper()
	ctx, source, caller := rpcStreamSessions(t, "quic")
	_, middleService, middleClient := rpcStreamSessions(t, "webtransport")
	_, backend, target := rpcStreamSessions(t, yamuxMemoryTransport)
	last, err := middleService.Bridge(ctx, api.BridgeConfig{
		Target: target,
		Routes: []api.BridgeRoute{{Procedure: "app.", Options: api.Options{"match": "prefix"}}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = last.Close() })
	first, err := source.Bridge(ctx, api.BridgeConfig{
		Target: middleClient,
		Routes: []api.BridgeRoute{{Procedure: "app.", Options: api.Options{"match": "prefix"}}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = first.Close() })
	return streamBridgeFixture{ctx: ctx, backend: backend, caller: caller, source: source, target: target, bridge: first}
}

func TestRPCStreamBridgeUploadAndDownload(t *testing.T) {
	for _, test := range []struct {
		name    string
		fixture func(*testing.T) streamBridgeFixture
	}{
		{"one bridge", newStreamBridgeFixture},
		{"two bridges", newChainedStreamBridgeFixture},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := test.fixture(t)
			payload := bytes.Repeat([]byte("bridged"), 256*1024)
			input := &streamFile{Reader: bytes.NewReader(payload)}
			output := &streamFile{Reader: bytes.NewReader(payload)}
			require.NoError(t, f.backend.Register("app.echo", func(_ context.Context, inv *api.Invocation) *api.Result {
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
				if err != nil || !bytes.Equal(body, payload) {
					return api.NewResultError("app.bad_upload")
				}
				return api.NewResult(name, api.ReadStream(output))
			}).ExecuteContext(f.ctx).Err)
			result := f.caller.Call("app.echo").Arg("report.pdf").Arg(api.ReadStream(input)).ExecuteContext(f.ctx)
			require.NoError(t, result.Err)
			require.Equal(t, "report.pdf", result.ArgStringOr(0, ""))
			stream, err := result.ArgStream(1)
			require.NoError(t, err)
			defer func() { _ = stream.Close() }()
			body, err := io.ReadAll(stream)
			require.NoError(t, err)
			require.Equal(t, payload, body)
			require.NoError(t, stream.Close())
			require.Eventually(t, func() bool { return input.closed.Load() == 1 && output.closed.Load() == 1 },
				time.Second, time.Millisecond)
		})
	}
}

func TestRPCStreamBridgeBidirectionalResult(t *testing.T) {
	f := newStreamBridgeFixture(t)
	finished := make(chan struct{})
	lifetimes := make(chan context.Context, 1)
	require.NoError(t, f.backend.Register("app.shell", func(ctx context.Context, _ *api.Invocation) *api.Result {
		lifetimes <- ctx
		endpoint, process := net.Pipe()
		stop := context.AfterFunc(ctx, func() { _ = process.Close() })
		go func() {
			defer close(finished)
			defer stop()
			defer func() { _ = process.Close() }()
			body := make([]byte, 4)
			if _, err := io.ReadFull(process, body); err == nil && string(body) == "ping" {
				_, _ = process.Write([]byte("pong"))
			}
		}()
		return api.NewResult(api.DuplexStream(endpoint))
	}).ExecuteContext(f.ctx).Err)
	result := f.caller.Call("app.shell").ExecuteContext(f.ctx)
	require.NoError(t, result.Err)
	lifetime := <-lifetimes
	require.NoError(t, lifetime.Err(), "the backend context must outlive both RPC replies")
	stream, err := result.ArgStream(0)
	require.NoError(t, err)
	defer func() { _ = stream.Close() }()
	_, err = stream.Write([]byte("ping"))
	require.NoError(t, err)
	body := make([]byte, 4)
	_, err = io.ReadFull(stream, body)
	require.NoError(t, err)
	require.Equal(t, "pong", string(body))
	require.NoError(t, stream.Close())
	select {
	case <-finished:
	case <-f.ctx.Done():
		t.Fatal("bridged shell did not finish")
	}
	select {
	case <-lifetime.Done():
	case <-f.ctx.Done():
		t.Fatal("backend context survived stream closure")
	}
}

func TestRPCStreamBridgeCloseStopsTransfersWithoutClosingSharedSessions(t *testing.T) {
	f := newStreamBridgeFixture(t)
	source := &blockingStreamFile{closed: make(chan struct{})}
	require.NoError(t, f.backend.Register("app.download", func(context.Context, *api.Invocation) *api.Result {
		return api.NewResult(api.ReadStream(source))
	}).ExecuteContext(f.ctx).Err)
	result := f.caller.Call("app.download").ExecuteContext(f.ctx)
	require.NoError(t, result.Err)
	stream, err := result.ArgStream(0)
	require.NoError(t, err)
	defer func() { _ = stream.Close() }()
	_, err = io.ReadFull(stream, make([]byte, 1))
	require.NoError(t, err)
	require.NoError(t, f.bridge.Close())
	_, err = stream.Read(make([]byte, 1))
	require.Error(t, err)
	require.Eventually(t, func() bool { return source.closes.Load() == 1 }, time.Second, time.Millisecond)
	require.True(t, f.source.Connected())
	require.True(t, f.target.Connected())
	require.True(t, f.backend.Connected())
}

func TestRPCStreamBridgeCancellationClosesTheBackendSource(t *testing.T) {
	f := newStreamBridgeFixture(t)
	source := &blockingStreamFile{closed: make(chan struct{})}
	require.NoError(t, f.backend.Register("app.download", func(context.Context, *api.Invocation) *api.Result {
		return api.NewResult(api.ReadStream(source))
	}).ExecuteContext(f.ctx).Err)
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	result := f.caller.Call("app.download").ExecuteContext(ctx)
	require.NoError(t, result.Err)
	stream, err := result.ArgStream(0)
	require.NoError(t, err)
	defer func() { _ = stream.Close() }()
	_, err = io.ReadFull(stream, make([]byte, 1))
	require.NoError(t, err)
	cancel()
	_, err = stream.Read(make([]byte, 1))
	require.Error(t, err)
	require.Eventually(t, func() bool { return source.closes.Load() == 1 }, time.Second, time.Millisecond)
}

func TestRPCStreamBridgeUnclaimedResultClosesWithTheBridge(t *testing.T) {
	f := newStreamBridgeFixture(t)
	source := &blockingStreamFile{closed: make(chan struct{})}
	require.NoError(t, f.backend.Register("app.download", func(context.Context, *api.Invocation) *api.Result {
		return api.NewResult(api.ReadStream(source))
	}).ExecuteContext(f.ctx).Err)
	result := f.caller.Call("app.download").ExecuteContext(f.ctx)
	require.NoError(t, result.Err)
	// Deliberately do not open result.ArgStream(0).
	require.NoError(t, f.bridge.Close())
	require.Eventually(t, func() bool { return source.closes.Load() == 1 }, time.Second, time.Millisecond)
}

func TestRPCStreamBridgePreservesApplicationErrors(t *testing.T) {
	f := newStreamBridgeFixture(t)
	require.NoError(t, f.backend.Register("app.upload", func(context.Context, *api.Invocation) *api.Result {
		return api.NewResultError("app.upload_denied", "read only")
	}).ExecuteContext(f.ctx).Err)
	source := &streamFile{Reader: bytes.NewReader([]byte("data"))}
	result := f.caller.Call("app.upload").Arg(api.ReadStream(source)).ExecuteContext(f.ctx)
	var app *api.Error
	require.ErrorAs(t, result.Err, &app)
	require.Equal(t, "app.upload_denied", app.URI)
	require.Equal(t, []any{"read only"}, app.Args)
	require.EqualValues(t, 1, source.closed.Load())
}

func TestRPCStreamBridgeCallerLeaveOnlyClosesItsOwnedTarget(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	roles := []api.RealmRole{{Name: "anonymous", Permissions: []api.Permission{{
		URI: "", MatchPolicy: "prefix", AllowCall: true, AllowRegister: true, AllowSubscribe: true,
	}}}}
	remote, err := api.NewRouter(nil)
	require.NoError(t, err)
	t.Cleanup(remote.Close)
	require.NoError(t, remote.AddRealm("remote", &api.RealmConfig{Roles: roles, Meta: true, AutoDiscloseCaller: true}))
	listener, err := remote.ListenQUIC(ctx, "127.0.0.1:0")
	require.NoError(t, err)
	connectRemote := func() *api.Session {
		connection, dialErr := api.DialQUIC(ctx, listener.Address, "localhost", listener.Certificate)
		require.NoError(t, dialErr)
		t.Cleanup(func() { _ = connection.Close() })
		session, joinErr := connection.OpenSession(ctx, "remote", nil)
		require.NoError(t, joinErr)
		t.Cleanup(func() { _ = session.Close() })
		return session
	}
	source, caller1, caller2 := connectRemote(), connectRemote(), connectRemote()
	local, err := api.NewRouter(nil)
	require.NoError(t, err)
	t.Cleanup(local.Close)
	require.NoError(t, local.AddRealm("local", &api.RealmConfig{Roles: roles, AutoDiscloseCaller: true}))
	server := api.NewServer(local, nil, nil)
	t.Cleanup(func() { _ = server.Close() })
	localListener, err := server.ListenAndServeYamux(api.NetworkTCP, "127.0.0.1:0")
	require.NoError(t, err)
	connectLocal := func() *api.Session {
		session, connectErr := api.ConnectAnonymous(ctx, "yamux://"+localListener.Address, "local")
		require.NoError(t, connectErr)
		t.Cleanup(func() { _ = session.Close() })
		return session
	}
	backend := connectLocal()
	fallback, err := api.ConnectInMemory(local, "local")
	require.NoError(t, err)
	t.Cleanup(func() { _ = fallback.Close() })
	first := &blockingStreamFile{closed: make(chan struct{})}
	second := &blockingStreamFile{closed: make(chan struct{})}
	require.NoError(t, backend.Register("app.download", func(_ context.Context, inv *api.Invocation) *api.Result {
		if inv.ArgStringOr(0, "") == "first" {
			return api.NewResult(api.ReadStream(first), inv.CallerAuthID(), inv.CallerAuthRole())
		}
		return api.NewResult(api.ReadStream(second), inv.CallerAuthID(), inv.CallerAuthRole())
	}).ExecuteContext(ctx).Err)
	type ownedTarget struct {
		caller  api.CallerIdentity
		session *api.Session
	}
	owned := make(chan ownedTarget, 2)
	bridge, err := source.Bridge(ctx, api.BridgeConfig{
		Target: fallback, Routes: []api.BridgeRoute{{Procedure: "app.download"}},
		NewSession: func(_ context.Context, caller api.CallerIdentity) (*api.Session, error) {
			//nolint:contextcheck // BaseSession constructors have no context API.
			base, connectErr := api.ConnectInMemoryBase(local, "local", caller.AuthID, caller.AuthRole,
				&serializers.CBORSerializer{}, 16)
			if connectErr != nil {
				return nil, connectErr
			}
			//nolint:contextcheck // NewSession transfers the existing peer.
			session, connectErr := api.NewSession(base, base.Serializer())
			if connectErr != nil {
				_ = base.Close()
				return nil, connectErr
			}
			t.Cleanup(func() { _ = session.Close() })
			owned <- ownedTarget{caller: caller, session: session}
			return session, nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bridge.Close() })
	open := func(caller *api.Session, name string) io.ReadWriteCloser {
		result := caller.Call("app.download").Arg(name).ExecuteContext(ctx)
		require.NoError(t, result.Err)
		require.Equal(t, caller.Details().AuthID(), result.ArgStringOr(1, ""))
		require.Equal(t, caller.Details().AuthRole(), result.ArgStringOr(2, ""))
		stream, openErr := result.ArgStream(0)
		require.NoError(t, openErr)
		t.Cleanup(func() { _ = stream.Close() })
		_, readErr := io.ReadFull(stream, make([]byte, 1))
		require.NoError(t, readErr)
		return stream
	}
	nextOwned := func(caller *api.Session) *api.Session {
		select {
		case record := <-owned:
			require.Equal(t, caller.ID(), record.caller.SessionID)
			require.Equal(t, caller.Details().AuthID(), record.caller.AuthID)
			require.Equal(t, caller.Details().AuthRole(), record.caller.AuthRole)
			return record.session
		case <-ctx.Done():
			t.Fatal("caller-specific session was not created")
			return nil
		}
	}
	_ = open(caller1, "first")
	owned1 := nextOwned(caller1)
	stream2 := open(caller2, "second")
	owned2 := nextOwned(caller2)
	require.NotEqual(t, owned1.ID(), owned2.ID())
	require.NoError(t, caller1.Close())
	require.Eventually(t, func() bool { return first.closes.Load() == 1 && !owned1.Connected() },
		time.Second, time.Millisecond)
	require.Zero(t, second.closes.Load())
	require.True(t, owned2.Connected())
	require.NoError(t, stream2.Close())
	require.Eventually(t, func() bool { return second.closes.Load() == 1 }, time.Second, time.Millisecond)
	require.NoError(t, bridge.Close())
	require.Eventually(t, func() bool { return !owned2.Connected() }, time.Second, time.Millisecond)
	require.True(t, fallback.Connected())
}

func TestRPCStreamBridgeCancellationStopsAnActiveUpload(t *testing.T) {
	f := newStreamBridgeFixture(t)
	started := make(chan struct{})
	finished := make(chan struct{})
	require.NoError(t, f.backend.Register("app.upload", func(_ context.Context, inv *api.Invocation) *api.Result {
		defer close(finished)
		stream, err := inv.ArgStream(0)
		if err != nil {
			return api.NewResultError("app.bad_stream", err.Error())
		}
		defer func() { _ = stream.Close() }()
		if _, err = io.ReadFull(stream, make([]byte, 1)); err != nil {
			return api.NewResultError("app.read_failed", err.Error())
		}
		close(started)
		_, _ = io.Copy(io.Discard, stream)
		return api.NewResult()
	}).ExecuteContext(f.ctx).Err)
	source := &blockingStreamFile{closed: make(chan struct{})}
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	result := make(chan *api.Result, 1)
	go func() {
		result <- f.caller.Call("app.upload").Arg(api.ReadStream(source)).ExecuteContext(ctx)
	}()
	select {
	case <-started:
	case <-f.ctx.Done():
		t.Fatal("upload never reached the backend")
	}
	cancel()
	select {
	case reply := <-result:
		require.ErrorIs(t, reply.Err, context.Canceled)
	case <-f.ctx.Done():
		t.Fatal("the cancelled call did not return")
	}
	select {
	case <-finished:
	case <-f.ctx.Done():
		t.Fatal("the backend upload did not stop")
	}
	require.Eventually(t, func() bool { return source.closes.Load() == 1 }, time.Second, time.Millisecond)
}

func TestRPCStreamBridgeChainPreservesHalfCloseAndLargeReply(t *testing.T) {
	f := newChainedStreamBridgeFixture(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	completed := make(chan error, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			completed <- acceptErr
			return
		}
		defer func() { _ = connection.Close() }()
		body, readErr := io.ReadAll(connection)
		if readErr != nil {
			completed <- readErr
			return
		}
		_, writeErr := io.Copy(connection, bytes.NewReader(bytes.Repeat(body, 2)))
		completed <- writeErr
	}()
	require.NoError(t, f.backend.Register("app.duplex", func(ctx context.Context, _ *api.Invocation) *api.Result {
		connection, dialErr := (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
		if dialErr != nil {
			return api.NewResultError("app.connection_failed", dialErr.Error())
		}
		return api.NewResult(api.DuplexStream(connection))
	}).ExecuteContext(f.ctx).Err)
	result := f.caller.Call("app.duplex").ExecuteContext(f.ctx)
	require.NoError(t, result.Err)
	stream, err := result.ArgStream(0)
	require.NoError(t, err)
	defer func() { _ = stream.Close() }()
	writer, ok := stream.(interface{ CloseWrite() error })
	require.True(t, ok)
	payload := bytes.Repeat([]byte{0, 255, 128, 3}, 384*1024)
	_, err = stream.Write(payload)
	require.NoError(t, err)
	require.NoError(t, writer.CloseWrite())
	reply, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat(payload, 2), reply)
	select {
	case err = <-completed:
		require.NoError(t, err)
	case <-f.ctx.Done():
		t.Fatal("the duplex backend did not finish")
	}
}
