package xconn_test

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	api "github.com/xconnio/xconn-core/bindings/go"
	"github.com/xconnio/xconn-core/bindings/go/auth"
)

func peerRouter(t *testing.T) (*api.Router, *api.Session) {
	t.Helper()
	router, err := api.NewRouter(nil)
	require.NoError(t, err)
	t.Cleanup(router.Close)
	require.NoError(t, router.AddRealm("peer-test", &api.RealmConfig{Meta: true, Roles: []api.RealmRole{{
		Name: "anonymous", Permissions: []api.Permission{{URI: "", MatchPolicy: "prefix",
			AllowCall: true, AllowRegister: true, AllowSubscribe: true, AllowPublish: true}},
	}}}))
	session, err := api.ConnectInMemory(router, "peer-test")
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	return router, session
}

func peerClient(t *testing.T, router *api.Router, client *api.Client) *api.Session {
	t.Helper()
	server := api.NewServer(router, nil, nil)
	t.Cleanup(func() { _ = server.Close() })
	listener, err := server.ListenAndServeYamux(api.NetworkTCP, "127.0.0.1:0")
	require.NoError(t, err)
	session, err := client.Connect(t.Context(), "yamux://"+listener.Address, "peer-test")
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func TestPeerRoutesNewCallsAndStreamsWithoutReplayingInterruptedWork(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	hubRouter, hub := peerRouter(t)
	signaling, err := api.ConnectInMemory(hubRouter, "peer-test")
	require.NoError(t, err)
	t.Cleanup(func() { _ = signaling.Close() })
	edge, backend := peerRouter(t)
	var routedExecutions atomic.Int32
	var directExecutions atomic.Int32
	started := make(chan struct{}, 1)
	for _, item := range []struct {
		session *api.Session
		path    string
	}{{hub, "routed"}, {backend, "direct"}} {
		require.NoError(t, item.session.Register("app.path", func(context.Context, *api.Invocation) *api.Result {
			return api.NewResult(item.path)
		}).ExecuteContext(ctx).Err)
		require.NoError(t, item.session.Register("app.transfer", func(_ context.Context, inv *api.Invocation) *api.Result {
			stream, err := inv.ArgStream(1)
			if err != nil {
				return api.NewResultError("app.bad_stream", err.Error())
			}
			defer stream.Close()
			body, err := io.ReadAll(stream)
			if err != nil {
				return api.NewResultError("app.read_failed", err.Error())
			}
			return api.NewResult(item.path, api.ReadStream(io.NopCloser(bytes.NewReader(body))))
		}).ExecuteContext(ctx).Err)
	}
	require.NoError(t, hub.Register("app.once", func(context.Context, *api.Invocation) *api.Result {
		routedExecutions.Add(1)
		return api.NewResult()
	}).ExecuteContext(ctx).Err)
	require.NoError(t, backend.Register("app.once", func(ctx context.Context, _ *api.Invocation) *api.Result {
		directExecutions.Add(1)
		started <- struct{}{}
		<-ctx.Done()
		return api.NewResultError("app.cancelled")
	}).ExecuteContext(ctx).Err)
	peer := peerClient(t, hubRouter, &api.Client{Direct: &api.DirectConfig{
		ICEServers: []api.WebRTCICEServer{}, RetryInterval: 10 * time.Millisecond, ConnectTimeout: time.Second,
	}})
	t.Cleanup(func() { _ = peer.Close() })
	releaseRouted := make(chan struct{})
	routedStarted := make(chan struct{})
	require.NoError(t, hub.Register("app.pending", func(ctx context.Context, _ *api.Invocation) *api.Result {
		close(routedStarted)
		select {
		case <-releaseRouted:
			return api.NewResult("routed")
		case <-ctx.Done():
			return api.NewResultError("app.cancelled")
		}
	}).ExecuteContext(ctx).Err)
	pendingRouted := make(chan *api.Result, 1)
	go func() { pendingRouted <- peer.Call("app.pending").ExecuteContext(ctx) }()
	select {
	case <-routedStarted:
	case <-ctx.Done():
		t.Fatal("routed call did not start")
	}
	request := peer.Call("app.path")
	require.Equal(t, api.PathRouted, peer.Path())
	require.Equal(t, "routed", request.ExecuteContext(ctx).ArgStringOr(0, ""))
	endpoint, err := signaling.Expose(ctx, edge, &api.ExposeConfig{ICEServers: []api.WebRTCICEServer{}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = endpoint.Close() })
	require.Eventually(t, func() bool { return peer.Path() == api.PathDirect }, 5*time.Second, time.Millisecond)
	require.Equal(t, "direct", request.ExecuteContext(ctx).ArgStringOr(0, ""), "select at execution, not construction")
	require.NoError(t, peer.Call("wamp.session.get").Arg(peer.ID()).ExecuteContext(ctx).Err,
		"meta calls must use the hub's session identity")
	raw, err := peer.CallContext(ctx, "app.path", nil, nil, nil)
	require.NoError(t, err)
	require.Equal(t, "direct", raw.ArgStringOr(0, ""))
	close(releaseRouted)
	select {
	case result := <-pendingRouted:
		require.NoError(t, result.Err)
		require.Equal(t, "routed", result.ArgStringOr(0, ""))
	case <-ctx.Done():
		t.Fatal("upgrade interrupted an existing hub call")
	}
	transfer := func(path string) {
		payload := bytes.Repeat([]byte{0, 255, 128, 42}, 256*1024)
		result := peer.Call("app.transfer").Arg("file").
			Arg(api.ReadStream(io.NopCloser(bytes.NewReader(payload)))).ExecuteContext(ctx)
		require.NoError(t, result.Err)
		require.Equal(t, path, result.ArgStringOr(0, ""))
		stream, err := result.ArgStream(1)
		require.NoError(t, err)
		defer stream.Close()
		body, err := io.ReadAll(stream)
		require.NoError(t, err)
		require.Equal(t, payload, body)
	}
	transfer("direct")
	interrupted := make(chan *api.Result, 1)
	go func() { interrupted <- peer.Call("app.once").ExecuteContext(ctx) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("direct invocation never started")
	}
	require.NoError(t, endpoint.Close())
	select {
	case result := <-interrupted:
		require.Error(t, result.Err)
	case <-ctx.Done():
		t.Fatal("interrupted call did not finish")
	}
	require.Eventually(t, func() bool { return peer.Path() == api.PathRouted }, time.Second, time.Millisecond)
	require.EqualValues(t, 1, directExecutions.Load())
	require.Zero(t, routedExecutions.Load(), "dispatched work must never be replayed")
	transfer("routed")
	endpoint, err = signaling.Expose(ctx, edge, &api.ExposeConfig{ICEServers: []api.WebRTCICEServer{}})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return peer.Path() == api.PathDirect }, 5*time.Second, time.Millisecond)
	transfer("direct")
	require.NoError(t, peer.Close())
	require.Equal(t, api.PathClosed, peer.Path())
	unused := &streamFile{Reader: bytes.NewReader([]byte("unused"))}
	require.Error(t, peer.Call("app.transfer").Arg(api.ReadStream(unused)).ExecuteContext(ctx).Err)
	require.EqualValues(t, 1, unused.closed.Load())
	require.NoError(t, hub.Call("app.path").ExecuteContext(ctx).Err,
		"closing a client must leave other hub sessions alive")
}

func TestPeerStopsWhenTheHubDisconnects(t *testing.T) {
	router, _ := peerRouter(t)
	peer := peerClient(t, router, &api.Client{Direct: &api.DirectConfig{ICEServers: []api.WebRTCICEServer{}}})
	defer peer.Close()
	router.Close()
	select {
	case <-peer.Done():
	case <-time.After(time.Second):
		t.Fatal("peer did not stop with hub")
	}
	require.Error(t, peer.Call("app.any").Execute().Err)
}

func TestPeerPreservesPlainWAMPAndRejectsUnsupportedStreamFallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	router, backend := peerRouter(t)
	listener, err := api.NewServer(router, nil, nil).ListenAndServeWebSocket(api.NetworkTCP, "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	dialing, stopDialing := context.WithCancel(ctx)
	client := &api.Client{Direct: &api.DirectConfig{ICEServers: []api.WebRTCICEServer{}}}
	peer, err := client.Connect(dialing, "ws://"+listener.Address, "peer-test")
	stopDialing()
	require.NoError(t, err)
	defer peer.Close()
	var executions atomic.Int32
	require.NoError(t, backend.Register("app.echo", func(_ context.Context, inv *api.Invocation) *api.Result {
		executions.Add(1)
		return api.NewResult(inv.Args()...)
	}).ExecuteContext(ctx).Err)
	require.NoError(t, peer.Call("app.echo").Arg("ordinary WAMP").ExecuteContext(ctx).Err)
	source := &streamFile{Reader: bytes.NewReader([]byte("upload"))}
	result := peer.Call("app.echo").Arg(api.ReadStream(source)).ExecuteContext(ctx)
	require.ErrorContains(t, result.Err, "does not support RPC streams")
	require.EqualValues(t, 1, source.closed.Load())
	require.EqualValues(t, 1, executions.Load(), "unsupported stream must fail before CALL dispatch")
	require.NoError(t, peer.Call("app.echo").Execute().Err, "canceling a completed dial must leave the session alive")
}

type peerAuthentication struct{}

func (peerAuthentication) Methods() []auth.Method { return []auth.Method{auth.WAMPCRA} }

func (peerAuthentication) Authenticate(request auth.Request) (auth.Response, error) {
	return auth.NewCRAResponse(request.AuthID(), "anonymous", "direct-secret", 0), nil
}

func TestPeerAuthenticatesDirectSessionIndependently(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	hubRouter, signaling := peerRouter(t)
	hub, err := api.ConnectInMemory(hubRouter, "peer-test")
	require.NoError(t, err)
	defer hub.Close()
	edge, backend := peerRouter(t)
	require.NoError(t, backend.Register("app.identity", func(_ context.Context, inv *api.Invocation) *api.Result {
		return api.NewResult(inv.Args()...)
	}).ExecuteContext(ctx).Err)
	endpoint, err := signaling.Expose(ctx, edge, &api.ExposeConfig{
		Authenticator: peerAuthentication{}, ICEServers: []api.WebRTCICEServer{},
	})
	require.NoError(t, err)
	defer endpoint.Close()
	denied := peerClient(t, hubRouter, &api.Client{Direct: &api.DirectConfig{
		Authenticator: auth.NewWAMPCRAAuthenticator("client", "incorrect", nil),
		ICEServers:    []api.WebRTCICEServer{}, RetryInterval: 10 * time.Millisecond,
	}})
	defer denied.Close()
	require.Eventually(t, func() bool { return denied.DirectError() != nil }, 3*time.Second, time.Millisecond)
	require.Equal(t, api.PathRouted, denied.Path())
	authenticatedHub := api.NewServer(hubRouter, peerAuthentication{}, nil)
	defer authenticatedHub.Close()
	listener, err := authenticatedHub.ListenAndServeYamux(api.NetworkTCP, "127.0.0.1:0")
	require.NoError(t, err)
	client := &api.Client{
		Authenticator: auth.NewWAMPCRAAuthenticator("client", "direct-secret", nil),
		Direct:        &api.DirectConfig{ICEServers: []api.WebRTCICEServer{}},
	}
	accepted, err := client.Connect(ctx, "yamux://"+listener.Address, "peer-test")
	require.NoError(t, err)
	defer accepted.Close()
	require.Eventually(t, func() bool { return accepted.Path() == api.PathDirect }, 3*time.Second, time.Millisecond)
	require.Nil(t, accepted.DirectError())
	require.NoError(t, accepted.Call("app.identity").Arg("authenticated").ExecuteContext(ctx).Err)
}

func TestRoutedOnlyClientDoesNotDiscoverDirectEndpoints(t *testing.T) {
	router, service := peerRouter(t)
	discovered := make(chan struct{}, 1)
	require.NoError(t, service.Register("io.xconn.peer.resolve", func(context.Context, *api.Invocation) *api.Result {
		discovered <- struct{}{}
		return api.NewResult("webrtc")
	}).ExecuteContext(t.Context()).Err)
	session := peerClient(t, router, &api.Client{RoutedOnly: true})
	require.Equal(t, api.PathRouted, session.Path())
	require.NoError(t, service.Register("app.echo", func(context.Context, *api.Invocation) *api.Result {
		return api.NewResult("routed")
	}).ExecuteContext(t.Context()).Err)
	require.Equal(t, "routed", session.Call("app.echo").ExecuteContext(t.Context()).ArgStringOr(0, ""))
	require.NoError(t, session.Close())
	select {
	case <-discovered:
		t.Fatal("routed-only client attempted direct discovery")
	default:
	}
}
