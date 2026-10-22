//go:build cgo && (linux || darwin)

package webrtc_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	xconn "github.com/xconnio/xconn-core/bindings/go"
	"github.com/xconnio/xconn-core/bindings/go/auth"
	"github.com/xconnio/xconn-core/bindings/go/messages"
	"github.com/xconnio/xconn-core/bindings/go/serializers"
	rtc "github.com/xconnio/xconn-core/bindings/go/webrtc"
)

type fixture struct {
	router    *xconn.Router
	provider  *rtc.WebRTCProvider
	callee    *xconn.Session
	signaling *xconn.Session
	ctx       context.Context
}

func newFixture(t *testing.T, policy auth.ServerAuthenticator) *fixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	router, err := xconn.NewRouter(nil)
	require.NoError(t, err)
	t.Cleanup(router.Close)
	require.NoError(t, router.AddRealm("realm", &xconn.RealmConfig{Roles: []xconn.RealmRole{{
		Name: "anonymous", Permissions: []xconn.Permission{{URI: "", MatchPolicy: "prefix",
			AllowCall: true, AllowRegister: true, AllowPublish: true, AllowSubscribe: true}},
	}}}))
	listener, err := xconn.NewServer(router, nil, nil).ListenAndServeWebSocket(xconn.NetworkTCP, "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })
	callee, err := router.Connect(ctx, "json")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, callee.Close()) })
	uri := "ws://" + listener.Address
	providerSignal, err := xconn.ConnectAnonymous(ctx, uri, "realm")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, providerSignal.Close()) })
	signaling, err := xconn.ConnectAnonymous(ctx, uri, "realm")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, signaling.Close()) })
	provider := rtc.NewWebRTCHandler()
	require.NoError(t, provider.SetupContext(ctx, &rtc.ProviderConfig{
		Session: providerSignal, Router: router, Authenticator: policy,
		ProcedureHandleOffer: "offer", TopicHandleRemoteCandidates: "to-provider",
		TopicPublishLocalCandidate: "to-client", ConnectTimeout: 5 * time.Second,
	}))
	t.Cleanup(func() { require.NoError(t, provider.Close()) })
	require.NoError(t, callee.Register("echo", func(_ context.Context, inv *xconn.Invocation) *xconn.Result {
		return &xconn.Result{Args: inv.Args(), Kwargs: inv.Kwargs()}
	}).Execute().Err)
	return &fixture{router: router, provider: provider, callee: callee, signaling: signaling, ctx: ctx}
}

func (f *fixture) config(spec xconn.SerializerSpec) *rtc.ClientConfig {
	return &rtc.ClientConfig{Session: f.signaling, Realm: "realm", Serializer: spec,
		ProcedureWebRTCOffer: "offer", TopicAnswererOnCandidate: "to-provider",
		TopicOffererOnCandidate: "to-client", ConnectTimeout: 5 * time.Second}
}

func connect(t *testing.T, f *fixture, config *rtc.ClientConfig) *rtc.WebRTCSession {
	t.Helper()
	session, err := rtc.ConnectWAMPContext(f.ctx, config)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = session.Close()
		require.NoError(t, session.Connection().Close())
	})
	return session
}

func TestWAMPSessionsAndRawChannels(t *testing.T) {
	specs := []xconn.SerializerSpec{xconn.JSONSerializerSpec, xconn.CBORSerializerSpec, xconn.MsgPackSerializerSpec}
	for _, spec := range specs {
		t.Run(spec.SubProtocol(), func(t *testing.T) {
			f := newFixture(t, nil)
			session := connect(t, f, f.config(spec))
			result := session.Call("echo").Arg("hello").Kwarg("enabled", true).ExecuteContext(f.ctx)
			require.NoError(t, result.Err)
			require.Equal(t, "hello", result.ArgStringOr(0, ""))
			another, err := session.OpenSessionContext(f.ctx, "realm", &rtc.OpenSessionConfig{
				Serializer: xconn.CBORSerializerSpec,
			})
			require.NoError(t, err)
			t.Cleanup(func() { _ = another.Close() })
			publication := make(chan *xconn.Event, 1)
			subscription := another.Subscribe("events", func(event *xconn.Event) { publication <- event }).Execute()
			require.NoError(t, subscription.Err)
			require.NoError(t, session.Publish("events").Arg("event").Option("acknowledge", true).Execute().Err)
			select {
			case event := <-publication:
				require.Equal(t, "event", event.ArgStringOr(0, ""))
			case <-f.ctx.Done():
				t.Fatal(f.ctx.Err())
			}
			require.NoError(t, subscription.Unsubscribe())

			outgoing, err := session.OpenChannel("files", nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = outgoing.Close() })
			incoming, err := f.provider.AcceptChannel(f.ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = incoming.Close() })
			require.Equal(t, "files", incoming.Label())
			require.NoError(t, incoming.SendText("text handshake"))
			packet, err := outgoing.ReadDataPacketContext(f.ctx)
			require.NoError(t, err)
			require.True(t, packet.IsString)
			require.Equal(t, "text handshake", string(packet.Data))
			require.NoError(t, incoming.Send([]byte("binary handshake")))
			packet, err = outgoing.ReadDataPacketContext(f.ctx)
			require.NoError(t, err)
			require.False(t, packet.IsString)
			require.Equal(t, "binary handshake", string(packet.Data))

			// The provider can send before the client sends any packet.
			require.NoError(t, incoming.Send([]byte("ready")))
			data, err := outgoing.ReadPacketContext(f.ctx)
			require.NoError(t, err)
			require.Equal(t, "ready", string(data))
			_, err = incoming.Write([]byte("abcdef"))
			require.NoError(t, err)
			first := make([]byte, 2)
			_, err = outgoing.Read(first)
			require.NoError(t, err)
			rest := make([]byte, 4)
			_, err = io.ReadFull(outgoing, rest)
			require.NoError(t, err)
			require.Equal(t, "abcdef", string(first)+string(rest))

			payload := bytes.Repeat([]byte{0, 42, 255}, 1_000_000)
			written := make(chan error, 1)
			go func() { written <- outgoing.WriteMessageContext(f.ctx, payload) }()
			data, err = incoming.ReadMessageContext(f.ctx)
			require.NoError(t, err)
			require.Equal(t, payload, data)
			require.NoError(t, <-written)

			readCtx, cancel := context.WithTimeout(f.ctx, 10*time.Millisecond)
			_, err = outgoing.ReadPacketContext(readCtx)
			cancel()
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.NoError(t, incoming.Send([]byte("after cancellation")))
			data, err = outgoing.ReadPacketContext(f.ctx)
			require.NoError(t, err)
			require.Equal(t, "after cancellation", string(data))

			providerConnection, err := incoming.Connection()
			require.NoError(t, err)
			t.Cleanup(func() { _ = providerConnection.Close() })
			reverse, err := providerConnection.OpenChannelContext(f.ctx, "reverse", nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = reverse.Close() })
			accepted, err := session.Connection().AcceptChannel(f.ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = accepted.Close() })
			require.NoError(t, reverse.Send([]byte("provider opened")))
			data, err = accepted.ReadPacketContext(f.ctx)
			require.NoError(t, err)
			require.Equal(t, "provider opened", string(data))

			require.NoError(t, session.Close())
			require.NoError(t, another.Call("echo").Arg("sibling alive").ExecuteContext(f.ctx).Err)
			require.NoError(t, incoming.Send([]byte("raw alive")))
			data, err = outgoing.ReadPacketContext(f.ctx)
			require.NoError(t, err)
			require.Equal(t, "raw alive", string(data))
			require.NoError(t, outgoing.Close())
			_, err = incoming.ReadPacketContext(f.ctx)
			require.ErrorIs(t, err, io.EOF)
			require.NoError(t, f.provider.UpdateICEServers(nil))
		})
	}
}

type authentication struct{}

func (authentication) Methods() []auth.Method {
	return []auth.Method{auth.Ticket, auth.WAMPCRA, auth.CryptoSign}
}

func (authentication) Authenticate(request auth.Request) (auth.Response, error) {
	if request.AuthMethod() == auth.Ticket {
		ticket, ok := request.(*auth.TicketRequest)
		if !ok || ticket.Ticket() != "secret" {
			return nil, errors.New("invalid ticket")
		}
	}
	if request.AuthMethod() == auth.WAMPCRA {
		return auth.NewCRAResponseSalted(request.AuthID(), "anonymous", "secret", "salt", 1000, 32, 0), nil
	}
	return auth.NewResponse(request.AuthID(), "anonymous", 0)
}

func TestAuthenticationAndProgressiveResults(t *testing.T) {
	f := newFixture(t, authentication{})
	registration := f.callee.Register("progress", func(_ context.Context, inv *xconn.Invocation) *xconn.Result {
		if err := inv.SendProgress([]any{"working"}, nil); err != nil {
			return xconn.NewResultError("wamp.error.runtime_error", err.Error())
		}
		return xconn.NewResult("done")
	}).Execute()
	require.NoError(t, registration.Err)
	crypto, err := auth.NewCryptoSignAuthenticator(
		"alice", "1111111111111111111111111111111111111111111111111111111111111111", nil,
	)
	require.NoError(t, err)
	for _, authenticator := range []auth.ClientAuthenticator{
		auth.NewTicketAuthenticator("alice", "secret", nil),
		auth.NewWAMPCRAAuthenticator("alice", "secret", nil), crypto,
	} {
		t.Run(authenticator.AuthMethod(), func(t *testing.T) {
			config := f.config(xconn.CBORSerializerSpec)
			config.Authenticator = authenticator
			session := connect(t, f, config)
			updates := make(chan string, 1)
			result := session.Call("progress").ProgressReceiver(func(result *xconn.Result) {
				updates <- result.ArgStringOr(0, "")
			}).Execute()
			require.NoError(t, result.Err)
			require.Equal(t, "done", result.ArgStringOr(0, ""))
			require.Equal(t, "working", <-updates)
		})
	}
	bad := f.config(nil)
	bad.Authenticator = auth.NewTicketAuthenticator("alice", "wrong", nil)
	_, err = rtc.ConnectWAMPContext(f.ctx, bad)
	require.Error(t, err)
}

func TestProviderCallbackAndShutdown(t *testing.T) {
	f := newFixture(t, nil)
	channels := make(chan *rtc.DataChannel, 1)
	identifiers := make(chan string, 1)
	f.provider.OnDataChannel(func(id string, channel *rtc.DataChannel, _ []byte) {
		identifiers <- id
		channels <- channel
	})
	config := f.config(nil)
	disconnected := make(chan struct{})
	config.OnDisconnect = func() { close(disconnected) }
	session := connect(t, f, config)
	outgoing, err := session.OpenChannel("callback", nil)
	require.NoError(t, err)
	defer outgoing.Close()
	var incoming *rtc.DataChannel
	select {
	case incoming = <-channels:
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	defer incoming.Close()
	require.NotEmpty(t, <-identifiers)
	messages := make(chan []byte, 1)
	outgoing.OnMessage(func(message rtc.DataChannelMessage) { messages <- message.Data })
	require.NoError(t, incoming.Send([]byte("callback")))
	select {
	case data := <-messages:
		require.Equal(t, "callback", string(data))
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	require.NoError(t, f.provider.Close())
	select {
	case <-outgoing.Done():
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	require.NoError(t, session.Connection().Close())
	select {
	case <-disconnected:
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	_, err = f.provider.AcceptChannel(f.ctx)
	require.ErrorIs(t, err, io.EOF)
}

func TestConnectionTimeoutCancellationAndRetry(t *testing.T) {
	f := newFixture(t, nil)
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	defer close(release)
	registration := f.callee.Register("hang", func(ctx context.Context, _ *xconn.Invocation) *xconn.Result {
		started <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return xconn.NewResult("released")
	}).Execute()
	require.NoError(t, registration.Err)
	config := f.config(nil)
	config.ProcedureWebRTCOffer = "hang"
	config.ConnectTimeout = 50 * time.Millisecond
	_, err := rtc.ConnectWAMPContext(f.ctx, config)
	require.ErrorContains(t, err, "connect timeout")
	<-started

	attemptContext, cancel := context.WithCancel(f.ctx)
	defer cancel()
	config.ConnectTimeout = 5 * time.Second
	failed := make(chan error, 1)
	go func() { _, err := rtc.ConnectWAMPContext(attemptContext, config); failed <- err }()
	select {
	case <-started:
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	cancel()
	require.ErrorIs(t, <-failed, context.Canceled)
	session := connect(t, f, f.config(nil))
	require.NoError(t, session.Call("echo").Arg("retried").ExecuteContext(f.ctx).Err)
	_, err = session.OpenSessionContext(f.ctx, "missing", nil)
	require.Error(t, err)
	require.NoError(t, session.Call("echo").Arg("still connected").ExecuteContext(f.ctx).Err)

	acceptContext, stop := context.WithTimeout(f.ctx, 10*time.Millisecond)
	_, err = f.provider.AcceptChannel(acceptContext)
	stop()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	channel, err := session.OpenChannel("after cancelled accept", nil)
	require.NoError(t, err)
	defer channel.Close()
	accepted, err := f.provider.AcceptChannel(f.ctx)
	require.NoError(t, err)
	defer accepted.Close()
	require.NoError(t, channel.Send([]byte("works")))
	data, err := accepted.ReadPacketContext(f.ctx)
	require.NoError(t, err)
	require.Equal(t, "works", string(data))
	require.Error(t, channel.Send(make([]byte, 1024*1024)))
	require.NoError(t, channel.Send([]byte("after oversized packet")))
	data, err = accepted.ReadPacketContext(f.ctx)
	require.NoError(t, err)
	require.Equal(t, "after oversized packet", string(data))
}

type countedSerializer struct {
	serializers.Serializer
	encoded atomic.Int32
	decoded atomic.Int32
}

func (s *countedSerializer) Serialize(message messages.Message) ([]byte, error) {
	s.encoded.Add(1)
	return s.Serializer.Serialize(message)
}

func (s *countedSerializer) Deserialize(data []byte) (messages.Message, error) {
	s.decoded.Add(1)
	return s.Serializer.Deserialize(data)
}

func TestCustomSerializerCallbacksRemainAvailableAfterWebRTCJoin(t *testing.T) {
	f := newFixture(t, nil)
	serializer := &countedSerializer{Serializer: &serializers.JSONSerializer{}}
	spec := xconn.NewSerializerSpec(xconn.JSONWebsocketProtocol, serializer, xconn.JSONSerializerID)
	session := connect(t, f, f.config(spec))
	result := session.Call("echo").Arg("custom codec").ExecuteContext(f.ctx)
	require.NoError(t, result.Err)
	require.Equal(t, "custom codec", result.ArgStringOr(0, ""))
	require.Positive(t, serializer.encoded.Load())
	require.Positive(t, serializer.decoded.Load())
}

func TestRawChannelCallbacks(t *testing.T) {
	f := newFixture(t, nil)
	session := connect(t, f, f.config(xconn.CBORSerializerSpec))
	address, err := session.Connection().SelectedRemoteAddress(f.ctx)
	require.NoError(t, err)
	require.NotEmpty(t, address)

	outgoing, err := session.OpenChannel("callbacks", nil)
	require.NoError(t, err)
	defer outgoing.Close()
	incoming, err := f.provider.AcceptChannel(f.ctx)
	require.NoError(t, err)
	defer incoming.Close()
	packets := make(chan xconn.WebRTCDataChannelMessage, 2)
	outgoing.OnMessage(func(packet xconn.WebRTCDataChannelMessage) { packets <- packet })
	failures := make(chan error, 1)
	outgoing.OnError(func(err error) { failures <- err })
	drained := make(chan struct{}, 1)
	incoming.SetBufferedAmountLowThreshold(0)
	incoming.OnBufferedAmountLow(func() {
		select {
		case drained <- struct{}{}:
		default:
		}
	})
	require.NoError(t, incoming.SendText("hello"))
	select {
	case packet := <-packets:
		require.True(t, packet.IsString)
		require.Equal(t, "hello", string(packet.Data))
	case <-f.ctx.Done():
		t.Fatal("text callback did not arrive")
	}
	select {
	case <-drained:
	case <-f.ctx.Done():
		t.Fatalf("buffered-low callback did not arrive; amount=%d", incoming.BufferedAmount())
	}
	require.Eventually(t, func() bool { return incoming.BufferedAmount() == 0 }, time.Second, time.Millisecond)
	require.NoError(t, incoming.Send([]byte{0, 255}))
	select {
	case packet := <-packets:
		require.False(t, packet.IsString)
		require.Equal(t, []byte{0, 255}, packet.Data)
	case <-f.ctx.Done():
		t.Fatal("binary callback did not arrive")
	}
	require.NoError(t, outgoing.Close())
	select {
	case err := <-failures:
		t.Fatalf("normal close reported as failure: %v", err)
	default:
	}
}

func TestCloseCallbackFollowsBufferedMessages(t *testing.T) {
	for _, local := range []bool{false, true} {
		name := "remote close drains messages"
		if local {
			name = "local close interrupts callbacks"
		}
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, nil)
			session := connect(t, f, f.config(xconn.CBORSerializerSpec))
			outgoing, err := session.OpenChannel("ordered-close", nil)
			require.NoError(t, err)
			defer outgoing.Close()
			incoming, err := f.provider.AcceptChannel(f.ctx)
			require.NoError(t, err)
			defer incoming.Close()
			started, release, closed := make(chan struct{}), make(chan struct{}), make(chan struct{})
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()

			var count atomic.Int32
			outgoing.OnMessage(func(xconn.WebRTCDataChannelMessage) {
				if count.Add(1) == 1 {
					close(started)
					<-release
				}
			})
			outgoing.OnClose(func() { close(closed) })
			require.NoError(t, incoming.Send([]byte("first")))
			select {
			case <-started:
			case <-f.ctx.Done():
				t.Fatal(f.ctx.Err())
			}
			if local {
				require.NoError(t, outgoing.Close())
				select {
				case <-closed:
				case <-f.ctx.Done():
					t.Fatal("local close waited for a blocked message callback")
				}
				close(release)
				return
			}
			for range 10 {
				require.NoError(t, incoming.Send([]byte("queued")))
			}
			require.NoError(t, incoming.Close())
			// Wait for transport closure while the first message callback is blocked.
			select {
			case <-outgoing.Done():
			case <-f.ctx.Done():
				t.Fatal(f.ctx.Err())
			}
			select {
			case <-closed:
				t.Error("remote close overtook queued message callbacks")
			default:
			}
			close(release)
			select {
			case <-closed:
				require.Equal(t, int32(11), count.Load())
			case <-f.ctx.Done():
				t.Fatal("close callback did not follow the buffered messages")
			}
		})
	}
}

func TestErrorCallbackFollowsBufferedMessages(t *testing.T) {
	f := newFixture(t, nil)
	session := connect(t, f, f.config(xconn.CBORSerializerSpec))
	outgoing, err := session.OpenChannel("overflow-order", nil)
	require.NoError(t, err)
	defer outgoing.Close()
	incoming, err := f.provider.AcceptChannel(f.ctx)
	require.NoError(t, err)
	defer incoming.Close()
	started, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()

	failures := make(chan error, 1)
	var count atomic.Int32
	outgoing.OnMessage(func(xconn.WebRTCDataChannelMessage) {
		if count.Add(1) == 1 {
			close(started)
			<-release
		}
	})
	outgoing.OnError(func(err error) { failures <- err })
	require.NoError(t, incoming.Send([]byte("first")))
	select {
	case <-started:
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	// Saturate the bounded native receive queue while its Go callback is blocked.
	for range 128 {
		if incoming.Send([]byte("queued")) != nil {
			break
		}
	}
	select {
	case <-outgoing.Done():
	case <-f.ctx.Done():
		close(release)
		t.Fatal("receive overflow did not close the transport")
	}
	select {
	case err := <-failures:
		t.Errorf("error overtook buffered message callbacks: %v", err)
	default:
	}
	close(release)
	select {
	case err := <-failures:
		require.ErrorContains(t, err, "overflow")
		require.Greater(t, count.Load(), int32(1))
	case <-f.ctx.Done():
		t.Fatal("error callback did not follow buffered messages")
	}
}

func TestChannelCloseFlushesFinalReply(t *testing.T) {
	f := newFixture(t, nil)
	session := connect(t, f, f.config(xconn.CBORSerializerSpec))
	for range 100 {
		outgoing, err := session.OpenChannel("one-shot", nil)
		require.NoError(t, err)
		incoming, err := f.provider.AcceptChannel(f.ctx)
		require.NoError(t, err)
		require.NoError(t, incoming.SendText("final reply"))
		require.NoError(t, incoming.Close())
		packet, err := outgoing.ReadDataPacketContext(f.ctx)
		require.NoError(t, err, "closing the sender must preserve its last queued message")
		require.Equal(t, "final reply", string(packet.Data))
		require.True(t, packet.IsString)
		require.NoError(t, outgoing.Close())
	}
}
