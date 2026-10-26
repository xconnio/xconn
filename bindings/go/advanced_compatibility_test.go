package xconn_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xconnio/xconn-core/bindings/go"
	"github.com/xconnio/xconn-core/bindings/go/auth"
	"github.com/xconnio/xconn-core/bindings/go/messages"
	"github.com/xconnio/xconn-core/bindings/go/serializers"
)

type legacyPolicy struct{}

func (legacyPolicy) Methods() []auth.Method {
	return []auth.Method{auth.Ticket, auth.WAMPCRA, auth.CryptoSign}
}

func (legacyPolicy) Authenticate(request auth.Request) (auth.Response, error) {
	if request.AuthMethod() == auth.Ticket {
		ticket, ok := request.(*auth.TicketRequest)
		if !ok || ticket.Ticket() != "secret" {
			return nil, errors.New("invalid ticket")
		}
	}
	if request.AuthMethod() == auth.WAMPCRA {
		key := auth.DeriveWAMPCRAKey("salt", "secret", 1000, 32)
		return auth.NewCRAResponseSalted(request.AuthID(), "trusted", string(key), "salt", 1000, 32, 0), nil
	}
	return auth.NewResponse(request.AuthID(), "trusted", 0)
}

type legacyAuthenticator struct {
	auth.ClientAuthenticator
	called  atomic.Int32
	failure error
}

func (a *legacyAuthenticator) Authenticate(challenge messages.Challenge) (*messages.Authenticate, error) {
	a.called.Add(1)
	if a.failure != nil {
		return nil, a.failure
	}
	return a.ClientAuthenticator.Authenticate(challenge)
}

type legacySerializer struct {
	serializers.Serializer
	encoded atomic.Int32
	decoded atomic.Int32
}

func (s *legacySerializer) Serialize(message messages.Message) ([]byte, error) {
	s.encoded.Add(1)
	return s.Serializer.Serialize(message)
}

func (s *legacySerializer) Deserialize(bytes []byte) (messages.Message, error) {
	s.decoded.Add(1)
	return s.Serializer.Deserialize(bytes)
}

func TestLegacyAuthenticationAndSerializerInterfaces(t *testing.T) {
	router, err := xconn.NewRouter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer router.Close()
	if err = router.AddRealm("legacy", nil); err != nil {
		t.Fatal(err)
	}
	server := xconn.NewServer(router, legacyPolicy{}, nil)
	listener, err := server.ListenAndServeWebSocket(xconn.NetworkTCP, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	uri := "ws://" + listener.Addr().String()
	for _, authenticator := range []auth.ClientAuthenticator{
		auth.NewTicketAuthenticator("alice", "secret", nil), auth.NewWAMPCRAAuthenticator("alice", "secret", nil),
	} {
		callback := &legacyAuthenticator{ClientAuthenticator: authenticator}
		serializer := &legacySerializer{Serializer: &serializers.JSONSerializer{}}
		client := xconn.Client{
			Authenticator:  callback,
			SerializerSpec: xconn.NewSerializerSpec(xconn.JSONWebsocketProtocol, serializer, xconn.JSONSerializerID),
		}
		session, err := client.Connect(ctx, uri, "legacy")
		if err != nil {
			t.Fatal(err)
		}
		registration := session.Register("echo", func(_ context.Context, inv *xconn.Invocation) *xconn.Result {
			return xconn.NewResult(inv.Args()...)
		}).
			Execute()
		if registration.Err != nil {
			t.Fatal(registration.Err)
		}
		result := session.Call("echo").Arg("callback").ExecuteContext(ctx)
		if result.Err != nil {
			t.Fatal(result.Err)
		}
		if result.ArgStringOr(0, "") != "callback" || callback.called.Load() != 1 {
			t.Fatal("callbacks not preserved")
		}
		if serializer.encoded.Load() < 3 || serializer.decoded.Load() < 3 {
			t.Fatal("serializer callbacks bypassed")
		}
		if err = session.Close(); err != nil {
			t.Fatal(err)
		}
	}
	sentinel := errors.New("custom challenge error")
	callback := &legacyAuthenticator{
		ClientAuthenticator: auth.NewTicketAuthenticator("alice", "secret", nil),
		failure:             sentinel,
	}
	_, err = (&xconn.Client{Authenticator: callback}).Connect(ctx, uri, "legacy")
	if !errors.Is(err, sentinel) {
		t.Fatalf("lost callback error: %v", err)
	}
}

type wrappedBase struct {
	xconn.BaseSession
	closed atomic.Int32
}

func (b *wrappedBase) Close() error { b.closed.Add(1); return b.BaseSession.Close() }

func TestLegacyCustomBaseSession(t *testing.T) {
	router, err := xconn.NewRouter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer router.Close()
	if err = router.AddRealm("legacy", nil); err != nil {
		t.Fatal(err)
	}
	base, err := xconn.ConnectInMemoryBase(router, "legacy", "local", "trusted", &serializers.CBORSerializer{}, 2)
	if err != nil {
		t.Fatal(err)
	}
	wrapped := &wrappedBase{BaseSession: base}
	session, err := xconn.NewSession(wrapped, base.Serializer())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	handler := func(_ context.Context, inv *xconn.Invocation) *xconn.Result {
		return xconn.NewResult(inv.Args()...)
	}
	registration := session.Register("custom.peer", handler).Execute()
	if registration.Err != nil {
		t.Fatal(registration.Err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := session.Call("custom.peer").Arg("host peer").ExecuteContext(ctx)
	if result.Err != nil {
		t.Fatal(result.Err)
	}
	if result.ArgStringOr(0, "") != "host peer" {
		t.Fatal(result.Args)
	}
	if err = session.Close(); err != nil {
		t.Fatal(err)
	}
	if wrapped.closed.Load() != 1 {
		t.Fatal("custom base close was not idempotent")
	}
}

func TestLegacyManagementConfiguration(t *testing.T) {
	router, err := xconn.NewRouter(&xconn.RouterConfig{Management: true})
	if err != nil {
		t.Fatal(err)
	}
	defer router.Close()
	if !router.HasRealm(xconn.ManagementRealm) {
		t.Fatal("management realm missing")
	}
	session, err := xconn.ConnectInMemory(router, xconn.ManagementRealm)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	result := session.Call("io.xconn.mgmt.realm.list").Execute()
	if result.Err != nil {
		t.Fatal(result.Err)
	}
	realms, err := result.ArgList(0)
	if err != nil || realms.Len() == 0 {
		t.Fatal(result.Args, err)
	}
}
