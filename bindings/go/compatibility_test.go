package xconn_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/xconnio/xconn-core/bindings/go"
	"github.com/xconnio/xconn-core/bindings/go/auth"
)

// Exercise fluent builders and handlers using the shared Result payload.
const missingProcedureURI = "wamp.error.no_such_procedure"

func TestLegacyClientAPI(t *testing.T) {
	for _, spec := range []xconn.SerializerSpec{
		xconn.JSONSerializerSpec, xconn.CBORSerializerSpec, xconn.MsgPackSerializerSpec,
	} {
		t.Run(spec.SubProtocol(), func(t *testing.T) {
			router, err := xconn.NewRouter(xconn.DefaultRouterConfig())
			if err != nil {
				t.Fatal(err)
			}
			defer router.Close()
			if err = router.AddRealm("legacy", anonymousRealmConfig()); err != nil {
				t.Fatal(err)
			}
			if !router.HasRealm("legacy") {
				t.Fatal("missing realm")
			}
			server := xconn.NewServer(router, nil, nil)
			listener, err := server.ListenAndServeWebSocket(xconn.NetworkTCP, "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client := xconn.Client{SerializerSpec: spec, Authenticator: auth.NewAnonymousAuthenticator("legacy", nil)}
			callee, err := client.Connect(ctx, "ws://"+listener.Addr().String()+"/ws", "legacy")
			if err != nil {
				t.Fatal(err)
			}
			defer callee.Close()
			caller, err := client.Connect(ctx, "ws://"+listener.Addr().String()+"/ws", "legacy")
			if err != nil {
				t.Fatal(err)
			}
			defer caller.Close()
			if caller.ID() == 0 || caller.Details().Realm() != "legacy" || !caller.Connected() {
				t.Fatal("invalid details")
			}
			registration := callee.Register("legacy.echo", func(
				_ context.Context, inv *xconn.Invocation,
			) *xconn.Result {
				return &xconn.Result{Args: inv.Args(), Kwargs: inv.Kwargs()}
			}).
				Execute()
			if registration.Err != nil {
				t.Fatal(registration.Err)
			}
			result := caller.Call("legacy.echo").Arg("hello").Kwarg("count", 7).ExecuteContext(ctx)
			if result.Err != nil {
				t.Fatal(result.Err)
			}
			if result.ArgStringOr(0, "") != "hello" || result.KwargInt64Or("count", 0) != 7 {
				t.Fatal(result.Args, result.Kwargs)
			}
			if err = registration.Unregister(); err != nil {
				t.Fatal(err)
			}
			result = caller.Call("legacy.echo").Execute()
			var application *xconn.Error
			if !errors.As(result.Err, &application) || application.URI != missingProcedureURI {
				t.Fatal(result.Err)
			}
			if result.Err.Error() != missingProcedureURI {
				t.Fatal(result.Err)
			}
			events := make(chan *xconn.Event, 1)
			subscription := callee.Subscribe("legacy.events", func(event *xconn.Event) { events <- event }).Execute()
			if subscription.Err != nil {
				t.Fatal(subscription.Err)
			}
			published := caller.Publish("legacy.events").Args("event").Acknowledge(true).ExcludeMe(false).Execute()
			if published.Err != nil {
				t.Fatal(published.Err)
			}
			select {
			case event := <-events:
				if event.ArgStringOr(0, "") != "event" {
					t.Fatal(event.Args())
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if err = subscription.Unsubscribe(); err != nil {
				t.Fatal(err)
			}
			left := make(chan struct{})
			caller.SetOnLeaveListener(func() { close(left) })
			if err = caller.Leave(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-left:
			case <-ctx.Done():
				t.Fatal("leave callback missing")
			}
			if caller.Connected() {
				t.Fatal("session still connected")
			}
		})
	}
}

func TestLegacyProgressiveCalls(t *testing.T) {
	router, err := newTestRouter()
	if err != nil {
		t.Fatal(err)
	}
	defer router.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	callee, err := router.Connect(ctx, "cbor")
	if err != nil {
		t.Fatal(err)
	}
	defer callee.Close()
	caller, err := router.Connect(ctx, "cbor")
	if err != nil {
		t.Fatal(err)
	}
	defer caller.Close()
	registration := callee.Register("progress", func(_ context.Context, inv *xconn.Invocation) *xconn.Result {
		if err := inv.SendProgress([]any{1}, nil); err != nil {
			return xconn.NewResultError("example.progress_error", err.Error())
		}
		return xconn.NewResult("done")
	}).Execute()
	if registration.Err != nil {
		t.Fatal(registration.Err)
	}
	var progress []int64
	result := caller.Call("progress").
		ProgressReceiver(func(result *xconn.Result) { progress = append(progress, result.ArgInt64Or(0, 0)) }).
		ExecuteContext(ctx)
	if result.Err != nil {
		t.Fatal(result.Err)
	}
	if result.ArgStringOr(0, "") != "done" || !reflect.DeepEqual(progress, []int64{1}) {
		t.Fatal(result.Args, progress)
	}
	if err = registration.Unregister(); err != nil {
		t.Fatal(err)
	}
	registration = callee.Register("inputs", func(_ context.Context, inv *xconn.Invocation) *xconn.Result {
		if inv.Progress() {
			return xconn.NewResultError(xconn.ErrNoResult)
		}
		return xconn.NewResult(inv.Args()...)
	}).Execute()
	if registration.Err != nil {
		t.Fatal(registration.Err)
	}
	count := 0
	result = caller.Call("inputs").ProgressSender(func(context.Context) *xconn.Progress {
		count++
		if count < 3 {
			return xconn.NewProgress(count)
		}
		return xconn.NewFinalProgress("last")
	}).ExecuteContext(ctx)
	if result.Err != nil {
		t.Fatal(result.Err)
	}
	if result.ArgStringOr(0, "") != "last" {
		t.Fatal(result.Args)
	}
}

func anonymousRealmConfig() *xconn.RealmConfig {
	return &xconn.RealmConfig{
		Roles: []xconn.RealmRole{
			{
				Name: "anonymous",
				Permissions: []xconn.Permission{
					{
						URI:            "",
						MatchPolicy:    "prefix",
						AllowCall:      true,
						AllowRegister:  true,
						AllowPublish:   true,
						AllowSubscribe: true,
					},
				},
			},
		},
	}
}
