//go:build cgo && (linux || darwin || (windows && amd64))

package xconn_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xconnio/xconn-core/bindings/go"
)

func TestClientRouterMatrix(t *testing.T) {
	for _, codec := range []string{"json", "cbor", "msgpack"} {
		for _, transport := range []string{"memory", "websocket", "rawsocket"} {
			t.Run(codec+"/"+transport, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				router, err := newTestRouter()
				if err != nil {
					t.Fatal(err)
				}
				defer router.Close()
				connect := func() (*xconn.Session, error) { return router.Connect(ctx, codec) }
				if transport != "memory" {
					listener, err := router.Listen(ctx, "127.0.0.1:0", transport)
					if err != nil {
						t.Fatal(err)
					}
					scheme := "ws"
					if transport == "rawsocket" {
						scheme = "rs"
					}
					connect = func() (*xconn.Session, error) {
						return (&xconn.Client{Serializer: codec}).Connect(ctx, scheme+"://"+listener.Address, "realm")
					}
				}
				callee, err := connect()
				if err != nil {
					t.Fatal(err)
				}
				defer callee.Close()
				caller, err := connect()
				if err != nil {
					t.Fatal(err)
				}
				defer caller.Close()
				registration, err := callee.RegisterContext(
					ctx,
					"echo",
					func(_ context.Context, inv *xconn.Result) (*xconn.Result, error) { return inv, nil },
					nil,
				)
				if err != nil {
					t.Fatal(err)
				}
				result, err := caller.CallContext(
					ctx,
					"echo",
					[]any{"hello", uint64(1<<63) + 7},
					map[string]any{"key": "value"},
					nil,
				)
				if err != nil {
					t.Fatal(err)
				}
				if result.Args[1] != uint64(1<<63)+7 || result.Kwargs["key"] != "value" {
					t.Fatalf("bad result: %#v", result)
				}
				if codec != "json" {
					result, err = caller.CallContext(ctx, "echo", []any{[]byte{0, 255}}, nil, nil)
					if err != nil {
						t.Fatal(err)
					}
					if string(result.Args[0].([]byte)) != string([]byte{0, 255}) {
						t.Fatal("binary changed")
					}
				}
				events := make(chan int, 4)
				first, err := callee.SubscribeContext(ctx, "events", func(*xconn.Result) { events <- 1 }, nil)
				if err != nil {
					t.Fatal(err)
				}
				second, err := callee.SubscribeContext(ctx, "events", func(*xconn.Result) { events <- 2 }, nil)
				if err != nil {
					t.Fatal(err)
				}
				if err = caller.PublishContext(ctx, "events", []any{42}, nil, xconn.Options{"acknowledge": true}); err != nil {
					t.Fatal(err)
				}
				labels := map[int]bool{}
				for range 2 {
					select {
					case label := <-events:
						labels[label] = true
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				if len(labels) != 2 {
					t.Fatal(labels)
				}
				if err = first.UnsubscribeContext(ctx); err != nil {
					t.Fatal(err)
				}
				if err = caller.PublishContext(ctx, "events", nil, nil, xconn.Options{"acknowledge": true}); err != nil {
					t.Fatal(err)
				}
				select {
				case label := <-events:
					if label != 2 {
						t.Fatal(label)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if err = second.UnsubscribeContext(ctx); err != nil {
					t.Fatal(err)
				}
				if err = registration.UnregisterContext(ctx); err != nil {
					t.Fatal(err)
				}
				_, err = caller.CallContext(ctx, "echo", nil, nil, nil)
				var app *xconn.Error
				if !errors.As(err, &app) || app.URI != missingProcedureURI {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestContextCancellationReachesCallee(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	router, err := newTestRouter()
	if err != nil {
		t.Fatal(err)
	}
	defer router.Close()
	callee, err := router.Connect(ctx, "json")
	if err != nil {
		t.Fatal(err)
	}
	defer callee.Close()
	caller, err := router.Connect(ctx, "json")
	if err != nil {
		t.Fatal(err)
	}
	defer caller.Close()
	started, interrupted := make(chan struct{}), make(chan struct{})
	_, err = callee.RegisterContext(ctx, "wait", func(ctx context.Context, _ *xconn.Result) (*xconn.Result, error) {
		close(started)
		<-ctx.Done()
		close(interrupted)
		return nil, ctx.Err()
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	callCtx, stop := context.WithCancel(ctx)
	response := make(chan error, 1)
	go func() {
		_, err := caller.CallContext(callCtx, "wait", nil, nil, nil)
		response <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	stop()
	if err = <-response; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	select {
	case <-interrupted:
	case <-ctx.Done():
		t.Fatal("remote context not canceled")
	}
	_, err = callee.RegisterContext(
		ctx,
		"echo",
		func(_ context.Context, inv *xconn.Result) (*xconn.Result, error) { return inv, nil },
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = caller.CallContext(ctx, "echo", nil, nil, nil); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentCallsAndApplicationErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	router, err := newTestRouter()
	if err != nil {
		t.Fatal(err)
	}
	defer router.Close()
	callee, err := router.Connect(ctx, "json")
	if err != nil {
		t.Fatal(err)
	}
	defer callee.Close()
	caller, err := router.Connect(ctx, "json")
	if err != nil {
		t.Fatal(err)
	}
	defer caller.Close()
	_, err = callee.RegisterContext(
		ctx,
		"echo",
		func(_ context.Context, inv *xconn.Result) (*xconn.Result, error) { return inv, nil },
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for i := range 30 {
		group.Go(func() {
			result, err := caller.CallContext(ctx, "echo", []any{i}, nil, nil)
			if err != nil {
				t.Error(err)
				return
			}
			if result.Args[0] != int64(i) {
				t.Error(result)
			}
		})
	}
	group.Wait()
	_, err = callee.RegisterContext(ctx, "fail", func(context.Context, *xconn.Result) (*xconn.Result, error) {
		return nil, fmt.Errorf(
			"wrapped: %w",
			&xconn.Error{
				URI:    "example.failure",
				Args:   []any{"detail"},
				Kwargs: map[string]any{"reason": "test"},
			},
		)
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = caller.CallContext(ctx, "fail", nil, nil, nil)
	var app *xconn.Error
	if !errors.As(err, &app) || app.URI != "example.failure" || app.Kwargs["reason"] != "test" {
		t.Fatal(err)
	}
}

func TestOriginalGoClient(t *testing.T) {
	binary := originalReference(t)
	for _, transport := range []string{"websocket", "rawsocket"} {
		t.Run(transport, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			router, err := newTestRouter()
			if err != nil {
				t.Fatal(err)
			}
			defer router.Close()
			callee, err := router.Connect(ctx, "json")
			if err != nil {
				t.Fatal(err)
			}
			defer callee.Close()
			_, err = callee.RegisterContext(
				ctx,
				"rust.echo",
				func(_ context.Context, inv *xconn.Result) (*xconn.Result, error) { return inv, nil },
				nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			listener, err := router.Listen(ctx, "127.0.0.1:0", transport)
			if err != nil {
				t.Fatal(err)
			}
			scheme := "ws"
			if transport == "rawsocket" {
				scheme = "rs"
			}
			output, err := exec.CommandContext(ctx, binary, "client", scheme+"://"+listener.Address).CombinedOutput()
			if err != nil {
				t.Fatalf("%v: %s", err, output)
			}
		})
	}
}

func TestOriginalGoRouterAuthentication(t *testing.T) {
	binary := originalReference(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary)
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = command.Process.Kill()
		_ = command.Wait()
	}()
	var metadata map[string]string
	if err = json.NewDecoder(stdout).Decode(&metadata); err != nil {
		t.Fatal(err)
	}
	for _, codec := range []string{"json", "cbor", "msgpack"} {
		for _, transport := range []string{"ws", "rs"} {
			for _, auth := range []xconn.Authenticator{
				{}, {Method: "ticket", ID: "interop", Secret: "secret"},
				{Method: "wampcra", ID: "interop", Secret: "secret"},
				{Method: "wampcra", ID: "salted", Secret: "secret"},
				{Method: "cryptosign", ID: "interop", Secret: strings.Repeat("11", 32)},
			} {
				client := xconn.Client{Serializer: codec, Authenticator: auth}
				session, err := client.Connect(ctx, metadata[transport], "realm")
				if err != nil {
					t.Fatal(codec, transport, auth.Method, err)
				}
				result, err := session.CallContext(ctx, "echo", []any{"Go binding"}, nil, nil)
				if err != nil || result.Args[0] != "Go binding" {
					t.Fatal(result, err)
				}
				if err = session.Close(); err != nil {
					t.Fatal(err)
				}
			}
			for _, auth := range []xconn.Authenticator{
				{Method: "ticket", ID: "interop", Secret: "wrong"},
				{Method: "wampcra", ID: "interop", Secret: "wrong"},
				{Method: "wampcra", ID: "salted", Secret: "wrong"},
				{Method: "cryptosign", ID: "interop", Secret: strings.Repeat("22", 32)},
			} {
				client := xconn.Client{Serializer: codec, Authenticator: auth}
				session, err := client.Connect(ctx, metadata[transport], "realm")
				if err == nil {
					_ = session.Close()
					t.Fatal("invalid credential accepted")
				}
			}
		}
	}
}

func TestCloseReleasesPendingCallAndRemoteContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	router, err := newTestRouter()
	if err != nil {
		t.Fatal(err)
	}
	defer router.Close()
	callee, err := router.Connect(ctx, "json")
	if err != nil {
		t.Fatal(err)
	}
	defer callee.Close()
	caller, err := router.Connect(ctx, "json")
	if err != nil {
		t.Fatal(err)
	}
	defer caller.Close()
	started, interrupted := make(chan struct{}), make(chan struct{})
	_, err = callee.RegisterContext(ctx, "wait", func(ctx context.Context, _ *xconn.Result) (*xconn.Result, error) {
		close(started)
		<-ctx.Done()
		close(interrupted)
		return nil, ctx.Err()
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	response := make(chan error, 1)
	go func() {
		_, err := caller.CallContext(context.Background(), "wait", nil, nil, nil)
		response <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	_ = caller.Close()
	select {
	case err = <-response:
		if err == nil {
			t.Fatal("pending call succeeded after close")
		}
	case <-ctx.Done():
		t.Fatal("pending call leaked")
	}
	select {
	case <-interrupted:
	case <-ctx.Done():
		t.Fatal("callee not interrupted")
	}
	select {
	case <-caller.Done():
	case <-ctx.Done():
		t.Fatal("event pump leaked")
	}
}

func originalReference(t *testing.T) string {
	t.Helper()
	binary := os.Getenv("XCONN_REFERENCE_GO_BIN")
	if binary == "" {
		t.Skip("requires original Go reference")
	}
	output, err := exec.Command(binary, "provenance").Output()
	if err != nil {
		t.Fatal(err)
	}
	var p struct {
		Kind      string            `json:"kind"`
		CGO       bool              `json:"cgo_enabled"`
		Revisions map[string]string `json:"revisions"`
	}
	if err = json.Unmarshal(output, &p); err != nil {
		t.Fatal(err)
	}
	if p.Kind != "independent-pure-go" || p.CGO ||
		p.Revisions["wampproto-go"] != "ecb54c6c2318d7d3184f77ca95cf5f49c779edae" {
		t.Fatal("fixture is not the independent Go reference")
	}
	return binary
}

func newTestRouter() (*xconn.Router, error) {
	router, err := xconn.NewRouter(nil)
	if err != nil {
		return nil, err
	}
	if err = router.AddRealm("realm", anonymousRealmConfig()); err != nil {
		router.Close()
		return nil, err
	}
	return router, nil
}
