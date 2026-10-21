package main

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/xconnio/wampproto-go/auth"
	xconn "github.com/xconnio/xconn-go"
)

const (
	authSalted          = "salted"
	invocationStarted   = "started"
	invocationCancelled = "cancelled"
)

// Deterministic test-only credentials, shared by the two independent runtimes.
func suiteAuth(method string, bad bool) auth.ClientAuthenticator {
	secret := "secret"
	if bad {
		secret = "wrong"
	}
	switch method {
	case "anonymous":
		return auth.NewAnonymousAuthenticator("interop", nil)
	case "ticket":
		return auth.NewTicketAuthenticator("interop", secret, nil)
	case "wampcra", authSalted:
		id := "interop"
		if method == authSalted {
			id = authSalted
		}
		return auth.NewWAMPCRAAuthenticator(id, secret, nil)
	case "cryptosign":
		seed := strings.Repeat("11", 32)
		if bad {
			seed = strings.Repeat("22", 32)
		}
		a, err := auth.NewCryptoSignAuthenticator("interop", seed, nil)
		must(err)
		return a
	default:
		panic("unknown authentication method")
	}
}

func (policy) authenticateSuite(request auth.Request) (auth.Response, error) {
	switch request.AuthMethod() {
	case auth.MethodAnonymous:
		return auth.NewResponse("go", "trusted", 0)
	case auth.MethodTicket:
		ticket, ok := request.(*auth.TicketRequest)
		if !ok || ticket.Ticket() != "secret" {
			return nil, fmt.Errorf("bad ticket")
		}
		return auth.NewResponse("interop", "trusted", 0)
	case auth.MethodCRA:
		if request.AuthID() == authSalted {
			key := auth.DeriveWAMPCRAKey("interop-salt", "secret", 1000, 32)
			return auth.NewCRAResponseSalted(authSalted, "trusted", string(key), "interop-salt", 1000, 32, 0), nil
		}
		return auth.NewCRAResponse("interop", "trusted", "secret", 0), nil
	case auth.MethodCryptoSign:
		key := ed25519.NewKeyFromSeed([]byte(strings.Repeat("\x11", 32))).Public().(ed25519.PublicKey)
		request, ok := request.(*auth.RequestCryptoSign)
		if !ok || request.PublicKey() != hex.EncodeToString(key) {
			return nil, fmt.Errorf("unknown key")
		}
		return auth.NewResponse("interop", "trusted", 0)
	default:
		return nil, fmt.Errorf("unsupported method")
	}
}

func assertSuite(ok bool, message string) {
	if !ok {
		panic(message)
	}
}

func expectURI(err error, uri string) {
	var wamp *xconn.Error
	assertSuite(errors.As(err, &wamp) && wamp.URI == uri, fmt.Sprintf("expected %s, got %v", uri, err))
}

func report(name string) { fmt.Fprintln(os.Stderr, "SCENARIO", name, "ok") }

func installSuite(session *xconn.Session) {
	must(session.Register("suite.error", func(_ context.Context, inv *xconn.Invocation) *xconn.InvocationResult {
		return &xconn.InvocationResult{
			Err:    "example.failure",
			Args:   []any{"detail"},
			Kwargs: map[string]any{"reason": "test"},
		}
	}).Do().Err)
	must(session.Register("suite.progress", func(_ context.Context, inv *xconn.Invocation) *xconn.InvocationResult {
		must(inv.SendProgress([]any{int64(1)}, nil))
		must(inv.SendProgress([]any{int64(2)}, nil))
		return xconn.NewInvocationResult(int64(3))
	}).Do().Err)
	must(session.Register("suite.payload", func(_ context.Context, inv *xconn.Invocation) *xconn.InvocationResult {
		return &xconn.InvocationResult{Args: inv.Args(), Kwargs: inv.Kwargs()}
	}).Do().Err)
	must(session.Register("suite.publish", func(_ context.Context, inv *xconn.Invocation) *xconn.InvocationResult {
		must(
			session.Publish("suite.events.item").
				Args(inv.Args()...).
				Kwargs(inv.Kwargs()).
				Option("acknowledge", true).
				Do().
				Err,
		)
		return xconn.NewInvocationResult()
	}).Do().Err)
	var mu sync.Mutex
	sums := map[string]int64{}
	must(session.Register("suite.inputs", func(_ context.Context, inv *xconn.Invocation) *xconn.InvocationResult {
		token, err := inv.ArgString(0)
		must(err)
		number, err := inv.ArgInt64(1)
		must(err)
		mu.Lock()
		sums[token] += number
		sum := sums[token]
		if !inv.Progress() {
			delete(sums, token)
		}
		mu.Unlock()
		if inv.Progress() {
			must(inv.SendProgress([]any{sum}, nil))
			return xconn.NewInvocationError(xconn.ErrNoResult)
		}
		return xconn.NewInvocationResult(sum)
	}).Do().Err)
	releases := map[string]chan struct{}{}
	states := map[string]string{}
	must(session.Register("suite.wait", func(ctx context.Context, inv *xconn.Invocation) *xconn.InvocationResult {
		token, err := inv.ArgString(0)
		must(err)
		release := make(chan struct{})
		mu.Lock()
		states[token] = invocationStarted
		releases[token] = release
		mu.Unlock()
		select {
		case <-ctx.Done():
			mu.Lock()
			states[token] = invocationCancelled
			delete(releases, token)
			mu.Unlock()
			return xconn.NewInvocationError("wamp.error.canceled")
		case <-release:
			mu.Lock()
			states[token] = "released"
			delete(releases, token)
			mu.Unlock()
			return xconn.NewInvocationResult("released")
		}
	}).Do().Err)
	must(session.Register("suite.state", func(_ context.Context, inv *xconn.Invocation) *xconn.InvocationResult {
		token, err := inv.ArgString(0)
		must(err)
		mu.Lock()
		state := states[token]
		mu.Unlock()
		return xconn.NewInvocationResult(state)
	}).Do().Err)
	must(session.Register("suite.release", func(_ context.Context, inv *xconn.Invocation) *xconn.InvocationResult {
		token, err := inv.ArgString(0)
		must(err)
		mu.Lock()
		if release := releases[token]; release != nil {
			close(release)
			delete(releases, token)
		}
		mu.Unlock()
		return xconn.NewInvocationResult()
	}).Do().Err)
	must(session.Subscribe("suite.to-go", func(event *xconn.Event) {
		must(
			session.Publish("suite.from-go").
				Args(event.Args()...).
				Kwargs(event.Kwargs()).
				Option("acknowledge", true).
				Do().
				Err,
		)
	}).Do().Err)
}

func suiteClient(uri string) {
	for _, spec := range serializerSpecs() {
		for _, method := range []string{"anonymous", "ticket", "wampcra", authSalted, "cryptosign"} {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			client := xconn.Client{SerializerSpec: spec, Authenticator: suiteAuth(method, false)}
			session, err := client.Connect(ctx, uri, "realm")
			must(err)
			response := session.Call("suite.payload").
				Args("auth", int64(42)).
				Kwarg("nested", map[string]any{"list": []any{true, "hello"}}).
				DoContext(ctx)
			must(response.Err)
			assertSuite(response.ArgsLen() == 2 && response.KwargsLen() == 1, "payload structure lost")
			must(session.Leave())
			cancel()
			report(spec.SubProtocol() + "/auth/" + method)
		}
		for _, method := range []string{"ticket", "wampcra", authSalted, "cryptosign"} {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			client := xconn.Client{SerializerSpec: spec, Authenticator: suiteAuth(method, true)}
			session, err := client.Connect(ctx, uri, "realm")
			if err == nil {
				_ = session.Leave()
				panic("invalid credential accepted: " + method)
			}
			cancel()
			report(spec.SubProtocol() + "/reject/" + method)
		}
		missing, err := (&xconn.Client{SerializerSpec: spec}).Connect(context.Background(), uri, "missing")
		if err == nil {
			_ = missing.Leave()
			panic("Rust router welcomed unknown realm")
		}
		report(spec.SubProtocol() + "/reject/missing-realm")
		client := xconn.Client{SerializerSpec: spec}
		session, err := client.Connect(context.Background(), uri, "realm")
		must(err)
		runClientScenarios(session, spec.SubProtocol(), func() *xconn.Session {
			session, err := (&xconn.Client{SerializerSpec: spec}).Connect(context.Background(), uri, "realm")
			must(err)
			return session
		})
		must(session.Leave())
	}
	fmt.Println("suite-ok")
}

func runClientScenarios(session *xconn.Session, label string, dial func() *xconn.Session) {
	echo := func(_ context.Context, inv *xconn.Invocation) *xconn.InvocationResult {
		return xconn.NewInvocationResult(inv.Args()...)
	}
	registration := session.Register("suite.client-owned", echo).Do()
	must(registration.Err)
	owned := session.Call("suite.client-owned").Args("callee-owned").Do()
	must(owned.Err)
	must(registration.Unregister())
	expectURI(session.Call("suite.client-owned").Do().Err, "wamp.error.no_such_procedure")
	report(label + "/register-unregister")

	response := session.Call("suite.error").Do()
	expectURI(response.Err, "example.failure")
	report(label + "/error")
	var progress []int64
	response = session.Call("suite.progress").ProgressReceiver(func(result *xconn.ProgressResult) {
		n, err := result.ArgInt64(0)
		must(err)
		progress = append(progress, n)
	}).Do()
	must(response.Err)
	number, err := response.ArgInt64(0)
	must(err)
	assertSuite(fmt.Sprint(progress) == "[1 2]" && number == 3, "progress order/final result lost")
	report(label + "/progress")
	inputAck := make(chan int64, 2)
	index := 0
	token := label + "-inputs"
	response = session.Call("suite.inputs").
		ProgressReceiver(func(result *xconn.ProgressResult) {
			number, err := result.ArgInt64(0)
			must(err)
			inputAck <- number
		}).
		ProgressSender(func(ctx context.Context) *xconn.Progress {
			if index > 0 {
				select {
				case sum := <-inputAck:
					assertSuite(sum == int64(index*(index+1)/2), "progress input acknowledgement mismatch")
				case <-ctx.Done():
					return &xconn.Progress{Err: ctx.Err()}
				case <-time.After(3 * time.Second):
					return &xconn.Progress{Err: fmt.Errorf("input acknowledgement timeout")}
				}
			}
			index++
			if index == 3 {
				return xconn.NewFinalProgress(token, int64(index))
			}
			return xconn.NewProgress(token, int64(index))
		}).
		Do()
	must(response.Err)
	total, err := response.ArgInt64(0)
	must(err)
	assertSuite(total == 6, "progressive input sum lost")
	report(label + "/progressive-inputs")

	events := make(chan string, 4)
	subscription := session.Subscribe("suite.from-go", func(event *xconn.Event) {
		token, err := event.ArgString(0)
		must(err)
		events <- token
	}).
		Do()
	must(subscription.Err)
	must(session.Publish("suite.to-go").Args("pubsub").Option("acknowledge", true).Do().Err)
	select {
	case token := <-events:
		assertSuite(token == "pubsub", "bad event")
	case <-time.After(3 * time.Second):
		panic("event timeout")
	}
	must(subscription.Unsubscribe())
	report(label + "/pubsub")
	for _, matching := range []string{"exact", "prefix", "wildcard"} {
		topic := "suite.events.item"
		if matching == "prefix" {
			topic = "suite.events."
		}
		if matching == "wildcard" {
			topic = "suite.*.item"
		}
		sub := session.Subscribe(topic, func(event *xconn.Event) {
			token, err := event.ArgString(0)
			must(err)
			events <- token
		}).
			Option("match", matching).
			Do()
		must(sub.Err)
		must(session.Call("suite.publish").Args(matching).Do().Err)
		select {
		case token := <-events:
			assertSuite(token == matching, "bad pattern event")
		case <-time.After(3 * time.Second):
			panic("pattern event timeout")
		}
		must(sub.Unsubscribe())
		report(label + "/pattern/" + matching)
	}
	// Context cancellation must be observed by a remote Rust callee, not just locally.
	ctx, cancel := context.WithCancel(context.Background())
	token = label + "-cancel"
	result := make(chan xconn.CallResponse, 1)
	go func() { result <- session.Call("suite.wait").Args(token).DoContext(ctx) }()
	for deadline := time.Now().Add(3 * time.Second); ; {
		state := session.Call("suite.state").Args(token).Do()
		must(state.Err)
		value, _ := state.ArgString(0)
		if value == invocationStarted {
			break
		}
		assertSuite(time.Now().Before(deadline), "callee never started")
		time.Sleep(time.Millisecond)
	}
	cancel()
	expectURI((<-result).Err, "wamp.error.canceled")
	for deadline := time.Now().Add(3 * time.Second); ; {
		state := session.Call("suite.state").Args(token).Do()
		must(state.Err)
		value, _ := state.ArgString(0)
		if value == invocationCancelled {
			break
		}
		assertSuite(time.Now().Before(deadline), "callee never cancelled")
		time.Sleep(time.Millisecond)
	}
	report(label + "/cancel")
	// A live connection must remain usable after a cancelled invocation.
	must(session.Call("suite.payload").Args("after-cancel").Do().Err)
	disconnectScenarios(session, label, dial)
}

func disconnectScenarios(session *xconn.Session, label string, dial func() *xconn.Session) {
	callee, caller := dial(), dial()
	started := make(chan struct{})
	release := make(chan struct{})
	must(callee.Register("suite.dying", func(_ context.Context, _ *xconn.Invocation) *xconn.InvocationResult {
		close(started)
		<-release
		return xconn.NewInvocationResult("late")
	}).Do().Err)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := make(chan xconn.CallResponse, 1)
	go func() { result <- caller.Call("suite.dying").DoContext(ctx) }()
	select {
	case <-started:
	case <-ctx.Done():
		panic("dying callee never started")
	}
	must(callee.Leave())
	response := <-result
	assertSuite(ctx.Err() == nil, "disconnect notification only arrived after caller deadline")
	expectURI(response.Err, "wamp.error.canceled")
	close(release)
	must(caller.Leave())
	report(label + "/callee-disconnect")
	disposable := dial()
	token := label + "-disconnect"
	result = make(chan xconn.CallResponse, 1)
	go func() { result <- disposable.Call("suite.wait").Args(token).Do() }()
	for deadline := time.Now().Add(3 * time.Second); ; {
		state := session.Call("suite.state").Args(token).Do()
		must(state.Err)
		value, _ := state.ArgString(0)
		if value == invocationStarted {
			break
		}
		assertSuite(time.Now().Before(deadline), "disposable call never started")
		time.Sleep(time.Millisecond)
	}
	must(disposable.Leave())
	assertSuite((<-result).Err != nil, "disconnected caller unexpectedly succeeded")
	for deadline := time.Now().Add(3 * time.Second); ; {
		state := session.Call("suite.state").Args(token).Do()
		must(state.Err)
		value, _ := state.ArgString(0)
		if value == invocationCancelled {
			break
		}
		assertSuite(time.Now().Before(deadline), "caller disconnect did not interrupt remote callee")
		time.Sleep(time.Millisecond)
	}
	report(label + "/caller-disconnect")
}

// Validate a standalone router, without installing application services in it.
func routerClient(uri string) {
	for _, spec := range serializerSpecs() {
		for _, method := range []string{"anonymous", "ticket", "wampcra", authSalted, "cryptosign"} {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			session, err := (&xconn.Client{SerializerSpec: spec, Authenticator: suiteAuth(method, false)}).Connect(
				ctx,
				uri,
				"realm",
			)
			must(err)
			must(
				session.Register("binary.echo", func(_ context.Context, inv *xconn.Invocation) *xconn.InvocationResult {
					return xconn.NewInvocationResult(inv.Args()...)
				}).Do().Err,
			)
			response := session.Call("binary.echo").Args("original Go", int64(42)).DoContext(ctx)
			must(response.Err)
			text, err := response.ArgString(0)
			must(err)
			assertSuite(text == "original Go", "bad executable RPC response")
			must(session.Leave())
			cancel()
			if method != "anonymous" {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				rejected, err := (&xconn.Client{SerializerSpec: spec, Authenticator: suiteAuth(method, true)}).Connect(
					ctx,
					uri,
					"realm",
				)
				if err == nil {
					_ = rejected.Leave()
					panic("executable accepted invalid credential")
				}
				cancel()
			}
		}
	}
	fmt.Println("router-ok")
}
