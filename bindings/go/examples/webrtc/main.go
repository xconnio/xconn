package main

import (
	"context"
	"fmt"
	"log"
	"time"

	xconn "github.com/xconnio/xconn-core/bindings/go"
	rtc "github.com/xconnio/xconn-core/bindings/go/webrtc"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	router, err := xconn.NewRouter(nil)
	if err != nil {
		return err
	}
	defer router.Close()
	if err = router.AddRealm("realm", &xconn.RealmConfig{Roles: []xconn.RealmRole{{
		Name: "anonymous", Permissions: []xconn.Permission{{URI: "", MatchPolicy: "prefix",
			AllowCall: true, AllowRegister: true, AllowPublish: true, AllowSubscribe: true}},
	}}}); err != nil {
		return err
	}

	// Use WebSocket signaling sessions instead when the peers are in separate processes.
	providerSignal, err := router.Connect(ctx, "json")
	if err != nil {
		return err
	}
	defer providerSignal.Close()
	clientSignal, err := router.Connect(ctx, "json")
	if err != nil {
		return err
	}
	defer clientSignal.Close()
	handler := func(_ context.Context, inv *xconn.Invocation) *xconn.Result {
		return xconn.NewResult(inv.Args()...)
	}
	registration := providerSignal.Register("echo", handler).Execute()
	if registration.Err != nil {
		return registration.Err
	}
	provider := rtc.NewWebRTCHandler()
	if err = provider.SetupContext(ctx, &rtc.ProviderConfig{
		Session: providerSignal, Router: router,
		ProcedureHandleOffer: "offer", TopicHandleRemoteCandidates: "to-provider",
		TopicPublishLocalCandidate: "to-client",
	}); err != nil {
		return err
	}
	defer provider.Close()
	session, err := rtc.ConnectWAMPContext(ctx, &rtc.ClientConfig{
		Session: clientSignal, Realm: "realm", Serializer: xconn.CBORSerializerSpec,
		ProcedureWebRTCOffer: "offer", TopicAnswererOnCandidate: "to-provider",
		TopicOffererOnCandidate: "to-client", ConnectTimeout: 5 * time.Second,
	})
	if err != nil {
		return err
	}
	defer func() { _ = session.Connection().CloseContext(context.WithoutCancel(ctx)) }()
	defer func() { _ = session.CloseContext(context.WithoutCancel(ctx)) }()
	result := session.Call("echo").Arg("Hello over WebRTC").ExecuteContext(ctx)
	if result.Err != nil {
		return result.Err
	}
	fmt.Println(result.ArgStringOr(0, ""))

	outgoing, err := session.OpenChannel("files", nil)
	if err != nil {
		return err
	}
	defer func() { _ = outgoing.CloseContext(context.WithoutCancel(ctx)) }()
	incoming, err := provider.AcceptChannel(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = incoming.CloseContext(context.WithoutCancel(ctx)) }()
	if err = incoming.WriteMessageContext(ctx, []byte("provider sent first")); err != nil {
		return err
	}
	data, err := outgoing.ReadMessageContext(ctx)
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}
