package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/xconnio/wampproto-go/auth"
	xconn "github.com/xconnio/xconn-go"
	rtc "github.com/xconnio/xconn-webrtc-go"
)

//nolint:gochecknoglobals // The independent builder replaces this fixture provenance.
var fixtureProvenance = `{"kind":"migrated-go-shared-rust-engine"}`

type policy struct{}

func (policy) Methods() []auth.Method {
	return []auth.Method{auth.MethodAnonymous, auth.MethodTicket, auth.MethodCRA, auth.MethodCryptoSign}
}

func (policy) Authenticate(request auth.Request) (auth.Response, error) {
	return policy{}.authenticateSuite(request)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "router-client" {
		routerClient(os.Args[2])
		return
	}

	if len(os.Args) > 1 && os.Args[1] == "suite-client" {
		suiteClient(os.Args[2])
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "provenance" {
		fmt.Println(fixtureProvenance)
		return
	}
	if len(os.Args) > 1 && (os.Args[1] == "quic-client" || os.Args[1] == "wt-client") {
		der, err := hex.DecodeString(os.Args[3])
		must(err)
		cert, err := x509.ParseCertificate(der)
		must(err)
		roots := x509.NewCertPool()
		roots.AddCert(cert)
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, ServerName: "localhost"}
		for _, spec := range serializerSpecs() {
			var session *xconn.Session
			if os.Args[1] == "quic-client" {
				quic, err := xconn.ConnectQUIC(
					context.Background(),
					os.Args[2],
					"realm",
					&xconn.QUICDialerConfig{SerializerSpec: spec, TLSConfig: tlsConfig},
				)
				must(err)
				session = quic.Session
				defer quic.Connection().Close()
			} else {
				config := &xconn.WebTransportDialerConfig{SerializerSpec: spec, TLSClientConfig: tlsConfig}
				wt, err := xconn.ConnectWebTransport(context.Background(), os.Args[2], "realm", config)
				must(err)
				session = wt.Session
			}
			response := session.Call("rust.echo").Args("multiplexed").Do()
			must(response.Err)
			value, err := response.ArgString(0)
			must(err)
			if value != "multiplexed" {
				panic(value)
			}
			must(session.Leave())
		}
		fmt.Println("ok")
		return
	}

	if len(os.Args) > 1 && os.Args[1] == "rtc-client" {
		signal, err := xconn.ConnectAnonymous(context.Background(), os.Args[2], "realm")
		must(err)
		defer func() { _ = signal.Leave() }()
		session, err := rtc.ConnectWAMP(
			&rtc.ClientConfig{
				Session:                  signal,
				Realm:                    "realm",
				ProcedureWebRTCOffer:     "offer",
				TopicAnswererOnCandidate: "to-provider",
				TopicOffererOnCandidate:  "to-client",
				Serializer:               xconn.CBORSerializerSpec,
			},
		)
		must(err)
		response := session.Call("rust.echo").Args("Go over WebRTC").Do()
		must(response.Err)
		value, err := response.ArgString(0)
		must(err)
		if value != "Go over WebRTC" {
			panic(value)
		}
		another, err := session.OpenSession("realm", &rtc.OpenSessionConfig{Serializer: xconn.JSONSerializerSpec})
		must(err)
		must(session.Close())
		response = another.Call("rust.echo").Args("second session").Do()
		must(response.Err)
		must(another.Close())
		must(session.Connection().Close())
		fmt.Println("ok")
		return
	}

	if len(os.Args) > 1 && os.Args[1] == "client" {
		for _, spec := range serializerSpecs() {
			client := xconn.Client{SerializerSpec: spec}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			session, err := client.Connect(ctx, os.Args[2], "realm")
			must(err)
			response := session.Call("rust.echo").Args("hello from Go").Do()
			must(response.Err)
			value, err := response.ArgString(0)
			must(err)
			if value != "hello from Go" {
				panic(value)
			}
			must(session.Leave())
			cancel()
		}
		fmt.Println("ok")
		return
	}
	router, err := xconn.NewRouter(nil)
	must(err)
	must(router.AddRealm("realm", nil))
	defer router.Close()
	local, err := xconn.ConnectInMemory(router, "realm")
	must(err)
	registration := local.Register("echo", func(_ context.Context, inv *xconn.Invocation) *xconn.InvocationResult {
		return xconn.NewInvocationResult(inv.Args()...)
	}).Do()
	must(registration.Err)
	installSuite(local)
	server := xconn.NewServer(router, policy{}, nil)
	ws, err := server.ListenAndServeWebSocket(xconn.NetworkTCP, "127.0.0.1:0")
	must(err)
	defer ws.Close()
	rs, err := server.ListenAndServeRawSocket(xconn.NetworkTCP, "127.0.0.1:0")
	must(err)
	defer rs.Close()
	provider := rtc.NewWebRTCHandler()
	must(
		provider.Setup(
			&rtc.ProviderConfig{
				Session:                     local,
				ProcedureHandleOffer:        "offer",
				TopicHandleRemoteCandidates: "to-provider",
				TopicPublishLocalCandidate:  "to-client",
				Router:                      router,
				Authenticator:               policy{},
			},
		),
	)
	quicTLS, err := xconn.GenerateSelfSignedTLSConfig()
	must(err)
	quic, err := server.ListenAndServeQUIC("127.0.0.1:0", quicTLS)
	must(err)
	defer quic.Close()
	go func() {
		for stream := range quic.AcceptStream() {
			go func() {
				defer stream.Close()
				payload := make([]byte, 4)
				if _, err := io.ReadFull(stream, payload); err == nil {
					_, _ = stream.Write(payload)
				}
			}()
		}
	}()
	wtTLS, wtHash, err := xconn.GenerateWebTransportTLSConfig()
	must(err)
	wt, err := server.ListenAndServeWebTransport("127.0.0.1:0", wtTLS, "/ws")
	must(err)
	defer wt.Close()
	info, _ := json.Marshal(
		map[string]string{
			"ws":        fmt.Sprintf("ws://127.0.0.1:%d", ws.Port()),
			"rs":        fmt.Sprintf("rs://127.0.0.1:%d", rs.Port()),
			"quic":      quic.Addr().String(),
			"quic_cert": hex.EncodeToString(quicTLS.Certificates[0].Certificate[0]),
			"wt":        "https://" + wt.Addr().String() + "/ws",
			"wt_hash":   hex.EncodeToString(wtHash),
		},
	)
	fmt.Println(string(info))
	select {}
}

func serializerSpecs() []xconn.SerializerSpec {
	return []xconn.SerializerSpec{xconn.JSONSerializerSpec, xconn.CBORSerializerSpec, xconn.MsgPackSerializerSpec}
}
