# Go binding

The module is `github.com/xconnio/xconn-core/bindings/go` and its package name is
`xconn`. It uses cgo and the shared Rust client/router runtime. Consumers install the
matching native SDK and set `PKG_CONFIG_PATH` to its `lib/pkgconfig` directory; no
Rust toolchain is needed. See [native installation](../../docs/native-install.md).
For source development:

```sh
cargo build --locked -p xconn-ffi --release
python3 scripts/package-native.py --stage-only target/native
export PKG_CONFIG_PATH="$PWD/target/native/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
cd bindings/go
go test -race -count=1 -timeout=90s ./...
```

The public client API retains xconn-go's fluent request builders, authenticator
and serializer interfaces, callbacks, response accessors and error types. Import
paths use this module and its `auth`, `messages`, `serializers`, `transports`
subpackages; no standalone Wampproto Go module is required.

```go
router, err := xconn.NewRouter(nil)
if err != nil { return err }
defer router.Close()
if err := router.AddRealm("realm1", nil); err != nil { return err }
session, err := xconn.ConnectInMemory(router, "realm1")
if err != nil { return err }
defer session.Close()

registration := session.Register("example.echo", func(ctx context.Context, inv *xconn.Invocation) *xconn.Result {
    return xconn.NewResult(inv.Args()...)
}).Execute()
if registration.Err != nil { return registration.Err }
result := session.Call("example.echo").Arg("hello").ExecuteContext(ctx)
if result.Err != nil { return result.Err }
fmt.Println(result.ArgStringOr(0, ""))
```

Network clients use `Client{Authenticator: ..., SerializerSpec: xconn.CBORSerializerSpec}`
and `Connect(ctx, uri, realm)`. `NewServer(router, authenticator, nil)` exposes
TCP/Unix WebSocket and RawSocket listeners. Go realms retain the original trusted
role default; add anonymous permissions explicitly for anonymous network clients.

Request options, `Session.Details`, `ID`, `Connected`, `GoodBye`, leave listeners and
`Leave()` are supported. Additive `CallContext`, `RegisterContext`,
`SubscribeContext`, `PublishContext` and `LeaveContext` methods remain available.
Application failures use `*xconn.Error`, preserving their URI and payloads. Context cancellation returns standard context errors.

Original authenticator and serializer objects are accepted, including custom
implementations for the built-in codecs. Rust performs protocol processing,
crypto, framing and default serialization. Custom `BaseSession` adapters can
supply an existing transport; the session receive loop stays in Rust. Local
`ConnectInMemoryBase` preserves queue capacity and nonblocking `TryWrite`.

Callbacks run in Go goroutines and can make nested calls. One native completion
waiter multiplexes operations; default networking needs no waiting thread per
idle session. Close resources explicitly; `Close` is idempotent.

Compatibility tests include the original client and parser suites, custom auth/serializers/base sessions, Unix listeners, management and an
independent original-Go router. This is not complete operational API parity:
custom `NetDial`, network queue tuning, custom server codecs, and
diagnostics/log publishing remain unbound.
Native SDK artifacts support installation outside the workspace; public release
publication remains pending. Unsupported exposed tuning options return errors.
See [binding scope](../README.md).

Fluent request terminals are `Execute()` and, for calls with a context,
`ExecuteContext(ctx)`. The former `Do()` / `DoContext(ctx)` names have been removed.

Invocation, Event and Result share strict typed payload accessors
with explicit defaults and consistent numeric/binary conversion rules.

Result construction and received values use one payload
type for handler replies, progressive results and completed calls.

## Static Linux linking

The native crate also produces `libxconn_ffi.a`. To stage a static SDK:

```sh
cargo build --locked --profile native -p xconn-ffi
python3 scripts/package-native.py --stage-only --static --profile native target/native
export PKG_CONFIG_PATH="$PWD/target/native/lib/pkgconfig"
CGO_ENABLED=1 go build -tags xconn_static,netgo,osusergo \
  -ldflags "-linkmode external -extldflags '-static -Wl,--gc-sections'" ./path/to/application
```

Run these commands from the XConn checkout and replace the application path.
The `xconn_static` tag selects `xconn-static.pc`, which explicitly links the Rust
archive instead of a neighboring shared library. The native profile enables thin LTO, one codegen unit, and size optimization
while retaining panic unwinding. The host needs a C toolchain and static libc.
TLS uses rustls/ring; no OpenSSL development libraries are required.
Relink Go applications after rebuilding the Rust archive: Go does not track
changes to external native archives in its build cache (use `go build -a`). Static packaging currently supports
Linux on the build host architecture. Certificates and any glibc runtime modules
remain system resources; this is not a hermetic cross-distribution build.

## Progressive RPC

Use `Stream()` or `StreamContext(ctx)` to receive owned progressive results.
Configure `StreamOptions{ProgressiveInput: true}` for input chunks, and
`Results: ResultsFinalOnly` to disable intermediate results independently.
`Send`, `Finish`, `Receive`, and `Close` manage one RPC. See
[streaming API and examples](../../docs/streaming.md).
