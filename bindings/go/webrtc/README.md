# Go WebRTC

This subpackage uses the existing XConn native library and Rust runtime. It does
not use Pion or introduce another Go module. Install the matching
[native SDK](../../../docs/native-install.md) and set `PKG_CONFIG_PATH` to its
`lib/pkgconfig` directory. Consumers need Go, cgo and a C compiler; only building
the SDK from source requires Rust. This is not a `CGO_ENABLED=0` dependency.
For source development, run the local router/provider/client example:

```sh
cargo build --locked -p xconn-ffi --release
python3 scripts/package-native.py --stage-only target/native
export PKG_CONFIG_PATH="$PWD/target/native/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
go -C bindings/go run ./examples/webrtc
```

The example uses in-memory WAMP signaling sessions and real local WebRTC data
channels. In a deployment, connect both signaling sessions to a WAMP router over
WebSocket. The provider attaches incoming WAMP sessions to its configured router.

```go
import (
    "context"
    "time"

    xconn "github.com/xconnio/xconn-core/bindings/go"
    rtc "github.com/xconnio/xconn-core/bindings/go/webrtc"
)

ctx := context.Background()
signaling, err := xconn.ConnectAnonymous(ctx, "ws://localhost:8080/ws", "realm1")
// Check err and arrange to close signaling.
session, err := rtc.ConnectWAMPContext(ctx, &rtc.ClientConfig{
    Session: signaling,
    Realm: "realm1",
    Serializer: xconn.CBORSerializerSpec,
    ProcedureWebRTCOffer: "io.xconn.webrtc.offer",
    TopicAnswererOnCandidate: "io.xconn.webrtc.to_provider",
    TopicOffererOnCandidate: "io.xconn.webrtc.to_client",
    ConnectTimeout: 15 * time.Second,
    OpenTimeout: 10 * time.Second,
})
// Check err. Existing WAMP request builders are available directly:
result := session.Call("com.example.echo").Arg("hello").ExecuteContext(ctx)
```

Create the provider with `NewWebRTCHandler()` and `SetupContext(ctx, &ProviderConfig{...})`.
The provider's `TopicHandleRemoteCandidates` receives the client's
`TopicAnswererOnCandidate`; `TopicPublishLocalCandidate` publishes to its
`TopicOffererOnCandidate`. Offers, answers, and trickled ICE candidates use the
original Go wire format. Configure STUN/TURN through `ICEServers`. Password
credentials are supported; the Rust backend does not support OAuth credentials.
`UpdateICEServers` applies to future connections and returns validation errors.

`ConnectWAMP`, `Setup`, and `OpenSession` retain the old blocking signatures;
context variants add cancellation. Zero timeout settings use 20 seconds. Negative
timeouts are rejected. `HandshakeTimeout` controls incoming channel classification;
router authentication has its own deadlines. Additional sessions inherit the
connection's opening timeout unless `OpenSessionConfig.OpenTimeout` overrides it.
Custom client authenticators, custom serializers using the standard serializer
IDs, and the provider's server authenticator reuse the existing callback bridge.

## Raw channels and ownership

- `session.OpenChannel("files", nil)` opens an explicit raw channel.
- `provider.AcceptChannel(ctx)` accepts it without requiring a client packet;
  the provider can send first.
- `session.Connection().AcceptChannel(ctx)` receives channels opened by the provider.
- `channel.Connection()` returns a connection handle that can open channels back
  to the same peer. Closing any connection handle closes the shared connection.
- `Send` / `ReadPacket` and `io.Reader` / `io.Writer` use raw binary packets,
  limited to 65,535 bytes per write. Stream reads retain partial packets.
- `WriteMessage` / `ReadMessage` use Go-compatible chunk framing, bounded at
  16 MiB. Use the same framing at both ends; chunk framing requires ordered,
  reliable delivery. Context variants allow cancellation. Cancelling a partial
  chunked write closes that channel to avoid corrupting later messages.
- Closing a WAMP session leaves sibling sessions and raw channels open.
  Explicitly close channels, the connection, and the signaling session when done.
- Provider shutdown closes its peers and wakes blocked channel accepts. `Done`
  channels and `OnClose` observe channel closure. `ClientConfig.OnDisconnect`
  observes connection failure/closure; detecting a remote network failure can
  take the ICE backend's detection interval.

The legacy `OnDataChannel` and `OnMessage` callbacks are available with Rust-backed
channel/message types. Choose either callbacks or explicit reads/accepts for each
resource. Provider callbacks retain the signaling request ID and any packet used
to classify a legacy channel; explicit raw channels have no consumed first packet.
`AcceptChannel` restores legacy classification packets automatically. An original
Go provider still requires a first packet to classify a raw channel.

Pion's `PeerConnection`, `DataChannel`, offerer/answerer helpers and backend hooks
are not exposed. Replace Pion channel options with this package's
`DataChannelInit`. Explicit raw channels reserve the `xconn.raw` protocol; a
custom protocol or an in-band explicit channel ID is rejected rather than ignored.
A provider requires a router. `OnAnswerer` is not needed for internal signaling
wiring and is not exposed.

The Go implementation is in the parent package to share private session,
authentication and completion-dispatch machinery. This subpackage aliases those
types and exposes the established WebRTC names; applications need not use FFI
handles.

## Validation

```sh
XCONN_REFERENCE_GO_BIN=/path/to/original-go-reference \
  go -C bindings/go test -race -count=1 -timeout=90s ./...
```

Tests use real WebSocket signaling and WebRTC channels, all three standard
serializers, authentication, progressive results, independent WAMP sessions, raw
channels in both directions, deadlines, callbacks, and original Go interoperability.
Browser and deployed STUN/TURN interoperability remain separate validation work.
