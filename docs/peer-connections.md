# Managed direct connections

A peer manager selects a path for new calls to one edge in a realm. The hub WAMP
session stays connected; a separately authenticated WebRTC session is established
in the background. Go clients manage this automatically; the public object remains
a `Session`. No additional connection wrapper is required.

## Go

Connect normally on the client:

```go
client := &xconn.Client{Authenticator: userAuthenticator}
session, err := client.Connect(ctx, hubURL, "office-pc")
if err != nil {
    return err
}
defer session.Close()

result := session.Call("app.upload").
    Arg("report.pdf").
    Arg(xconn.ReadStream(file)).
    ExecuteContext(ctx)
```

The edge connects to the same hub and realm, then accepts direct connections:

```go
edgeClient := &xconn.Client{
    Authenticator: edgeAuthenticator,
    RoutedOnly: true,
}
upstream, err := edgeClient.Connect(ctx, hubURL, "office-pc")
if err != nil {
    return err
}
defer upstream.Close()

endpoint, err := upstream.Expose(ctx, localRouter, &xconn.ExposeConfig{
    Authenticator: directAuthenticationPolicy,
})
if err != nil {
    return err
}
defer endpoint.Close()
```

`RoutedOnly: true` disables outgoing discovery for that connection. An edge still
accepts direct connections with `Expose`. Discovery also skips its own session's
advertisement, so it cannot initiate a connection to itself.

The hub and local router must already expose equivalent application procedures.
Existing RPC bridges provide that forwarding; `Expose` only advertises and accepts
direct connections. It does not create realms, permissions, application bridges,
or credentials. Do not configure an existing WebRTC provider with the same names.

Nil ICE servers use `stun:stun.l.google.com:19302`. An explicitly empty
`ICEServers` slice disables external servers; supplied servers replace the default.
No TURN server is configured automatically. Use
`Client.Direct: &xconn.DirectConfig{...}` to override ICE servers,
retry settings, serializer, or direct authentication. Without an authentication
override, direct connections reuse the client's authenticator, but perform a new,
independent WAMP handshake. `ExposeConfig` configures the edge's ICE servers and
server authentication policy.

`session.Path()` reports `PathRouted`, `PathDirect`, or `PathClosed`.
`session.DirectError()` reports the last failed negotiation and clears after success.
`Call(...).ExecuteContext(...)` and `Call(...).StreamContext(...)` select once at
execution. All arguments, cancellation, progressive chunks and results remain on
that selected session. Byte-stream arguments retain the existing restriction to
`Execute`/`ExecuteContext`.

## Lifecycle and fallback

- The public session owns its manager. `Session.Close` stops negotiation and
  retries, closes direct connections, and closes the hub connection/session using
  its existing ownership rules.
- Dial contexts bound connection setup. Canceling one after a successful connect
  does not close the session or stop automatic recovery. `Session.Done` closes on
  hub disconnection or explicit closure, not on recoverable direct-path failure.
- New calls prefer a healthy direct session and otherwise use the hub immediately.
  Negotiation never blocks ordinary routed calls.
- Failed attempts retry with exponential backoff, initially one second, capped at
  30 seconds. `RetryInterval` changes the initial delay. `ConnectTimeout` bounds
  discovery plus connection establishment (20 seconds by default). An established
  direct session's disconnection resets the backoff.
- Existing hub operations continue during an upgrade. Interrupted direct calls and
  streams fail; they are never replayed or resumed through the hub.
- Subscriptions, registrations and publications stay on the original hub session.
  Calls under `wamp.` and `io.xconn.peer.` also stay there, preserving meta-session
  identity and signaling ownership. Other calls select a path once, including the
  lower-level `CallContext` API.
- Existing explicit WebRTC constructors and in-process sessions retain their
  lower-level ownership; they do not recursively start another manager.
- Byte-stream fallback requires a stream-capable hub path, such as QUIC or Yamux.
  Plain WebSocket/RawSocket still supports ordinary WAMP. Without an active direct
  path, byte-stream arguments fail before the application call is dispatched.

## Realm-scoped signaling

There is one advertised edge per realm. The edge registers these procedures:

```text
io.xconn.peer.resolve                 -> ["webrtc", advertising-session-id]
io.xconn.peer.offer                   -> existing offer/answer protocol
```

Candidates use the existing connection-ID-correlated signaling protocol:

```text
io.xconn.peer.offerer_candidates      client -> edge
io.xconn.peer.answerer_candidates     edge -> client
```

`resolve` is registered after the provider starts and is unregistered on endpoint
closure or signaling-session loss. Registration ownership supplies presence, so
this slice does not need a directory service or changes to WAMP HELLO.

Applications must restrict registration of these procedures to their edge and
configure permissions for discovery, offers, and candidate publication/subscription.
Discovery never bypasses WAMP authorization, and the direct WAMP handshake applies
the edge's independent authentication and realm permissions. The realm identifies
the edge, not an individual backend behind it.

## Rust

Discovery, signaling defaults, STUN defaults, advertisement ownership and
connection establishment are implemented by `xconn_webrtc::peer` (feature
`services`). These APIs need no binding callbacks:

```rust
use xconn_core::{CBORSerializerSpec, wampproto::authenticators::anonymous::AnonymousAuthenticator};
use xconn_webrtc::peer::{self, Config, Endpoint};

// Edge: local_router carries its authentication and realm policy.
let endpoint = Endpoint::expose(upstream.clone(), local_router, Config::default()).await?;

// Caller: factories provide fresh codec/authentication state per attempt.
let managed = peer::connect(
    hub.clone(),
    Config::default(),
    || Ok(Box::new(CBORSerializerSpec)),
    || Ok(Box::new(AnonymousAuthenticator::default())),
)?;
let session = managed.session_for("app.echo")?;
let result = session.call("app.echo").arg("hello").execute().await?;
managed.close().await; // hub remains open
endpoint.close().await;
```

Anonymous authentication above is for a local example; authenticated deployments
supply their own authenticator factory and local router authentication policy.
`Config.ice_servers` defaults to Google STUN and can be replaced or cleared.

The transport-neutral ownership/retry building block remains
`xconn_core::async_::services::peer::Peer<C>` (feature `services`). A cancel-safe
`Connector<C>` supplies `C: DirectConnection`. `session_for` keeps meta/signaling
operations on the hub and selects one path for other operations. Additional
transport connectors can implement this trait without changing RPC or stream APIs.

The shared native bridge calls these same Rust APIs. Go supplies configuration,
optional host authentication/serialization callbacks, and handle lifetime glue;
it does not implement discovery or advertise endpoints. Other bindings can expose
this native interface without reproducing the protocol. Their public convenience
APIs and transparent operation resumption are outside this change.
