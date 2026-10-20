# xconn-webrtc

Reusable native Rust WebRTC data transport with optional WAMP integration.

- `transport`: offer/answer, ICE candidates, ordered data channels, bounded
  Go-compatible message chunking, raw channels and connection cleanup.
- `wamp` (default feature): RawSocket-style serializer negotiation, multiple
  independent WAMP sessions on one connection, signaling provider/client using
  the existing xconn-webrtc-go offer/candidate wire format.

The transport layer does not depend on WAMP or xconn-core when built using
`cargo check -p xconn-webrtc --no-default-features`. Language bindings can share this implementation. Binding adapters will be
introduced in later milestones.

```sh
cargo test -p xconn-webrtc --all-features
cargo clippy -p xconn-webrtc --all-features --all-targets -- -D warnings
cargo check -p xconn-webrtc --no-default-features
```

Run these commands from the xconn-core workspace root. The WAMP engine is included under `crates/wampproto`. Normal xconn-core package builds do not depend
on this crate or the WebRTC stack. Consumers can depend on xconn-webrtc separately
and disable its default `wamp` feature for raw transport use. Tests establish real
local WebRTC connections, exchange multi-chunk payloads and raw packets, open two
WAMP sessions using different serializers on one connection, close those sessions
independently, and run the complete WAMP offer/answer signaling flow.

This initial native adapter uses the Tokio-based webrtc 0.14 API. WebRTC backend
upgrades can be made behind this library's API. WAMP signaling uses trickle ICE in both directions and buffers early remote
candidates until their remote description is installed. Remote
TURN/STUN deployments, browser interoperability, legacy pre-handshake channel validation,
and reusable foreign-language runtime ownership need further validation.

The provider dispatches non-WAMP data channels through Provider::accept_raw and
preserves the first packet consumed during legacy channel classification. Closing one WAMP
session leaves other sessions/raw channels open. Negotiated WAMP message limits
are enforced independently of the raw channel limit.

## WAMP and raw channels

Run the complete local provider/client example:

```sh
cargo run -p xconn-webrtc --example wamp
```

`wamp::connect` and `Provider::start` retain their existing signatures and use
`Timeouts::default()`. Use `connect_with_timeouts` and
`Provider::start_with_timeouts` to configure:

```rust
use std::time::Duration;
use xconn_webrtc::wamp::Timeouts;

let timeouts = Timeouts {
    connect: Duration::from_secs(15),      // Signaling through the first WAMP join; provider peer establishment.
    open_session: Duration::from_secs(10), // Opening a WAMP session or waiting for a raw channel to open.
    handshake: Duration::from_secs(5),     // Classifying an incoming channel; standalone accept handshake.
};
```

All durations must be positive. Initial connection setup uses the smaller of its
remaining connection deadline and the session-opening deadline. Additional
sessions inherit these settings; override them with
`WebRTCSession::open_session_with_timeouts`. Router authentication retains the
router's own deadlines. Cancellation or timeout closes a pending session's
channel; cancelling an initial connection closes its peer connection and releases
its candidate subscription. Cleanup after cancellation runs asynchronously on the
Tokio runtime, which must remain alive to finish cleanup.

Open an independent raw channel with
`session.open_channel("files", Default::default()).await?`, and receive it with
`provider.accept_channel().await`. The provider may send first. Use
`DataChannel::send_packet`/`read_packet` for raw packets (at most 65,535 bytes) or
`write_message`/`read_message` for Go-compatible chunked messages. Keep both ends
on the same framing mode. Raw channel options use `RTCDataChannelInit`; ordered,
reliable delivery is required for chunked messages. On a client, use
`session.accept_channel()` for channels opened by the remote peer. At transport
level, use `Connection::open_raw_channel` and `Connection::accept_channel`.

Explicit raw channels use the `xconn.raw` protocol, so the Rust provider can
identify them without consuming a packet. Legacy raw channels are still
classified from their first packet. `Provider::accept_channel` restores that
packet automatically. The lower-level `accept_raw` returns `RawChannel`, whose
`first_packet` is now `Option<Bytes>`: `None` for explicitly identified channels,
`Some(packet)` for legacy channels. `RawChannel::into_channel` restores it too.
`RawChannel::connection` lets a provider open additional channels back to that peer.
An older Go provider still needs a first packet to classify a raw channel.

Closing a WAMP session leaves sibling sessions and raw channels open. Close the
shared `session.connection` to close every channel. Closing the provider wakes
pending channel accepts with `None`.

## Trickle ICE transport

`Connection::offer_trickle` and `answer_trickle` return initial SDP without
waiting for gathering. Forward every `next_candidate()` result to the remote
peer's `add_candidate()`; an empty candidate marks the end of gathering. Candidates
received before `set_remote` are buffered, with a limit of 256. WAMP signaling
handles this exchange automatically using the configured candidate topics and
request IDs. The candidate subscription remains active until the connection
closes, allowing candidates that arrive after the first session opens.

`offer` and `answer` retain full-gathering behavior for callers exchanging SDP
without separate candidate signaling.

Go interoperability requires an external Go test harness:

```sh
XCONN_GO_INTEROP_BIN=/tmp/xconn-go-interop-bin cargo test -p xconn-webrtc --all-features --test go_interop -- --ignored
```

Both Rust-client/Go-provider and Go-client/Rust-provider directions have passed,
including multiple serializers and independently closing sessions. Native language
bindings can share this runtime. Browser applications require browser WebRTC APIs;
shared signaling/chunking could later be exposed through WASM.

## CI

The workspace workflow runs native transport and WAMP tests, strict Clippy,
and a transport-only build. External Go interoperability tests are ignored in
the normal suite and can be run with the command above. Reference-build
automation will arrive with the cross-language validation milestone.

MIT. Copyright XConnIO Ltd.
