# xconn-webrtc

Reusable native Rust WebRTC data transport.

- `transport`: offer/answer, ICE candidates, ordered data channels, bounded
  Go-compatible message chunking, raw channels and connection cleanup.

```sh
cargo test -p xconn-webrtc --all-features
cargo clippy -p xconn-webrtc --all-features --all-targets -- -D warnings
```

Run these commands from the xconn-core workspace root. Normal xconn-core package
builds do not depend on this crate or the WebRTC stack. Tests establish real local
WebRTC connections and exchange multi-chunk payloads and raw packets.

This initial native adapter uses the Tokio-based webrtc 0.14 API. WebRTC backend
upgrades can be made behind this library's API.

## Raw channels

Use `DataChannel::send_packet`/`read_packet` for raw packets (at most 65,535 bytes)
or `write_message`/`read_message` for Go-compatible chunked messages. Keep both ends
on the same framing mode. Raw channel options use `RTCDataChannelInit`; ordered,
reliable delivery is required for chunked messages. Use
`Connection::open_raw_channel` and `Connection::accept_channel` to exchange
channels. Explicit raw channels use the `xconn.raw` protocol.

## Trickle ICE transport

`Connection::offer_trickle` and `answer_trickle` return initial SDP without
waiting for gathering. Forward every `next_candidate()` result to the remote
peer's `add_candidate()`; an empty candidate marks the end of gathering. Candidates
received before `set_remote` are buffered, with a limit of 256.

`offer` and `answer` retain full-gathering behavior for callers exchanging SDP
without separate candidate signaling.

## CI

The workspace workflow runs native transport tests and strict Clippy.

MIT. Copyright XConnIO Ltd.
