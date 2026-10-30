# Changelog

## 0.1.2

- Allow WebRTC WAMP responses up to 16 MiB and advertise that receive limit
  during negotiation, preventing disconnects on multi-megabyte results.
- Keep peer send limits and bounded receive assembly enforced.

## 0.1.1

- Fix browser ICE candidate signaling arguments being sent as a nested array,
  preventing the backend from receiving trickled candidates for NAT traversal.

## 0.1.0

- Initial browser WAMP client backed by the Rust protocol engine.
- Fluent RPC and publish/subscribe APIs, including progressive calls and results.
- JSON, CBOR and MessagePack codecs with WAMP authentication and packet crypto.
- Browser WebRTC and WebTransport connections using browser networking APIs.
- Bundled WebAssembly and TypeScript declarations; no runtime npm dependencies.
