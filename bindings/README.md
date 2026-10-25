# XConn Core language bindings

Bindings share the Rust protocol engine. Native adapters delegate networking,
routing, cryptography, and lifecycle management to the shared binding runtime.

| Binding | Package | Supported APIs |
| --- | --- | --- |
| [Browser JS / TypeScript](js) | `@xconnio/core` | WebSocket, WebTransport and WebRTC clients through WASM |
| [Dart](dart) | `xconn_core` | Future-based clients, QUIC, WebTransport and WebRTC |
| [Python](python) | `xconn-core` / `xconn_core` | Blocking/asyncio clients, QUIC, WebTransport and WebRTC |
| [Go](go) | `github.com/xconnio/xconn-core/bindings/go` | Client, router/server, QUIC, WebTransport and WebRTC |

`runtime` implements shared handle, job, and callback ownership. `ffi` exposes
owned buffers through a C ABI. Build the [native SDK](../docs/native-install.md)
for Go and Dart. Python wheels carry their own PyO3 extension. Public package
publication is a separate release step.

Browser networking uses native WebSocket, WebTransport and WebRTC APIs. Its WASM
adapter contains the sans-I/O protocol engine, not the native networking runtime.
Python and Dart are client-only; Rust and Go also expose routers and servers.

## Transport support

| Platform | WebSocket/WSS | Native QUIC | HTTP/3 WebTransport | WebRTC |
| --- | --- | --- | --- | --- |
| Rust | Yes | Yes | Yes | Yes |
| Go | Yes | Yes | Yes | Yes |
| Python | Yes | Yes | Yes | Yes |
| Dart / Flutter native | Yes | Yes | Yes | Yes |
| Browser JS/TS | Yes | No direct socket API | Yes | Yes |

Browser QUIC connectivity requires a WebTransport listener; native QUIC and
WebTransport are different wire protocols. Dart's FFI package targets native
platforms, not Flutter Web. Native platform libraries still need to be built or
packaged for the deployment target. Browser availability is checked at runtime.

Connection ownership is explicit: a connection can contain several WAMP sessions
and independent streams/channels. Closing one session leaves siblings open;
closing the connection closes all of them. Signaling sessions are borrowed and
remain application-owned. Configure your own ICE servers for NAT traversal.
