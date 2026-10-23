# XConn Core language bindings

Bindings share the Rust protocol engine. Native adapters delegate networking,
routing, cryptography, and lifecycle management to the shared binding runtime.

| Binding | Package | Supported APIs |
| --- | --- | --- |
| [Browser JS / TypeScript](js) | `@xconnio/core` | Promise-based WebSocket clients and WASM |
| [Dart](dart) | `xconn_core` | Future-based clients |
| [Python](python) | `xconn-core` / `xconn_core` | Blocking and asyncio clients |
| [Go](go) | `github.com/xconnio/xconn-core/bindings/go` | Client, router/server, WebRTC |

`runtime` implements shared handle, job, and callback ownership. `ffi` exposes
owned buffers through a C ABI. Build the [native SDK](../docs/native-install.md)
for native consumers. Public package publication is a separate release step.

Browser networking uses native WebSocket APIs. Its WASM adapter contains the
sans-I/O protocol engine, not the native networking runtime. Python and Dart are
client-only; Rust and Go also expose routers and servers.
