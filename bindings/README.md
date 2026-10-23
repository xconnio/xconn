# XConn Core language bindings

Bindings share the Rust protocol engine. Native adapters delegate networking,
routing, cryptography, and lifecycle management to the shared binding runtime.

| Binding | Package | Supported APIs |
| --- | --- | --- |
| [Dart / Flutter](dart) | `xconn_core` | Future-based clients |
| [Python](python) | `xconn-core` / `xconn_core` | Blocking and asyncio clients |
| [Go](go) | `github.com/xconnio/xconn-core/bindings/go` | Client, router/server, WebRTC |

`runtime` implements shared handle, job, and callback ownership. `ffi` exposes
owned buffers through a C ABI. Build the [native SDK](../docs/native-install.md)
for native consumers. Public package publication is a separate release step.
