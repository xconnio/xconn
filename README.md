# XConn Core

Reusable connectivity technology implemented in Rust, with language bindings.

The workspace contains the sans-I/O WAMP engine and its Rust networking runtime.
Reusable services, WebRTC, and language bindings will follow in separate milestones.

## Workspace

- [`crates/xconn-core`](crates/xconn-core): async clients, routers, and transport
  connections.

- [`crates/wampproto`](crates/wampproto): protocol messages, codecs,
  authentication, client joining, router acceptance, broker, and dealer.

The protocol crate has no socket or async runtime dependency. Applications own
network I/O and drive its state machines with incoming protocol messages.

## Development

```sh
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-features
```

CI runs these checks on Linux, macOS, and Windows, plus async-only and protocol-only
builds. Go is not required for the normal Rust tests.

[Progressive RPC APIs](docs/streaming.md) describe runtime behavior.

## License

MIT. Copyright XConnIO Ltd.
