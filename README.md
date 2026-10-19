# XConn Core

Reusable connectivity technology implemented in Rust, with language bindings.

This workspace starts with `wampproto`, the sans-I/O WAMP protocol engine.
Networking runtimes, transport streams, reusable services, and language bindings
will be introduced in subsequent milestones.

## Workspace

- [`crates/wampproto`](crates/wampproto): protocol messages, codecs,
  broker, and dealer.

The protocol crate has no socket or async runtime dependency. Applications own
network I/O and drive its state machines with incoming protocol messages.

## Development

```sh
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-features
```

CI runs these checks on Linux, macOS, and Windows. Go is not required for the
Rust tests; router reference transcripts are checked in with their provenance.

## License

MIT. Copyright XConnIO Ltd.
