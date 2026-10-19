# Rust examples

Run these from the workspace root against a router serving `realm1`:

```sh
cargo run -p xconn-core --example async_example
cargo run -p xconn-core --example sync_example
```

The `streaming_peer` example is an integration-test peer for cross-language
progressive RPC validation.
