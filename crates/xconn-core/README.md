# XConn Core for Rust

Async and blocking WAMP clients and routers using the sans-I/O `wampproto`
engine. Network I/O and execution live here; protocol state and serialization
remain in `wampproto`.

## Client

```rust,no_run
use xconn_core::async_::client::connect_anonymous;

#[tokio::main]
async fn main() -> Result<(), xconn_core::Error> {
    let session = connect_anonymous("ws://localhost:8080/ws", "realm1").await?;
    let result = session.call("example.echo").arg("hello").execute().await?;
    println!("{:?}", result.args);
    session.leave().await
}
```

## Router

```rust,no_run
use xconn_core::{JSONSerializerSpec, Result as RpcResult};
use xconn_core::async_::router::{RealmConfig, Router};

# async fn example() -> Result<(), xconn_core::Error> {
let router = Router::new();
router.add_realm("realm1", RealmConfig::default())?;
let session = router.connect_internal(
    "realm1", "backend", "trusted", Box::new(JSONSerializerSpec),
).await?;
session.register("example.echo", |invocation| async move {
    RpcResult::new(invocation.args, invocation.kwargs)
}).execute().await?;
let result = session.call("example.echo").arg("hello").execute().await?;
assert!(result.error.is_none());
session.leave().await?;
router.close();
# Ok(())
# }
```

The `sync` module exposes blocking APIs backed by the same runtime. Events,
invocations, and RPC results share the `Payload` parsing helpers.

## Features

| Feature | Purpose |
| --- | --- |
| `async` | Async client, router, WebSocket and RawSocket transports |
| `sync` | Blocking client and router adapters; enabled by default with `async` |

`stream()` starts an owned progressive RPC call. Progressive input and results
are configured independently with `StreamOptions`. These RPCs are distinct
from transport byte streams. Close unfinished calls to cancel remote work.
See the workspace guide at `docs/streaming.md` for ownership and buffering rules.

## Verification

```sh
cargo test --locked -p xconn-core --all-features
```

MIT. Copyright XConnIO Ltd.
