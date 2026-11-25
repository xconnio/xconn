# XConn for Rust

Async and blocking WAMP clients and routers using the sans-I/O `wampproto`
engine. Network I/O and execution live here; protocol state and serialization
remain in `wampproto`.

## Client

```rust,no_run
use xconn::async_::client::connect_anonymous;

#[tokio::main]
async fn main() -> Result<(), xconn::Error> {
    let session = connect_anonymous("ws://localhost:8080/ws", "realm1").await?;
    let result = session.call("example.echo").arg("hello").execute().await?;
    println!("{:?}", result.args);
    session.leave().await
}
```

## Router

```rust,no_run
use xconn::{JSONSerializerSpec, Result as RpcResult};
use xconn::async_::router::{RealmConfig, Router};

# async fn example() -> Result<(), xconn::Error> {
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
| `services` | Generic RPC bridges, endpoint discovery, and attachment supervision |
| `crypto` | Re-export portable packet encryption as `xconn::crypto` |
| `async` | Async client, router, WebSocket and RawSocket transports |
| `sync` | Blocking client and router adapters; enabled by default with `async` |
| `router` | Configurable `xconn router` command, including TLS listeners |
| `quic` | QUIC connections and multiplexed byte streams |
| `webtransport` | WebTransport connections and multiplexed byte streams |

`stream()` starts an owned progressive RPC call. Progressive input and results
are configured independently with `StreamOptions`. These RPCs are distinct
from transport byte streams. Close unfinished calls to cancel remote work.
See the workspace guide at `docs/streaming.md` for ownership and buffering rules.

## Router executable

From the repository root, run `make build` to build `target/debug/xconn`.
To install the CLI on your PATH:

```sh
cargo install --locked --path crates/xconn --features router --bin xconn
xconn router init
xconn router validate
xconn router start
```

`init` creates `xconn.yaml` in the current directory and refuses to overwrite
it. `validate` checks the configuration and TLS files without opening listeners.
`start` runs in the foreground. Use `--config <path>` with any of the three
commands to select another file.

To use an existing router configuration, run
`xconn router start --config .nxt/config.yaml`.

Add a QUIC listener for WAMP and RPC streams over UDP:

```yaml
transports:
  - type: quic
    listener: udp
    address: 127.0.0.1:8081
    serializers: [cbor, msgpack]
    tls:
      certificate: cert.pem
      key: key.pem
```

Use an IP socket address. Each WAMP stream proposes a serializer, and the
listener rejects proposals outside its `serializers` list. Omitting the list
allows JSON, CBOR, and MessagePack on QUIC and WebTransport. QUIC requires a
PEM certificate and private key. For local development, use a
self-signed certificate for the hostname you dial and trust that certificate
in the client.

WebTransport also uses a UDP listener and negotiates a serializer per WAMP
stream. The default CONNECT path is `/wamp`, which can be changed with `path`.

```yaml
transports:
  - type: webtransport
    listener: udp
    address: 127.0.0.1:8082
    serializers: [cbor]
    path: /wamp
    cert_hash_address: 127.0.0.1:8083
```

With no `tls` configuration, WebTransport uses a short-lived self-signed
certificate generated on startup. The optional `cert_hash_address` serves
`GET /wt-cert-hash` over HTTP with a base64 SHA-256 fingerprint in the JSON
`hash` field. Serve this endpoint to browsers through a trusted HTTPS reverse
proxy so they can use the fingerprint with `serverCertificateHashes`. Restart
the router before its generated certificate expires. For a CA-issued
certificate, supply `tls` as in the QUIC example and omit `cert_hash_address`.

For Bash completion with a Cargo-installed CLI, add
`source <(xconn completions bash)` to your `~/.bashrc`.

### Snap package

From a Git checkout on a Linux Snapcraft host, run `snapcraft pack` to build
the strictly confined Core 26 snap. A matching release tag such as `v0.6.6`
produces version `0.6.6`. Other commits append `+<first-six-commit-characters>`
to the Cargo version. Install the locally built file with
`sudo snap install --dangerous ./xconn_<version>_<arch>.snap`. The snap exposes
the same `xconn router` commands and includes Bash completion. Run the router
commands from a non-hidden directory in your home directory so the snap can
read and write the configuration.

## Verification

```sh
cargo test --locked -p xconn --all-features
```

Tests requiring an external Go reference are explicitly ignored in the normal
suite. They accept `XCONN_REFERENCE_GO_BIN` or `XCONN_GO_INTEROP_BIN`; Go reference
build automation will arrive with the cross-language validation milestone.
The manual WebTransport Retry test documents a known upstream backend issue;
normal builds use the tested Retry-disabled configuration.

MIT. Copyright XConnIO Ltd.
