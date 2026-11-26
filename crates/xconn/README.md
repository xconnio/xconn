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
| `router` | XConn CLI, including client commands and a configurable router with TLS listeners |
| `quic` | QUIC connections and multiplexed byte streams |
| `webtransport` | WebTransport connections and multiplexed byte streams |

`stream()` starts an owned progressive RPC call. Progressive input and results
are configured independently with `StreamOptions`. These RPCs are distinct
from transport byte streams. Close unfinished calls to cancel remote work.
See the workspace guide at `docs/streaming.md` for ownership and buffering rules.

## Client commands

[Start a local router](#router-executable). Run each pair in separate terminals.

| Start first | Then run |
| --- | --- |
| `xconn register example.echo` | `xconn call example.echo 42 --kwarg ready=true` |
| `xconn subscribe example.events` | `xconn publish example.events hello` |

The client defaults to `ws://localhost:8080/ws`, realm `realm1`, and JSON.
Change these with `--url`, `--realm`, and `--serializer`. Run a command with
`--help` to see all its options.

For structured arguments, use `--payload` in place of positional arguments
and `--kwarg`.

```sh
xconn call example.echo --payload '{"args":["42"],"kwargs":{"ready":true}}'
```

`xconn keygen` prints a cryptosign key pair. Use `xconn keygen --output-file my-key`
to create `my-key` and `my-key.pub`. Configure the router with the public key,
then pass `--authid <ID> --private-key-file my-key` to a client command.

### Scripting

`call`, `register`, and `subscribe` write JSON objects to stdout. Objects may
span multiple lines. Status messages and errors go to stderr.

| Exit code | Meaning |
| --- | --- |
| `0` | Success, help displayed, or a clean Ctrl+C shutdown of a listener |
| `1` | Command failed |
| `2` | Invalid command-line arguments |

`publish` waits for broker acknowledgement by default. With `--no-ack`, success
means the client sent the request. A failure can occur after a request succeeds
or after key files are written.

## Router executable

From the repository root, run `make build` to build `target/debug/xconn`.
To install the CLI on your PATH:

```sh
cargo install --locked --path crates/xconn --features router --bin xconn
xconn router init
xconn router validate
xconn router start
```

`init` creates `xconn/xconn.yaml`, the hex-encoded cryptosign keys
`router.key` and `router.pub`, and a local TLS certificate and key
(`tls-cert.pem` and `tls-key.pem`). The starter listeners use plaintext.
To enable TLS, reference the generated PEM files in a listener's `tls` block.
The certificate is valid for one year and covers `localhost`, `127.0.0.1`, and
`::1`. `init` refuses to overwrite existing files.

`start` and `validate` look for `xconn/xconn.yaml` and `xconn.yaml` relative
to the current directory, so they also work from inside `xconn/`. If both
files exist, use `--config <path>` to choose one. Relative TLS paths are
resolved from the config file's directory.

`validate` checks the YAML and TLS files without opening listeners. `start`
runs in the foreground and logs to stderr. At `info` level, the logs show
listener URLs, realms and roles, the config path, and shutdown. Set
`config.loglevel` in the YAML or override it with
`xconn router start --log-level debug`.

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
      certificate: tls-cert.pem
      key: tls-key.pem
```

Use an IP socket address and a PEM certificate and key. For local
development, use the files from `router init` and pass
`--ca-cert xconn/tls-cert.pem` to the client. Omitting `serializers` allows JSON,
CBOR, and MessagePack on QUIC and WebTransport.

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

### Procedure readiness

Use `session.call(uri).execute_when_available(timeout).await` when a service
may register its procedure after the caller connects. The helper retries only
`wamp.error.no_such_procedure`, with capped exponential backoff. A callee can
return the same error, so use the helper only for calls that are safe to repeat.
The timeout covers the entire operation, including the wait for a reply after
the router dispatches the call.

### Load realms from a WAMP service

An optional `realm_source` loads a realm inventory from a WAMP service on a
statically configured control realm. For example, a Deskconn deployment can
use:

```yaml
realm_source:
  control_realm: com.deskconn
  role: 'xconnio:deskconn:router'
  list: com.deskconn.realm.list
  add: com.deskconn.realm.add
  remove: com.deskconn.realm.remove
```

Set `disclose_caller: true` on the control realm. Give the source role
permission to call `list` and register `add` and `remove`, and leave it
without network credentials. Give the inventory service permission to
register `list` and call `add` and `remove`.

The list call takes no arguments and returns one positional argument
containing a list of `{realm, authid}` records. Both update procedures take
the realm and auth ID as positional arguments.

XConn opens its listeners so the inventory service can connect, then waits up
to 30 seconds for the initial inventory. It prints `Ready` after applying the
inventory. Updates during the fetch take precedence over the snapshot. A
pending removal applies only to a matching realm and auth ID.

The router holds sourced realms in memory. To enable network joins, configure
`realm_source.authentication` and grant the source role permission to call
each verification RPC. Configure static control-realm credentials for the
inventory and authentication provider so it can connect before serving RPCs.
A static identity always uses its configured credentials in the control realm.

The `wampcra` RPC receives the auth ID and realm. It returns one dictionary
with `authrole`, the derived `password` verifier, and `salt`. It may also
supply `iterations` and `key_length`. The `cryptosign` RPC receives the auth
ID, public key, and realm. It returns one dictionary with the same `authid`
and its `authrole`. The router verifies the client's proof for both methods.

List remotely assignable control-realm roles in `control_roles` and define
the sourced realms' user roles in `realm_roles`. The optional
`realm_machine_role` and `control_machine_role` templates use `{authid}` in
the role name to bind permissions to the machine from the inventory.
Permission URIs that include `{authid}` use exact matching.

The [Deskconn staging example](../../docs/examples/deskconn-router.yaml)
shows the Management RPCs, Hub role rules, and listener settings. Fill in the
public keys and TLS file paths before validating or running it.

### Router management

The router executable can expose the library's management WAMP procedures.
Set `management: true` under `config`, add a realm named `io.xconn.mgmt`, and configure
its roles and credentials in the same YAML file. For example, merge these
entries into your existing configuration:

```yaml
config:
  management: true

realms:
  - name: io.xconn.mgmt
    roles:
      - name: operator
        permissions:
          - uri: io.xconn.mgmt.realm.list
            match: exact
            allow_call: true
          - uri: io.xconn.mgmt.session.list
            match: exact
            allow_call: true
          - uri: io.xconn.mgmt.session.kill
            match: exact
            allow_call: true

authenticators:
  ticket:
    - authid: operator
      realm: io.xconn.mgmt
      role: operator
      ticket: replace-with-a-secret
```

Connect a WAMP client to `io.xconn.mgmt` and call the procedures its role
allows.

- `io.xconn.mgmt.realm.list` takes no arguments.
- `io.xconn.mgmt.session.list` takes a realm name and accepts the optional
  keyword arguments `offset` and `limit`.
- `io.xconn.mgmt.session.kill` takes a realm name and session ID and accepts
  the optional keyword argument `reason`.

To let an operator provision realms, grant call permissions for
`io.xconn.mgmt.realm.create` and `io.xconn.mgmt.realm.remove`. An existing
prefix permission for `io.xconn.mgmt.realm.` also covers both procedures.

`io.xconn.mgmt.realm.create` takes the realm name as its first argument and
a configuration dictionary as its second. The dictionary uses the same
`roles`, optional `meta_api`, and `authenticators` fields as the router
configuration. Each credential must reference the new realm and one of its
roles. To create `example.temporary`, pass this dictionary as the second
argument:

```yaml
roles:
  - name: member
    permissions:
      - uri: example.echo
        match: exact
        allow_call: true
authenticators:
  ticket:
    - authid: member
      realm: example.temporary
      role: member
      ticket: replace-with-a-secret
```

`io.xconn.mgmt.realm.remove` takes the name of a realm created through this
API and disconnects its sessions. The router keeps provisioned realms in
memory until an operator removes them or the process exits.

Management is off by default. The management realm requires authenticated
credentials and reserves the `trusted` role for the internal session. The
management realm is reachable through the configured listeners. Keep plaintext
listeners local and use TLS-protected listeners for remote access.

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
