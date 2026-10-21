# Reusable services and application ownership

Enable `xconn-core`'s optional `services` feature to use RPC bridging,
attachment supervision, and endpoint discovery. The separate `crypto` feature
re-exports portable packet encryption. Ordinary WAMP clients and routers do not
need either feature.

| Responsibility | Owner |
| --- | --- |
| WAMP protocol, clients, routers, transport connections and TLS | XConn Core |
| RPC forwarding, cancellation and owned caller-session cleanup | XConn Core |
| Attachment retry/backoff and detachment supervision | XConn Core |
| Endpoint directory, validation, ownership and presence cleanup | XConn Core |
| Packet encryption and key-agreement primitives | XConn Core |
| Enrollment protocol and credential issuance/revocation | Application or service provider |
| Credential storage, machine identifiers, roles and permissions | Application |
| Shell, files, VPN, UI and operating-system integration | Application |

Core services impose no product-specific procedure prefix, identity format,
credential files, enrollment server, or local socket architecture.

## RPC bridges

`async_::services::bridge::Bridge` owns forwarding registrations. It can forward
to a shared target session or use an application-supplied factory that creates
an owned target session for each disclosed caller. It preserves payloads,
progressive inputs/results, and WAMP application errors, and propagates call
cancellation.

The application owns the source and shared target sessions. The bridge owns
sessions created by its factory and closes them when their callers leave.
Factory mode requires caller disclosure and `wamp.session.on_leave` meta events.
Explicit `close` waits for cleanup. Dropping the bridge schedules cleanup on its
original Tokio runtime, which must remain alive for that work to complete.

A forwarded progressive call remains correlated until its source invocation
finishes writing the final reply or is interrupted. Trailing input chunks cannot
reopen an already-completed backend invocation. Tests cover this race, independent
input/results options, and cancellation of the backend handler.

## Attachment lifecycle

`async_::services::attachment::supervise` runs application-provided attachment
cycles with bounded exponential retry backoff. A disconnected established
attachment resets backoff. A detached outcome returns control to the application
so it can apply enrollment and credential policy.

Dropping the supervisor future cancels its current cycle or retry wait. A cycle
must own and release its resources when cancelled. The supervisor does not issue
credentials or prescribe an enrollment RPC or storage format.

## Endpoint discovery

`DiscoveryService::start(session, prefix, policy)` hosts an in-memory directory
over ordinary WAMP RPC. It requires an explicit authorization callback. Enable
caller disclosure and session meta events on the hosting realm; realm permissions
remain the application's responsibility.

An endpoint is a service, application, or device. A record contains:

- `id`: nonempty string.
- `addresses`: array of connection hints.
- `capabilities`: string array.
- `metadata`: dictionary visible to authorized readers; omit credentials.

| Procedure suffix | Positional arguments | First result argument |
| --- | --- | --- |
| `.advertise` | Endpoint record | Normalized endpoint record |
| `.get` | Endpoint ID | Endpoint record |
| `.list` | None | Endpoints sorted by ID |
| `.withdraw` | Endpoint ID | Removed endpoint record |

Every operation applies host policy, including filtering list results per caller.
Another authid cannot overwrite or withdraw an existing record. Advertisements
belong to the announcing session and disappear when it leaves. Refreshing a
record from a new session of the same identity transfers its session ownership.

`DiscoveryClient` provides the Rust client API. Discovery does not authenticate
an advertised address or connect automatically. The directory currently supports
queries, not pushed availability subscriptions or persistent storage.

Run the example host:

```sh
cargo run -p xconn-core --features services --example discovery
```

It prints a WebSocket URI and serves realm `example` with prefix `example.peers`.
Its example policy permits public discovery/advertising; production hosts supply
their own authorization policy.

## Packet encryption

The optional `crypto` feature exposes `xconn_core::crypto`. The independent
[`xconn-crypto`](../crates/xconn-crypto) crate provides X25519, HKDF-SHA256, and
ChaCha20-Poly1305 packets for native and browser WASM consumers.

Packets contain `12-byte random nonce || ciphertext || 16-byte tag`. Associated
data can authenticate application context without transmitting it. Key agreement
does not authenticate a peer: the application must establish trust in the public
key. Core does not impose an encrypted-stream handshake, replay policy, or key
persistence format.
