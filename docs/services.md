# Reusable services and application ownership

Enable `xconn-core`'s optional `services` feature to use RPC bridging. The separate `crypto` feature
re-exports portable packet encryption. Ordinary WAMP clients and routers do not
need either feature.

| Responsibility | Owner |
| --- | --- |
| WAMP protocol, clients, routers, transport connections and TLS | XConn Core |
| RPC forwarding, cancellation and owned caller-session cleanup | XConn Core |
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

## Packet encryption

The optional `crypto` feature exposes `xconn_core::crypto`. The independent
[`xconn-crypto`](../crates/xconn-crypto) crate provides X25519, HKDF-SHA256, and
ChaCha20-Poly1305 packets for native and browser WASM consumers.

Packets contain `12-byte random nonce || ciphertext || 16-byte tag`. Associated
data can authenticate application context without transmitting it. Key agreement
does not authenticate a peer: the application must establish trust in the public
key. Core does not impose an encrypted-stream handshake, replay policy, or key
persistence format.
