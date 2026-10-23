# Shared native binding runtime

This crate handles host values, callbacks, scheduling and ownership for Python,
Go and Dart. WAMP state and networking remain in `xconn-core`/`wampproto`; WebRTC
signaling and transport remain in `xconn-webrtc`.

| Module | Responsibility |
| --- | --- |
| `lib.rs` | Public entry points, shared executor/IDs and operation routing |
| `jobs.rs` | Pending jobs, readiness, completion notification and cancellation |
| `resources.rs` | Resource handles, transfer ownership and compensating cleanup |
| `callbacks.rs` | Host callbacks and authentication/serializer/peer adapters |
| `client.rs` | Client/session operations, registrations, subscriptions and calls |
| `router.rs` | Router realms, authorization policy and listeners |
| `values.rs` | Request-field conversion and result/error representations |
| `protocol/` | Internal adapters over the shared protocol implementation |
| `webrtc.rs` | Optional WebRTC resource adapters |

Only the existing crate entry points are public; the new modules are internal.
An `Output` owns newly created resources until its result is claimed. Cancelling
or dropping an unclaimed output closes its handles or compensates a completed
registration/subscription. Resource-producing operations keep this ownership path
even when their host job is cancelled. Keep that classification in `jobs.rs` in
sync with added operations.

Host callbacks run outside resource-map locks. Shared handle IDs and resource/job
maps remain process-scoped. This module split does not add a protocol API or a
second engine.

The ownership tests live with jobs and client operations. Run them and optional
feature checks with:

```sh
cargo test --locked -p xconn-binding-runtime --all-features
cargo check --locked -p xconn-binding-runtime --no-default-features
cargo clippy --locked -p xconn-binding-runtime --all-features --all-targets -- -D warnings
```

Integration coverage comes from `ci/test-bindings.sh`, `ci/test-dart.sh` and the
[packaged consumer checks](../../docs/releases.md). The browser WASM binding calls
`wampproto` directly and does not use this native runtime.
