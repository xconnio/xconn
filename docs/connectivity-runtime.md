# Connectivity runtime: extraction plan

The first goal is to share connection lifecycle and forwarding across applications,
without requiring a DeskConn account service, realm layout, or wire protocol.
Ordinary WAMP clients and routers keep their existing APIs.

## Source audit

This audit compares XConn `c102a26`, DeskConn's migration through `10e8d68`,
and Hub main `f199ef5` (the checkout and module are still named
`deskconn-router`). Hub currently imports `deskconn/relay`; its account/database
adapter is separate from the relay implementation.

| Current source | Mechanism | Application policy | Destination |
| --- | --- | --- | --- |
| DeskConn `internal/connectivity/app.go` | Forward registrations and preserve caller identity | Local Unix socket, exposed procedures, registration options | Existing Rust `Bridge`; retain a small application adapter |
| `cloud.go`: attachment loop | Retry, reconnect, restore services, release resources | Credentials-file polling, device/cloud realms, enrollment and detach messages | Existing supervisor plus a reusable attachment lifecycle; keep enrollment/storage adapters in DeskConn |
| `cloud.go`: initial key snapshot and events | Subscribe before snapshot, buffer updates, detect overflow | Principal schema, key-list/add/remove RPCs and topics, disk persistence | Keep the current protocol adapter; extract synchronization only with a second concrete use case |
| `auth.go`, `credentials.go`, `machineid_*.go` | Verify authentication material | Principal lookup, roles, credential locations, host identity choice | Rust authentication primitives plus application identity/storage adapters |
| `standalone.go` | Own listeners, sessions, bridges and direct connections until shutdown | Which transports to expose, realm policy, configured users and keys | Reusable service-host lifecycle; application supplies configuration and identity policy |
| `relay.go` | Forward accepted QUIC streams and WebRTC channels | Local socket paths and DeskConn text/binary framing | Generic stream forwarding in Rust; preserve legacy framing in an application adapter |
| DeskConn `relay/relay.go` | Track live service connections and open destination streams | One machine per realm, role checks, `realm/op/path/recursive` request header | Generic authorized stream broker in Rust; keep existing request parser as a compatibility adapter |
| DeskConn `relay/router.go` | Start listeners, add/remove realms, accept authenticated connections | Cloud realm, machine-role prefix, procedure permissions, management RPCs | Existing router/listeners plus shared host/broker lifecycle; retain DeskConn realm policy |
| Hub `accountservice.go`, `db.go` | Supply authentication and authorization decisions | Account RPCs, database schema, identities, machine inventory | Hub |

Generic does not mean accepting DeskConn procedure names as configuration for an
otherwise unchanged DeskConn runtime. A generic operation needs explicit inputs,
resource ownership, authorization decisions and transport-independent behavior.

## Existing XConn building blocks

The optional Rust `services` feature already provides:

- `attachment::supervise`: bounded exponential retry and detachment outcomes.
- `bridge::Bridge`: RPC forwarding, progressive calls/results, cancellation,
  and owned per-caller sessions.
- `discovery`: an optional endpoint directory with explicit authorization and
  presence cleanup. Neither migration currently requires adopting its protocol.

Go already exposes these services. The main gap is composing them with connection
and stream ownership, rather than implementing another WAMP or transport engine.
Session multiplexing on one QUIC connection must remain possible: DeskConn opens
both device and control sessions on that connection.

## First slice: attachment and RPC exposure

`service_attachment` is a runnable Rust example using the existing supervisor,
client, router and bridge. A backend keeps its local service session alive while
its upstream connection is replaced. Every successful attachment reinstalls the
forwarding registrations. Cancelling the attachment releases its upstream bridge
and session while leaving the local backend available.

```sh
# Terminal 1: upstream router, realm "example", localhost only.
cargo run -p xconn-core --features services --example service_attachment -- router

# Terminal 2: local echo service attached to that router.
cargo run -p xconn-core --features services --example service_attachment -- backend

# Terminal 3: call the forwarded service.
cargo run -p xconn-core --features services --example service_attachment -- call
```

Restart the router and repeat the call. The backend stays running and restores
its registration automatically. Stop each process with Ctrl+C. The example uses
anonymous loopback connections; authentication remains an explicit application
choice, not a production default for a future service host.

The example's integration test exercises a real WebSocket connection, loss and
replacement of the upstream sessions, restoration of RPC exposure, and
cancellation cleanup.
Existing supervisor and bridge tests cover backoff, caller isolation, progressive
calls/results, and cancellation propagation.

The initial example established a baseline using existing services. It now uses
`attachment::Attachment` to own the upstream sessions and bridges of each cycle.
Tests also exercise two sessions on one QUIC connection, loss of either session,
explicit closure, handle drop, and cancellation of a cleanup wait. A caller-supplied
shutdown callback owns the shared connection and additional workers. Partial
initialization rollback stays with the cycle. No binding changes are introduced.

## Stream broker prototype

`stream_broker::StreamBroker` now supplies the shared destination registry,
per-open authorization, replacement-safe registration ownership, and opaque byte
forwarding. QUIC integration tests use identities supplied by the authenticated
listener and relay between separate client and backend connections. Bounded
buffers, request half-close/reply draining, denied opens, and cancellation are
covered independently of DeskConn framing.

This is not yet wired into either Go application. The existing header parser can
request an authorized stream, write the original header, then use the generic
forwarder. Applications still own admission policy, connection shutdown and
forwarding tasks. No new discovery or enrollment protocol is required.

## Remaining integration work

1. Apply the attachment prototype to DeskConn's full initialization/shutdown
   sequence, including its WebRTC provider and authorization subscriptions.
   Connection/session setup and partial initialization rollback stay explicit;
   avoid one callback per protocol detail across FFI.
2. Connect Hub's existing stream-header adapter to the broker while preserving
   its current destination admission and authorization decisions.
3. Compose listener ownership, attachment, RPC bridging and stream forwarding
   into a service host. Reuse the existing WebRTC provider for direct connections;
   signaling must preserve existing peers' compatibility.
4. Expose the proven runtime through Go first. Adapt DeskConn and Hub separately,
   measuring removed orchestration and preserving their existing contracts.
   Extend other language bindings after this path is verified.

Do not rewrite both applications or introduce a new discovery/enrollment protocol
in the same change. Keep the working DeskConn migration as a compatibility
reference until the replacement covers attachment, authentication, forwarding,
streaming, reconnection and shutdown end to end.
