# XConn browser client

A fluent browser WAMP client backed by Rust/WASM. Rust owns joining,
authentication, IDs, message layouts/validation and JSON/CBOR/MessagePack.
TypeScript owns browser WebSocket IO, Promises, request correlation, callbacks,
cancellation and session lifecycle. There are no npm runtime dependencies.

This package provides WebSocket/WSS clients and direct WebRTC/WebTransport
connections through browser APIs. Tokio, native networking, router algorithms
and C FFI are not shipped to browsers.

## Use

```js
import { connectAnonymous, Result } from "@xconnio/core";

const session = await connectAnonymous("ws://localhost:8080/ws", "realm1");
try {
    const registration = await session
        .register("com.example.echo", invocation => new Result(invocation.args, invocation.kwargs))
        .execute();

    const result = await session.call("com.example.echo").arg("hello").execute();
    console.log(result.argString(0));

    const subscription = await session
        .subscribe("com.example.news", event => console.log(event.argString(0)))
        .execute();

    await session.publish("com.example.news")
        .arg("Hello").acknowledge().excludeMe(false).execute();

    await subscription.unsubscribe();
    await registration.unregister();
} finally {
    await session.close();
}
```

`call`, `register`, `subscribe` and `publish` create inert request builders.
`execute()` sends the operation and returns a Promise. There are no parallel
`callRequest`/`registerRequest`/`subscribeRequest`/`publishRequest` entry points.
This is a fresh fluent API, not a drop-in replacement for positional xconn-js
session methods. Common connect/authentication helpers retain their names.

Builders offer `.option(key, value)` / `.options(object)`. Call and publication
builders also offer `.arg(value)`, `.args(...values)`, `.kwarg(key, value)` and
`.kwargs(object)`. Register/subscribe offer `.match(value)`, registration adds
`.invoke(value)`, and publication adds `.acknowledge()` / `.excludeMe(value)`.

```js
import { Client, JSONSerializer, TicketAuthenticator } from "@xconnio/core";
const client = new Client({
    serializer: new JSONSerializer(),
    authenticator: new TicketAuthenticator("alice", "secret", {}),
    connectTimeout: 20_000,
    closeTimeout: 10_000,
});
const session = await client.connect("wss://example.com/ws", "realm1");
```

Connections initialize WASM automatically. Direct synchronous codecs/auth metadata
and manually constructed Session objects require `await initializeWasm()` first.
Full builds default to CBOR; JSON-only builds default to JSON. Keep the generated
WASM asset beside `dist/wasm/xconn_browser.js`, or supply its URL explicitly to
`initializeWasm(wasmURL)`. Serve WASM as `application/wasm`. Use WSS on HTTPS pages.

The package is modern ESM with TypeScript declarations. Binary codecs preserve
Uint8Array and 64-bit BigInt values. Node 24's global WebSocket can run the same
client with WASM initialized explicitly from a file buffer.

## Cancellation and progressive results

```js
const controller = new AbortController();
const call = await session.call("com.example.long_task")
    .timeout(30_000)
    .signal(controller.signal)
    .stream();
try {
    for await (const result of call.results()) {
        console.log(result.args, result.progress);
    }
} finally {
    await call.close();
}
```

Register handlers may use `await invocation.sendProgress(args, kwargs)` when the
caller requests progressive results. `invocation.signal` observes cancellation.
Use `.stream({ progressiveInput: true })` to send additional chunks with
`call.send(args, kwargs)` and close input with `call.finish(args, kwargs)`. Receive
results concurrently for long producers. Existing callback helpers remain available.

Connect timeout defaults to 20 seconds and covers WASM initialization, socket
connection and authentication. Close timeout defaults to 10 seconds. Calls have
no implicit local timeout; `.timeout(milliseconds)` adds one. Zero disables the
corresponding local deadline. WAMP `.option("timeout", value)` is a distinct
router-side option.

Pending requests reject on disconnect. `session.onDisconnect(callback)` runs once;
`session.onError(callback)` observes asynchronous handler failures. Close is
idempotent. Custom base sessions extend `BaseSession` and use the raw send/receive
interface; custom serializers/authenticators retain structural interfaces. The
WebSocket-specific `WebSocketBaseSession` does not preserve the old peer-based constructor.

Invocation, Event and Result expose the same [strict payload accessors](https://github.com/xconnio/xconn-core/blob/main/docs/payload.md).
JS `argInt64` / `kwargInt64` return BigInt; typed defaults use names such as
`argStringOr(0, "fallback")`. See the [cross-language API contract](https://github.com/xconnio/xconn-core/blob/main/docs/fluent-api.md).

## Build and package

Install the browser client from npm:

```sh
npm install @xconnio/core
```

The package includes JavaScript, TypeScript declarations and the compiled WASM
binary. Application installs require no Rust toolchain or native SDK downloads.
Bundlers must preserve or explicitly relocate the WASM asset; pass its URL to
`initializeWasm(wasmURL)` when configuring a custom asset location.

Maintainer builds require Rust,
wasm32-unknown-unknown, wasm-bindgen-cli 0.2.129, Binaryen wasm-opt and Node 20.11+.

```sh
rustup target add wasm32-unknown-unknown
cargo install wasm-bindgen-cli --version 0.2.129 --locked
npm --prefix bindings/js ci
npm --prefix bindings/js run build
mkdir -p target/browser-package
(cd bindings/js && npm pack --ignore-scripts --pack-destination ../../target/browser-package)
```

Applications can install the resulting .tgz without Rust, C or native libraries.
The WASM binary is included and installation has no native build step. Bundlers
must preserve or explicitly relocate the WASM asset.

The build discovers wasm-bindgen/wasm-opt on PATH or in wasm-pack's cache;
WASM_BINDGEN_BIN / WASM_OPT_BIN override discovery. Use
`bindings/js/scripts/build.sh json` or `... codecs` for reduced builds. Artifacts
are in dist/. Serve bindings/js over HTTP and open example.html for a plain-browser
example against an existing router; the example starts no router itself.

## Validation

```sh
npm --prefix bindings/js run typecheck
npm --prefix bindings/js run lint
npm --prefix bindings/js run format:check
XCONN_REFERENCE_GO_BIN=/path/to/original-go-reference \
CHROMIUM_BIN=/path/to/chromium npm --prefix bindings/js test
```

Node and real Chromium run the same tests against the original Go router:
authentication across codecs, concurrent/reentrant RPC, PubSub, progressive
results, cancellation, custom adapters, malformed packets, lifecycle and the
shared payload parsing matrix. Without the reference only local tests run;
without Chromium browser tests are skipped. CI requires both and also installs
and tests the packed package outside the checkout. Chromium tests use an isolated
profile and --no-sandbox for the local test process.

[Size measurements](https://github.com/xconnio/xconn-core/blob/main/benchmarks/browser-wasm/README.md) include WASM,
generated glue and all high-level client modules.

[Result construction and received values](https://github.com/xconnio/xconn-core/blob/main/docs/results.md) use one payload
type for handler replies, progressive results and completed calls.

Progressive RPCs use the fluent `stream()` terminal (`Stream()`/`StreamContext()` in Go).
See the [cross-language streaming API](https://github.com/xconnio/xconn-core/blob/main/docs/streaming.md) for examples and lifecycle rules.

### Native adapter ownership

Built-in serializers and authenticators expose an idempotent `free()` method to
release their WASM storage immediately. Reusing the adapter allocates fresh native
storage lazily. A session releases its default serializer on disconnect; connection
setup releases its default authenticator on success or failure. Supplied adapters
remain caller-owned, so free them when all connections using them have finished.

## Browser QUIC connectivity through WebTransport

```typescript
import { WebTransportConnection } from "@xconnio/core";

const connection = await WebTransportConnection.connect("https://router:443/wamp");
try {
    const session = await connection.openSession("realm");
    try {
        console.log((await session.call("echo").arg("hello").execute()).args);
    } finally {
        await session.close();
    }
} finally {
    await connection.close();
}
```

Networking uses the browser's native WebTransport API; WAMP joining, codecs and
RawSocket framing use Rust/WASM. A WebTransport listener is required; arbitrary
QUIC listeners cannot accept browser connections. Browser certificate validation
is mandatory. For development certificates, pass the standard
`serverCertificateHashes` option with SHA-256 hashes obtained through a trusted
channel. WAMP sessions are independent of sibling byte streams. `openStream()`
and `acceptStream()` expose `read`, `write`, `writeEof`, and `close`; EOF is
`undefined`. `connectTimeout` and `signal` bound connection setup.

## Browser WebRTC

```typescript
import { Client, WebRTCConnection } from "@xconnio/core";

const signaling = await new Client().connect("wss://router/ws", "realm");
try {
    const peer = await WebRTCConnection.connect(signaling, "realm", {
        offerProcedure: "offer",
        remoteCandidatesTopic: "to-client",
        localCandidatesTopic: "to-provider",
    });
    try {
        console.log((await peer.call("echo").arg("hello").execute()).args);
        const channel = await peer.connection.openChannel("files");
        try {
            await channel.writeMessage(new TextEncoder().encode("file contents"));
        } finally {
            await channel.close();
        }
    } finally {
        await peer.close();
        await peer.connection.close();
    }
} finally {
    await signaling.close();
}
```

Configure STUN/TURN servers with the standard `iceServers` option.
Browser APIs own ICE, DTLS and SCTP. Rust/WASM owns WAMP state, signaling JSON,
serializer negotiation and the same bounded chunk framing used by native Rust.
The Rust signaling provider's offer/candidate contract is preserved. Early remote
candidates are buffered and applied in order; the signaling subscription remains
active for late trickle candidates until the peer connection closes.

`connectTimeout`, `openTimeout`, `handshakeTimeout` and `signal` bound setup.
Additional sessions use `peer.connection.openSession`. Closing a session leaves
its connection and sibling channels open. The signaling session remains owned by
the application. `openChannel` and `acceptChannel` support remote-initiated raw
channels. `send`/`receive` preserve text and binary packets; `writeMessage` and
`readMessage` use chunk framing for ordered, reliable channels. Reads accept an
optional AbortSignal. Do not mix packet and chunk framing on the same channel.
Incoming and outgoing queues are bounded; large producers should receive
concurrently and handle explicit queue-limit errors. Close resources explicitly.

## Publishing

The `npm release` GitHub Actions workflow builds and tests the packed package,
including browser WebRTC and WebTransport, before publishing `@xconnio/core`.
Manual runs produce a tested package artifact without publishing.

One-time setup:

1. Create a GitHub environment named `npm` with a required reviewer.
2. In the npm package settings, add a GitHub Actions trusted publisher:
   owner `xconnio`, repository `xconn-core`, workflow `npm-release.yaml`,
   environment `npm`. Allow publishing. No npm token secret is needed.

For a release, update `package.json`, `package-lock.json`, and `CHANGELOG.md`,
then merge the changes. Tag that commit with the matching package version:

```sh
git tag bindings/js/v0.1.1
git push origin bindings/js/v0.1.1
```

Approve the `npm` environment deployment after the build succeeds. The publish
job uploads the tested tarball using npm trusted publishing with provenance.
Native SDK and other language releases are independent of this tag.
