# Python client binding

Python provides blocking and asyncio clients. Routing and server listeners are
provided by Rust/Go or an external WAMP router.

```python
from xconn_core import Client

with Client().connect("ws://127.0.0.1:8080/ws", "realm1") as session:
    result = session.call("example.echo").arg("hello").execute()
    print(result.args)
```

```python
from xconn_core import AsyncClient


async def main():
    async with await AsyncClient().connect("ws://127.0.0.1:8080/ws", "realm1") as session:
        result = await session.call("example.echo").arg("hello").execute()
        print(result.args)
```

The package includes its own `xconn_core.auth`, `xconn_core.serializers` and custom-peer
message views. Built-in JSON/CBOR/MessagePack codecs and anonymous/ticket/CRA/
cryptosign authentication execute in Rust. One native PyO3 extension handles all
operations; no standalone Wampproto Python package is required. Client/session
operations use fluent `call`/`register`/`subscribe`/`publish` builders with an
explicit `execute()` terminal. Application callbacks, progress and custom
transport/codec adapters remain available.

`App`/`Component` helpers remain optional client composition APIs. Embedded router,
server, `start_router` and in-memory router conveniences have been removed.

## Installation

```sh
uv add xconn-core
# Or install into an existing environment:
uv pip install xconn-core
```

The package imports as `xconn_core`. A matching prebuilt ABI3 wheel contains the
native extension; users need no Rust toolchain or separate SDK. Wheels support
CPython 3.10 and newer. Source builds require Rust and the platform build tools.
Linux wheels must be built in manylinux containers and repaired to bundle
external TLS libraries before publication. See
[binding packaging](https://github.com/xconnio/xconn-core/blob/main/docs/releases.md).

Application configuration helpers have optional dependencies:

```sh
uv add 'xconn-core[framework]'
```

The optional Cap'n Proto serializer is distributed separately from GitHub.
Its compatibility import remains available after installing it explicitly:

```sh
uv pip install 'git+https://github.com/xconnio/wampproto-messages-capnproto@main#subdirectory=python'
```

## Development and release builds

From the workspace root, use `uv` to build the source archive and a wheel from
that archive, then install the wheel in an isolated environment:

```sh
uv build bindings/python --out-dir target/python-dist
uv venv target/python-release-venv
uv pip install --python target/python-release-venv/bin/python target/python-dist/xconn_core-*.whl
uv pip install --python target/python-release-venv/bin/python pytest pytest-asyncio ruff==0.17.1
cargo build -p xconn-core --features router --bin xconn-router
target/python-release-venv/bin/python -m pytest bindings/python/tests
```

The manually triggered `Python release artifacts` workflow uses `uv` to build
manylinux 2.28 wheels for Linux x86-64/ARM64 and a macOS ARM64 wheel. It repairs
Linux wheels, checks metadata, tests the installed extension, and uploads artifacts
without publishing. Windows, Intel macOS and other targets are not in this matrix.

Before publishing, download those artifacts, keep one source archive, run
`uvx twine check target/python-dist/*`, and test the installed wheels. A local
Linux wheel is not a replacement for a repaired manylinux release wheel.
`uv publish target/python-dist/*` is a separate maintainer action requiring
PyPI credentials; building and testing do not publish anything.

The Rust executable is used only by test fixtures. Client tests also exercise the
independent original Go router when `XCONN_REFERENCE_GO_BIN` is configured.
The Python CI workflow runs inherited lint rules and Python tests, including
streaming against a separate Go binding process. Python callbacks execute outside Rust locks;
asyncio completions wake the event loop without polling or one waiting native
thread per operation. Explicitly close sessions.

Fluent requests use `execute()` exclusively: for example,
`session.call("echo").arg("hello").execute()`, or
`await session.call("echo").arg("hello").execute()` with AsyncSession.
The former `do()` spelling has been removed. See the
[shared execution contract](https://github.com/xconnio/xconn-core/blob/main/docs/fluent-api.md).

Invocation, Event and Result share [strict typed payload accessors](https://github.com/xconnio/xconn-core/blob/main/docs/payload.md)
with explicit defaults and consistent numeric/binary conversion rules.

[Result construction and received values](https://github.com/xconnio/xconn-core/blob/main/docs/results.md) use one payload
type for handler replies, progressive results and completed calls.

Progressive RPCs use the fluent `stream()` terminal (`Stream()`/`StreamContext()` in Go).
See the [cross-language streaming API](https://github.com/xconnio/xconn-core/blob/main/docs/streaming.md) for examples and lifecycle rules.

## Progressive results

```python
with session.call("example.process").arg("input").stream(timeout=30) as call:
    for result in call.results():
        print(result.args, result.progress)
```

With asyncio, use `async with await ...stream()` and `async for`. Enable input
chunks with `stream(progressive_input=True)`, then `send(...)` and `finish(...)`.

## QUIC connections

```python
from xconn_core.quic import QUICConnection

with QUICConnection.connect("host:443", server_name="host") as connection:
    session = connection.open_session("realm")
    try:
        print(session.call("echo").arg("hello").execute().args)
    finally:
        session.close()
```

TLS uses system roots, or an explicit DER `certificate`. `AsyncQUICConnection`
provides the same operations with `await` and `async with`. Connections own their
sessions and byte streams. Closing a session leaves the connection usable.
`open_stream()`/`accept_stream()` return independent byte streams with `read`,
`write`, `write_all`, `write_eof`, and `close`.

## WebTransport

`xconn_core.webtransport.WebTransportConnection` and
`AsyncWebTransportConnection` provide the same session and byte-stream operations
as QUIC. Connect using an HTTPS URL:

```python
from xconn_core.webtransport import AsyncWebTransportConnection

async with await AsyncWebTransportConnection.connect("https://host:443/wamp") as connection:
    session = await connection.open_session("realm")
    try:
        print((await session.call("echo").arg("hello").execute()).args)
    finally:
        await session.close()
```

Rust verifies the server hostname and certificate chain using system roots, or
an explicitly supplied DER `certificate`. Certificate verification is mandatory.

## WebRTC

```python
from xconn_core import AsyncClient
from xconn_core.webrtc import WebRTCOptions, connect_async

signaling = await AsyncClient().connect("wss://router/ws", "realm")
try:
    peer = await connect_async(
        signaling,
        "realm",
        WebRTCOptions(
            offer_procedure="offer",
            remote_candidates_topic="to-client",
            local_candidates_topic="to-provider",
        ),
    )
    async with peer.connection as connection:
        try:
            print((await peer.call("echo").arg("hello").execute()).args)
            async with await connection.open_channel("files") as channel:
                await channel.write_message(b"file contents")
        finally:
            await peer.close()
finally:
    await signaling.close()
```

`connect` is the blocking equivalent. ICE servers and connection, session-open,
and handshake deadlines are configured through `WebRTCOptions`. Rust handles
trickle ICE, candidate ordering, signaling, and channel framing. A connection can
open additional WAMP sessions and accept remote channels. Closing a WAMP session
leaves its connection and other channels open; close the connection explicitly.
Raw `send`/`receive` preserve text versus binary packets. `write_message` and
`read_message` use the shared chunk framing for larger messages; use the same
framing mode at both ends. Packet channels may be configured as ordered or
unreliable; chunked messages require ordered, reliable channels.
