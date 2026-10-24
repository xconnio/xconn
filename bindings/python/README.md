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

Install the matching prebuilt ABI3 wheel with `python -m pip install /path/to/xconn_core-*.whl`.
The wheel contains the native extension; users need no Rust toolchain or separate
SDK. Linux release wheels are built in manylinux containers and repaired to bundle
external TLS libraries. See [binding packaging](../../docs/releases.md).

For source development, build from the workspace root:

```sh
maturin develop --manifest-path bindings/python/Cargo.toml
cargo build -p xconn-core --features router --bin xconn-router
pytest bindings/python/tests
```

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
[shared execution contract](../../docs/fluent-api.md).

Invocation, Event and Result share [strict typed payload accessors](../../docs/payload.md)
with explicit defaults and consistent numeric/binary conversion rules.

[Result construction and received values](../../docs/results.md) use one payload
type for handler replies, progressive results and completed calls.

Progressive RPCs use the fluent `stream()` terminal (`Stream()`/`StreamContext()` in Go).
See the [cross-language streaming API](../../docs/streaming.md) for examples and lifecycle rules.

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
