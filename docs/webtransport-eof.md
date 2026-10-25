# WebTransport graceful EOF dependency issue

A migration test found a race in the Quion 0.2.2 / webtrans-quion 0.6.0 native
server path: a browser can receive its complete reply but have `writeEof()` reject
with `WebTransportError: Received STOP_SENDING` during graceful stream shutdown.
The receiver's drop path sends STOP_SENDING even after EOF. Waiting for the
server's outgoing FIN acknowledgement did not reliably eliminate the failure,
so Core does not carry that workaround or suppress transport errors.

With a connected browser `WebTransportConnection` and a native raw echo server,
this ordering reproduces the failure:

```js
const stream = await connection.openStream();
try {
    await stream.write(new Uint8Array([0, 255, 42]));
    await stream.read();
    await stream.writeEof(); // May reject after the server reads EOF and drops its stream.
    await stream.read();
} finally {
    await stream.close();
}
```

Core's shared `transport_peer` example provides a raw echo server for this test.
A fix should cover both write-EOF-before-read and duplex response-before-write-EOF
ordering, preserve real cancellation errors, and avoid retaining completed stream
handles indefinitely. Carrying the entire upstream crate solely for a drop-path
patch is deferred until the dependency strategy is decided.

DeskConn's existing shell/file relay protocols use control records followed by
explicit stream cancellation rather than graceful writer-close. The app migration
tests exercise that cancellation path. This issue remains relevant to other Core
users relying on graceful WebTransport half-close, and is not claimed as fixed.
