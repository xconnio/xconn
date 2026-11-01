# Streams as RPC arguments and results

A byte stream occupies an ordinary positional argument. An upload can carry a
filename at index 0 and its content stream at index 1. Downloads and interactive
endpoints return streams in result arguments. WAMP selects and authorizes the
procedure; the native transport carries the bytes.

Enable Rust's optional `rpc-streams` feature together with `quic`, `webtransport`
or [yamux](yamux.md). In-process sessions also support the extension. The native
FFI SDK enables RPC streams by default; Go requires an SDK built with this support.
Existing WAMP and progressive RPC APIs retain their behavior.

## Upload

```go
file, err := os.Open("report.pdf")
if err != nil {
    return err
}

result := session.Call("app.upload").
    Arg("report.pdf").
    Arg(xconn.ReadStream(file)).
    ExecuteContext(ctx)
return result.Err
```

Register an ordinary procedure. `saveUpload` is application code responsible for
choosing the destination, enforcing access policy and copying the content.

```go
session.Register("app.upload", func(ctx context.Context, inv *xconn.Invocation) *xconn.Result {
    name, err := inv.ArgString(0)
    if err != nil {
        return xconn.NewResultError("app.invalid_argument", err.Error())
    }

    stream, err := inv.ArgStream(1)
    if err != nil {
        return xconn.NewResultError("app.invalid_argument", err.Error())
    }
    defer stream.Close()

    if err := saveUpload(ctx, name, stream); err != nil {
        return xconn.NewResultError("app.upload_failed", err.Error())
    }
    return xconn.NewResult()
}).ExecuteContext(ctx)
```

## Download

The handler returns `xconn.NewResult(xconn.ReadStream(file))`, after opening the
file under its application policy. It transfers ownership of that file to XConn;
it must not defer closing it inside the handler.

```go
result := session.Call("app.download").Arg("report.pdf").ExecuteContext(ctx)
if result.Err != nil {
    return result.Err
}

stream, err := result.ArgStream(0)
if err != nil {
    return err
}
defer stream.Close()

_, err = io.Copy(destination, stream)
return err
```

The RPC result becomes available before the transfer completes. Returning from
the handler does not close its result streams.

## Bidirectional endpoints

Return `xconn.NewResult(xconn.DuplexStream(endpoint))` from a procedure such as
`app.shell`. The endpoint implements `io.ReadWriteCloser`, for example a PTY or
socket. The caller obtains it with `result.ArgStream(0)` and uses ordinary reads
and writes. Stream closure closes the supplied endpoint; the application's PTY
adapter remains responsible for terminating/reaping its process. Socket sources
implementing `CloseWrite` retain half-close behavior.

## Ownership

- `ReadStream` takes an `io.ReadCloser`; `DuplexStream` takes an
  `io.ReadWriteCloser`. Their I/O must unblock when `Close` is called.
- Executing a request transfers ownership of accepted sources, including cleanup
  on RPC rejection. Returning sources in a successful reply transfers ownership
  when the runtime prepares that reply. A `StreamSource` can be transferred once.
- Plain `io.ReadCloser` arguments/results are also treated as readable sources.
  Use `DuplexStream` explicitly when both directions are needed.
- `ArgStream` uses the same positional index as `ArgString` and other helpers.
  An incoming endpoint may be opened once. Close it when finished.
- In Go, the execution context controls received result streams even after the RPC has
  returned. Input streams end with their invocation. When a handler returns
  streams, its context stays alive until those transfers close, so context-bound
  shell processes survive the final RPC reply. Consumer or transport closure
  ends those transfers and then cancels the handler context.
- An unclaimed association expires after 30 seconds. Established transfers have
  no fixed duration limit. Associations and pending streams are bounded per realm.

## Authorization and transport

The router binds references only after procedure authorization and dealer
selection. Each reference is restricted to the selected caller/callee session
pair in that realm. Knowing another call's reference does not authorize a third
session to claim it. Input associations end on invocation completion/interruption;
result associations can outlive the final result. Leaving either session revokes
its associations. Forwarding preserves transport backpressure and half-close.

WAMP carries a reserved dictionary containing `io.xconn.stream.v1` and an opaque
256-bit random reference. The transport control stream uses the otherwise invalid
RawSocket handshake `7f 00 00 00`, followed by that reference. Ordinary raw streams
and valid WAMP RawSocket handshakes retain their previous classification.

## RPC bridges

The existing bridge API forwards stream arguments and results when `services`
and `rpc-streams` are enabled. Source and target sessions must both support
RPC streams. Connect them normally, then configure ordinary procedure routes:

```go
bridge, err := upstream.Bridge(ctx, xconn.BridgeConfig{
    Target: localApplicationSession,
    Routes: []xconn.BridgeRoute{{
        Procedure: "app.",
        Options: xconn.Options{"match": "prefix"},
    }},
})
```

Rust uses the existing `async_::services::bridge::Bridge` and `BridgeRoute` APIs.
Uploads, downloads and duplex endpoints keep the same application APIs on either
side; no application-level stream relay is required.

Each bridge opens a stream on its next session, replaces the reference in the
forwarded payload, and copies opaque bytes with bounded buffers and backpressure.
References stay authorized to their session pair on each router. Bridges can be
chained across QUIC, WebTransport, Yamux and in-process sessions.

Write-side EOF preserves the reverse direction until its reply drains. Input
streams end with the source invocation; result streams survive the final RPC
reply. Cancellation, session loss and bridge closure release the relevant relays.
If translation fails partway through a payload, its new relays are rolled back.

Closing the bridge stops active transfers but leaves source and shared target
sessions open. Sessions created by `BridgeConfig.NewSession` remain bridge-owned;
a caller's departure closes its target without affecting other callers. Factory
mode retains its existing caller-disclosure and session-meta requirements. See
[bridge ownership](services.md#rpc-bridges).

## Current boundaries

- The stream-capable endpoints and router must use this implementation. There is
  no negotiation or compatibility promise with an older router for this extension.
- The first authenticated WAMP session on each QUIC/WebTransport connection owns
  its stream capability. Secondary sessions retain ordinary WAMP support and reject
  stream offers explicitly.
- Only top-level positional arguments/results are supported, through Go's
  `Execute`/`ExecuteContext` API. Keyword and nested streams, byte streams combined
  with progressive RPC, and automatic direct-peer selection are subsequent work.
- WebRTC carriers are a separate addition.
- Plain WebSocket/RawSocket sessions have no byte-stream carrier in this slice.
  Yamux provides a stream-capable Unix/TCP alternative. Other language bindings
  are not exposed yet.
- Rust supplies lower-level `Session::offer_stream`, `open_stream_argument` and
  `InvocationContext::arg_stream`; fluent adapters for Rust-owned I/O are separate
  work. The Go binding adapts host-owned readers/writers to these native streams.

