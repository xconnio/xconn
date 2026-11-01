# Yamux transport

Enable the optional `yamux` Rust feature to run WAMP over a yamux connection.
The native FFI SDK enables it by default. Multiplexing uses
[rust-yamux](https://github.com/libp2p/rust-yamux) with Tokio's futures I/O adapter.

| URI | Carrier | Protection |
| --- | --- | --- |
| `unix+yamux:///path/app.sock` | Unix-domain socket | Local socket permissions |
| `yamux://localhost:8080` | TCP | Plaintext; use on trusted connections |
| `yamuxs://example.org:8443` | TCP with TLS | Server certificate and hostname validation |

The URI and listener explicitly select yamux. Existing RawSocket and WebSocket
connections do not change, and TLS connections never fall back to plaintext.
WAMP authentication and realm permissions apply as usual.

## Go

Configure a router and its authentication policy, then create a listener:

```go
server := xconn.NewServer(router, authenticator, nil)
listener, err := server.ListenAndServeYamux(xconn.NetworkUnix, "/path/app.sock")
session, err := client.Connect(ctx, "unix+yamux:///path/app.sock", "example")
```

For TCP, use `ListenAndServeYamux(xconn.NetworkTCP, "127.0.0.1:8080")`.
For TLS:

```go
certificate, err := tls.LoadX509KeyPair("cert.pem", "key.pem")
listener, err := server.ListenAndServeYamuxTLS(":8443", certificate)
session, err := client.Connect(ctx, "yamuxs://example.org:8443", "example")
```

Handle errors and close sessions/listeners as with the other transports. Clients
use native system trust roots; private CA deployments must configure host trust.
Clients do not need their own TLS certificate. WAMP supplies client authentication.

## Rust

The async `Client::connect` accepts all three URI schemes. Listeners use
`Server::yamux`, `Server::yamux_tls`, or Unix-only `UnixServer::yamux`.

`async_::yamux::connect` and `serve` also accept a Tokio `BoxStream`, so callers
can supply an already-configured TLS or other ordered byte connection.

## Lifecycle and limits

Closing a client session closes its yamux connection. Closing a listener cancels
its accepted connections and their tasks. The connection driver polls independently
of logical stream I/O. Yamux supplies flow control, half-close and stream resets;
the configuration allows 64 logical streams and a maximum aggregate receive window
of 16 MiB per connection.

This change exposes the WAMP transport. RPC-associated application streams and
forwarding through bridges are separate additions.
