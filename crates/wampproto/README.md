# Wampproto

A sans-I/O implementation of WAMP for clients and routers.

The crate provides:

- WAMP message schemas and validation.
- JSON, MessagePack, and CBOR codecs.
- Broker and dealer routing, including progressive RPC protocol handling.
- Request/session ID generation.

It does not open sockets, spawn tasks, or choose an application execution model.
Feed decoded messages into the protocol components and send their returned
messages using your own transport.

Protocol tests cover message validation, codec round trips, routing, cancellation, and
progressive call correlation. Checked-in
Go reference transcripts exercise interoperable router behavior without adding
a Go dependency to the Rust test suite.

```sh
cargo test --locked -p wampproto
```

MIT. Copyright XConnIO Ltd.
