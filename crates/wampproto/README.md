# Wampproto

A sans-I/O implementation of WAMP for clients and routers.

The crate provides:

- WAMP message schemas and validation.
- JSON, MessagePack, and CBOR codecs.

It does not open sockets, spawn tasks, or choose an application execution model.
Feed decoded messages into the protocol components and send their returned
messages using your own transport.

Protocol tests cover message validation and codec round trips.

```sh
cargo test --locked -p wampproto
```

MIT. Copyright XConnIO Ltd.
