# XConn data-channel framing

Bounded message framing for WebRTC data channels, shared by the native
`xconn-webrtc` transport and the browser WASM adapter. It has no networking
dependency.

Each chunk is a one-byte flag followed by at most `MTU - 1` payload bytes. Flag
`1` marks the final chunk, so an empty message is a single final chunk.
`Assembler` rejects malformed flags and oversized chunks, enforces a receive
limit, and discards the partial message after any error. The format matches the
Go `xconn-webrtc-go` implementation.

```sh
cargo test --locked -p xconn-framing
```

MIT. Copyright XConnIO Ltd.
