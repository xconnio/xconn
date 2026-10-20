# XConn packet encryption

Portable X25519 key agreement, HKDF-SHA256 key derivation, and
ChaCha20-Poly1305 authenticated packet encryption. The implementation has no
networking or WAMP dependency and supports native Rust and browser WASM.

Packets use `12-byte random nonce || ciphertext || 16-byte authentication tag`.
Optional associated data binds a packet to application context without adding
that data to the packet. Private and derived keys use zeroizing storage.

X25519 does not authenticate a peer. Applications establish trust in public keys
through an authenticated session or another trusted mechanism. This crate does
not define key exchange RPCs, key persistence, replay policy, or authorization.

The optional `crypto` feature of `xconn-core` re-exports this crate as
`xconn_core::crypto`. It can also be used directly as `xconn_crypto`.

```sh
cargo test --locked -p xconn-crypto
cargo check --locked -p xconn-crypto --target wasm32-unknown-unknown
```

Tests verify a checked-in independent Go key-derivation/encryption vector,
directional key agreement, weak-key rejection, and rejection of altered packets,
keys, or associated data. Tests do not require Go to be installed.

MIT. Copyright XConnIO Ltd.
