# Python client binding

The `xconn_core` package wraps the shared Rust runtime in one native PyO3
extension. `xconn_core.auth`, `xconn_core.serializers` and the message views
execute built-in JSON/CBOR/MessagePack codecs and anonymous/ticket/CRA/cryptosign
authentication in Rust; no standalone Wampproto Python package is required.

Invocation, Event and Result share strict typed payload accessors with explicit
defaults and consistent numeric/binary conversion rules. `xconn_core.crypto`
exposes the portable packet encryption.

For source development, build from the workspace root:

```sh
maturin develop --manifest-path bindings/python/Cargo.toml
pytest bindings/python/tests
```
