# Go binding

The module is `github.com/xconnio/xconn-core/bindings/go`. It uses cgo and the shared
Rust runtime. Consumers install the matching native SDK and set `PKG_CONFIG_PATH`
to its `lib/pkgconfig` directory; no Rust toolchain is needed.
See [native installation](../../docs/native-install.md). For source development:

```sh
cargo build --locked -p xconn-ffi --release
python3 scripts/package-native.py --stage-only target/native
export PKG_CONFIG_PATH="$PWD/target/native/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
cd bindings/go
go test -race -count=1 -timeout=90s ./...
```

The `auth`, `messages`, `serializers`, `transports` and `crypto` subpackages
delegate protocol processing, framing and cryptography to Rust; no standalone
Wampproto Go module is required.
