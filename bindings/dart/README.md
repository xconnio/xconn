# Dart client binding

The package wraps the shared Rust runtime through `dart:ffi`. Custom authenticator
interfaces live in `package:xconn_core/auth.dart`, serializers in
`package:xconn_core/serializers.dart`, custom-codec message views in
`package:xconn_core/messages.dart`, and portable packet encryption in
`package:xconn_core/crypto.dart`. No Wampproto Dart package or separate protocol
native library is needed: Rust owns codecs and crypto.

```sh
cargo build --locked -p xconn-ffi --release
cd bindings/dart
dart pub get --enforce-lockfile
dart format --output=none --set-exit-if-changed --line-length 120 .
dart analyze --fatal-infos
dart test
```

Development builds find `libxconn_ffi` in the workspace `target/release`;
`XCONN_LIBRARY` overrides its location.
