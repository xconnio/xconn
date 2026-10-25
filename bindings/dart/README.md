# Dart client binding

The package provides Future-based WAMP clients over Rust networking:

```dart
import "package:xconn_core/xconn_core.dart";

Future<void> main() async {
  final session = await connectAnonymous("ws://127.0.0.1:8080/ws", "realm1");
  try {
    print((await session.call("example.echo").arg("hello").execute()).args);
  } finally {
    await session.close();
  }
}
```

Dart has no Router, Server or in-memory router session helpers. Use a Rust/Go or
external WAMP router. Client operations use fluent `call`/`register`/`subscribe`/`publish`
builders and `execute()`. Progressive calls/results and application callbacks are
supported. Custom `BaseSession` implementations and the `Peer`-backed `PeerBaseSession`
adapter remain available. Import custom authenticator interfaces from
`package:xconn_core/auth.dart`, serializers from `package:xconn_core/serializers.dart`,
and custom-codec message views from `package:xconn_core/messages.dart`. No Wampproto
Dart package or separate protocol native library is needed.

Rust owns handshakes, WebSocket/RawSocket networking, codecs and crypto. Operations
run on Tokio; Dart completion ports wake the owning isolate. Custom application
callbacks run in Dart. Sessions require explicit closure: always use `try/finally`
and `await session.close()`, including before disposing isolates. Idle sessions
also have a GC cleanup fallback; active handlers and application references can
keep them alive, and finalizer timing is not guaranteed.

A direct `Client.connect(...).timeout(duration)` cancels ownership of the native
connection attempt. If Rust finishes connecting afterwards, the unclaimed session
is closed. This also applies to `connectAnonymous` and the authentication helpers.
Dart's ordinary Future chaining does not propagate cancellation: applying a timeout
after `.then(...)`, or to unrelated operations, retains normal Dart semantics.

Invocation handlers can await `invocation.onCancel` or check `invocation.cancelled`
to stop work when the caller cancels or the session disconnects. Cancellation is
cooperative; the binding cannot forcibly stop an arbitrary Dart Future. JSON, CBOR and
MessagePack, all authentication methods, progressive inputs/results, custom
adapters and concurrent isolates are tested against the original Go router.

```sh
cargo build --locked -p xconn-ffi --release
cd bindings/dart
dart pub get --enforce-lockfile
dart format --output=none --set-exit-if-changed --line-length 120 .
dart analyze --fatal-infos
XCONN_REFERENCE_GO_BIN=/path/to/original-go-reference dart test
```

Dart 3.10+ build hooks compile the shared Rust C ABI and declare a native code
asset. `dart run`, `dart test`, and Flutter builds bundle it automatically; no
manual library copying is required. Keep the full Core checkout available when
using this source package and install Rust plus the matching Rust target.
For example, Android arm64 needs `rustup target add aarch64-linux-android` and
the Android NDK; iOS needs Xcode and the device/simulator Rust targets on macOS.
Linux, macOS, Windows, Android arm64/x64, and iOS device/simulator target mappings
are included. The hook uses Flutter's NDK linker for Android builds.

For a standalone Dart application, use `dart build cli` (Dart 3.13+) to produce
an executable **and its bundled native library**. `dart compile exe` alone does
not invoke the native-assets hook. `XCONN_LIBRARY` remains an explicit loader
override for development and tests.

Native-assets loading and packaged CLI execution are verified on Linux x86_64.
Apple/Android cross-target mappings require their platform toolchains and are
not a claim of device validation. Browser clients use the JavaScript binding.
Forced isolate termination still does not guarantee finalizer cleanup.

Fluent requests use `execute()`, for example:
`await session.call('echo').arg('hello').execute()`.
The former `do_()` alias has been removed. See the
[shared execution contract](../../docs/fluent-api.md).

Invocation, Event and Result share [strict typed payload accessors](../../docs/payload.md)
with explicit defaults and consistent numeric/binary conversion rules.

[Result construction and received values](../../docs/results.md) use one payload
type for handler replies, progressive results and completed calls.

Progressive RPCs use the fluent `stream()` terminal (`Stream()`/`StreamContext()` in Go).
See the [cross-language streaming API](../../docs/streaming.md) for examples and lifecycle rules.

## Progressive results

```dart
final call = await session.call("example.process").arg("input")
    .stream(timeout: const Duration(seconds: 30));
try {
  await for (final result in call.results()) {
    print(result.args);
  }
} finally {
  await call.close();
}
```

Enable input chunks with `stream(progressiveInput: true)`, then `send(args, kwargs)`
and `finish(args, kwargs)`. Consume results concurrently for long producers.

## QUIC

```dart
import 'package:xconn_core/quic.dart';

final connection = await QUICConnection.connect('host:443', serverName: 'host');
try {
  final session = await connection.openSession('realm');
  try {
    print((await session.call('echo').arg('hello').execute()).args);
  } finally {
    await session.close();
  }
} finally {
  await connection.close();
}
```

TLS verifies the hostname and system roots, or an explicit DER `certificate`.
`openStream` and `acceptStream` provide independent byte streams with `read`,
`write`, `writeAll`, `writeEof`, and `close`. Closing a WAMP session leaves sibling
sessions and streams open. Connection timeouts cancel native attempts.

## WebTransport

Import `package:xconn_core/webtransport.dart` and use
`WebTransportConnection.connect('https://host:443/wamp')`. The connection exposes
`openSession`, `openStream`, `acceptStream`, and `close`, like QUIC. Rust verifies
the server identity using system roots or an explicit DER `certificate`.

## WebRTC

```dart
import 'dart:typed_data';
import 'package:xconn_core/webrtc.dart';
import 'package:xconn_core/xconn_core.dart';

final signaling = await Client().connect('wss://router/ws', 'realm');
try {
  final peer = await WebRTCConnection.connect(signaling, 'realm', const WebRTCOptions(
    offerProcedure: 'offer',
    remoteCandidatesTopic: 'to-client',
    localCandidatesTopic: 'to-provider',
  ));
  try {
    print((await peer.call('echo').arg('hello').execute()).args);
    final channel = await peer.connection.openChannel('files');
    try {
      await channel.writeMessage(Uint8List.fromList([1, 2, 3]));
    } finally {
      await channel.close();
    }
  } finally {
    await peer.close();
    await peer.connection.close();
  }
} finally {
  await signaling.close();
}
```

ICE servers and connection/open/handshake deadlines are configurable. Rust owns
signaling, trickle ICE and chunk framing. WAMP sessions and raw channels share a
connection; closing one session leaves its siblings open. Raw `send`/`receive`
preserve strings versus bytes; `writeMessage`/`readMessage` use ordered, reliable
chunk framing. The Dart binding is client-only.

## Reusing a host native build

By default, the build hook compiles the Rust library. To reuse a library built
from the same revision for the current host, configure the root pubspec:

```yaml
hooks:
  user_defines:
    xconn_core:
      native_library: ../../target/release/libxconn_ffi.so
```

The path is relative to that pubspec. The hook tracks the library as a build
dependency and bundles it as usual. Cross-compilation continues to use the
source build path. CI uses this option to share one native build across Go,
Python interoperability tests, and Dart.
