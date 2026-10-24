import "dart:typed_data";

import "package:xconn_core/src/callbacks.dart";
import "package:xconn_core/src/client.dart";
import "package:xconn_core/src/native_transport.dart";
import "package:xconn_core/src/session.dart";

/// Independent bidirectional stream. Closing its write half still permits reads.
class ByteStream extends NativeTransportResource {
  ByteStream.fromNative(super.handle);

  Future<Uint8List> read([int size = 65536]) async => (await request("quic_read", {"size": size}))! as Uint8List;

  /// Returns the number written; a write can be partial.
  Future<int> write(Uint8List data) async => (await request("quic_write", {"data": data}))! as int;

  Future<void> writeAll(Uint8List data) async {
    var offset = 0;
    while (offset < data.length) {
      final count = await write(Uint8List.sublistView(data, offset));
      if (count == 0) {
        throw StateError("Stream write made no progress");
      }
      offset += count;
    }
  }

  Future<void> writeEof() async {
    await request("quic_finish");
  }
}

/// Verified native QUIC connection owning WAMP sessions and byte streams.
class QUICConnection extends NativeTransportResource {
  QUICConnection._(super.handle);

  static Future<QUICConnection> connect(
    String address, {
    required String serverName,
    Uint8List? certificate,
    Duration timeout = const Duration(seconds: 20),
  }) => dialNative(
    "quic_dial",
    {"address": address, "server_name": serverName, "certificate": ?certificate},
    timeout,
    (response) => QUICConnection._(response["handle"] as int),
  );

  Future<Session> openSession(String realm, {Client? client}) =>
      connectNative(client ?? Client(), "quic_session", {"handle": handle, "realm": realm});

  Future<ByteStream> openStream() async => ByteStream.fromNative(fields(await request("quic_open"))["handle"] as int);

  Future<ByteStream> acceptStream() async =>
      ByteStream.fromNative(fields(await request("quic_accept_remote"))["handle"] as int);
}
