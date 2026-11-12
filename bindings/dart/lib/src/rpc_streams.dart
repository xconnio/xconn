import "dart:typed_data";

import "package:xconn/src/native_transport.dart";

/// Independent bidirectional stream. Closing its write half still permits reads.
class RPCStream extends NativeTransportResource {
  RPCStream.fromNative(super.handle);

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
