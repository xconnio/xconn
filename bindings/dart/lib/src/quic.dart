import "dart:typed_data";

import "package:xconn_core/src/callbacks.dart";
import "package:xconn_core/src/client.dart";
import "package:xconn_core/src/native_transport.dart";
import "package:xconn_core/src/session.dart";

/// Independent bidirectional stream. Closing its write half still permits reads.
class ByteStream extends NativeTransportResource {
  ByteStream.fromNative(super.handle, {required this.connection});

  final NativeTransportResource connection;

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

  /// [verifyPeerCertificate] replaces root and hostname verification: it gets the
  /// peer's DER chain, leaf first, and the connection is closed unless it returns true.
  static Future<QUICConnection> connect(
    String address, {
    required String serverName,
    Uint8List? certificate,
    bool Function(List<Uint8List> certificates)? verifyPeerCertificate,
    Duration timeout = const Duration(seconds: 20),
  }) {
    if (verifyPeerCertificate == null) {
      return dialNative(
        "quic_dial",
        {"address": address, "server_name": serverName, "certificate": ?certificate},
        timeout,
        (response) => QUICConnection._(response["handle"] as int),
      );
    }
    if (certificate != null) {
      throw ArgumentError("certificate and verifyPeerCertificate cannot be combined");
    }
    return _connectVerified(address, serverName, verifyPeerCertificate, timeout);
  }

  static Future<QUICConnection> _connectVerified(
    String address,
    String serverName,
    bool Function(List<Uint8List> certificates) verify,
    Duration timeout,
  ) async {
    late Map<String, dynamic> response;
    final connection = await dialNative(
      "quic_dial",
      {"address": address, "server_name": serverName, "host_verification": true},
      timeout,
      (fields) {
        response = fields;
        return QUICConnection._(fields["handle"] as int);
      },
    );
    var trusted = false;
    try {
      final certificates = [for (final certificate in response["certificates"] as List) certificate as Uint8List];
      trusted = certificates.isNotEmpty && verify(List.unmodifiable(certificates));
    } finally {
      if (!trusted) {
        await connection.close();
      }
    }
    if (!trusted) {
      throw StateError("QUIC peer certificate was rejected");
    }
    return connection;
  }

  Future<Session> openSession(String realm, {Client? client}) => connectNative(client ?? Client(), "quic_session", {
    "handle": handle,
    "realm": realm,
  }, sessionFactory: (response, callbacks) => NativeTransportSession(response, this, callbacks: callbacks));

  Future<ByteStream> openStream() async =>
      ByteStream.fromNative(fields(await request("quic_open"))["handle"] as int, connection: this);

  Future<ByteStream> acceptStream() async =>
      ByteStream.fromNative(fields(await request("quic_accept_remote"))["handle"] as int, connection: this);
}
