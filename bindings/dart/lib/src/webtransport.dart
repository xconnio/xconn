import "dart:typed_data";

import "package:xconn_core/src/callbacks.dart";
import "package:xconn_core/src/client.dart";
import "package:xconn_core/src/native_transport.dart";
import "package:xconn_core/src/quic.dart";
import "package:xconn_core/src/session.dart";

/// HTTP/3 WebTransport with mandatory Rust-side certificate verification.
class WebTransportConnection extends NativeTransportResource {
  WebTransportConnection._(super.handle);

  static Future<WebTransportConnection> connect(
    String url, {
    Uint8List? certificate,
    Duration timeout = const Duration(seconds: 20),
  }) => dialNative(
    "webtransport_dial",
    {"url": url, "verify_server": true, "certificate": ?certificate},
    timeout,
    (response) => WebTransportConnection._(response["handle"] as int),
  );

  Future<Session> openSession(String realm, {Client? client}) => connectNative(
    client ?? Client(),
    "webtransport_session",
    {"handle": handle, "realm": realm},
    sessionFactory: (response, callbacks) => NativeTransportSession(response, this, callbacks: callbacks),
  );

  Future<ByteStream> openStream() async =>
      ByteStream.fromNative(fields(await request("webtransport_open"))["handle"] as int, connection: this);

  Future<ByteStream> acceptStream() async =>
      ByteStream.fromNative(fields(await request("webtransport_accept_remote"))["handle"] as int, connection: this);
}
