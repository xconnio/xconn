import "dart:typed_data";

import "package:test/test.dart";
import "package:xconn_core/webtransport.dart";

import "transport_peer.dart";

void main() {
  test("WebTransport verifies certificates and transports WAMP and raw bytes", () async {
    final peer = await TransportPeer.start();
    addTearDown(peer.close);
    final url = peer.addresses["webtransport"] as String;
    await expectLater(WebTransportConnection.connect(url), throwsException);
    final connection = await WebTransportConnection.connect(url, certificate: peer.certificate("webtransport"));
    addTearDown(connection.close);
    final session = await connection.openSession("realm");
    addTearDown(session.close);
    expect((await session.call("echo").arg("HTTP/3").execute()).args, ["HTTP/3"]);
    final stream = await connection.openStream();
    addTearDown(stream.close);
    await stream.writeAll(Uint8List.fromList([1, 2, 3]));
    await stream.writeEof();
    expect(await stream.read(), [1, 2, 3]);
    expect(await stream.read(), isEmpty);
  }, skip: TransportPeer.binary == null ? "Set XCONN_TRANSPORT_PEER" : false);
}
