import "dart:typed_data";

import "package:test/test.dart";
import "package:xconn_core/quic.dart";

import "transport_peer.dart";

void main() {
  test("QUIC verifies identity and multiplexes WAMP with byte streams", () async {
    final peer = await TransportPeer.start();
    addTearDown(peer.close);
    await expectLater(
      QUICConnection.connect(
        peer.addresses["quic"] as String,
        serverName: "wrong.invalid",
        certificate: peer.certificate("quic"),
      ),
      throwsException,
    );
    final connection = await QUICConnection.connect(
      peer.addresses["quic"] as String,
      serverName: "localhost",
      certificate: peer.certificate("quic"),
    );
    addTearDown(connection.close);
    final first = await connection.openSession("realm");
    final second = await connection.openSession("realm");
    addTearDown(second.close);
    expect((await first.call("echo").arg("hello").execute()).args, ["hello"]);
    await first.close();
    expect((await second.call("echo").arg(42).execute()).args, [42]);
    final stream = await connection.openStream();
    addTearDown(stream.close);
    await stream.writeAll(Uint8List.fromList([1, 2, 3]));
    await stream.writeEof();
    expect(await stream.read(), [1, 2, 3]);
    expect(await stream.read(), isEmpty);
  }, skip: TransportPeer.binary == null ? "Set XCONN_TRANSPORT_PEER" : false);
}
