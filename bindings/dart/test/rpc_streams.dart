import "dart:typed_data";

import "package:test/test.dart";
import "package:xconn_core/xconn_core.dart";

Future<void> exerciseRPCStreams(Session session) async {
  await expectLater(session.openStreamArgument({}), throwsA(isA<ProtocolError>()));
  final result = await session.call("rpc.stream").execute();
  final stream = await result.argStream(1);
  try {
    final data = Uint8List.fromList(List.generate(70000, (index) => index % 256));
    final reading = () async {
      final received = BytesBuilder();
      while (true) {
        final chunk = await stream.read();
        if (chunk.isEmpty) {
          break;
        }
        received.add(chunk);
      }
      expect(received.takeBytes(), data);
    }();
    await stream.writeAll(data);
    await stream.writeEof();
    await reading;
  } finally {
    await stream.close();
  }
  final offered = await session.offerStream();
  final call = session.call("rpc.upload").arg(null).arg(offered.reference).execute();
  try {
    await offered.stream.writeAll(Uint8List.fromList([0, 255, 42]));
    await offered.stream.writeEof();
    final reply = BytesBuilder();
    while (true) {
      final chunk = await offered.stream.read();
      if (chunk.isEmpty) {
        break;
      }
      reply.add(chunk);
    }
    expect(reply.takeBytes(), [0, 255, 42]);
    await call;
  } finally {
    await offered.stream.close();
  }
}
