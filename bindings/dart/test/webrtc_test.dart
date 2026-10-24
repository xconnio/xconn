import "dart:typed_data";

import "package:test/test.dart";
import "package:xconn_core/webrtc.dart";
import "package:xconn_core/xconn_core.dart";

import "transport_peer.dart";

void main() {
  test("WebRTC trickles ICE and multiplexes WAMP and large raw messages", () async {
    final server = await TransportPeer.start();
    addTearDown(server.close);
    final signaling = await Client().connect(server.addresses["ws"] as String, "realm");
    addTearDown(signaling.close);
    final peer = await WebRTCConnection.connect(
      signaling,
      "realm",
      const WebRTCOptions(
        offerProcedure: "offer",
        remoteCandidatesTopic: "to-client",
        localCandidatesTopic: "to-provider",
        iceServers: [
          {"urls": ["stun:127.0.0.1:9"]},
        ],
      ),
    );
    addTearDown(peer.connection.close);
    await expectLater(
      peer.connection.acceptChannel(timeout: const Duration(milliseconds: 10)),
      throwsA(isA<ProtocolError>().having((error) => error.message, "message", contains("timed out"))),
    );
    final second = await peer.connection.openSession("realm");
    addTearDown(second.close);
    expect((await peer.call("echo").arg("rtc").execute()).args, ["rtc"]);
    await peer.close();
    expect((await second.call("echo").arg(42).execute()).args, [42]);
    final channel = await peer.connection.openChannel("files");
    addTearDown(channel.close);
    final data = Uint8List.fromList(List.generate(150000, (i) => i % 256));
    await channel.writeMessage(data);
    expect(await channel.readMessage(), data);
    await channel.writeMessage(Uint8List(0));
    expect(await channel.readMessage(), isEmpty);
    final packets = await peer.connection.openChannel("packets");
    addTearDown(packets.close);
    await expectLater(packets.waitBufferedBelow(-1), throwsArgumentError);
    expect(await packets.bufferedAmount, greaterThanOrEqualTo(0));
    await packets.send("text packet");
    await packets.waitBufferedBelow(0);
    expect(await packets.receive(), "text packet");
    await packets.send(Uint8List.fromList([0, 255]));
    expect(await packets.receive(), [0, 255]);
    final closed = packets.closed;
    await packets.close();
    await closed.timeout(const Duration(seconds: 3));
    final trigger = await peer.connection.openChannel("trigger-remote");
    addTearDown(trigger.close);
    final remote = await peer.connection.acceptChannel();
    addTearDown(remote.close);
    expect(await remote.readMessage(), "server first".codeUnits);
    expect((await signaling.call("echo").arg("signaling alive").execute()).args, ["signaling alive"]);
  }, skip: TransportPeer.binary == null ? "Set XCONN_TRANSPORT_PEER" : false);
}
