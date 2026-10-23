import "dart:convert";
import "dart:io";

import "package:test/test.dart";
import "package:xconn_core/xconn_core.dart";

void main() {
  final binary = Platform.environment["XCONN_DISCOVERY_BIN"];
  test("discovers endpoints through an independent Rust service", () async {
    final host = await Process.start(binary!, const []);
    addTearDown(() async {
      host.kill();
      await host.exitCode;
    });
    final uri = await host.stdout.transform(utf8.decoder).transform(const LineSplitter()).first;
    final session = await connectAnonymous(uri, "example");
    addTearDown(session.close);
    final discovery = session.discovery("example.peers");
    const endpoint = Endpoint(id: "dart-sensor", addresses: ["quic://sensor:443"], capabilities: ["telemetry"]);
    await discovery.advertise(endpoint);
    final found = await discovery.get(endpoint.id);
    expect(found.id, endpoint.id);
    expect(found.addresses, endpoint.addresses);
    expect((await discovery.list()).map((value) => value.id), contains(endpoint.id));
    await discovery.withdraw(endpoint.id);
    expect(await discovery.list(), isEmpty);
  }, skip: binary == null ? "Set XCONN_DISCOVERY_BIN to the Rust discovery example" : false);
}
