import "dart:async";
import "dart:convert";
import "dart:io";

import "package:test/test.dart";
import "package:xconn_core/xconn_core.dart";

void main() {
  test("a direct connect timeout releases a late successful connection", () async {
    final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    addTearDown(() => server.close(force: true));
    final accepted = Completer<WebSocket>();
    final hello = Completer<void>();
    final disconnected = Completer<void>();
    server.listen((request) async {
      final socket = await WebSocketTransformer.upgrade(request, protocolSelector: (offered) => offered.first);
      accepted.complete(socket);
      socket.listen((_) {
        if (!hello.isCompleted) {
          hello.complete();
        }
      }, onDone: disconnected.complete);
    });
    final connecting = Client(
      config: ClientConfig(serializer: JSONSerializer()),
    ).connect("ws://127.0.0.1:${server.port}", "realm").timeout(const Duration(milliseconds: 100));
    final timeout = expectLater(connecting, throwsA(isA<TimeoutException>()));
    await hello.future.timeout(const Duration(seconds: 3));
    await timeout;
    final socket = await accepted.future;
    addTearDown(socket.close);
    socket.add(
      jsonEncode([
        2,
        1,
        {"roles": <String, dynamic>{}},
      ]),
    );
    await disconnected.future.timeout(const Duration(seconds: 3));
  });

  test("an idle abandoned session is collected and closes its transport", () async {
    final process = await Process.start(Platform.resolvedExecutable, [
      "--enable-vm-service=0",
      "run",
      "test/fixtures/session_gc.dart",
    ]);
    final output = process.stdout.transform(utf8.decoder).join();
    final errors = process.stderr.transform(utf8.decoder).join();
    final status = await process.exitCode.timeout(
      const Duration(seconds: 20),
      onTimeout: () {
        process.kill();
        throw TimeoutException("Session GC subprocess did not release its ports");
      },
    );
    expect(status, 0, reason: "${await output}\n${await errors}");
    expect(await output, contains("Idle session collected and transport closed"));
  });
}
