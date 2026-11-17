import "dart:convert";
import "dart:io";

import "package:flutter/services.dart";
import "package:flutter_test/flutter_test.dart";
import "package:integration_test/integration_test.dart";
import "package:xconn/crypto.dart" as crypto;
import "package:xconn/xconn.dart";
import "package:xconn_ios_smoke/main.dart" as app;

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  testWidgets("iOS bundles and loads the Rust library", (tester) async {
    app.main();
    await tester.pump();
    final auth = CryptoSignAuthenticator("ios-smoke", "11" * 32);
    expect(find.text(auth.authExtra!["pubkey"] as String), findsOneWidget);

    final key = Uint8List(32);
    final plaintext = Uint8List.fromList([0, 255, 42]);
    expect(crypto.decrypt(crypto.encrypt(plaintext, key), key), plaintext);

    // Exercise asynchronous native completions, not only synchronous FFI calls.
    final socket = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
    final port = socket.port;
    await socket.close();
    await expectLater(
      Client().connect("ws://127.0.0.1:$port/ws", "realm1").timeout(const Duration(seconds: 10)),
      throwsA(isA<ProtocolError>().having((error) => error.message, "message", contains("Connection refused"))),
    );
  });

  testWidgets("secure WAMP trusts the iOS keychain and checks hostnames", (tester) async {
    final certificate = await rootBundle.load("assets/server.pem");
    final privateKey = await rootBundle.load("assets/server.key");
    final context = SecurityContext()
      ..useCertificateChainBytes(certificate.buffer.asUint8List())
      ..usePrivateKeyBytes(privateKey.buffer.asUint8List());
    final server = await HttpServer.bindSecure(InternetAddress.loopbackIPv4, 0, context);
    final requests = server.listen((request) async {
      final socket = await WebSocketTransformer.upgrade(request, protocolSelector: (_) => "wamp.2.json");
      socket.listen((message) {
        final fields = jsonDecode(message as String) as List;
        if (fields.first == 1) {
          socket.add(
            jsonEncode([
              2,
              123,
              {
                "roles": {"broker": {}, "dealer": {}},
              },
            ]),
          );
        } else if (fields.first == 6) {
          socket.add(jsonEncode([6, {}, "wamp.close.goodbye_and_out"]));
        }
      });
    }, onError: (Object error) => expect(error, isA<HandshakeException>()));
    addTearDown(() async {
      await requests.cancel();
      await server.close(force: true);
    });

    final client = Client(config: ClientConfig(serializer: JSONSerializer()));
    final session = await client.connect("wss://localhost:${server.port}/ws", "realm1");
    await session.close();
    await expectLater(
      client.connect("wss://127.0.0.1:${server.port}/ws", "realm1").timeout(const Duration(seconds: 10)),
      throwsA(isA<ProtocolError>().having((error) => error.message, "message", contains("NotValidForName"))),
    );
  });
}
