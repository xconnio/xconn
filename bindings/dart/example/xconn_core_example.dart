import "package:xconn_core/xconn_core.dart";

Future<void> main() async {
  final session = await connectAnonymous("ws://127.0.0.1:8080/ws", "realm1");
  try {
    final result = await session.call("example.echo").arg("hello").execute();
    print(result.args);
  } finally {
    await session.close();
  }
}
