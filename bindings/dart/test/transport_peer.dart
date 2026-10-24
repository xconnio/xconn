import "dart:convert";
import "dart:io";
import "dart:typed_data";

class TransportPeer {
  TransportPeer(this.process, this.addresses);

  final Process process;
  final Map<String, dynamic> addresses;

  static String? get binary => Platform.environment["XCONN_TRANSPORT_PEER"];

  static Future<TransportPeer> start() async {
    final process = await Process.start(binary!, const []);
    final line = await process.stdout.transform(utf8.decoder).transform(const LineSplitter()).first;
    return TransportPeer(process, Map<String, dynamic>.from(jsonDecode(line) as Map));
  }

  Uint8List certificate(String name) {
    final value = addresses["${name}_certificate"] as String;
    return Uint8List.fromList([
      for (var i = 0; i < value.length; i += 2) int.parse(value.substring(i, i + 2), radix: 16),
    ]);
  }

  Future<void> close() async {
    await process.stdin.close();
    await process.exitCode;
  }
}
