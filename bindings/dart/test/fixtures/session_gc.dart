import "dart:async";
import "dart:convert";
import "dart:developer";
import "dart:io";

import "package:xconn_core/src/native.dart" as native;
import "package:xconn_core/src/types.dart" show SessionDetails;
import "package:xconn_core/xconn_core.dart";

class TrackingPeer implements Peer {
  TrackingPeer(this.handle);

  final int handle;
  final closed = Completer<void>();

  @override
  Future<Object> read() async => (await native.executeAsync("base_read", {"handle": handle}))!;

  @override
  Future<void> write(Object data) async {
    await native.executeAsync("base_write", {"handle": handle, "bytes": data});
  }

  @override
  Future<void> close() async {
    await native.executeAsync("base_close", {"handle": handle});
    native.execute("close", {"handle": handle});
    closed.complete();
  }
}

WeakReference<Session> abandon(TrackingPeer peer, int id) {
  final session = Session(PeerBaseSession(peer, SessionDetails(id, "local", "", "anonymous"), CBORSerializer()));
  return WeakReference(session);
}

bool retained(WeakReference<Session> reference) => reference.target != null;

Future<void> main() async {
  final router = (native.execute("router_new", {"realm": "local"})! as Map)["handle"] as int;
  final client = HttpClient();
  try {
    final base =
        native.execute("connect_base", {
              "handle": router,
              "realm": "local",
              "authid": "",
              "authrole": "anonymous",
              "serializer": "cbor",
              "out_queue_size": 2,
            })!
            as Map;
    final peer = TrackingPeer(base["handle"] as int);
    final reference = abandon(peer, base["session_id"] as int);
    final service = (await Service.getInfo()).serverUri!;
    Future<Map> rpc(String method, [Map<String, String>? parameters]) async {
      final request = await client.getUrl(service.resolve(method).replace(queryParameters: parameters));
      final response = await request.close();
      final result = jsonDecode(await response.transform(utf8.decoder).join()) as Map;
      if (result["error"] != null) {
        throw StateError("VM service: ${result['error']}");
      }
      return result["result"] as Map;
    }

    final vm = await rpc("getVM");
    final isolate = (vm["isolates"] as List).singleWhere((value) => (value as Map)["name"] == "main") as Map;
    for (var attempt = 0; attempt < 20 && retained(reference); attempt++) {
      await rpc("getAllocationProfile", {"isolateId": isolate["id"] as String, "gc": "true"});
      await Future<void>.delayed(const Duration(milliseconds: 50));
    }
    if (retained(reference)) {
      throw StateError("Idle session is still strongly retained by its reader");
    }
    await peer.closed.future.timeout(const Duration(seconds: 3));
    print("Idle session collected and transport closed");
  } finally {
    client.close(force: true);
    native.execute("close", {"handle": router});
  }
}
