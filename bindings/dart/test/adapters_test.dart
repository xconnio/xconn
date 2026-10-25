import "dart:async";
import "dart:convert";
import "dart:io";
import "dart:typed_data";

import "package:test/test.dart";
import "package:xconn_core/auth.dart";
import "package:xconn_core/messages.dart" show Authenticate, Challenge, Message;
import "package:xconn_core/src/native.dart" as native;
import "package:xconn_core/xconn_core.dart";

class TrackingSerializer extends CBORSerializer {
  int encoded = 0;
  int decoded = 0;
  @override
  Uint8List serialize(Message message) {
    encoded++;
    return super.serialize(message);
  }

  @override
  Message deserialize(Object message) {
    decoded++;
    return super.deserialize(message);
  }
}

class TrackingAuthenticator extends IClientAuthenticator {
  TrackingAuthenticator(this.inner, {this.failure}) : super(inner.authMethod, inner.authID, inner.authExtra);
  final IClientAuthenticator inner;
  final Exception? failure;
  int challenges = 0;
  @override
  Authenticate authenticate(Challenge challenge) {
    challenges++;
    if (failure case final error?) {
      throw error;
    }
    return inner.authenticate(challenge);
  }
}

class LocalPeer implements Peer {
  LocalPeer(this.handle);
  final int handle;
  int closes = 0;
  @override
  Future<Object> read() async => (await native.executeAsync("base_read", {"handle": handle}))!;

  @override
  Future<void> write(Object data) async {
    await native.executeAsync("base_write", {"handle": handle, "bytes": data});
  }

  @override
  Future<void> close() async {
    closes++;
    await native.executeAsync("base_close", {"handle": handle});
    native.execute("close", {"handle": handle});
  }
}

void main() {
  test("PeerBaseSession and Peer adapters retain Rust session ownership", () async {
    final router = (native.execute("router_new", {"realm": "local"})! as Map)["handle"] as int;
    addTearDown(() => native.execute("close", {"handle": router}));
    final base =
        native.execute("connect_base", {
              "handle": router,
              "realm": "local",
              "authid": "alice",
              "authrole": "trusted",
              "serializer": "cbor",
              "out_queue_size": 2,
            })!
            as Map;
    final peer = LocalPeer(base["handle"] as int);
    final session = Session(
      PeerBaseSession(peer, SessionDetails(base["session_id"] as int, "local", "alice", "trusted"), CBORSerializer()),
    );
    addTearDown(session.close);
    await session.register("echo", (inv) async => Result(args: inv.args, kwargs: inv.kwargs)).execute();
    expect((await session.call("echo").args(["custom peer"]).execute()).args, ["custom peer"]);
    await session.close();
    await session.close();
    expect(peer.closes, 1);
  });

  final binary = Platform.environment["XCONN_REFERENCE_GO_BIN"];
  test(
    "custom auth/serializer interfaces and original exception identity",
    skip: binary == null ? "Set XCONN_REFERENCE_GO_BIN" : false,
    () async {
      final router = await Process.start(binary!, const []);
      addTearDown(() async {
        router.kill();
        await router.exitCode;
      });
      final metadata =
          jsonDecode(await router.stdout.transform(utf8.decoder).transform(const LineSplitter()).first) as Map;
      final auth = TrackingAuthenticator(TicketAuthenticator("interop", "secret"));
      final serializer = TrackingSerializer();
      final session = await Client(
        config: ClientConfig(authenticator: auth, serializer: serializer),
      ).connect(metadata["ws"] as String, "realm");
      addTearDown(session.close);
      expect((await session.call("echo").args(["callbacks"]).execute()).args, ["callbacks"]);
      expect(auth.challenges, 1);
      expect(serializer.encoded, greaterThan(1));
      expect(serializer.decoded, greaterThan(1));
      await session.close();
      final sentinel = Exception("custom authentication failure");
      await expectLater(
        Client(
          config: ClientConfig(authenticator: TrackingAuthenticator(auth.inner, failure: sentinel)),
        ).connect(metadata["ws"] as String, "realm"),
        throwsA(same(sentinel)),
      );
    },
  );
}
