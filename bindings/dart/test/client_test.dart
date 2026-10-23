import "dart:async";
import "dart:convert";
import "dart:io";
import "dart:isolate";
import "dart:typed_data";

import "package:test/test.dart";
import "package:xconn_core/auth.dart";
import "package:xconn_core/xconn_core.dart";

void main() {
  final binary = Platform.environment["XCONN_REFERENCE_GO_BIN"];
  group(
    "original Go router",
    skip: binary == null ? "Set XCONN_REFERENCE_GO_BIN to the original Go fixture" : false,
    () {
      late Process router;
      late Map<String, dynamic> addresses;
      setUpAll(() async {
        router = await Process.start(binary!, const []);
        final line = await router.stdout.transform(utf8.decoder).transform(const LineSplitter()).first;
        addresses = Map<String, dynamic>.from(jsonDecode(line) as Map);
      });
      tearDownAll(() async {
        router.kill();
        await router.exitCode;
      });

      for (final serializer in [JSONSerializer(), CBORSerializer(), MsgPackSerializer()]) {
        for (final transport in ["ws", "rs"]) {
          test("${serializer.runtimeType} $transport RPC, PubSub, errors and lifecycle", () async {
            final session = await Client(
              config: ClientConfig(serializer: serializer),
            ).connect(addresses[transport] as String, "realm");
            addTearDown(session.close);
            var disconnected = 0;
            session.onDisconnect(() {
              disconnected++;
            });
            expect((await session.call("echo").args(["Dart"]).execute()).args, ["Dart"]);
            final registration = await session
                .register("dart.echo", (inv) async => Result(args: inv.args, kwargs: inv.kwargs))
                .execute();
            expect((await session.call("dart.echo").args([42]).kwargs({"answer": true}).execute()).kwargs, {
              "answer": true,
            });
            if (serializer is! JSONSerializer) {
              final payload = [
                Uint8List.fromList([0, 255]),
                int.parse("9007199254740993"),
              ];
              expect((await session.call("dart.echo").args(payload).execute()).args, payload);
            }
            await registration.unregister();
            await expectLater(
              session.call("dart.echo").execute(),
              throwsA(isA<ApplicationError>().having((error) => error.message, "URI", "wamp.error.no_such_procedure")),
            );
            final first = Completer<Event>();
            final second = Completer<Event>();
            final a = await session.subscribe("dart.events", first.complete).execute();
            final b = await session.subscribe("dart.events", second.complete).execute();
            expect(a.subscriptionID, b.subscriptionID);
            await a.unsubscribe();
            await session.publish("dart.events").args(["hello"]).options({
              "acknowledge": true,
              "exclude_me": false,
            }).execute();
            expect((await second.future.timeout(const Duration(seconds: 2))).args, ["hello"]);
            expect(first.isCompleted, isFalse);
            await b.unsubscribe();
            await session.close();
            await session.close();
            expect(session.isConnected, isFalse);
            expect(disconnected, 1);
          });

          test("${serializer.runtimeType} $transport progressive input and results", () async {
            final session = await Client(
              config: ClientConfig(serializer: serializer),
            ).connect(addresses[transport] as String, "realm");
            addTearDown(session.close);
            await session.register("dart.progress", (inv) async {
              for (var n = 1; n <= 3; n++) {
                await inv.sendProgress([n], null);
              }
              return Result(args: ["done"]);
            }).execute();
            final updates = <int>[];
            final result = await session.callProgress("dart.progress", (result) {
              updates.add(result.args.first as int);
            });
            expect(updates, [1, 2, 3]);
            expect(result.args, ["done"]);

            final inputs = <int>[];
            await session.register("dart.input", (inv) async {
              inputs.add(inv.args.first as int);
              return inv.details["progress"] == true ? null : Result(args: ["complete"]);
            }).execute();
            var n = 0;
            final inputResult = await session.callProgressive("dart.input", () {
              n++;
              return Progress(args: [n], options: {"progress": n < 3});
            });
            expect(inputs, [1, 2, 3]);
            expect(inputResult.args, ["complete"]);

            await session.register("dart.duplex", (inv) async {
              await inv.sendProgress(inv.args, null);
              return inv.details["progress"] == true ? null : Result(args: ["complete"]);
            }).execute();
            n = 0;
            updates.clear();
            final duplex = await session.callProgressiveProgress(
              "dart.duplex",
              () async {
                n++;
                return Progress(args: [n], options: {"progress": n < 3});
              },
              (result) {
                updates.add(result.args.first as int);
              },
            );
            expect(updates, [1, 2, 3]);
            expect(duplex.args, ["complete"]);
          });
        }
      }

      test("fluent requests share execution across Future listeners", () async {
        final session = await connectAnonymous(addresses["ws"] as String, "realm");
        addTearDown(session.close);
        var calls = 0;
        final register = session.register("dart.fluent", (inv) async {
          calls++;
          return Result.create(inv.args);
        });
        final registrations = await Future.wait([register.execute(), register.then((value) => value)]);
        expect(identical(registrations[0], registrations[1]), isTrue);
        final call = session.call("dart.fluent").arg("hello");
        final results = await Future.wait([call.execute(), call.asStream().single, call.then((value) => value)]);
        expect(results.map((result) => result.args), everyElement(["hello"]));
        expect(calls, 1);
        final received = Completer<Event>();
        var events = 0;
        final subscribe = session.subscribe("dart.fluent.events", (event) {
          events++;
          received.complete(event);
        });
        final subscriptions = await Future.wait([subscribe.execute(), subscribe.then((value) => value)]);
        expect(identical(subscriptions[0], subscriptions[1]), isTrue);
        final publish = session
            .publish("dart.fluent.events")
            .arg("event")
            .option("acknowledge", true)
            .option("exclude_me", false);
        await Future.wait([publish.execute(), publish.whenComplete(() {})]);
        expect((await received.future.timeout(const Duration(seconds: 2))).args, ["event"]);
        expect(events, 1);
        await subscriptions.first.unsubscribe();
        await registrations.first.unregister();
      });

      test("fluent progressive calls await asynchronous result handlers", () async {
        final session = await connectAnonymous(addresses["ws"] as String, "realm");
        addTearDown(session.close);
        await session.register("dart.fluent.progress", (inv) async {
          await inv.sendProgress([1], null);
          return Result(args: ["done"]);
        }).execute();
        final started = Completer<void>();
        final release = Completer<void>();
        var completed = false;
        final call = session
            .call("dart.fluent.progress")
            .onProgress((result) async {
              started.complete();
              await release.future;
            })
            .execute()
            .then((result) {
              completed = true;
              return result;
            });
        await started.future;
        expect(completed, isFalse);
        release.complete();
        expect((await call).args, ["done"]);
        final failure = StateError("progress callback failed");
        await expectLater(
          session.call("dart.fluent.progress").onProgress((result) async => throw failure).execute(),
          throwsA(same(failure)),
        );
        expect((await session.call("echo").args(["still connected"]).execute()).args, ["still connected"]);
      });

      test("existing authentication interfaces across every Rust codec", () async {
        for (final serializer in [JSONSerializer(), CBORSerializer(), MsgPackSerializer()]) {
          for (final transport in ["ws", "rs"]) {
            for (final auth in <IClientAuthenticator>[
              AnonymousAuthenticator(""),
              TicketAuthenticator("interop", "secret"),
              WAMPCRAAuthenticator("interop", "secret"),
              WAMPCRAAuthenticator("salted", "secret"),
              CryptoSignAuthenticator("interop", List.filled(32, "11").join()),
            ]) {
              final session = await Client(
                config: ClientConfig(serializer: serializer, authenticator: auth),
              ).connect(addresses[transport] as String, "realm");
              try {
                expect((await session.call("echo").args([auth.authMethod]).execute()).args, [auth.authMethod]);
              } finally {
                await session.close();
              }
            }
          }
        }
      });

      test("concurrent isolates receive only their own native completions", () async {
        final uri = addresses["ws"] as String;
        expect(await Future.wait(List.generate(8, (_) => isolatedEcho(uri))), List.filled(8, "isolated"));
      });

      test("waiting for Rust does not block the Dart event loop", () async {
        final session = await connectAnonymous(addresses["ws"] as String, "realm");
        addTearDown(session.close);
        final started = Completer<void>();
        final release = Completer<void>();
        await session.register("dart.wait", (inv) async {
          started.complete();
          await release.future;
          return Result(args: ["ready"]);
        }).execute();
        final call = session.call("dart.wait").execute();
        await started.future;
        var ticks = 0;
        final timer = Timer.periodic(const Duration(milliseconds: 1), (_) {
          ticks++;
        });
        await Future<void>.delayed(const Duration(milliseconds: 25));
        timer.cancel();
        expect(ticks, greaterThan(0));
        release.complete();
        expect((await call).args, ["ready"]);
      });
    },
  );
}

Future<String> isolatedEcho(String uri) => Isolate.run(() async {
  final session = await connectAnonymous(uri, "realm");
  try {
    return (await session.call("echo").args(["isolated"]).execute()).args.first as String;
  } finally {
    await session.close();
  }
});
