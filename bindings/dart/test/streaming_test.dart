import "dart:async";
import "dart:convert";
import "dart:io";

import "package:test/test.dart";
import "package:xconn_core/xconn_core.dart";

void main() {
  final binary = Platform.environment["XCONN_REFERENCE_GO_BIN"];
  group("streaming against original Go router", skip: binary == null ? "Set XCONN_REFERENCE_GO_BIN" : false, () {
    late Process router;
    late String uri;
    setUpAll(() async {
      router = await Process.start(binary!, const []);
      final line = await router.stdout.transform(utf8.decoder).transform(const LineSplitter()).first;
      uri = (jsonDecode(line) as Map)["ws"] as String;
    });
    tearDownAll(() async {
      router.kill();
      await router.exitCode;
    });
    for (final serializer in [JSONSerializer(), CBORSerializer(), MsgPackSerializer()]) {
      test("${serializer.runtimeType} duplex and completion", () async {
        final session = await Client(config: ClientConfig(serializer: serializer)).connect(uri, "realm");
        addTearDown(session.close);
        final registration = await session.register("dart.stream.duplex", (inv) async {
          await inv.sendProgress(inv.args, null);
          return inv.details["progress"] == true ? null : Result.create(["done"]);
        }).execute();
        addTearDown(registration.unregister);
        final call = await session.call("dart.stream.duplex").arg("first").stream(progressiveInput: true);
        addTearDown(call.close);
        final first = (await call.receive())!;
        expect(first.progress, isTrue);
        expect(first.args, ["first"]);
        final receiving = call.receive();
        await expectLater(call.receive(), throwsStateError);
        await call.send(["second"]);
        expect((await receiving)!.args, ["second"]);
        await call.finish(["last"]);
        final results = await call.results().toList();
        expect(results.map((result) => result.progress), [true, false]);
        expect(results.last.args, ["done"]);
        expect(await call.receive(), isNull);
        await expectLater(call.send([]), throwsStateError);
      });
    }
    for (final progressiveInput in [false, true]) {
      for (final progressiveResults in [false, true]) {
        test("input=$progressiveInput results=$progressiveResults", () async {
          final session = await Client().connect(uri, "realm");
          addTearDown(session.close);
          await session.register("dart.modes", (invocation) async {
            expect(invocation.details["receive_progress"] == true, progressiveResults);
            if (progressiveResults) {
              await invocation.sendProgress(["partial"], null);
            }
            return invocation.details["progress"] == true ? null : Result.create(["final"]);
          }).execute();
          final call = await session
              .call("dart.modes")
              .option("receive_progress", !progressiveResults)
              .stream(progressiveInput: progressiveInput, progressiveResults: progressiveResults);
          addTearDown(call.close);
          if (progressiveInput) {
            if (progressiveResults) {
              expect((await call.receive())!.progress, isTrue);
            }
            await call.finish();
          }
          if (progressiveResults) {
            expect((await call.receive())!.progress, isTrue);
          }
          final result = (await call.receive())!;
          expect(result.progress, isFalse);
          expect(result.args, ["final"]);
          expect(await call.receive(), isNull);
        });
      }
    }

    test("null on a non-progressive invocation returns empty success", () async {
      final session = await Client().connect(uri, "realm");
      addTearDown(session.close);
      await session.register("dart.empty", (_) async => null).execute();
      final result = await session.call("dart.empty").execute().timeout(const Duration(seconds: 2));
      expect(result.args, isEmpty);
      expect(result.kwargs, isEmpty);
    });

    test("invocation observes caller cancellation", () async {
      final session = await Client().connect(uri, "realm");
      addTearDown(session.close);
      final started = Completer<void>();
      final cancelled = Completer<void>();
      await session.register("dart.cancelled", (invocation) async {
        started.complete();
        await invocation.onCancel;
        expect(invocation.cancelled, isTrue);
        cancelled.complete();
        return null;
      }).execute();
      final call = await session.call("dart.cancelled").stream();
      await started.future.timeout(const Duration(seconds: 2));
      await call.close();
      await cancelled.future.timeout(const Duration(seconds: 2));
    });

    test("errors, timeout and stream cancellation", () async {
      final session = await Client().connect(uri, "realm");
      addTearDown(session.close);
      await session.register("dart.stream.fail", (_) async {
        throw ApplicationError("example.failure", args: ["detail"]);
      }).execute();
      final call = await session.call("dart.stream.fail").stream();
      await expectLater(
        call.receive(),
        throwsA(isA<ApplicationError>().having((error) => error.args, "args", ["detail"])),
      );
      expect(await call.receive(), isNull);
      final release = Completer<void>();
      addTearDown(release.complete);
      await session.register("dart.stream.wait", (inv) async {
        await inv.sendProgress(["started"], null);
        await release.future;
        return null;
      }).execute();
      final waiting = await session.call("dart.stream.wait").stream();
      await for (final frame in waiting.results()) {
        expect(frame.args, ["started"]);
        break;
      }
      await expectLater(waiting.receive(), throwsStateError);
      expect(await waiting.receive(), isNull);
      final timed = await session.call("dart.stream.wait").stream(timeout: const Duration(milliseconds: 30));
      await timed.receive();
      await expectLater(timed.receive(), throwsA(isA<Object>()));
      expect((await session.call("echo").arg("healthy").execute()).args, ["healthy"]);
    });
  });
}
