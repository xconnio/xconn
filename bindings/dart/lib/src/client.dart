import "dart:async";
import "dart:convert";
import "dart:typed_data";

import "package:xconn_core/auth.dart";
import "package:xconn_core/messages.dart";
import "package:xconn_core/serializers.dart";
import "package:xconn_core/src/callbacks.dart";
import "package:xconn_core/src/native.dart" as native;
import "package:xconn_core/src/native_transport.dart";
import "package:xconn_core/src/session.dart";
import "package:xconn_core/src/types.dart";

String serializerName(Serializer serializer) => switch (serializer) {
  JSONSerializer() => "json",
  CBORSerializer() => "cbor",
  MsgPackSerializer() => "msgpack",
  _ => throw UnsupportedError("Custom serializers must extend a built-in codec"),
};

bool customSerializer(Serializer serializer) =>
    serializer.runtimeType != JSONSerializer &&
    serializer.runtimeType != CBORSerializer &&
    serializer.runtimeType != MsgPackSerializer;

Object? serializerCallback(Serializer serializer, String method, Object? value) {
  switch (method) {
    case "serialize":
      return serializer.serialize(toMessage((value! as List).cast<Object?>()));
    case "deserialize":
      final bytes = value! as Uint8List;
      return serializer.deserialize(serializer is JSONSerializer ? utf8.decode(bytes) : bytes).marshal();
    default:
      throw StateError("Unexpected serializer callback $method");
  }
}

class Client {
  Client({ClientConfig? config}) : _config = config ?? ClientConfig();

  final ClientConfig _config;

  /// Timing out this returned Future also releases the underlying connection attempt.
  Future<Session> connect(String url, String realm) => connectNative(this, "connect", {"uri": url, "realm": realm});

  Future<Session> _connect(
    String method,
    Map<String, Object?> request,
    _ConnectionAttempt attempt,
    Session Function(Map<String, dynamic>, HostCallbacks?)? sessionFactory,
  ) async {
    final serializer = _config.serializer;
    final name = serializerName(serializer);
    final authenticator = _config.authenticator;
    final callbacks = HostCallbacks((method, value) {
      if (method == "authenticate") {
        final challenge = fields(value);
        final response = authenticator.authenticate(
          Challenge(challenge["method"] as String, fields(challenge["extra"])),
        );
        return {"signature": response.signature, "extra": response.extra};
      }

      return serializerCallback(serializer, method, value);
    });
    var retained = false;

    try {
      final operation = native.NativeOperation(method, {
        ...request,
        "auth": {
          "method": authenticator.authMethod,
          "authid": authenticator.authID,
          "extra": authenticator.authExtra ?? <String, Object?>{},
          "callback": callbacks.handle,
        },
        "serializer": customSerializer(serializer)
            ? {"name": name, "callback": callbacks.handle, "static": true}
            : name,
        if (_config.keepAliveInterval != null)
          "ping_interval": _config.keepAliveInterval!.inMicroseconds / Duration.microsecondsPerSecond,
      });
      attempt.operation = operation;
      final response = fields(await operation.future);
      attempt.connectionHandle = response["connection"] as int?;
      if (attempt.cancelled) {
        native.execute("close", {"handle": response["handle"]});
        if (attempt.connectionHandle != null) {
          native.execute("close", {"handle": attempt.connectionHandle});
        }
        throw StateError("Connection attempt cancelled");
      }

      retained = customSerializer(serializer);
      final session =
          sessionFactory?.call(response, retained ? callbacks : null) ??
          Session.fromNative(response, callbacks: retained ? callbacks : null);
      attempt.session = session;
      return session;
    } on Object {
      if (callbacks.failure != null) {
        Error.throwWithStackTrace(callbacks.failure!, callbacks.failureStack!);
      }

      rethrow;
    } finally {
      if (!retained) {
        await callbacks.close();
      }
    }
  }
}

Future<Session> connectAnonymous(String uri, String realm) => Client().connect(uri, realm);

Future<Session> connectTicket(String uri, String realm, String authid, String ticket) =>
    Client(config: ClientConfig(authenticator: TicketAuthenticator(authid, ticket))).connect(uri, realm);

Future<Session> connectCRA(String uri, String realm, String authid, String secret) =>
    Client(config: ClientConfig(authenticator: WAMPCRAAuthenticator(authid, secret))).connect(uri, realm);

Future<Session> connectCryptosign(String uri, String realm, String authid, String privateKey) =>
    Client(config: ClientConfig(authenticator: CryptoSignAuthenticator(authid, privateKey))).connect(uri, realm);

/// Internal adapter entry point; public callers use transport connection methods.
Future<T> connectNative<T extends Session>(
  Client client,
  String method,
  Map<String, Object?> request, {
  Session Function(Map<String, dynamic>, HostCallbacks?)? sessionFactory,
}) {
  final attempt = _ConnectionAttempt();
  return CancellableFuture<T>(
    client._connect(method, request, attempt, sessionFactory).then((session) => session as T),
    attempt.cancel,
  );
}

class _ConnectionAttempt {
  native.NativeOperation? operation;
  int? connectionHandle;
  Session? session;
  bool cancelled = false;

  void cancel() {
    cancelled = true;
    operation?.cancel();
    if (connectionHandle != null) {
      native.execute("close", {"handle": connectionHandle});
    }
    session?.close().ignore();
  }
}
