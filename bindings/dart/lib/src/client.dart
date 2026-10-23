import "dart:async";
import "dart:convert";
import "dart:typed_data";

import "package:xconn_core/auth.dart";
import "package:xconn_core/messages.dart";
import "package:xconn_core/serializers.dart";
import "package:xconn_core/src/callbacks.dart";
import "package:xconn_core/src/native.dart" as native;
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
  Future<Session> connect(String url, String realm) {
    final attempt = _ConnectionAttempt();
    return _ConnectionFuture(_connect(url, realm, attempt), attempt.cancel);
  }

  Future<Session> _connect(String url, String realm, _ConnectionAttempt attempt) async {
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
      final operation = native.NativeOperation("connect", {
        "uri": url,
        "realm": realm,
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
      if (attempt.cancelled) {
        native.execute("close", {"handle": response["handle"]});
        throw StateError("Connection attempt cancelled");
      }

      retained = customSerializer(serializer);
      final session = Session.fromNative(response, callbacks: retained ? callbacks : null);
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

class _ConnectionAttempt {
  native.NativeOperation? operation;
  Session? session;
  bool cancelled = false;

  void cancel() {
    cancelled = true;
    operation?.cancel();
    session?.close().ignore();
  }
}

// Future.timeout normally leaves its source running. A connect attempt owns native
// resources, so direct timeouts must relinquish that ownership as well.
class _ConnectionFuture implements Future<Session> {
  _ConnectionFuture(this._future, this._cancel);

  final Future<Session> _future;
  final void Function() _cancel;

  @override
  Stream<Session> asStream() => _future.asStream();

  @override
  Future<Session> catchError(Function onError, {bool Function(Object)? test}) =>
      _future.catchError(onError, test: test);

  @override
  Future<R> then<R>(FutureOr<R> Function(Session) onValue, {Function? onError}) =>
      _future.then(onValue, onError: onError);

  @override
  Future<Session> whenComplete(FutureOr<void> Function() action) => _future.whenComplete(action);

  @override
  Future<Session> timeout(Duration timeLimit, {FutureOr<Session> Function()? onTimeout}) => _future.timeout(
    timeLimit,
    onTimeout: () {
      _cancel();
      if (onTimeout != null) {
        return onTimeout();
      }
      throw TimeoutException("Connection timed out", timeLimit);
    },
  );
}
