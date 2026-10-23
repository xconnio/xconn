import "dart:async";

import "package:xconn_core/auth.dart";
import "package:xconn_core/messages.dart";
import "package:xconn_core/serializers.dart";
import "package:xconn_core/src/payload.dart";
import "package:xconn_core/src/session.dart";

abstract class BaseSession {
  int get id;

  String get realm;

  String get authid;

  String get authrole;

  Serializer get serializer;

  Future<Object> read();

  Future<void> write(Object payload);

  Future<Message> readMessage();

  Future<void> writeMessage(Message msg);

  Future<void> close();
}

class PeerBaseSession implements BaseSession {
  PeerBaseSession(this._peer, this._details, this._serializer);

  final Peer _peer;
  final SessionDetails _details;
  final Serializer _serializer;

  @override
  int get id => _details.sessionID;

  @override
  String get realm => _details.realm;

  @override
  String get authid => _details.authid;

  @override
  String get authrole => _details.authrole;

  @override
  Serializer get serializer => _serializer;

  @override
  Future<Object> read() => _peer.read();

  @override
  Future<void> write(Object payload) => _peer.write(payload);

  @override
  Future<Message> readMessage() async {
    final payload = await read();
    return _serializer.deserialize(payload);
  }

  @override
  Future<void> writeMessage(Message msg) async {
    final payload = _serializer.serialize(msg);
    await write(payload);
  }

  @override
  Future<void> close() => _peer.close();
}

abstract class Peer {
  Future<Object> read();

  Future<void> write(Object data);

  Future<void> close();
}

class Result with PayloadAccess {
  Result({List<dynamic>? args, Map<String, dynamic>? kwargs, Map<String, dynamic>? details})
    : args = args ?? [],
      kwargs = kwargs ?? {},
      details = details ?? {};

  factory Result.create(List<dynamic> args, [Map<String, dynamic>? kwargs]) => Result(args: args, kwargs: kwargs);

  bool get progress => details["progress"] == true;

  @override
  final List<dynamic> args;

  @override
  final Map<String, dynamic> kwargs;
  final Map<String, dynamic> details;
}

class Registration {
  Registration(this.registrationID, this._session);

  final int registrationID;
  final Session _session;

  Future<void> unregister() {
    return _session.unregister(this);
  }
}

class Invocation with PayloadAccess {
  Invocation({List<dynamic>? args, Map<String, dynamic>? kwargs, Map<String, dynamic>? details})
    : args = args ?? [],
      kwargs = kwargs ?? {},
      details = details ?? {};

  @override
  final List<dynamic> args;

  @override
  final Map<String, dynamic> kwargs;
  final Map<String, dynamic> details;

  late Function(List<dynamic>? args, Map<String, dynamic>? kwargs) sendProgress;

  final _cancellation = Completer<void>();

  /// Whether the caller cancelled this invocation or its session disconnected.
  bool get cancelled => _cancellation.isCompleted;

  /// Completes when cancellation is requested. Handlers stop their own work cooperatively.
  Future<void> get onCancel => _cancellation.future;

  /// Called by the session when an interrupt or disconnect arrives.
  void markCancelled() {
    if (!cancelled) {
      _cancellation.complete();
    }
  }
}

class Subscription {
  Subscription(this.subscriptionID, this._eventHandler, this._session);

  final int subscriptionID;
  final void Function(Event) _eventHandler;
  final Session _session;

  void Function(Event) get eventHandler => _eventHandler;

  Future<void> unsubscribe() {
    return _session.unsubscribe(this);
  }
}

class Event with PayloadAccess {
  Event({List<dynamic>? args, Map<String, dynamic>? kwargs, Map<String, dynamic>? details})
    : args = args ?? [],
      kwargs = kwargs ?? {},
      details = details ?? {};

  @override
  final List<dynamic> args;

  @override
  final Map<String, dynamic> kwargs;
  final Map<String, dynamic> details;
}

class Progress {
  Progress({List<dynamic>? args, Map<String, dynamic>? kwargs, Map<String, dynamic>? options})
    : args = args ?? [],
      kwargs = kwargs ?? {},
      options = options ?? {};

  final List<dynamic> args;
  final Map<String, dynamic> kwargs;
  final Map<String, dynamic> options;
}

class ClientConfig {
  ClientConfig({IClientAuthenticator? authenticator, Serializer? serializer, this.keepAliveInterval})
    : authenticator = authenticator ?? AnonymousAuthenticator(""),
      serializer = serializer ?? CBORSerializer();

  final IClientAuthenticator authenticator;
  final Serializer serializer;
  final Duration? keepAliveInterval;
}

class SessionDetails {
  SessionDetails(this.sessionID, this.realm, this.authid, this.authrole);
  final int sessionID;
  final String realm;
  final String authid;
  final String authrole;
}
