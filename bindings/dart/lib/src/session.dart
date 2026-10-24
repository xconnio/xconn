import "dart:async";
import "dart:convert";

import "package:xconn_core/serializers.dart";
import "package:xconn_core/src/call.dart";
import "package:xconn_core/src/callbacks.dart";
import "package:xconn_core/src/client.dart";
import "package:xconn_core/src/discovery.dart";
import "package:xconn_core/src/exception.dart";
import "package:xconn_core/src/native.dart" as native;
import "package:xconn_core/src/requests.dart";
import "package:xconn_core/src/types.dart";

Result resultFromNative(Object? value) {
  final response = fields(value);
  return Result(
    args: (response["args"] as List?)?.cast<Object?>(),
    kwargs: (response["kwargs"] as Map?)?.cast<String, Object?>(),
    details: (response["details"] as Map?)?.cast<String, Object?>(),
  );
}

class Session {
  factory Session(BaseSession base) {
    var closedBase = false;
    Future<void> closeBase() async {
      if (!closedBase) {
        closedBase = true;
        await base.close();
      }
    }

    final callbacks = HostCallbacks((method, value) async {
      switch (method) {
        case "peer_read":
          final payload = await base.read();
          return payload is String ? utf8.encode(payload) : payload;
        case "peer_write":
          await base.write(base.serializer is JSONSerializer ? utf8.decode(value! as List<int>) : value!);
          return null;
        case "peer_close":
          await closeBase();
          return null;
        default:
          return serializerCallback(base.serializer, method, value);
      }
    });

    try {
      final name = serializerName(base.serializer);
      final response = fields(
        native.execute("session_from_peer", {
          "session_id": base.id,
          "realm": base.realm,
          "authid": base.authid,
          "authrole": base.authrole,
          "authmethod": "",
          "authextra": <String, Object?>{},
          "callback": callbacks.handle,
          "serializer": customSerializer(base.serializer)
              ? {"name": name, "callback": callbacks.handle, "static": true}
              : name,
        }),
      );
      return Session.fromNative(response, callbacks: callbacks, closeBase: closeBase);
    } on Object {
      unawaited(callbacks.close());
      rethrow;
    }
  }

  Session.fromNative(Map<String, dynamic> response, {HostCallbacks? callbacks, Future<void> Function()? closeBase})
    : _handle = response["handle"] as int,
      _id = response["session_id"] as int {
    _cleanup = _SessionCleanup(_handle, callbacks, closeBase);
    _finalizer.attach(this, _cleanup, detach: this);
    _reader = _readEvents(WeakReference(this), _handle);
  }

  static final _finalizer = Finalizer<_SessionCleanup>((cleanup) {
    cleanup.close().ignore();
  });

  late final _SessionCleanup _cleanup;
  final int _handle;
  final int _id;
  late final Future<void> _reader;
  final _invocations = <int, Invocation>{};
  bool _connected = true;
  Future<void>? _closing;

  void Function()? _onDisconnect;
  int _token = 0;
  final _registrations = <int, Future<Result?> Function(Invocation)>{};
  final _registrationTokens = <int, int>{};
  final _eventsByToken = <int, void Function(Event)>{};
  final _subscriptions = <Subscription, int>{};

  Discovery discovery(String prefix) => Discovery(_handle, prefix);

  int get id => _id;

  bool get isConnected => _connected;

  // Preserve the established onDisconnect(callback) method.
  // ignore: use_setters_to_change_properties
  void onDisconnect(void Function() callback) {
    _onDisconnect = callback;
  }

  Future<Object?> _request(String method, [Map<String, Object?> value = const {}]) =>
      native.executeAsync(method, {"handle": _handle, ...value});

  // Only the weak owner crosses the queue await. An idle reader must not keep an
  // abandoned Session alive and prevent its finalizer from releasing the handle.
  static Future<void> _readEvents(WeakReference<Session> owner, int handle) async {
    try {
      while (_isReading(owner)) {
        final value = await native.executeAsync("next_event", {"handle": handle});
        if (value == null) {
          break;
        }
        _dispatch(owner, fields(value));
      }
    } on Exception {
      // Rust closes this queue when a transport is lost or the session closes.
    } finally {
      _disconnected(owner);
    }
  }

  static bool _isReading(WeakReference<Session> owner) => owner.target?._connected ?? false;

  static void _dispatch(WeakReference<Session> owner, Map<String, dynamic> event) {
    final session = owner.target;
    if (session == null) {
      return;
    }
    if (event["kind"] == "invocation") {
      unawaited(session._invoke(event));
    } else if (event["kind"] == "interrupt") {
      session._invocations.remove(event["id"])?.markCancelled();
    } else if (event["kind"] == "event") {
      final result = resultFromNative(event);
      try {
        session._eventsByToken[event["token"]]?.call(
          Event(args: result.args, kwargs: result.kwargs, details: result.details),
        );
      } on Object catch (error, stack) {
        Zone.current.handleUncaughtError(error, stack);
      }
    }
  }

  static void _disconnected(WeakReference<Session> owner) {
    final session = owner.target;
    if (session == null) {
      return;
    }
    session._connected = false;
    for (final invocation in session._invocations.values) {
      invocation.markCancelled();
    }
    session._invocations.clear();
    session._cleanup.close().ignore();
    session._onDisconnect?.call();
  }

  Future<void> _invoke(Map<String, dynamic> event) async {
    final reply = <String, Object?>{"id": event["id"]};

    try {
      final handler = _registrations[event["token"]];

      if (handler == null) {
        throw ApplicationError("wamp.error.canceled");
      }

      final payload = resultFromNative(event);
      var progress = Future<void>.value();
      final invocation = Invocation(args: payload.args, kwargs: payload.kwargs, details: payload.details)
        ..sendProgress = (List<dynamic>? args, Map<String, dynamic>? kwargs) {
          final sent = progress.then((_) async {
            await _request("invocation_progress", {"id": event["id"], "args": args, "kwargs": kwargs});
          });
          progress = sent;
          // The handler may use the original fire-and-forget calling convention.
          sent.ignore();
          return sent;
        };
      final id = event["id"] as int;
      _invocations[id] = invocation;
      Result? result;

      try {
        result = await handler(invocation);
      } finally {
        try {
          await progress;
        } finally {
          _invocations.remove(id);
        }
      }

      if (invocation.cancelled) {
        return;
      }

      if ((result == null && payload.details["progress"] == true) || result?.details["progress"] == true) {
        if (result != null) {
          await invocation.sendProgress(result.args, result.kwargs);
        }

        reply["error"] = {"uri": "io.xconn.no_result"};
      } else {
        reply["args"] = result?.args ?? const [];
        reply["kwargs"] = result?.kwargs ?? const <String, Object?>{};
      }
    } on ApplicationError catch (error) {
      reply["error"] = {"uri": error.message, "args": error.args, "kwargs": error.kwargs};
    } on Object catch (error) {
      reply["error"] = {
        "uri": "wamp.error.runtime_error",
        "args": [error.toString()],
      };
    }

    try {
      await _request("reply", reply);
    } on Exception {
      // Cancellation can remove an invocation before its Dart handler returns.
    }
  }

  CallRequest call(String procedure) => CallRequest(procedure, session: this);

  RegisterRequest register(String procedure, Future<Result?> Function(Invocation) invocationHandler) =>
      RegisterRequest(procedure, invocationHandler, session: this);

  PublishRequest publish(String topic) => PublishRequest(topic, session: this);

  SubscribeRequest subscribe(String topic, void Function(Event) eventHandler) =>
      SubscribeRequest(topic, eventHandler, session: this);

  Future<Call> openStream(
    CallRequest request, {
    bool progressiveInput = false,
    bool progressiveResults = true,
    int bufferSize = 32,
    Duration? timeout,
  }) async {
    final response = fields(
      await _request("stream_open", {
        "uri": request.procedure,
        "args": request.arguments,
        "kwargs": request.keywordArguments,
        "options": request.requestOptions,
        "progressive_input": progressiveInput,
        "progressive_results": progressiveResults,
        "buffer_size": bufferSize,
        "timeout": timeout == null ? null : timeout.inMicroseconds / Duration.microsecondsPerSecond,
      }),
    );
    return Call.fromNative(response["handle"] as int);
  }

  Future<Result> executeCall(CallRequest req) {
    if (req.progressReceiver != null || req.progressSender != null) {
      return _stream(
        req.procedure,
        args: req.arguments,
        kwargs: req.keywordArguments,
        options: req.requestOptions,
        receive: req.progressReceiver,
        send: req.progressSender,
      );
    }

    return _call(req.procedure, args: req.arguments, kwargs: req.keywordArguments, options: req.requestOptions);
  }

  Future<Registration> executeRegister(RegisterRequest req) =>
      _register(req.procedure, req.handler, options: req.requestOptions);

  Future<void> executePublish(PublishRequest req) =>
      _publish(req.topic, args: req.arguments, kwargs: req.keywordArguments, options: req.requestOptions);

  Future<Subscription> executeSubscribe(SubscribeRequest req) =>
      _subscribe(req.topic, req.handler, options: req.requestOptions);

  Future<Result> _call(
    String procedure, {
    List<dynamic>? args,
    Map<String, dynamic>? kwargs,
    Map<String, dynamic>? options,
  }) async =>
      resultFromNative(await _request("call", {"uri": procedure, "args": args, "kwargs": kwargs, "options": options}));

  Future<Registration> _register(
    String procedure,
    Future<Result?> Function(Invocation) invocationHandler, {
    Map<String, dynamic>? options,
  }) async {
    final token = ++_token;
    _registrations[token] = invocationHandler;

    try {
      final response = fields(await _request("register", {"uri": procedure, "options": options, "token": token}));
      final id = response["id"] as int;
      _registrationTokens[id] = token;
      return Registration(id, this);
    } on Object {
      _registrations.remove(token);
      rethrow;
    }
  }

  Future<void> unregister(Registration registration) async {
    await _request("unregister", {"id": registration.registrationID});
    _registrations.remove(_registrationTokens.remove(registration.registrationID));
  }

  Future<Subscription> _subscribe(
    String topic,
    void Function(Event) eventHandler, {
    Map<String, dynamic>? options,
  }) async {
    final token = ++_token;
    _eventsByToken[token] = eventHandler;

    try {
      final response = fields(await _request("subscribe", {"uri": topic, "options": options, "token": token}));
      final subscription = Subscription(response["id"] as int, eventHandler, this);
      _subscriptions[subscription] = token;
      return subscription;
    } on Object {
      _eventsByToken.remove(token);
      rethrow;
    }
  }

  Future<void> unsubscribe(Subscription subscription) async {
    final tokens = _subscriptions[subscription];

    if (tokens == null) {
      return;
    }

    await _request("unsubscribe", {"id": subscription.subscriptionID, "token": tokens});
    _subscriptions.remove(subscription);
    _eventsByToken.remove(tokens);
  }

  Future<void> _publish(
    String topic, {
    List<dynamic>? args,
    Map<String, dynamic>? kwargs,
    Map<String, dynamic>? options,
  }) async {
    await _request("publish", {"uri": topic, "args": args, "kwargs": kwargs, "options": options});
  }

  Future<Result> callProgress(
    String procedure,
    Function(Result result) progressHandler, {
    List<dynamic>? args,
    Map<String, dynamic>? kwargs,
    Map<String, dynamic>? options,
  }) => _stream(procedure, args: args, kwargs: kwargs, options: options, receive: progressHandler);

  Future<Result> callProgressive(String procedure, Progress Function() progressFunc) =>
      _stream(procedure, send: progressFunc);

  Future<Result> callProgressiveProgress(
    String procedure,
    Future<Progress> Function() progressSender,
    Function(Result result) progressReceiver,
  ) => _stream(procedure, send: progressSender, receive: progressReceiver);

  Future<Result> _stream(
    String uri, {
    List<dynamic>? args,
    Map<String, dynamic>? kwargs,
    Map<String, dynamic>? options,
    FutureOr<Progress> Function()? send,
    Function(Result)? receive,
  }) async {
    final first = await send?.call();
    final response = fields(
      await _request("call_open", {
        "uri": uri,
        "args": first?.args ?? args,
        "kwargs": first?.kwargs ?? kwargs,
        "options": {...?options, ...?first?.options},
      }),
    );
    final handle = response["handle"] as int;
    var finished = false;
    ({Object error, StackTrace stack})? failure;
    Future<void> produce() async {
      try {
        var more = first?.options["progress"] == true;

        while (more && !finished) {
          final next = await send!();

          if (finished) {
            return;
          }

          more = next.options["progress"] == true;
          await native.executeAsync("call_input", {
            "handle": handle,
            "args": next.args,
            "kwargs": next.kwargs,
            "more": more,
          });
        }
      } on Object catch (error, stack) {
        failure = (error: error, stack: stack);
        native.execute("close", {"handle": handle});
      }
    }

    unawaited(produce());

    try {
      while (true) {
        final result = resultFromNative(await native.executeAsync("call_next", {"handle": handle}));

        if (result.details["progress"] != true) {
          return result;
        }
        await Future.sync(() => receive?.call(result));
      }
    } on Object {
      if (failure case final error?) {
        Error.throwWithStackTrace(error.error, error.stack);
      }

      rethrow;
    } finally {
      finished = true;
      native.execute("close", {"handle": handle});
    }
  }

  Future<void> close() => _closing ??= _close();

  Future<void> _close() async {
    try {
      if (_connected) {
        await _request("leave").timeout(const Duration(seconds: 10));
      }
    } finally {
      _finalizer.detach(this);
      await _cleanup.close();
      await _reader;
      _registrations.clear();
      _registrationTokens.clear();
      _eventsByToken.clear();
      _subscriptions.clear();
    }
  }
}

// This token must never retain its Session: it is held by the GC finalizer.
class _SessionCleanup {
  _SessionCleanup(this.handle, this.callbacks, this.closeBase);

  final int handle;
  final HostCallbacks? callbacks;
  final Future<void> Function()? closeBase;
  Future<void>? _closing;

  Future<void> close() => _closing ??= _close();

  Future<void> _close() async {
    try {
      native.execute("close", {"handle": handle});
    } finally {
      try {
        await closeBase?.call();
      } finally {
        await callbacks?.close();
      }
    }
  }
}

/// Internal bridge access for transport adapters; not exported by the public barrel.
int nativeSessionHandle(Session session) => session._handle;
