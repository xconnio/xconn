// Fluent builders retain the established request chaining API.
// ignore_for_file: avoid_returning_this

import "dart:async";

import "package:xconn_core/src/call.dart";
import "package:xconn_core/src/session.dart";
import "package:xconn_core/src/types.dart";

/// A request is one operation, even when its Future has multiple listeners.
abstract class _Request<T> implements Future<T> {
  Future<T>? _completion;

  Future<T> _run();

  Future<T> execute() => _completion ??= Future<T>.sync(_run);

  @override
  Stream<T> asStream() => execute().asStream();

  @override
  Future<T> catchError(Function onError, {bool Function(Object error)? test}) =>
      execute().catchError(onError, test: test);

  @override
  Future<R> then<R>(FutureOr<R> Function(T value) onValue, {Function? onError}) =>
      execute().then(onValue, onError: onError);

  @override
  Future<T> timeout(Duration timeLimit, {FutureOr<T> Function()? onTimeout}) =>
      execute().timeout(timeLimit, onTimeout: onTimeout);

  @override
  Future<T> whenComplete(FutureOr<void> Function() action) => execute().whenComplete(action);
}

class CallRequest extends _Request<Result> {
  CallRequest(this.procedure, {this.session});

  final String procedure;
  Session? session;

  List<dynamic>? _args;
  Map<String, dynamic>? _kwargs;
  Map<String, dynamic>? _options;
  Function(Result)? _progressReceiver;
  FutureOr<Progress> Function()? _progressSender;

  List<dynamic>? get arguments => _args;
  Map<String, dynamic>? get keywordArguments => _kwargs;
  Map<String, dynamic>? get requestOptions => _options;
  Function(Result)? get progressReceiver => _progressReceiver;
  FutureOr<Progress> Function()? get progressSender => _progressSender;

  CallRequest arg(Object? value) {
    (_args ??= []).add(value);
    return this;
  }

  CallRequest args(List<dynamic> values) {
    (_args ??= []).addAll(values);
    return this;
  }

  CallRequest kwarg(String key, Object? value) {
    (_kwargs ??= {})[key] = value;
    return this;
  }

  CallRequest kwargs(Map<String, dynamic> values) {
    (_kwargs ??= {}).addAll(values);
    return this;
  }

  CallRequest option(String key, Object? value) {
    (_options ??= {})[key] = value;
    return this;
  }

  CallRequest options(Map<String, dynamic> values) {
    (_options ??= {}).addAll(values);
    return this;
  }

  CallRequest onProgress(Function(Result) receiver) {
    _progressReceiver = receiver;
    return this;
  }

  CallRequest onInput(FutureOr<Progress> Function() sender) {
    _progressSender = sender;
    return this;
  }

  Future<Call> stream({
    bool progressiveInput = false,
    bool progressiveResults = true,
    int bufferSize = 32,
    Duration? timeout,
  }) {
    final sess = session;

    if (sess == null) {
      throw StateError("CallRequest is not attached to a session");
    }

    if (_progressReceiver != null || _progressSender != null) {
      throw StateError("streaming calls use send/finish/receive instead of callbacks");
    }

    return sess.openStream(
      this,
      progressiveInput: progressiveInput,
      progressiveResults: progressiveResults,
      bufferSize: bufferSize,
      timeout: timeout,
    );
  }

  @override
  Future<Result> _run() {
    final sess = session;

    if (sess == null) {
      throw StateError("CallRequest is not attached to a session");
    }

    return sess.executeCall(this);
  }
}

class RegisterRequest extends _Request<Registration> {
  RegisterRequest(this.procedure, this.handler, {this.session});

  final String procedure;
  final Future<Result?> Function(Invocation) handler;
  Session? session;

  Map<String, dynamic>? _options;

  Map<String, dynamic>? get requestOptions => _options;

  RegisterRequest option(String key, Object? value) {
    (_options ??= {})[key] = value;
    return this;
  }

  RegisterRequest options(Map<String, dynamic> values) {
    (_options ??= {}).addAll(values);
    return this;
  }

  RegisterRequest match(String value) {
    return option("match", value);
  }

  RegisterRequest invoke(String value) {
    return option("invoke", value);
  }

  @override
  Future<Registration> _run() {
    final sess = session;

    if (sess == null) {
      throw StateError("RegisterRequest is not attached to a session");
    }

    return sess.executeRegister(this);
  }
}

class PublishRequest extends _Request<void> {
  PublishRequest(this.topic, {this.session});

  final String topic;
  Session? session;

  List<dynamic>? _args;
  Map<String, dynamic>? _kwargs;
  Map<String, dynamic>? _options;

  List<dynamic>? get arguments => _args;
  Map<String, dynamic>? get keywordArguments => _kwargs;
  Map<String, dynamic>? get requestOptions => _options;

  PublishRequest arg(Object? value) {
    (_args ??= []).add(value);
    return this;
  }

  PublishRequest args(List<dynamic> values) {
    (_args ??= []).addAll(values);
    return this;
  }

  PublishRequest kwarg(String key, Object? value) {
    (_kwargs ??= {})[key] = value;
    return this;
  }

  PublishRequest kwargs(Map<String, dynamic> values) {
    (_kwargs ??= {}).addAll(values);
    return this;
  }

  PublishRequest option(String key, Object? value) {
    (_options ??= {})[key] = value;
    return this;
  }

  PublishRequest options(Map<String, dynamic> values) {
    (_options ??= {}).addAll(values);
    return this;
  }

  @override
  Future<void> _run() {
    final sess = session;

    if (sess == null) {
      throw StateError("PublishRequest is not attached to a session");
    }

    return sess.executePublish(this);
  }
}

class SubscribeRequest extends _Request<Subscription> {
  SubscribeRequest(this.topic, this.handler, {this.session});

  final String topic;
  final void Function(Event) handler;
  Session? session;

  Map<String, dynamic>? _options;

  Map<String, dynamic>? get requestOptions => _options;

  SubscribeRequest option(String key, Object? value) {
    (_options ??= {})[key] = value;
    return this;
  }

  SubscribeRequest options(Map<String, dynamic> values) {
    (_options ??= {}).addAll(values);
    return this;
  }

  SubscribeRequest match(String value) {
    return option("match", value);
  }

  @override
  Future<Subscription> _run() {
    final sess = session;

    if (sess == null) {
      throw StateError("SubscribeRequest is not attached to a session");
    }

    return sess.executeSubscribe(this);
  }
}
