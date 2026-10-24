import "dart:async";

import "package:xconn_core/src/callbacks.dart";
import "package:xconn_core/src/native.dart" as native;

final _finalizer = Finalizer<int>((handle) {
  native.execute("close", {"handle": handle});
});

/// Native requests take durations as fractional seconds.
double seconds(Duration duration) => duration.inMicroseconds / Duration.microsecondsPerSecond;

/// Ownership common to native transport connections and independent streams.
abstract class NativeTransportResource {
  NativeTransportResource(this.handle) {
    _finalizer.attach(this, handle, detach: this);
  }

  final int handle;
  bool _closed = false;

  Future<Object?> request(String method, [Map<String, Object?> args = const {}]) async {
    if (_closed) {
      throw StateError("Transport resource closed");
    }
    return native.executeAsync(method, {"handle": handle, ...args});
  }

  Future<void> close() async {
    if (!_closed) {
      _closed = true;
      _finalizer.detach(this);
      native.execute("close", {"handle": handle});
    }
  }
}

/// A transport attempt owns any native handle until delivered to its caller.
Future<T> dialNative<T extends NativeTransportResource>(
  String method,
  Map<String, Object?> args,
  Duration timeout,
  T Function(Map<String, dynamic>) create,
) {
  if (timeout.isNegative) {
    throw ArgumentError.value(timeout, "timeout", "must not be negative");
  }
  final operation = native.NativeOperation(method, {...args, "operation_timeout": seconds(timeout)});
  T? resource;
  var cancelled = false;
  final future = operation.future.then((value) {
    final response = fields(value);
    if (cancelled) {
      native.execute("close", {"handle": response["handle"]});
      throw StateError("Connection attempt cancelled");
    }
    return resource = create(response);
  });
  return CancellableFuture<T>(future, () {
    cancelled = true;
    operation.cancel();
    resource?.close().ignore();
  });
}

// Future.timeout normally leaves its source running. A connect attempt owns native
// resources, so direct timeouts must relinquish that ownership as well.
class CancellableFuture<T> implements Future<T> {
  CancellableFuture(this._future, this._cancel);

  final Future<T> _future;
  final void Function() _cancel;

  @override
  Stream<T> asStream() => _future.asStream();

  @override
  Future<T> catchError(Function onError, {bool Function(Object)? test}) => _future.catchError(onError, test: test);

  @override
  Future<R> then<R>(FutureOr<R> Function(T) onValue, {Function? onError}) => _future.then(onValue, onError: onError);

  @override
  Future<T> whenComplete(FutureOr<void> Function() action) => _future.whenComplete(action);

  @override
  Future<T> timeout(Duration timeLimit, {FutureOr<T> Function()? onTimeout}) => _future.timeout(
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
