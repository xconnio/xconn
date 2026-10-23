import "dart:async";

import "package:xconn_core/src/native.dart" as native;
import "package:xconn_core/src/session.dart" show resultFromNative;
import "package:xconn_core/src/types.dart";

/// An owned progressive RPC. Send inputs while consuming results independently.
class Call {
  Call.fromNative(this._handle) {
    _finalizer.attach(this, _handle, detach: this);
  }

  static final _finalizer = Finalizer<int>((handle) => native.execute("close", {"handle": handle}));
  final int _handle;
  bool _closed = false;
  bool _receiving = false;
  Object? _error;
  bool _resultsClaimed = false;

  Future<Result?> receive() async {
    if (_receiving) {
      throw StateError("another receive is already pending");
    }

    _receiving = true;

    try {
      if (_closed) {
        final error = _error;
        _error = null;

        if (error != null) {
          throw error;
        }

        return null;
      }

      Object? response;

      try {
        response = await native.executeAsync("stream_receive", {"handle": _handle});
      } on Object {
        _error = null;
        _complete();
        rethrow;
      }

      if (response == null) {
        _complete();
        return null;
      }

      final result = resultFromNative(response);

      if (!result.progress) {
        _complete();
      }

      return result;
    } finally {
      _receiving = false;
    }
  }

  Future<void> send(List<dynamic> args, [Map<String, dynamic>? kwargs]) => _input(args, kwargs, finish: false);

  Future<void> finish([List<dynamic> args = const [], Map<String, dynamic>? kwargs]) =>
      _input(args, kwargs, finish: true);

  Future<void> _input(List<dynamic> args, Map<String, dynamic>? kwargs, {required bool finish}) async {
    if (_closed) {
      throw StateError("call is finished");
    }

    await native.executeAsync("stream_input", {"handle": _handle, "args": args, "kwargs": kwargs, "finish": finish});
  }

  void _complete() {
    if (_closed) {
      return;
    }

    _closed = true;
    _finalizer.detach(this);
    native.execute("close", {"handle": _handle});
  }

  Future<void> cancel() async {
    if (_closed) {
      return;
    }

    _error = StateError("call cancelled");
    _complete();
  }

  Future<void> close() => cancel();

  /// A single-subscription stream, including the final result. Cancellation closes the call.
  Stream<Result> results() {
    if (_resultsClaimed || _receiving) {
      throw StateError("another result consumer is already active");
    }

    _resultsClaimed = true;
    late StreamController<Result> controller;
    var cancelled = false;
    Completer<void>? resumed;
    Future<void> pump() async {
      try {
        while (!cancelled) {
          await resumed?.future;

          if (cancelled) {
            break;
          }

          final result = await receive();

          if (cancelled || result == null) {
            break;
          }

          controller.add(result);
        }
      } on Object catch (error, stack) {
        if (!cancelled) {
          controller.addError(error, stack);
        }
      } finally {
        await close();
        await controller.close();
      }
    }

    controller = StreamController<Result>(
      onListen: () {
        unawaited(pump());
      },
      onPause: () {
        resumed ??= Completer<void>();
      },
      onResume: () {
        resumed?.complete();
        resumed = null;
      },
      onCancel: () async {
        cancelled = true;
        resumed?.complete();
        resumed = null;
        await close();
      },
    );
    return controller.stream;
  }
}
