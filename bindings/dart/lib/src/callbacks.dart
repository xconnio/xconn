import "dart:async";

import "package:xconn_core/src/native.dart" as native;

Map<String, dynamic> fields(Object? value) => Map<String, Object?>.from(value! as Map);

/// Own a Rust callback queue; application callbacks execute in their Dart isolate.
class HostCallbacks {
  HostCallbacks(this._handler) : handle = fields(native.execute("callbacks_new"))["handle"] as int {
    _reader = _read();
  }

  final int handle;
  final FutureOr<Object?> Function(String, Object?) _handler;
  late final Future<void> _reader;
  Object? failure;
  StackTrace? failureStack;
  bool _closed = false;

  Future<void> _read() async {
    try {
      while (!_closed) {
        final event = await native.executeAsync("callbacks_next", {"handle": handle});

        if (event == null) {
          return;
        }

        unawaited(_respond(fields(event)));
      }
    } on Exception {
      if (!_closed) {
        rethrow;
      }
    }
  }

  Future<void> _respond(Map<String, dynamic> event) async {
    final response = <String, Object?>{"handle": handle, "id": event["id"]};

    try {
      response["value"] = await _handler(event["method"] as String, event["value"]);
    } on Object catch (error, stack) {
      failure = error;
      failureStack = stack;
      response["error"] = error.toString();
    }

    try {
      await native.executeAsync("callbacks_reply", response);
    } on Exception {
      if (!_closed) {
        rethrow;
      }
    }
  }

  Future<void> close() async {
    if (_closed) {
      return _reader;
    }

    _closed = true;
    native.execute("close", {"handle": handle});
    await _reader;
  }
}
