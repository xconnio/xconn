import "dart:async";
import "dart:convert";
import "dart:ffi";
import "dart:io";
import "dart:isolate";
import "dart:typed_data";

import "package:ffi/ffi.dart";
import "package:xconn_core/src/exception.dart";
import "package:xconn_core/src/values.dart";

final class _NativeResult extends Struct {
  @Uint32()
  external int status;
  external Pointer<Uint8> data;
  @IntPtr()
  external int length;
}

// Match the full ABI-v2 Dart_CObject union, including its largest member.
// The integer variant alone has the right offset but an undersized allocation.
final class _ExternalTypedData extends Struct {
  @Int32()
  external int type;
  @IntPtr()
  external int length;
  external Pointer<Void> data;
  external Pointer<Void> peer;
  external Pointer<Void> callback;
}

final class _ObjectValue extends Union {
  @Int64()
  external int integer;
  external _ExternalTypedData externalData;
}

final class _IntegerMessage extends Struct {
  @Int32()
  external int type;
  external _ObjectValue value;
}

@Native<Pointer<_NativeResult> Function(Uint32, Uint64, Pointer<Uint8>, IntPtr)>(symbol: "xc_process")
external Pointer<_NativeResult> _assetProcess(int operation, int handle, Pointer<Uint8> data, int length);

@Native<Void Function(Pointer<_NativeResult>)>(symbol: "xc_result_free")
external void _assetFree(Pointer<_NativeResult> result);

@Native<Uint32 Function()>(symbol: "xc_abi_version")
external int _assetVersion();

@Native<
  Void Function(
    Uint64,
    Int64,
    Pointer<Void>,
    Pointer<NativeFunction<Int8 Function(Int64, Pointer<Dart_CObject>)>>,
    Pointer<NativeFunction<Void Function(Pointer<Void>)>>,
  )
>(symbol: "xc_notify")
external void _assetNotify(
  int handle,
  int port,
  Pointer<Void> message,
  Pointer<NativeFunction<Int8 Function(Int64, Pointer<Dart_CObject>)>> post,
  Pointer<NativeFunction<Void Function(Pointer<Void>)>> free,
);

DynamicLibrary? _overrideLibrary() {
  final path = Platform.environment["XCONN_LIBRARY"];
  return path == null ? null : DynamicLibrary.open(path);
}

final DynamicLibrary? _library = _overrideLibrary();
final _process =
    _library?.lookupFunction<
      Pointer<_NativeResult> Function(Uint32, Uint64, Pointer<Uint8>, IntPtr),
      Pointer<_NativeResult> Function(int, int, Pointer<Uint8>, int)
    >("xc_process") ??
    _assetProcess;
final _free =
    _library?.lookupFunction<Void Function(Pointer<_NativeResult>), void Function(Pointer<_NativeResult>)>(
      "xc_result_free",
    ) ??
    _assetFree;
final _notify =
    _library?.lookupFunction<
      Void Function(
        Uint64,
        Int64,
        Pointer<Void>,
        Pointer<NativeFunction<Int8 Function(Int64, Pointer<Dart_CObject>)>>,
        Pointer<NativeFunction<Void Function(Pointer<Void>)>>,
      ),
      void Function(
        int,
        int,
        Pointer<Void>,
        Pointer<NativeFunction<Int8 Function(Int64, Pointer<Dart_CObject>)>>,
        Pointer<NativeFunction<Void Function(Pointer<Void>)>>,
      )
    >("xc_notify") ??
    _assetNotify;

final _version = _library?.lookupFunction<Uint32 Function(), int Function()>("xc_abi_version")() ?? _assetVersion();

ReceivePort? _port;
final _pending = <int, Completer<Object?>>{};

Object? _invoke(int operation, int handle, [Object? value]) {
  if (_version != 1 || NativeApi.majorVersion != 2) {
    throw StateError("Unsupported native ABI: xconn $_version, Dart ${NativeApi.majorVersion}");
  }
  final input = value == null ? Uint8List(0) : encodeValue(value);
  return using((arena) {
    final buffer = arena<Uint8>(input.isEmpty ? 1 : input.length);
    buffer.asTypedList(input.length).setAll(0, input);
    final result = _process(operation, handle, buffer, input.length);

    if (result == nullptr) {
      throw StateError("Rust returned a null result");
    }

    try {
      final output = Uint8List.fromList(result.ref.data.asTypedList(result.ref.length));

      if (result.ref.status != 0) {
        throw ProtocolError(utf8.decode(output));
      }

      return decodeValue(output);
    } finally {
      _free(result);
    }
  });
}

Object? _checked(Object? value) {
  if (value case {"error": final Map<Object?, Object?> error}) {
    throw ApplicationError(
      error["uri"]! as String,
      args: (error["args"] as List?)?.cast<Object?>(),
      kwargs: (error["kwargs"] as Map?)?.cast<String, Object?>(),
    );
  }

  return value;
}

void _completed(Object? message) {
  final handle = message! as int;
  final completer = _pending.remove(handle);

  if (completer == null) {
    return;
  }

  try {
    completer.complete(_checked(_invoke(3, handle)));
  } on Object catch (error, stack) {
    completer.completeError(error, stack);
  } finally {
    _invoke(4, handle);

    if (_pending.isEmpty) {
      _port?.close();
      _port = null;
    }
  }
}

/// Run metadata operations without a Dart networking or protocol implementation.
Object? execute(String method, [Map<String, Object?> fields = const {}]) =>
    _checked(_invoke(5, 0, {"method": method, ...fields}));

/// Submit work to Tokio and wake this isolate through a Dart native port.
Future<Object?> executeAsync(String method, [Map<String, Object?> fields = const {}]) {
  return NativeOperation(method, fields).future;
}

/// Own a submitted job until its output is claimed or cancellation releases it.
class NativeOperation {
  NativeOperation(String method, Map<String, Object?> fields)
    : handle = _invoke(7, 0, {"method": method, ...fields})! as int {
    _pending[handle] = _completer;
    _port ??= ReceivePort()..listen(_completed);
    final message = calloc<_IntegerMessage>()
      ..ref.type = 3
      ..ref.value.integer = handle;
    _notify(handle, _port!.sendPort.nativePort, message.cast(), NativeApi.postCObject, calloc.nativeFree);
  }

  final int handle;
  final _completer = Completer<Object?>();
  Future<Object?> get future => _completer.future;

  void cancel() {
    final pending = _pending.remove(handle);
    if (pending == null) {
      return;
    }

    _invoke(4, handle);
    pending.completeError(StateError("Native operation cancelled"));
    if (_pending.isEmpty) {
      _port?.close();
      _port = null;
    }
  }
}
