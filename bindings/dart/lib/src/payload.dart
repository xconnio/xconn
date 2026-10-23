import "dart:typed_data";

class PayloadError implements Exception {
  PayloadError(this.kind, this.message);
  final String kind;
  final String message;

  @override
  String toString() => "PayloadError($kind): $message";
}

Object _convert(Object? value, String kind) {
  if (value == null) {
    throw PayloadError("null", "Payload value is null");
  }

  if (kind == "string" && value is String) {
    return value;
  }

  if (kind == "bool" && value is bool) {
    return value;
  }

  if (["int", "uint", "float"].contains(kind) && (value is num || value is BigInt)) {
    if (kind == "float") {
      final result = value is BigInt ? value.toDouble() : (value as num).toDouble();

      if (result.isFinite) {
        return result;
      }

      throw PayloadError("range", "Numeric payload must be finite");
    }

    BigInt result;

    if (value is double) {
      if (!value.isFinite || value.abs() > 9007199254740991) {
        throw PayloadError("range", "Integer float must be finite and safely representable");
      }

      if (value.truncateToDouble() != value) {
        throw PayloadError("type", "Expected an integer");
      }

      result = BigInt.from(value);
    } else {
      result = value is BigInt ? value : BigInt.from(value as int);
    }

    final lower = kind == "int" ? -(BigInt.one << 63) : BigInt.zero;
    final upper = kind == "int" ? BigInt.one << 63 : BigInt.one << 64;

    if (result < lower || result >= upper) {
      throw PayloadError("range", "Integer payload is out of range");
    }

    return kind == "int" ? result.toInt() : result;
  }

  if (kind == "bytes" && value is Uint8List) {
    return value;
  }

  if (kind == "list" && value is List && value is! Uint8List) {
    return value;
  }
  if (kind == "dict" && value is Map && value.keys.every((key) => key is String)) {
    return Map<String, Object?>.from(value);
  }

  throw PayloadError("type", "Expected $kind payload");
}

mixin PayloadAccess {
  List<dynamic> get args;
  Map<String, dynamic> get kwargs;

  dynamic requireArg(int index) {
    if (index < 0 || index >= args.length) {
      throw PayloadError("missing", "Missing positional argument $index");
    }

    return args[index];
  }

  dynamic requireKwarg(String key) {
    if (!kwargs.containsKey(key)) {
      throw PayloadError("missing", "Missing keyword argument $key");
    }

    return kwargs[key];
  }

  dynamic arg(int index) => index >= 0 && index < args.length ? args[index] : null;

  dynamic kwarg(String key) => kwargs[key];

  Object? argOr(int index, Object? fallback) => arg(index) ?? fallback;

  Object? kwargOr(String key, Object? fallback) => kwarg(key) ?? fallback;

  T? argAt<T>(int index) {
    final value = arg(index);
    return value is T ? value : null;
  }

  T? kwargAt<T>(String key) {
    final value = kwarg(key);
    return value is T ? value : null;
  }

  String argString(int index) => _convert(requireArg(index), "string") as String;

  String argStringOr(int index, String fallback) {
    try {
      return argString(index);
    } on PayloadError {
      return fallback;
    }
  }

  bool argBool(int index) => _convert(requireArg(index), "bool") as bool;

  // ignore: avoid_positional_boolean_parameters
  bool argBoolOr(int index, bool fallback) {
    try {
      return argBool(index);
    } on PayloadError {
      return fallback;
    }
  }

  int argInt(int index) => _convert(requireArg(index), "int") as int;

  int argIntOr(int index, int fallback) {
    try {
      return argInt(index);
    } on PayloadError {
      return fallback;
    }
  }

  BigInt argUInt(int index) => _convert(requireArg(index), "uint") as BigInt;

  BigInt argUIntOr(int index, BigInt fallback) {
    try {
      return argUInt(index);
    } on PayloadError {
      return fallback;
    }
  }

  double argFloat(int index) => _convert(requireArg(index), "float") as double;

  double argFloatOr(int index, double fallback) {
    try {
      return argFloat(index);
    } on PayloadError {
      return fallback;
    }
  }

  Uint8List argBytes(int index) => _convert(requireArg(index), "bytes") as Uint8List;

  Uint8List argBytesOr(int index, Uint8List fallback) {
    try {
      return argBytes(index);
    } on PayloadError {
      return fallback;
    }
  }

  List<dynamic> argList(int index) => _convert(requireArg(index), "list") as List<dynamic>;

  List<dynamic> argListOr(int index, List<dynamic> fallback) {
    try {
      return argList(index);
    } on PayloadError {
      return fallback;
    }
  }

  Map<String, dynamic> argMap(int index) => _convert(requireArg(index), "dict") as Map<String, dynamic>;

  Map<String, dynamic> argMapOr(int index, Map<String, dynamic> fallback) {
    try {
      return argMap(index);
    } on PayloadError {
      return fallback;
    }
  }

  String kwargString(String key) => _convert(requireKwarg(key), "string") as String;

  String kwargStringOr(String key, String fallback) {
    try {
      return kwargString(key);
    } on PayloadError {
      return fallback;
    }
  }

  bool kwargBool(String key) => _convert(requireKwarg(key), "bool") as bool;

  // ignore: avoid_positional_boolean_parameters
  bool kwargBoolOr(String key, bool fallback) {
    try {
      return kwargBool(key);
    } on PayloadError {
      return fallback;
    }
  }

  int kwargInt(String key) => _convert(requireKwarg(key), "int") as int;

  int kwargIntOr(String key, int fallback) {
    try {
      return kwargInt(key);
    } on PayloadError {
      return fallback;
    }
  }

  BigInt kwargUInt(String key) => _convert(requireKwarg(key), "uint") as BigInt;

  BigInt kwargUIntOr(String key, BigInt fallback) {
    try {
      return kwargUInt(key);
    } on PayloadError {
      return fallback;
    }
  }

  double kwargFloat(String key) => _convert(requireKwarg(key), "float") as double;

  double kwargFloatOr(String key, double fallback) {
    try {
      return kwargFloat(key);
    } on PayloadError {
      return fallback;
    }
  }

  Uint8List kwargBytes(String key) => _convert(requireKwarg(key), "bytes") as Uint8List;

  Uint8List kwargBytesOr(String key, Uint8List fallback) {
    try {
      return kwargBytes(key);
    } on PayloadError {
      return fallback;
    }
  }

  List<dynamic> kwargList(String key) => _convert(requireKwarg(key), "list") as List<dynamic>;

  List<dynamic> kwargListOr(String key, List<dynamic> fallback) {
    try {
      return kwargList(key);
    } on PayloadError {
      return fallback;
    }
  }

  Map<String, dynamic> kwargMap(String key) => _convert(requireKwarg(key), "dict") as Map<String, dynamic>;

  Map<String, dynamic> kwargMapOr(String key, Map<String, dynamic> fallback) {
    try {
      return kwargMap(key);
    } on PayloadError {
      return fallback;
    }
  }
}
