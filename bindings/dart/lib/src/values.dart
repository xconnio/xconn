import "dart:convert";
import "dart:typed_data";

const _maxBytes = 64 * 1024 * 1024;
const _maxDepth = 64;

/// Encode values for the FFI boundary; WAMP serialization belongs to Rust.
Uint8List encodeValue(Object? value) {
  final output = BytesBuilder(copy: false);

  void addLength(int length) {
    final bytes = ByteData(4)..setUint32(0, length, Endian.little);
    output.add(bytes.buffer.asUint8List());
  }

  void write(Object? item, int depth) {
    if (depth > _maxDepth) {
      throw const FormatException("Value nesting exceeds 64");
    }

    switch (item) {
      case null:
        output.addByte(0);
      case bool():
        output.addByte(item ? 2 : 1);
      case int():
        output.addByte(3);
        final bytes = ByteData(8)..setInt64(0, item, Endian.little);
        output.add(bytes.buffer.asUint8List());
      case BigInt():
        if (item.isNegative || item.bitLength > 64) {
          throw const FormatException("Unsigned integer exceeds 64 bits");
        }

        output
          ..addByte(4)
          ..add(List<int>.generate(8, (index) => ((item >> (index * 8)) & BigInt.from(255)).toInt()));
      case double():
        if (!item.isFinite) {
          throw const FormatException("Non-finite numbers are unsupported");
        }

        output.addByte(5);
        final bytes = ByteData(8)..setFloat64(0, item, Endian.little);
        output.add(bytes.buffer.asUint8List());
      case String():
        final bytes = utf8.encode(item);
        output.addByte(6);
        addLength(bytes.length);
        output.add(bytes);
      case Uint8List():
        output.addByte(7);
        addLength(item.length);
        output.add(item);
      case List<Object?>():
        output.addByte(8);
        addLength(item.length);

        for (final child in item) {
          write(child, depth + 1);
        }
      case Map<Object?, Object?>():
        output.addByte(9);
        addLength(item.length);

        for (final entry in item.entries) {
          final key = entry.key;

          if (key is! String) {
            throw ArgumentError.value(key, "key", "Native map keys must be strings");
          }

          final keyBytes = utf8.encode(key);
          addLength(keyBytes.length);
          output.add(keyBytes);
          write(entry.value, depth + 1);
        }
      default:
        throw ArgumentError.value(item, "value", "Unsupported native value");
    }

    if (output.length > _maxBytes) {
      throw const FormatException("Value transfer exceeds 64 MiB");
    }
  }

  write(value, 0);
  return output.takeBytes();
}

/// Decode the Rust FFI value format, preserving binary and integer payloads.
Object? decodeValue(Uint8List bytes) {
  if (bytes.length > _maxBytes) {
    throw const FormatException("Value transfer exceeds 64 MiB");
  }

  final data = ByteData.sublistView(bytes);
  var offset = 0;

  int take(int length) {
    if (length < 0 || length > bytes.length - offset) {
      throw const FormatException("Truncated native value");
    }

    final start = offset;
    offset += length;
    return start;
  }

  int length() => data.getUint32(take(4), Endian.little);

  String readString() {
    final size = length();
    final start = take(size);
    return utf8.decode(Uint8List.sublistView(bytes, start, start + size));
  }

  Object? read(int depth) {
    if (depth > _maxDepth) {
      throw const FormatException("Value nesting exceeds 64");
    }

    final tag = bytes[take(1)];
    switch (tag) {
      case 0:
        return null;
      case 1:
        return false;
      case 2:
        return true;
      case 3:
        return data.getInt64(take(8), Endian.little);
      case 4:
        final value = data.getUint64(take(8), Endian.little);
        return value < 0 ? BigInt.from(value) + (BigInt.one << 64) : value;
      case 5:
        final value = data.getFloat64(take(8), Endian.little);

        if (!value.isFinite) {
          throw const FormatException("Non-finite native number");
        }

        return value;
      case 6:
        return readString();
      case 7:
        final size = length();
        final start = take(size);
        return Uint8List.fromList(Uint8List.sublistView(bytes, start, start + size));
      case 8:
        final size = length();

        if (size > bytes.length - offset) {
          throw const FormatException("Invalid native list length");
        }
        return List<Object?>.generate(size, (_) => read(depth + 1));
      case 9:
        final size = length();

        if (size > (bytes.length - offset) ~/ 5) {
          throw const FormatException("Invalid native map length");
        }

        final result = <String, Object?>{};

        for (var index = 0; index < size; index++) {
          final key = readString();

          if (result.containsKey(key)) {
            throw const FormatException("Duplicate native map key");
          }

          result[key] = read(depth + 1);
        }

        return result;
      default:
        throw FormatException("Unknown native value tag $tag");
    }
  }

  final result = read(0);

  if (offset != bytes.length) {
    throw const FormatException("Trailing native value data");
  }

  return result;
}
