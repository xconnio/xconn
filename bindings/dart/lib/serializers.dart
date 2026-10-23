import "dart:convert";
import "dart:typed_data";
import "package:xconn_core/messages.dart";
import "package:xconn_core/src/native.dart" as native;
import "package:xconn_core/src/values.dart";

abstract class Serializer {
  Object serialize(Message message);
  Message deserialize(Object payload);
}

abstract class _Serializer implements Serializer {
  _Serializer(this._codec);
  final int _codec;

  @override
  Object serialize(Message message) {
    final result =
        native.execute("protocol", {"operation": 1, "codec": _codec, "input": encodeValue(message.marshal())})!
            as Uint8List;
    return _codec == 1 ? utf8.decode(result) : result;
  }

  @override
  Message deserialize(Object payload) {
    final bytes = payload is String ? utf8.encode(payload) : payload;
    final result = native.execute("protocol", {"operation": 2, "codec": _codec, "input": bytes})! as Uint8List;
    return toMessage((decodeValue(result)! as List).cast<Object?>());
  }
}

class JSONSerializer extends _Serializer {
  JSONSerializer() : super(1);

  @override
  String serialize(Message message) => super.serialize(message) as String;
}

class MsgPackSerializer extends _Serializer {
  MsgPackSerializer() : super(2);

  @override
  Uint8List serialize(Message message) => super.serialize(message) as Uint8List;
}

class CBORSerializer extends _Serializer {
  CBORSerializer() : super(3);

  @override
  Uint8List serialize(Message message) => super.serialize(message) as Uint8List;
}
