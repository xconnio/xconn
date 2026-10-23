import "package:xconn_core/src/protocol.dart";

abstract class Message {
  int messageType();

  List<dynamic> marshal();
}

class _Message implements Message {
  _Message(this._wire) {
    callNative("identify", [_wire]);
  }

  final List<dynamic> _wire;

  @override
  int messageType() => _wire.first as int;

  @override
  List<dynamic> marshal() => List<Object?>.from(_wire);
}

Message toMessage(List<dynamic> wire) => _Message(wire);

class Challenge implements Message {
  Challenge(this.authMethod, this.extra);
  final String authMethod;
  final Map<String, dynamic> extra;

  @override
  int messageType() => 4;

  @override
  List<dynamic> marshal() =>
      (callNative("marshal", [
                4,
                {"authmethod": authMethod, "extra": extra},
              ])!
              as List)
          .cast<Object?>();
}

class Authenticate implements Message {
  Authenticate(this.signature, this.extra);
  final String signature;
  final Map<String, dynamic> extra;
  static Authenticate parse(List<dynamic> wire) {
    final fields = Map<String, Object?>.from(callNative("parse", [5, wire])! as Map);
    return Authenticate(fields["signature"]! as String, Map<String, Object?>.from(fields["extra"]! as Map));
  }

  @override
  int messageType() => 5;

  @override
  List<dynamic> marshal() =>
      (callNative("marshal", [
                5,
                {"signature": signature, "extra": extra},
              ])!
              as List)
          .cast<Object?>();
}
