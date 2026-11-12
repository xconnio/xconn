import "package:xconn/src/native.dart" as native;

Object? callNative(String name, [List<Object?> args = const []]) =>
    native.execute("protocol_call", {"name": name, "args": args});
