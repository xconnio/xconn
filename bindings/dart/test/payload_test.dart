import "dart:convert";
import "dart:io";
import "dart:typed_data";

import "package:test/test.dart";
import "package:xconn_core/src/payload.dart";
import "package:xconn_core/xconn_core.dart";

Object? decode(Map<String, dynamic> entry) {
  final value = entry["value"];
  switch (entry["tag"]) {
    case "int":
      return int.parse(value as String);
    case "uint":
      return BigInt.parse(value as String);
    case "bytes":
      return Uint8List.fromList(List<int>.from(value as List));
    case "special":
      return {"nan": double.nan, "infinity": double.infinity, "-infinity": double.negativeInfinity}[value];
    default:
      return value;
  }
}

void main() {
  final cases = jsonDecode(File("../../tests/payload/cases.json").readAsStringSync()) as List;
  for (final raw in cases) {
    final entry = Map<String, dynamic>.from(raw as Map);
    final value = decode(entry);
    for (final payload in <PayloadAccess>[
      Invocation(args: [value], kwargs: {"value": value}),
      Event(args: [value], kwargs: {"value": value}),
      Result(args: [value], kwargs: {"value": value}),
    ]) {
      test("${entry['name']} ${payload.runtimeType}", () {
        if ((entry["accept"] as List).contains("string")) {
          expect(payload.argString(0), equals(value));
          expect(payload.argStringOr(0, "fallback"), equals(value));
        } else {
          expect(() => payload.argString(0), throwsA(isA<PayloadError>()));
          expect(payload.argStringOr(0, "fallback"), equals("fallback"));
        }
        expect(
          () => payload.argString(-1),
          throwsA(isA<PayloadError>().having((error) => error.kind, "kind", "missing")),
        );
        expect(payload.argStringOr(-1, "fallback"), equals("fallback"));
        if ((entry["accept"] as List).contains("string")) {
          expect(payload.kwargString("value"), equals(value));
          expect(payload.kwargStringOr("value", "fallback"), equals(value));
        } else {
          expect(() => payload.kwargString("value"), throwsA(isA<PayloadError>()));
          expect(payload.kwargStringOr("value", "fallback"), equals("fallback"));
        }
        expect(
          () => payload.kwargString("absent"),
          throwsA(isA<PayloadError>().having((error) => error.kind, "kind", "missing")),
        );
        expect(payload.kwargStringOr("absent", "fallback"), equals("fallback"));
        if ((entry["accept"] as List).contains("bool")) {
          expect(payload.argBool(0), equals(value));
          expect(payload.argBoolOr(0, false), equals(value));
        } else {
          expect(() => payload.argBool(0), throwsA(isA<PayloadError>()));
          expect(payload.argBoolOr(0, false), equals(false));
        }
        expect(
          () => payload.argBool(-1),
          throwsA(isA<PayloadError>().having((error) => error.kind, "kind", "missing")),
        );
        expect(payload.argBoolOr(-1, false), equals(false));
        if ((entry["accept"] as List).contains("bool")) {
          expect(payload.kwargBool("value"), equals(value));
          expect(payload.kwargBoolOr("value", false), equals(value));
        } else {
          expect(() => payload.kwargBool("value"), throwsA(isA<PayloadError>()));
          expect(payload.kwargBoolOr("value", false), equals(false));
        }
        expect(
          () => payload.kwargBool("absent"),
          throwsA(isA<PayloadError>().having((error) => error.kind, "kind", "missing")),
        );
        expect(payload.kwargBoolOr("absent", false), equals(false));
        if ((entry["accept"] as List).contains("int")) {
          expect(payload.argInt(0), equals((value is BigInt ? value : BigInt.from(value! as num)).toInt()));
          expect(payload.argIntOr(0, 7), equals((value is BigInt ? value : BigInt.from(value! as num)).toInt()));
        } else {
          expect(() => payload.argInt(0), throwsA(isA<PayloadError>()));
          expect(payload.argIntOr(0, 7), equals(7));
        }
        expect(() => payload.argInt(-1), throwsA(isA<PayloadError>().having((error) => error.kind, "kind", "missing")));
        expect(payload.argIntOr(-1, 7), equals(7));
        if ((entry["accept"] as List).contains("int")) {
          expect(payload.kwargInt("value"), equals((value is BigInt ? value : BigInt.from(value! as num)).toInt()));
          expect(
            payload.kwargIntOr("value", 7),
            equals((value is BigInt ? value : BigInt.from(value! as num)).toInt()),
          );
        } else {
          expect(() => payload.kwargInt("value"), throwsA(isA<PayloadError>()));
          expect(payload.kwargIntOr("value", 7), equals(7));
        }
        expect(
          () => payload.kwargInt("absent"),
          throwsA(isA<PayloadError>().having((error) => error.kind, "kind", "missing")),
        );
        expect(payload.kwargIntOr("absent", 7), equals(7));
        if ((entry["accept"] as List).contains("uint")) {
          expect(payload.argUInt(0), equals(value is BigInt ? value : BigInt.from(value! as num)));
          expect(payload.argUIntOr(0, BigInt.from(7)), equals(value is BigInt ? value : BigInt.from(value! as num)));
        } else {
          expect(() => payload.argUInt(0), throwsA(isA<PayloadError>()));
          expect(payload.argUIntOr(0, BigInt.from(7)), equals(BigInt.from(7)));
        }
        expect(
          () => payload.argUInt(-1),
          throwsA(isA<PayloadError>().having((error) => error.kind, "kind", "missing")),
        );
        expect(payload.argUIntOr(-1, BigInt.from(7)), equals(BigInt.from(7)));
        if ((entry["accept"] as List).contains("uint")) {
          expect(payload.kwargUInt("value"), equals(value is BigInt ? value : BigInt.from(value! as num)));
          expect(
            payload.kwargUIntOr("value", BigInt.from(7)),
            equals(value is BigInt ? value : BigInt.from(value! as num)),
          );
        } else {
          expect(() => payload.kwargUInt("value"), throwsA(isA<PayloadError>()));
          expect(payload.kwargUIntOr("value", BigInt.from(7)), equals(BigInt.from(7)));
        }
        expect(
          () => payload.kwargUInt("absent"),
          throwsA(isA<PayloadError>().having((error) => error.kind, "kind", "missing")),
        );
        expect(payload.kwargUIntOr("absent", BigInt.from(7)), equals(BigInt.from(7)));
        if ((entry["accept"] as List).contains("float")) {
          expect(payload.argFloat(0), equals(value is BigInt ? value.toDouble() : (value! as num).toDouble()));
          expect(payload.argFloatOr(0, 7), equals(value is BigInt ? value.toDouble() : (value! as num).toDouble()));
        } else {
          expect(() => payload.argFloat(0), throwsA(isA<PayloadError>()));
          expect(payload.argFloatOr(0, 7), equals(7.0));
        }
        expect(
          () => payload.argFloat(-1),
          throwsA(isA<PayloadError>().having((error) => error.kind, "kind", "missing")),
        );
        expect(payload.argFloatOr(-1, 7), equals(7.0));
        if ((entry["accept"] as List).contains("float")) {
          expect(payload.kwargFloat("value"), equals(value is BigInt ? value.toDouble() : (value! as num).toDouble()));
          expect(
            payload.kwargFloatOr("value", 7),
            equals(value is BigInt ? value.toDouble() : (value! as num).toDouble()),
          );
        } else {
          expect(() => payload.kwargFloat("value"), throwsA(isA<PayloadError>()));
          expect(payload.kwargFloatOr("value", 7), equals(7.0));
        }
        expect(
          () => payload.kwargFloat("absent"),
          throwsA(isA<PayloadError>().having((error) => error.kind, "kind", "missing")),
        );
        expect(payload.kwargFloatOr("absent", 7), equals(7.0));
        if ((entry["accept"] as List).contains("bytes")) {
          expect(payload.argBytes(0), equals(value));
          expect(payload.argBytesOr(0, Uint8List(0)), equals(value));
        } else {
          expect(() => payload.argBytes(0), throwsA(isA<PayloadError>()));
          expect(payload.argBytesOr(0, Uint8List(0)), equals(Uint8List(0)));
        }
        expect(
          () => payload.argBytes(-1),
          throwsA(isA<PayloadError>().having((error) => error.kind, "kind", "missing")),
        );
        expect(payload.argBytesOr(-1, Uint8List(0)), equals(Uint8List(0)));
        if ((entry["accept"] as List).contains("bytes")) {
          expect(payload.kwargBytes("value"), equals(value));
          expect(payload.kwargBytesOr("value", Uint8List(0)), equals(value));
        } else {
          expect(() => payload.kwargBytes("value"), throwsA(isA<PayloadError>()));
          expect(payload.kwargBytesOr("value", Uint8List(0)), equals(Uint8List(0)));
        }
        expect(
          () => payload.kwargBytes("absent"),
          throwsA(isA<PayloadError>().having((error) => error.kind, "kind", "missing")),
        );
        expect(payload.kwargBytesOr("absent", Uint8List(0)), equals(Uint8List(0)));
        if ((entry["accept"] as List).contains("list")) {
          expect(payload.argList(0), equals(value));
          expect(payload.argListOr(0, <dynamic>[]), equals(value));
        } else {
          expect(() => payload.argList(0), throwsA(isA<PayloadError>()));
          expect(payload.argListOr(0, <dynamic>[]), equals(<dynamic>[]));
        }
        expect(
          () => payload.argList(-1),
          throwsA(isA<PayloadError>().having((error) => error.kind, "kind", "missing")),
        );
        expect(payload.argListOr(-1, <dynamic>[]), equals(<dynamic>[]));
        if ((entry["accept"] as List).contains("list")) {
          expect(payload.kwargList("value"), equals(value));
          expect(payload.kwargListOr("value", <dynamic>[]), equals(value));
        } else {
          expect(() => payload.kwargList("value"), throwsA(isA<PayloadError>()));
          expect(payload.kwargListOr("value", <dynamic>[]), equals(<dynamic>[]));
        }
        expect(
          () => payload.kwargList("absent"),
          throwsA(isA<PayloadError>().having((error) => error.kind, "kind", "missing")),
        );
        expect(payload.kwargListOr("absent", <dynamic>[]), equals(<dynamic>[]));
        if ((entry["accept"] as List).contains("dict")) {
          expect(payload.argMap(0), equals(value));
          expect(payload.argMapOr(0, <String, dynamic>{}), equals(value));
        } else {
          expect(() => payload.argMap(0), throwsA(isA<PayloadError>()));
          expect(payload.argMapOr(0, <String, dynamic>{}), equals(<String, dynamic>{}));
        }
        expect(() => payload.argMap(-1), throwsA(isA<PayloadError>().having((error) => error.kind, "kind", "missing")));
        expect(payload.argMapOr(-1, <String, dynamic>{}), equals(<String, dynamic>{}));
        if ((entry["accept"] as List).contains("dict")) {
          expect(payload.kwargMap("value"), equals(value));
          expect(payload.kwargMapOr("value", <String, dynamic>{}), equals(value));
        } else {
          expect(() => payload.kwargMap("value"), throwsA(isA<PayloadError>()));
          expect(payload.kwargMapOr("value", <String, dynamic>{}), equals(<String, dynamic>{}));
        }
        expect(
          () => payload.kwargMap("absent"),
          throwsA(isA<PayloadError>().having((error) => error.kind, "kind", "missing")),
        );
        expect(payload.kwargMapOr("absent", <String, dynamic>{}), equals(<String, dynamic>{}));
        expect(payload.requireArg(0), same(value));
      });
    }
  }
}
