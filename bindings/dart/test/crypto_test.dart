import "dart:convert";
import "dart:io";
import "dart:typed_data";

import "package:test/test.dart";
import "package:xconn_core/crypto.dart" as crypto;

void main() {
  test("packet cryptography matches the independent Go implementation", () {
    final fixture = jsonDecode(File("../../tests/crypto/go-reference.json").readAsStringSync()) as Map;
    Uint8List data(String key) {
      final value = fixture[key] as String;
      return Uint8List.fromList([
        for (var index = 0; index < value.length; index += 2) int.parse(value.substring(index, index + 2), radix: 16),
      ]);
    }

    final secret = crypto.sharedSecret(data("private"), data("public"));
    expect(secret, data("shared"));
    final key = crypto.deriveKey(secret, data("info"));
    expect(key, data("key"));
    expect(crypto.decrypt(data("packet"), key, additionalData: data("aad")), data("plaintext"));
    final packet = crypto.encrypt(data("plaintext"), key, additionalData: data("aad"));
    expect(crypto.decrypt(packet, key, additionalData: data("aad")), data("plaintext"));
    expect(() => crypto.decrypt(packet, key), throwsException);
    final pair = crypto.generateKeyPair();
    expect(pair.publicKey, hasLength(32));
    expect(pair.privateKey, hasLength(32));
    expect(() => crypto.sharedSecret(pair.privateKey, Uint8List(32)), throwsException);
  });
}
