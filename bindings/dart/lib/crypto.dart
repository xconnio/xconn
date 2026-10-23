/// Shared Rust packet encryption. Applications establish trust in peer keys.
library;

import "dart:typed_data";

import "package:xconn_core/src/protocol.dart";

({Uint8List publicKey, Uint8List privateKey}) generateKeyPair() {
  final pair = callNative("crypto_key_pair")! as Map;
  return (publicKey: pair["public"] as Uint8List, privateKey: pair["private"] as Uint8List);
}

Uint8List sharedSecret(Uint8List privateKey, Uint8List publicKey) =>
    callNative("crypto_shared_secret", [privateKey, publicKey])! as Uint8List;

Uint8List deriveKey(Uint8List secret, Uint8List info) => callNative("crypto_derive_key", [secret, info])! as Uint8List;

Uint8List encrypt(Uint8List plaintext, Uint8List key, {Uint8List? additionalData}) =>
    callNative("crypto_encrypt", [plaintext, key, additionalData ?? Uint8List(0)])! as Uint8List;

Uint8List decrypt(Uint8List packet, Uint8List key, {Uint8List? additionalData}) =>
    callNative("crypto_decrypt", [packet, key, additionalData ?? Uint8List(0)])! as Uint8List;
