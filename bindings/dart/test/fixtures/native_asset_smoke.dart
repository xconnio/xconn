import "package:xconn_core/xconn_core.dart";

void main() {
  final auth = CryptoSignAuthenticator("native-assets", "11" * 32);
  if (auth.authExtra?["pubkey"] is! String) {
    throw StateError("Native cryptosign did not return a public key");
  }
  print("Bundled Rust native asset loaded");
}
