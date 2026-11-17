import "package:flutter/widgets.dart";
import "package:xconn/xconn.dart";

void main() {
  final auth = CryptoSignAuthenticator("ios-smoke", "11" * 32);
  runApp(Directionality(textDirection: TextDirection.ltr, child: Text(auth.authExtra!["pubkey"] as String)));
}
