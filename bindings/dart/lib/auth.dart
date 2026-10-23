import "package:xconn_core/messages.dart";
import "package:xconn_core/src/protocol.dart";

abstract class IClientAuthenticator {
  IClientAuthenticator(this.authMethod, this.authID, [Map<String, dynamic>? extra]) : authExtra = extra ?? {};
  final String authMethod;
  final String authID;
  Map<String, dynamic>? authExtra;
  Authenticate authenticate(Challenge challenge);
}

class AnonymousAuthenticator extends IClientAuthenticator {
  AnonymousAuthenticator(String authID, [Map<String, dynamic>? authExtra]) : super("anonymous", authID, authExtra);

  @override
  Authenticate authenticate(Challenge challenge) => throw UnsupportedError("Anonymous authentication has no challenge");
}

abstract class _Authenticator extends IClientAuthenticator {
  _Authenticator(super.authMethod, super.authID, this._credential, [super.extra]);
  final String _credential;

  @override
  Authenticate authenticate(Challenge challenge) => Authenticate.parse(
    (callNative("auth", [authMethod, authID, _credential, challenge.marshal()])! as List).cast<Object?>(),
  );
}

class TicketAuthenticator extends _Authenticator {
  TicketAuthenticator(String authID, String ticket, [Map<String, dynamic>? extra])
    : super("ticket", authID, ticket, extra);
}

class WAMPCRAAuthenticator extends _Authenticator {
  WAMPCRAAuthenticator(String authID, String secret, [Map<String, dynamic>? extra])
    : super("wampcra", authID, secret, extra);
}

class CryptoSignAuthenticator extends _Authenticator {
  CryptoSignAuthenticator(String authID, String key, [Map<String, dynamic>? extra])
    : super(
        "cryptosign",
        authID,
        key,
        Map<String, Object?>.from(callNative("crypto_extra", [authID, key, extra ?? {}])! as Map),
      );
}
