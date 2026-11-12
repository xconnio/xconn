"""Client authenticators backed by the shared Rust runtime."""

from .messages import Authenticate, call


class IClientAuthenticator:
    def __init__(self, method, authid, auth_extra=None):
        self.auth_method = method
        self.authid = authid
        self.auth_extra = {} if auth_extra is None else auth_extra

    def authenticate(self, challenge):
        raise NotImplementedError()

    def _fields(self):
        return {"method": self.auth_method, "authid": self.authid, "extra": self.auth_extra}


class AnonymousAuthenticator(IClientAuthenticator):
    def __init__(self, authid="", auth_extra=None):
        super().__init__("anonymous", authid, auth_extra)


class _ChallengeAuthenticator(IClientAuthenticator):
    def __init__(self, method, authid, credential, auth_extra):
        super().__init__(method, authid, auth_extra)
        self._credential = credential

    def authenticate(self, challenge):
        wire = call("auth", self.auth_method, self.authid, self._credential, challenge.marshal())
        return Authenticate(wire[1], wire[2])


class TicketAuthenticator(_ChallengeAuthenticator):
    def __init__(self, authid, ticket, auth_extra=None):
        super().__init__("ticket", authid, ticket, auth_extra)


class WAMPCRAAuthenticator(_ChallengeAuthenticator):
    def __init__(self, authid, secret, auth_extra=None):
        super().__init__("wampcra", authid, secret, auth_extra)


class CryptoSignAuthenticator(_ChallengeAuthenticator):
    def __init__(self, authid, private_key, auth_extra=None):
        extra = call("crypto_extra", authid, private_key, auth_extra or {})
        super().__init__("cryptosign", authid, private_key, extra)
