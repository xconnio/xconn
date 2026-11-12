"""Verified QUIC connections, WAMP sessions, and independent byte streams."""

from . import _bridge as bridge
from ._transport import AsyncByteStream, ByteStream, _AsyncStreamConnection, _StreamConnection

__all__ = ["AsyncByteStream", "AsyncQUICConnection", "ByteStream", "QUICConnection"]


def _dial_fields(address, server_name, certificate, timeout):
    fields = {"address": address, "server_name": server_name, "operation_timeout": timeout}
    if certificate is not None:
        fields["certificate"] = bytes(certificate)
    return fields


class QUICConnection(_StreamConnection):
    _prefix = "quic"

    @classmethod
    def connect(cls, address, *, server_name, certificate=None, timeout=20):
        return cls(bridge.execute("quic_dial", **_dial_fields(address, server_name, certificate, timeout)))


class AsyncQUICConnection(_AsyncStreamConnection):
    _prefix = "quic"

    @classmethod
    async def connect(cls, address, *, server_name, certificate=None, timeout=20):
        return cls(await bridge.execute_async("quic_dial", **_dial_fields(address, server_name, certificate, timeout)))
