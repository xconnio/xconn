"""Verified HTTP/3 WebTransport connections using the shared Rust transport."""

from . import _bridge as bridge
from ._transport import AsyncByteStream, ByteStream, _AsyncStreamConnection, _StreamConnection

__all__ = ["AsyncByteStream", "AsyncWebTransportConnection", "ByteStream", "WebTransportConnection"]


def _dial_fields(url, certificate, timeout):
    fields = {"url": url, "verify_server": True, "operation_timeout": timeout}
    if certificate is not None:
        fields["certificate"] = bytes(certificate)
    return fields


class WebTransportConnection(_StreamConnection):
    _prefix = "webtransport"

    @classmethod
    def connect(cls, url, *, certificate=None, timeout=20):
        return cls(bridge.execute("webtransport_dial", **_dial_fields(url, certificate, timeout)))


class AsyncWebTransportConnection(_AsyncStreamConnection):
    _prefix = "webtransport"

    @classmethod
    async def connect(cls, url, *, certificate=None, timeout=20):
        return cls(await bridge.execute_async("webtransport_dial", **_dial_fields(url, certificate, timeout)))
