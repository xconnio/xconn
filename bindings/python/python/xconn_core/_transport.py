"""Native handle ownership shared by connection-oriented transports."""

from . import _bridge as bridge
from .async_client import AsyncClient
from .client import Client


class _Resource:
    def __init__(self, response):
        self._handle = response["handle"]

    def _request(self, method, **fields):
        if not self._handle:
            raise RuntimeError("transport resource closed")
        return bridge.execute(method, handle=self._handle, **fields)

    def close(self):
        handle, self._handle = self._handle, 0
        if handle:
            bridge.execute("close", handle=handle)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class _AsyncResource:
    def __init__(self, response):
        self._handle = response["handle"]

    async def _request(self, method, **fields):
        if not self._handle:
            raise RuntimeError("transport resource closed")
        return await bridge.execute_async(method, handle=self._handle, **fields)

    async def close(self):
        handle, self._handle = self._handle, 0
        if handle:
            # Releasing a handle is immediate and cannot be interrupted by task cancellation.
            bridge.execute("close", handle=handle)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()


class ByteStream(_Resource):
    def read(self, size=65536):
        return self._request("quic_read", size=size)

    def write(self, data):
        """Write bytes and return the number accepted; writes may be partial."""
        return self._request("quic_write", data=bytes(data))

    def write_all(self, data):
        data = memoryview(data)
        while data:
            count = self.write(data)
            if not count:
                raise RuntimeError("stream write made no progress")
            data = data[count:]

    def write_eof(self):
        self._request("quic_finish")


class AsyncByteStream(_AsyncResource):
    async def read(self, size=65536):
        return await self._request("quic_read", size=size)

    async def write(self, data):
        """Write bytes and return the number accepted; writes may be partial."""
        return await self._request("quic_write", data=bytes(data))

    async def write_all(self, data):
        data = memoryview(data)
        while data:
            count = await self.write(data)
            if not count:
                raise RuntimeError("stream write made no progress")
            data = data[count:]

    async def write_eof(self):
        await self._request("quic_finish")


class _StreamConnection(_Resource):
    """A connection carrying WAMP sessions and byte streams; `_prefix` selects the transport."""

    _prefix = ""

    def open_session(self, realm, *, client=None):
        if not self._handle:
            raise RuntimeError("connection closed")
        return (client or Client())._connect_native(f"{self._prefix}_session", {"handle": self._handle, "realm": realm})

    def open_stream(self):
        return ByteStream(self._request(f"{self._prefix}_open"))

    def accept_stream(self):
        return ByteStream(self._request(f"{self._prefix}_accept_remote"))


class _AsyncStreamConnection(_AsyncResource):
    _prefix = ""

    async def open_session(self, realm, *, client=None):
        if not self._handle:
            raise RuntimeError("connection closed")
        return await (client or AsyncClient())._connect_native(
            f"{self._prefix}_session", {"handle": self._handle, "realm": realm}
        )

    async def open_stream(self):
        return AsyncByteStream(await self._request(f"{self._prefix}_open"))

    async def accept_stream(self):
        return AsyncByteStream(await self._request(f"{self._prefix}_accept_remote"))
