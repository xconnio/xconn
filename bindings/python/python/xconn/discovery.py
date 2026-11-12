"""Endpoint discovery through the shared Rust service client."""

from dataclasses import dataclass, field

from . import _bridge as bridge


@dataclass
class Endpoint:
    id: str
    addresses: list[str] = field(default_factory=list)
    capabilities: list[str] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)

    def _fields(self):
        return {
            "id": self.id,
            "addresses": self.addresses,
            "capabilities": self.capabilities,
            "metadata": self.metadata,
        }


class Discovery:
    def __init__(self, session, prefix):
        self._session = session
        self._prefix = prefix

    def _request(self, action, *args):
        return bridge.execute(
            "service_discovery_request",
            session=self._session._handle,
            prefix=self._prefix,
            action=action,
            args=list(args),
        )

    def advertise(self, endpoint: Endpoint):
        self._request("advertise", endpoint._fields())

    def withdraw(self, endpoint_id: str):
        self._request("withdraw", endpoint_id)

    def get(self, endpoint_id: str) -> Endpoint:
        return Endpoint(**self._request("get", endpoint_id))

    def list(self) -> list[Endpoint]:
        return [Endpoint(**value) for value in self._request("list")]


class AsyncDiscovery:
    def __init__(self, session, prefix):
        self._session = session
        self._prefix = prefix

    async def _request(self, action, *args):
        return await bridge.execute_async(
            "service_discovery_request",
            session=self._session._handle,
            prefix=self._prefix,
            action=action,
            args=list(args),
        )

    async def advertise(self, endpoint: Endpoint):
        await self._request("advertise", endpoint._fields())

    async def withdraw(self, endpoint_id: str):
        await self._request("withdraw", endpoint_id)

    async def get(self, endpoint_id: str) -> Endpoint:
        return Endpoint(**(await self._request("get", endpoint_id)))

    async def list(self) -> list[Endpoint]:
        return [Endpoint(**value) for value in await self._request("list")]
