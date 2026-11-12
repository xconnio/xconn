"""WAMP-signaled WebRTC clients and raw channels backed by Rust."""

from dataclasses import asdict, dataclass, field

from ._transport import _AsyncResource, _Resource
from .async_client import AsyncClient, AsyncSession
from .client import Client, Session


@dataclass(frozen=True)
class WebRTCOptions:
    offer_procedure: str
    remote_candidates_topic: str
    local_candidates_topic: str
    ice_servers: list[dict] = field(default_factory=list)
    connect_timeout: float = 20
    open_timeout: float = 20
    handshake_timeout: float = 5


def _channel_options(ordered, max_retransmits, max_packet_life_time):
    options = {"ordered": ordered}
    if max_retransmits is not None:
        options["max_retransmits"] = max_retransmits
    if max_packet_life_time is not None:
        options["max_packet_life_time"] = max_packet_life_time
    return options


class DataChannel(_Resource):
    def __init__(self, response):
        super().__init__(response)
        self.label = response["label"]
        self.protocol = response["protocol"]

    def send(self, data):
        if isinstance(data, str):
            self._request("webrtc_send_text", text=data)
        else:
            self._request("webrtc_write_packet", data=bytes(data))

    def receive(self):
        packet = self._request("webrtc_read_data_packet")
        return packet["data"].decode("utf-8") if packet["is_string"] else packet["data"]

    def write_message(self, data):
        self._request("webrtc_write_message", data=bytes(data))

    def read_message(self):
        return self._request("webrtc_read_message")

    def close(self):
        if self._handle:
            try:
                self._request("webrtc_channel_close")
            finally:
                super().close()


class AsyncDataChannel(_AsyncResource):
    def __init__(self, response):
        super().__init__(response)
        self.label = response["label"]
        self.protocol = response["protocol"]

    async def send(self, data):
        if isinstance(data, str):
            await self._request("webrtc_send_text", text=data)
        else:
            await self._request("webrtc_write_packet", data=bytes(data))

    async def receive(self):
        packet = await self._request("webrtc_read_data_packet")
        return packet["data"].decode("utf-8") if packet["is_string"] else packet["data"]

    async def write_message(self, data):
        await self._request("webrtc_write_message", data=bytes(data))

    async def read_message(self):
        return await self._request("webrtc_read_message")

    async def close(self):
        if self._handle:
            try:
                await self._request("webrtc_channel_close")
            finally:
                await super().close()


class WebRTCConnection(_Resource):
    def open_session(self, realm, *, client=None):
        return (client or Client())._connect_native(
            "webrtc_open_session",
            {"handle": self._handle, "realm": realm},
            session_factory=lambda response, config: WebRTCSession(response, config, connection=self),
        )

    def open_channel(self, label, *, ordered=True, max_retransmits=None, max_packet_life_time=None, timeout=20):
        options = _channel_options(ordered, max_retransmits, max_packet_life_time)
        return DataChannel(
            self._request(
                "webrtc_channel_open", label=label, options=options, open_timeout=timeout, operation_timeout=timeout
            )
        )

    def accept_channel(self, *, timeout=20):
        return DataChannel(self._request("webrtc_channel_accept", open_timeout=timeout, operation_timeout=timeout))

    def close(self):
        if self._handle:
            try:
                self._request("webrtc_connection_close")
            finally:
                super().close()


class AsyncWebRTCConnection(_AsyncResource):
    async def open_session(self, realm, *, client=None):
        return await (client or AsyncClient())._connect_native(
            "webrtc_open_session",
            {"handle": self._handle, "realm": realm},
            session_factory=lambda response, config: AsyncWebRTCSession(response, config, connection=self),
        )

    async def open_channel(self, label, *, ordered=True, max_retransmits=None, max_packet_life_time=None, timeout=20):
        options = _channel_options(ordered, max_retransmits, max_packet_life_time)
        return AsyncDataChannel(
            await self._request(
                "webrtc_channel_open", label=label, options=options, open_timeout=timeout, operation_timeout=timeout
            )
        )

    async def accept_channel(self, *, timeout=20):
        return AsyncDataChannel(
            await self._request("webrtc_channel_accept", open_timeout=timeout, operation_timeout=timeout)
        )

    async def close(self):
        if self._handle:
            try:
                await self._request("webrtc_connection_close")
            finally:
                await super().close()


class WebRTCSession(Session):
    def __init__(self, response, config=None, *, connection=None):
        super().__init__(response, config)
        self.connection = connection or WebRTCConnection({"handle": response["connection"]})


class AsyncWebRTCSession(AsyncSession):
    def __init__(self, response, config=None, *, connection=None):
        super().__init__(response, config)
        self.connection = connection or AsyncWebRTCConnection({"handle": response["connection"]})


def connect(signaling, realm, options, *, client=None):
    """Join WAMP over a new WebRTC peer; signaling remains caller-owned."""
    return (client or Client())._connect_native(
        "webrtc_connect",
        {"signaling": signaling._handle, "realm": realm, **asdict(options)},
        session_factory=WebRTCSession,
    )


async def connect_async(signaling, realm, options, *, client=None):
    """Asyncio equivalent of connect; cancelling setup releases native resources."""
    return await (client or AsyncClient())._connect_native(
        "webrtc_connect",
        {"signaling": signaling._handle, "realm": realm, **asdict(options)},
        session_factory=AsyncWebRTCSession,
    )
