from __future__ import annotations

import contextlib
import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from xconn_core import messages, serializers
from xconn_core.payload import Payload


@dataclass
class Result(Payload):
    args: list[Any] | None = None
    kwargs: dict[str, Any] | None = None
    details: dict[str, Any] | None = None

    @property
    def progress(self) -> bool:
        return bool(self.details and self.details.get("progress") is True)

    @classmethod
    def create(cls, *args, **kwargs) -> Result:
        return cls(args=list(args) if args else [], kwargs=kwargs if kwargs else {})


@dataclass
class Progress:
    args: list[Any] | None = None
    kwargs: dict[str, Any] | None = None
    options: dict[str, Any] | None = None


@dataclass
class Invocation(Payload):
    args: list[Any] | None
    kwargs: dict[str, Any] | None
    details: dict[str, Any] | None
    _cancelled: bool = field(default=False, init=False, repr=False)
    _send_progress_fn: Callable[[list[Any] | None, dict[str, Any] | None], Any] | None = field(
        default=None, init=False, repr=False
    )

    @property
    def cancelled(self):
        return self._cancelled

    def send_progress(self, *args, **kwargs):
        if self._send_progress_fn is not None:
            return self._send_progress_fn(list(args) if args else None, kwargs or None)


@dataclass
class Event(Payload):
    args: list[Any] | None
    kwargs: dict[str, Any] | None
    details: dict[str, Any] | None


@dataclass(frozen=True)
class SessionDetails:
    session_id: int
    authid: str
    authrole: str
    realm: str = ""


@dataclass
class TransportConfig:
    # max wait time for connection to be established
    open_timeout: float | None = 10

    # send ping automatically after every x seconds
    ping_interval: float | None = 20

    # wait for x seconds for a pong from server before closing the connection
    ping_timeout: float | None = 20

    # max wait time for closing the connection
    close_timeout: float | None = 10

    # whether to enable TCP_NODELAY flag on the socket
    tcp_nodelay: bool = False


class _IncomingDetails(dict):
    def __init__(self, details: dict | None = None):
        super().__init__()

        if details:
            for k, v in details.items():
                self[k] = v


class CallDetails(_IncomingDetails):
    def __init__(self, details: dict | None = None):
        super().__init__(details)

    @property
    def session_id(self) -> int | None:
        return self.get("caller")

    @property
    def authid(self) -> str | None:
        return self.get("caller_authid")

    @property
    def authrole(self) -> str | None:
        return self.get("caller_authrole")


class EventDetails(_IncomingDetails):
    def __init__(self, details: dict | None = None):
        super().__init__(details)

    @property
    def session_id(self) -> int | None:
        return self.get("publisher")

    @property
    def authid(self) -> str | None:
        return self.get("publisher_authid")

    @property
    def authrole(self) -> str | None:
        return self.get("publisher_authrole")


class InvokeOptions(Enum):
    SINGLE = "single"
    ROUNDROBIN = "roundrobin"
    RANDOM = "random"
    FIRST = "first"
    LAST = "last"


class MatchOptions(Enum):
    EXACT = "exact"
    PREFIX = "prefix"
    WILDCARD = "wildcard"


class RegisterOptions(dict):
    def __init__(self, invoke: InvokeOptions = None, match: MatchOptions = None, concurrency: int = None, **kwargs):
        super().__init__()

        if invoke is not None:
            if not isinstance(invoke, InvokeOptions):
                raise ValueError("expected InvokeOptions for 'invoke' WAMP option")

            self["invoke"] = invoke.value

        if match is not None:
            if not isinstance(match, MatchOptions):
                raise ValueError("expected MatchOptions for 'match' WAMP option")

            self["match"] = match.value

        if concurrency is not None:
            if not isinstance(concurrency, int):
                raise ValueError("expected int for 'concurrency' WAMP option")

            self["concurrency"] = concurrency

        for k, v in kwargs.items():
            self[k] = v


class SubscribeOptions(dict):
    def __init__(self, match: MatchOptions = None, **kwargs):
        super().__init__()

        if match is not None:
            if not isinstance(match, MatchOptions):
                raise ValueError("expected MatchOptions for 'match' WAMP option")

            self["match"] = match.value

        for k, v in kwargs.items():
            self[k] = v


class CallOptions(dict):
    def __init__(self, timeout: int = None, disclose_me: bool | None = None, **kwargs):
        super().__init__()

        if timeout is not None:
            if not isinstance(timeout, int):
                raise ValueError("expected int for 'timeout' WAMP option")

            self["timeout"] = timeout

        if disclose_me is not None:
            if not isinstance(disclose_me, bool):
                raise ValueError("expected bool for 'disclose_me' WAMP option")

            self["disclose_me"] = disclose_me

        for k, v in kwargs.items():
            self[k] = v


class PublishOptions(dict):
    def __init__(
        self,
        acknowledge: bool | None = None,
        exclude_me: bool | None = None,
        disclose_me: bool | None = None,
        exclude: list[int] | None = None,
        eligible: list[int] | None = None,
        exclude_authid: list[int] | None = None,
        eligible_authid: list[int] | None = None,
        exclude_authrole: list[int] | None = None,
        eligible_authrole: list[int] | None = None,
        **kwargs,
    ):
        super().__init__()

        if acknowledge is not None:
            if not isinstance(acknowledge, bool):
                raise ValueError("expected bool for 'acknowledge' WAMP option")

            self["acknowledge"] = acknowledge

        if exclude_me is not None:
            if not isinstance(exclude_me, bool):
                raise ValueError("expected bool for 'exclude_me' WAMP option")

            self["exclude_me"] = exclude_me

        if disclose_me is not None:
            if not isinstance(disclose_me, bool):
                raise ValueError("expected bool for 'disclose_me' WAMP option")

            self["disclose_me"] = disclose_me

        if exclude is not None:
            if not isinstance(exclude, list):
                raise ValueError("expected list for 'exclude' WAMP option")

            self["exclude"] = exclude

        if eligible is not None:
            if not isinstance(eligible, list):
                raise ValueError("expected list for 'eligible' WAMP option")

            self["eligible"] = eligible

        if exclude_authid is not None:
            if not isinstance(exclude_authid, list):
                raise ValueError("expected list for 'exclude_authid' WAMP option")

            self["exclude_authid"] = exclude_authid

        if eligible_authid is not None:
            if not isinstance(eligible_authid, list):
                raise ValueError("expected list for 'eligible_authid' WAMP option")

            self["eligible_authid"] = eligible_authid

        if exclude_authrole is not None:
            if not isinstance(exclude_authrole, list):
                raise ValueError("expected list for 'exclude_authrole' WAMP option")

            self["exclude_authrole"] = exclude_authrole

        if eligible_authrole is not None:
            if not isinstance(eligible_authrole, list):
                raise ValueError("expected list for 'eligible_authrole' WAMP option")

            self["eligible_authrole"] = eligible_authrole

        for k, v in kwargs.items():
            self[k] = v


class Depends:
    def __init__(self, dependency: Callable | Awaitable):
        self._is_async = False
        self._is_async_gen = False

        self.dependency = self._setup(dependency)

    @property
    def is_async(self) -> bool:
        return self._is_async

    @property
    def is_async_gen(self) -> bool:
        return self._is_async_gen

    def _setup(self, func: Callable | Awaitable):
        if inspect.iscoroutinefunction(func):
            self._is_async = True
        elif inspect.isasyncgenfunction(func):
            self._is_async_gen = True
            return contextlib.asynccontextmanager(func)

        return func


WebsocketConfig = TransportConfig


class ITransport:
    def read(self) -> str | bytes:
        raise NotImplementedError()

    def write(self, data: str | bytes):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()

    def is_connected(self) -> bool:
        raise NotImplementedError()

    def ping(self, timeout: int = 10) -> float:
        raise NotImplementedError()


class IAsyncTransport:
    async def read(self) -> str | bytes:
        raise NotImplementedError()

    async def write(self, data: str | bytes):
        raise NotImplementedError()

    async def close(self):
        raise NotImplementedError()

    async def is_connected(self) -> bool:
        raise NotImplementedError()

    async def ping(self, timeout: int = 10) -> float:
        raise NotImplementedError()


class IBaseSession:
    @property
    def transport(self) -> ITransport:
        raise NotImplementedError()

    @property
    def id(self) -> int:
        raise NotImplementedError()

    @property
    def realm(self) -> str:
        raise NotImplementedError()

    @property
    def authid(self) -> str:
        raise NotImplementedError()

    @property
    def authrole(self) -> str:
        raise NotImplementedError()

    def send(self, data: bytes):
        raise NotImplementedError()

    def receive(self) -> bytes:
        raise NotImplementedError()

    def send_message(self, msg: messages.Message):
        raise NotImplementedError()

    def receive_message(self) -> messages.Message:
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()


class BaseSession(IBaseSession):
    def __init__(self, transport: ITransport, session_details: SessionDetails, serializer: serializers.Serializer):
        super().__init__()
        self._transport = transport
        self.session_details = session_details
        self.serializer = serializer

    @property
    def transport(self) -> ITransport:
        return self._transport

    @property
    def id(self) -> int:
        return self.session_details.session_id

    @property
    def realm(self) -> str:
        return self.session_details.realm

    @property
    def authid(self) -> str:
        return self.session_details.authid

    @property
    def authrole(self) -> str:
        return self.session_details.authrole

    def send(self, data: bytes):
        self._transport.write(data)

    def receive(self) -> bytes:
        return self._transport.read()

    def send_message(self, msg: messages.Message):
        self.send(self.serializer.serialize(msg))

    def receive_message(self) -> messages.Message:
        return self.serializer.deserialize(self.receive())

    def close(self):
        self._transport.close()


class IAsyncBaseSession:
    @property
    def transport(self) -> IAsyncTransport:
        raise NotImplementedError()

    @property
    def id(self) -> int:
        raise NotImplementedError()

    @property
    def realm(self) -> str:
        raise NotImplementedError()

    @property
    def authid(self) -> str:
        raise NotImplementedError()

    @property
    def authrole(self) -> str:
        raise NotImplementedError()

    @property
    def serializer(self) -> serializers.Serializer:
        raise NotImplementedError()

    async def send(self, data: bytes | str):
        raise NotImplementedError()

    async def receive(self) -> bytes | str:
        raise NotImplementedError()

    async def send_message(self, msg: messages.Message):
        raise NotImplementedError()

    async def receive_message(self) -> messages.Message:
        raise NotImplementedError()

    async def close(self):
        raise NotImplementedError()


class AsyncBaseSession(IAsyncBaseSession):
    def __init__(self, transport: IAsyncTransport, session_details: SessionDetails, serializer: serializers.Serializer):
        super().__init__()
        self._transport = transport
        self.session_details = session_details
        self._serializer = serializer

    @property
    def transport(self) -> IAsyncTransport:
        return self._transport

    @property
    def id(self) -> int:
        return self.session_details.session_id

    @property
    def realm(self) -> str:
        return self.session_details.realm

    @property
    def authid(self) -> str:
        return self.session_details.authid

    @property
    def authrole(self) -> str:
        return self.session_details.authrole

    @property
    def serializer(self) -> serializers.Serializer:
        return self._serializer

    async def send(self, data: bytes):
        return await self._transport.write(data)

    async def receive(self) -> bytes:
        return await self._transport.read()

    async def send_message(self, msg: messages.Message):
        await self.send(self.serializer.serialize(msg))

    async def receive_message(self) -> messages.Message:
        return self.serializer.deserialize(await self.receive())

    async def close(self):
        await self._transport.close()
