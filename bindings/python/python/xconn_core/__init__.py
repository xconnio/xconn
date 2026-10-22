"""Python XConn client APIs backed by the shared Rust runtime."""

from xconn_core.auth import AnonymousAuthenticator, CryptoSignAuthenticator, TicketAuthenticator, WAMPCRAAuthenticator
from xconn_core.serializers import CBORSerializer, JSONSerializer, MsgPackSerializer

from .app import App, Component, register, subscribe
from .async_client import AsyncClient, AsyncSession
from .call import AsyncCall, Call
from .client import Client, Session, connect_anonymous
from .discovery import AsyncDiscovery, Discovery, Endpoint
from .exception import ApplicationError
from .payload import PayloadError
from .types import Event, Invocation, Result
from .utils import run


def connect(app, config, directory="."):
    # Application/configuration dependencies are loaded only when this API is used.
    from ._client.helpers import connect as connect_app

    return connect_app(app, config, directory=directory)


try:
    from wamp_msgs_capnp.serializer import CapnProtoSerializer
except ImportError:

    class CapnProtoSerializer:
        def __init__(self, *args, **kwargs):
            raise ImportError("Cap'n Proto serializer support is not installed. Install xconn-core[capnproto].")


__all__ = [
    "Endpoint",
    "Discovery",
    "AsyncDiscovery",
    "App",
    "Component",
    "register",
    "subscribe",
    "Client",
    "AsyncClient",
    "AsyncSession",
    "run",
    "connect",
    "AnonymousAuthenticator",
    "TicketAuthenticator",
    "WAMPCRAAuthenticator",
    "CryptoSignAuthenticator",
    "JSONSerializer",
    "MsgPackSerializer",
    "CBORSerializer",
    "CapnProtoSerializer",
    "ApplicationError",
    "PayloadError",
    "Event",
    "Invocation",
    "Result",
    "Call",
    "AsyncCall",
    "Session",
    "connect_anonymous",
]
