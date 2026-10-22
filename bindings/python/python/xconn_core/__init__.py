"""Python XConn client APIs backed by the shared Rust runtime."""

from xconn_core.auth import AnonymousAuthenticator, CryptoSignAuthenticator, TicketAuthenticator, WAMPCRAAuthenticator
from xconn_core.serializers import CBORSerializer, JSONSerializer, MsgPackSerializer

from .exception import ApplicationError
from .payload import PayloadError
from .types import Event, Invocation, Result

try:
    from wamp_msgs_capnp.serializer import CapnProtoSerializer
except ImportError:

    class CapnProtoSerializer:
        def __init__(self, *args, **kwargs):
            raise ImportError("Cap'n Proto serializer support is not installed. Install xconn-core[capnproto].")


__all__ = [
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
]
