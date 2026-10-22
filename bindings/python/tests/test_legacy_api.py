import asyncio
import threading

import pytest
from router_fixture import Router
from xconn_core import AsyncClient, AsyncSession, Client, JSONSerializer, Session, auth, serializers
from xconn_core.client import connect
from xconn_core.exception import ApplicationError
from xconn_core.types import (
    CallOptions,
    EventDetails,
    InvokeOptions,
    MatchOptions,
    RegisterOptions,
    Result,
    SubscribeOptions,
)


def test_original_sync_api_and_lifecycle():
    assert JSONSerializer is serializers.JSONSerializer
    with Router() as router:
        router.add_realm("legacy")
        listener = router.listen()
        uri = f"ws://{listener.address}/ws"
        connected = []
        disconnected = threading.Event()
        client = Client(auth.AnonymousAuthenticator("alice"), serializers.JSONSerializer())
        with client.connect(uri, "legacy", lambda: connected.append(True), disconnected.set) as callee:
            assert isinstance(callee, Session)
            with connect(uri, "legacy", serializer=serializers.JSONSerializer()) as caller:
                registration = (
                    callee.register("legacy.echo", lambda inv: Result(inv.args, inv.kwargs))
                    .options(**RegisterOptions(invoke=InvokeOptions.SINGLE) or {})
                    .execute()
                )
                result = (
                    caller.call("legacy.echo")
                    .args(1)
                    .kwargs(**{"name": "value"} or {})
                    .options(**CallOptions(timeout=1000) or {})
                    .execute()
                )
                assert result.args == [1]
                assert result.kwargs == {"name": "value"}
                assert callee.details.realm == "legacy"
                registration.unregister()
                with pytest.raises(ApplicationError) as raised:
                    caller.call("legacy.echo").execute()
                assert raised.value.message == "wamp.error.no_such_procedure"
                assert str(raised.value) == "wamp.error.no_such_procedure"
        assert connected == [True]
        assert disconnected.wait(2)


def test_original_async_names_callbacks_and_result_types():
    async def scenario():
        with Router() as router:
            router.add_realm("legacy")
            listener = router.listen()
            calls = []

            async def connected():
                calls.append("connected")

            async def disconnected():
                calls.append("disconnected")

            client = AsyncClient(auth.AnonymousAuthenticator("alice"), serializers.CBORSerializer())
            session = await client.connect(f"ws://{listener.address}", "legacy", connected, disconnected)
            assert isinstance(session, AsyncSession)
            registration = await session.register("legacy.echo", lambda inv: Result(inv.args, inv.kwargs)).execute()
            result = await session.call("legacy.echo").args("async").execute()
            assert result.args == ["async"]
            await registration.unregister()
            await session.leave()
            assert calls == ["connected", "disconnected"]

    asyncio.run(asyncio.wait_for(scenario(), 5))


def test_existing_option_and_exception_helpers():
    assert SubscribeOptions(match=MatchOptions.PREFIX) == {"match": "prefix"}
    details = EventDetails({"publisher": 42, "publisher_authid": "alice", "publisher_authrole": "trusted"})
    assert (details.session_id, details.authid, details.authrole) == (42, "alice", "trusted")
    error = ApplicationError("example.error", "detail", key="value")
    assert error.args == ("detail",)
    assert error.kwargs == {"key": "value"}
    assert str(error) == "example.error: detail: key=value"


@pytest.mark.parametrize("transport", ["websocket", "rawsocket"])
def test_legacy_ping_and_nullable_payloads(transport):
    from xconn_core.types import TransportConfig

    assert Result() == Result(None, None, None)
    config = TransportConfig(open_timeout=2, ping_interval=0.01, ping_timeout=1, close_timeout=2)
    with Router("legacy") as router:
        listener = router.listen(transport=transport)
        scheme = "ws" if transport == "websocket" else "rs"
        with Client(serializer=serializers.CBORSerializer(), config=config).connect(
            f"{scheme}://{listener.address}", "legacy"
        ) as session:
            assert 0 <= session.ping() < 1
            session.register("echo", lambda inv: Result(inv.args, inv.kwargs)).execute()
            for args, kwargs, expected_args in [(None, None, None), ([], None, []), (None, {}, []), ([], {}, [])]:
                request = session.call("echo")
                if args is not None:
                    request.args(*args)
                if kwargs is not None:
                    request.kwargs(**kwargs)
                result = request.execute()
                assert result.args == expected_args
                assert result.kwargs == kwargs


def test_custom_serializer_callbacks_are_retained():
    class TrackingSerializer(serializers.JSONSerializer):
        def __init__(self):
            self.encoded = []
            self.decoded = []

        def serialize(self, message):
            self.encoded.append(message.TYPE)
            return super().serialize(message)

        def deserialize(self, payload):
            self.decoded.append(type(payload))
            return super().deserialize(payload)

    serializer = TrackingSerializer()
    with Router("legacy") as router:
        listener = router.listen()
        with Client(serializer=serializer).connect(f"ws://{listener.address}", "legacy") as session:
            session.register("echo", lambda inv: Result(inv.args)).execute()
            assert session.call("echo").args("custom").execute().args == ["custom"]
        assert serializer.encoded and serializer.decoded
        assert all(kind is str for kind in serializer.decoded)


def test_original_application_composition_api():
    from xconn_core import App, Component, register, subscribe

    class Handlers(Component):
        @register("example.echo")
        def echo(self, invocation):
            return Result(invocation.args)

        @subscribe("example.events")
        def events(self, event):
            pass

    component = Handlers()
    app = App()
    app.include_component(component)
    assert "example.echo" in app.procedures
    assert "example.events" in app.topics


@pytest.mark.parametrize("asynchronous", [False, True])
def test_original_base_session_transport_adapter(asynchronous):
    from xconn_core import _bridge as bridge
    from xconn_core.types import AsyncBaseSession, BaseSession, SessionDetails

    class Transport:
        def __init__(self, handle):
            self.handle = handle
            self.closes = 0

        def read(self):
            return bridge.execute("base_read", handle=self.handle)

        def write(self, data):
            bridge.execute("base_write", handle=self.handle, bytes=data)

        def close(self):
            self.closes += 1
            bridge.execute("base_close", handle=self.handle)
            bridge.execute("close", handle=self.handle)

    class AsyncTransport(Transport):
        async def read(self):
            return await bridge.execute_async("base_read", handle=self.handle)

        async def write(self, data):
            await bridge.execute_async("base_write", handle=self.handle, bytes=data)

        async def close(self):
            await asyncio.to_thread(super().close)

    router_handle = bridge.execute("router_new", realm="legacy")["handle"]
    try:
        response = bridge.execute(
            "connect_base",
            handle=router_handle,
            realm="legacy",
            authid="local",
            authrole="trusted",
            serializer="cbor",
            out_queue_size=2,
        )
        details = SessionDetails(response["session_id"], "local", "trusted", "legacy")
        if asynchronous:

            async def scenario():
                transport = AsyncTransport(response["handle"])
                base = AsyncBaseSession(transport, details, serializers.CBORSerializer())
                session = AsyncSession(base)
                try:
                    await session.register("peer.echo", lambda inv: Result(inv.args, inv.kwargs)).execute()
                    assert (await session.call("peer.echo").args("async peer").execute()).args == ["async peer"]
                finally:
                    await session.close()
                    await session.close()
                assert transport.closes == 1

            asyncio.run(asyncio.wait_for(scenario(), 5))
        else:
            transport = Transport(response["handle"])
            base = BaseSession(transport, details, serializers.CBORSerializer())
            session = Session(base)
            try:
                session.register("peer.echo", lambda inv: Result(inv.args, inv.kwargs)).execute()
                assert session.call("peer.echo").args("sync peer").execute().args == ["sync peer"]
            finally:
                session.close()
                session.close()
            assert transport.closes == 1

    finally:
        bridge.execute("close", handle=router_handle)
