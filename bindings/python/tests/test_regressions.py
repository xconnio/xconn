import asyncio

import pytest
from router_fixture import Router
from xconn_core import AsyncClient, Client, serializers
from xconn_core.exception import ApplicationError
from xconn_core.types import Result


@pytest.mark.parametrize(
    "serializer", [serializers.JSONSerializer, serializers.CBORSerializer, serializers.MsgPackSerializer]
)
def test_nested_tuple_payloads_cross_the_native_boundary(serializer):
    with Router() as router:
        router.add_realm("realm1")
        listener = router.listen()
        with Client(serializer=serializer()).connect(f"ws://{listener.address}", "realm1") as session:
            session.register("tuples", lambda invocation: Result(invocation.args, {"nested": (1, (2, 3))})).execute()
            result = session.call("tuples").arg(("first", ("second",))).execute()
            assert result.args == [["first", ["second"]]]
            assert result.kwargs == {"nested": [1, [2, 3]]}


def test_application_error_separates_uri_and_payload():
    assert str(ApplicationError("app.error", "detail", key="value")) == "app.error: detail: key=value"


@pytest.mark.parametrize("return_awaitable", [False, True])
def test_async_lifecycle_accepts_sync_callbacks_and_returned_awaitables(return_awaitable):
    async def scenario():
        observed = []

        def callback(name):
            observed.append(name)
            return asyncio.sleep(0) if return_awaitable else None

        with Router() as router:
            router.add_realm("realm1")
            listener = router.listen()
            session = await AsyncClient().connect(
                f"ws://{listener.address}",
                "realm1",
                lambda: callback("connect"),
                lambda: callback("disconnect"),
            )
            await session.close()
            assert observed == ["connect", "disconnect"]

    asyncio.run(asyncio.wait_for(scenario(), 5))


@pytest.mark.parametrize("closing", [False, True])
def test_async_reader_reports_unexpected_native_errors(closing):
    from xconn_core.async_client import AsyncSession

    async def scenario():
        session = AsyncSession.__new__(AsyncSession)
        session._closing = closing
        session._tasks = set()
        session._disconnect_callbacks = []
        failure = RuntimeError("native queue failed")

        async def request(method):
            raise failure

        session._request = request
        observed = []
        loop = asyncio.get_running_loop()
        previous = loop.get_exception_handler()
        loop.set_exception_handler(lambda loop, context: observed.append(context))
        try:
            await session._events()
        finally:
            loop.set_exception_handler(previous)

        assert len(observed) == (0 if closing else 1)
        if observed:
            assert observed[0]["exception"] is failure

    asyncio.run(scenario())
