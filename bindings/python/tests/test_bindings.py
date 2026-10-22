import asyncio
import os
import queue
import subprocess

import pytest
from router_fixture import AsyncRouter, Router
from xconn_core import ApplicationError, AsyncClient, Client, Result


@pytest.mark.parametrize("serializer", ["json", "cbor", "msgpack"])
@pytest.mark.parametrize("transport", ["websocket", "rawsocket"])
def test_blocking_rpc_pubsub_errors_and_cleanup(serializer, transport):
    with Router("realm") as router:
        if transport == "memory":

            def connect():
                return router.connect(serializer)
        else:
            listener = router.listen(transport=transport)
            uri = ("ws" if transport == "websocket" else "rs") + "://" + listener.address

            def connect():
                return Client(serializer=serializer).connect(uri, "realm")

        with connect() as callee, connect() as caller:
            registration = callee.register(
                "echo", lambda invocation: Result(invocation.args, invocation.kwargs)
            ).execute()
            values = ["مرحبا 🦀", (1 << 63) + 7, {"nested": [True, None, 3.5]}]
            if serializer != "json":
                values.append(b"\x00\xffbinary")
            response = caller.call("echo").args(*(values or [])).kwargs(**{"key": "value"} or {}).execute()
            assert response.args == values
            assert response.kwargs == {"key": "value"}
            events = queue.Queue()
            first = callee.subscribe("events", lambda event: events.put((1, event.args))).execute()
            second = callee.subscribe("events", lambda event: events.put((2, event.args))).execute()
            caller.publish("events").args(42).options(**{"acknowledge": True} or {}).execute()
            assert {events.get(timeout=2)[0], events.get(timeout=2)[0]} == {1, 2}
            first.unsubscribe()
            caller.publish("events").args(43).options(**{"acknowledge": True} or {}).execute()
            assert events.get(timeout=2) == (2, [43])
            second.unsubscribe()

            def fail(invocation):
                raise ApplicationError("example.failure", "detail", reason="test")

            failure = callee.register("fail", fail).execute()
            with pytest.raises(ApplicationError) as error:
                caller.call("fail").execute()
            assert error.value.uri == "example.failure"
            assert error.value.args == ("detail",)
            assert error.value.kwargs == {"reason": "test"}
            failure.unregister()
            registration.unregister()
            with pytest.raises(ApplicationError, match=r"wamp\.error\.no_such_procedure"):
                caller.call("echo").execute()


@pytest.mark.parametrize("serializer", ["json", "cbor", "msgpack"])
@pytest.mark.parametrize("transport", ["websocket", "rawsocket"])
def test_async_rpc_pubsub_and_loop_progress(serializer, transport):
    async def scenario():
        async with AsyncRouter("realm") as router:
            if transport == "memory":

                async def connect():
                    return await router.connect(serializer)
            else:
                listener = await router.listen(transport=transport)
                uri = ("ws" if transport == "websocket" else "rs") + "://" + listener.address

                async def connect():
                    return await AsyncClient(serializer=serializer).connect(uri, "realm")

            async with await connect() as callee, await connect() as caller:

                async def echo(invocation):
                    await asyncio.sleep(0)
                    return Result(invocation.args, invocation.kwargs)

                registration = await callee.register("echo", echo).execute()
                results = await asyncio.gather(*(caller.call("echo").args(i).execute() for i in range(25)))
                assert [r.args for r in results] == [[i] for i in range(25)]
                events = asyncio.Queue()

                async def event_handler(event):
                    await events.put(event.args)

                subscription = await callee.subscribe("events", event_handler).execute()
                await caller.publish("events").args(42).options(**{"acknowledge": True} or {}).execute()
                assert await asyncio.wait_for(events.get(), 2) == [42]
                await subscription.unsubscribe()
                await registration.unregister()

    asyncio.run(asyncio.wait_for(scenario(), 10))


def test_async_cancellation_interrupts_remote_handler_and_preserves_session():
    async def scenario():
        async with AsyncRouter("realm") as router:
            async with (
                await router.connect() as callee,
                await router.connect() as caller,
            ):
                started, cancelled = asyncio.Event(), asyncio.Event()

                async def wait(invocation):
                    started.set()
                    try:
                        await asyncio.Future()
                    finally:
                        cancelled.set()

                await callee.register("wait", wait).execute()
                call = asyncio.create_task(caller.call("wait").execute())
                await started.wait()
                call.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await call
                await asyncio.wait_for(cancelled.wait(), 2)
                await callee.register("echo", lambda invocation: Result(invocation.args)).execute()
                assert (await caller.call("echo").args(42).execute()).args == [42]

    asyncio.run(asyncio.wait_for(scenario(), 10))


def test_blocking_callback_can_make_a_nested_call():
    with (
        Router("realm") as router,
        router.connect() as callee,
        router.connect() as caller,
    ):
        callee.register("inner", lambda invocation: Result(invocation.args)).execute()
        callee.register(
            "outer", lambda invocation: callee.call("inner").args(*(invocation.args or [])).execute()
        ).execute()
        assert caller.call("outer").args(42).execute().args == [42]


@pytest.fixture(scope="module")
def original_reference():
    binary = os.environ.get("XCONN_REFERENCE_GO_BIN")
    if not binary:
        pytest.skip("requires original Go reference")
    import json

    provenance = json.loads(subprocess.check_output([binary, "provenance"], text=True, timeout=5))
    assert provenance["kind"] == "independent-pure-go"
    assert provenance["cgo_enabled"] is False
    assert provenance["revisions"]["wampproto-go"] == "ecb54c6c2318d7d3184f77ca95cf5f49c779edae"
    return binary


@pytest.fixture(scope="module")
def original_router(original_reference):
    binary = original_reference
    import json  # Fixture metadata only; runtime serializers remain entirely Rust.

    process = subprocess.Popen([binary], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    try:
        yield json.loads(process.stdout.readline())
    finally:
        process.kill()
        process.wait(timeout=5)


@pytest.mark.parametrize("serializer", ["json", "cbor", "msgpack"])
@pytest.mark.parametrize("transport", ["ws", "rs"])
def test_python_authentication_against_original_go(original_router, serializer, transport):
    from xconn_core import auth

    profiles = [
        auth.AnonymousAuthenticator(""),
        auth.TicketAuthenticator("interop", "secret"),
        auth.WAMPCRAAuthenticator("interop", "secret"),
        auth.WAMPCRAAuthenticator("salted", "secret"),
        auth.CryptoSignAuthenticator("interop", "11" * 32),
    ]
    for credentials in profiles:
        with Client(credentials, serializer).connect(original_router[transport], "realm") as session:
            assert session.call("echo").args("python").execute().args == ["python"]
    for credentials in [
        auth.TicketAuthenticator("interop", "wrong"),
        auth.WAMPCRAAuthenticator("interop", "wrong"),
        auth.WAMPCRAAuthenticator("salted", "wrong"),
        auth.CryptoSignAuthenticator("interop", "22" * 32),
    ]:
        with pytest.raises(RuntimeError):
            Client(credentials, serializer).connect(original_router[transport], "realm")


def test_async_many_idle_sessions_do_not_starve_native_futures():
    async def scenario():
        async with AsyncRouter("realm") as router:
            sessions = [await router.connect() for _ in range(40)]
            try:
                await sessions[0].register("echo", lambda invocation: Result(invocation.args)).execute()
                assert (await sessions[-1].call("echo").args(42).execute()).args == [42]
            finally:
                await asyncio.gather(*(session.close() for session in sessions))

    asyncio.run(asyncio.wait_for(scenario(), 10))


def test_async_close_releases_pending_call_and_interrupts_callee():
    async def scenario():
        async with AsyncRouter("realm") as router:
            async with (
                await router.connect() as callee,
                await router.connect() as caller,
            ):
                started, interrupted = asyncio.Event(), asyncio.Event()

                async def wait(invocation):
                    started.set()
                    try:
                        await asyncio.Future()
                    finally:
                        interrupted.set()

                await callee.register("wait", wait).execute()
                pending = asyncio.create_task(caller.call("wait").execute())
                await started.wait()
                await caller.close()
                with pytest.raises(RuntimeError):
                    await pending
                await interrupted.wait()

    asyncio.run(asyncio.wait_for(scenario(), 5))


def test_concurrent_progress_callbacks_keep_their_invocation_identity():
    import threading
    from concurrent.futures import ThreadPoolExecutor

    with Router("realm") as router, router.connect() as callee, router.connect() as caller:
        entered = queue.Queue()
        gates = {name: threading.Event() for name in ["first", "second"]}
        updates = {name: [] for name in gates}

        def handler(invocation):
            name = invocation.args[0]
            entered.put(name)
            assert gates[name].wait(5)
            invocation.send_progress(name)
            return Result([name])

        callee.register("progress.identity", handler).execute()

        def call(name):
            request = caller.call("progress.identity").arg(name)
            request.on_progress(lambda result: updates[name].append(result.args[0]))
            return request.execute()

        with ThreadPoolExecutor(max_workers=2) as workers:
            first = workers.submit(call, "first")
            assert entered.get(timeout=5) == "first"
            second = workers.submit(call, "second")
            assert entered.get(timeout=5) == "second"
            gates["first"].set()
            try:
                assert first.result(timeout=5).args == ["first"]
            finally:
                gates["second"].set()
            assert second.result(timeout=5).args == ["second"]
        assert updates == {"first": ["first"], "second": ["second"]}
