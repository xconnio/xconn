"""App setup checks everything first, registers concurrently, and keeps nothing registered if any fails."""

import asyncio
import threading

import pytest
from router_fixture import Router
from xconn import App, AsyncClient, Client
from xconn._client import async_, sync
from xconn.async_client import Registration
from xconn.exception import ApplicationError


async def test_async_setup_registers_all_procedures_and_topics():
    app = App()
    received = asyncio.Event()

    @app.register("x.one")
    async def one() -> str:
        return "one"

    @app.register("x.two")
    async def two() -> str:
        return "two"

    @app.subscribe("x.topic")
    async def topic(value: int):
        received.set()

    with Router() as router:
        router.listen()
        url = f"ws://{router.address}/ws"
        callee = await AsyncClient().connect(url, router.realm)
        caller = await AsyncClient().connect(url, router.realm)
        try:
            await async_._setup(app, callee)
            results = [(await caller.call(uri).execute()).args for uri in ("x.one", "x.two")]
            await caller.publish("x.topic").args(1).options(acknowledge=True).execute()
            await asyncio.wait_for(received.wait(), 5)
        finally:
            await caller.leave()
            await callee.leave()

    assert results == [["one"], ["two"]]


def test_sync_setup_registers_all_procedures_and_topics():
    app = App()
    received = threading.Event()

    @app.register("x.one")
    def one() -> str:
        return "one"

    @app.register("x.two")
    def two() -> str:
        return "two"

    @app.subscribe("x.topic")
    def topic(value: int):
        received.set()

    with Router() as router:
        router.listen()
        url = f"ws://{router.address}/ws"
        callee = Client().connect(url, router.realm)
        caller = Client().connect(url, router.realm)
        try:
            sync._setup(app, callee)
            results = [caller.call(uri).execute().args for uri in ("x.one", "x.two")]
            caller.publish("x.topic").args(1).options(acknowledge=True).execute()
            assert received.wait(5)
        finally:
            caller.leave()
            callee.leave()

    assert results == [["one"], ["two"]]


async def test_async_setup_checks_every_function_before_registering_any(capsys):
    app = App()

    @app.register("x.good")
    async def good() -> str:
        return "good"

    @app.subscribe("x.topic")
    async def topic(value: int):
        pass

    @app.register("x.bad1")
    def bad1() -> str:
        return "bad"

    @app.register("x.bad2")
    def bad2() -> str:
        return "bad"

    with Router() as router:
        router.listen()
        url = f"ws://{router.address}/ws"
        callee = await AsyncClient().connect(url, router.realm)
        caller = await AsyncClient().connect(url, router.realm)
        try:
            with pytest.raises(RuntimeError, match=r"2 procedures/topics are invalid") as error:
                await async_._setup(app, callee)

            with pytest.raises(ApplicationError, match=r"wamp\.error\.no_such_procedure"):
                await caller.call("x.good").execute()

            assert not callee._callbacks
        finally:
            await caller.leave()
            await callee.leave()

    assert "x.bad1" in str(error.value) and "x.bad2" in str(error.value)
    assert "Registered" not in capsys.readouterr().out


def test_sync_setup_checks_every_function_before_registering_any(capsys):
    app = App()

    @app.register("x.good")
    def good() -> str:
        return "good"

    @app.subscribe("x.topic")
    def topic(value: int):
        pass

    @app.register("x.bad")
    async def bad() -> str:
        return "bad"

    with Router() as router:
        router.listen()
        url = f"ws://{router.address}/ws"
        callee = Client().connect(url, router.realm)
        caller = Client().connect(url, router.realm)
        try:
            with pytest.raises(RuntimeError, match=r"function bad for procedure 'x.bad' must not be a coroutine"):
                sync._setup(app, callee)

            with pytest.raises(ApplicationError, match=r"wamp\.error\.no_such_procedure"):
                caller.call("x.good").execute()

            assert not callee._callbacks
        finally:
            caller.leave()
            callee.leave()

    assert "Registered" not in capsys.readouterr().out


def taken_app(kind: str) -> App:
    """x.good and x.topic register fine; x.taken is already held by another session, so the router rejects it."""

    app = App()
    if kind == "async":

        async def good() -> str:
            return "good"

        async def topic(value: int):
            pass

        async def taken() -> str:
            return "taken"
    else:

        def good() -> str:
            return "good"

        def topic(value: int):
            pass

        def taken() -> str:
            return "taken"

    app.register("x.good")(good)
    app.subscribe("x.topic")(topic)
    app.register("x.taken")(taken)
    return app


async def test_async_setup_rolls_back_when_router_rejects_one(capsys):
    with Router() as router:
        router.listen()
        url = f"ws://{router.address}/ws"
        owner = await AsyncClient().connect(url, router.realm)
        callee = await AsyncClient().connect(url, router.realm)
        caller = await AsyncClient().connect(url, router.realm)
        try:
            await owner.register("x.taken", lambda inv: None).execute()

            with pytest.raises(ApplicationError, match=r"procedure_already_exists"):
                await async_._setup(taken_app("async"), callee)

            with pytest.raises(ApplicationError, match=r"wamp\.error\.no_such_procedure"):
                await caller.call("x.good").execute()

            assert not callee._callbacks
        finally:
            for session in (caller, callee, owner):
                await session.leave()

    out = capsys.readouterr().out
    assert "Rolled back procedure 'x.good'" in out and "Rolled back topic 'x.topic'" in out


def test_sync_setup_rolls_back_when_router_rejects_one(capsys):
    with Router() as router:
        router.listen()
        url = f"ws://{router.address}/ws"
        owner = Client().connect(url, router.realm)
        callee = Client().connect(url, router.realm)
        caller = Client().connect(url, router.realm)
        try:
            owner.register("x.taken", lambda inv: None).execute()

            with pytest.raises(ApplicationError, match=r"procedure_already_exists"):
                sync._setup(taken_app("sync"), callee)

            with pytest.raises(ApplicationError, match=r"wamp\.error\.no_such_procedure"):
                caller.call("x.good").execute()

            assert not callee._callbacks
        finally:
            for session in (caller, callee, owner):
                session.leave()

    out = capsys.readouterr().out
    assert "Rolled back procedure 'x.good'" in out and "Rolled back topic 'x.topic'" in out


async def test_async_setup_reports_registrations_it_could_not_undo(monkeypatch):
    async def broken_unregister(self):
        raise RuntimeError("connection lost")

    with Router() as router:
        router.listen()
        url = f"ws://{router.address}/ws"
        owner = await AsyncClient().connect(url, router.realm)
        callee = await AsyncClient().connect(url, router.realm)
        try:
            await owner.register("x.taken", lambda inv: None).execute()
            monkeypatch.setattr(Registration, "unregister", broken_unregister)

            with pytest.raises(RuntimeError) as error:
                await async_._setup(taken_app("async"), callee)
        finally:
            monkeypatch.undo()
            await callee.leave()
            await owner.leave()

    assert "procedure_already_exists" in str(error.value)
    assert "could not undo procedure 'x.good': RuntimeError: connection lost" in str(error.value)


async def test_async_setup_cancelled_midway_rolls_back(monkeypatch):
    app = taken_app("async")
    stuck = asyncio.Event()

    async def never_subscribes(session, uri, func):
        stuck.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(async_, "subscribe_async", never_subscribes)

    with Router() as router:
        router.listen()
        url = f"ws://{router.address}/ws"
        callee = await AsyncClient().connect(url, router.realm)
        caller = await AsyncClient().connect(url, router.realm)
        try:
            setup = asyncio.create_task(async_._setup(app, callee))
            await asyncio.wait_for(stuck.wait(), 5)

            async def both_registered():  # confirmed by the router, not just requested
                for uri in ("x.good", "x.taken"):
                    while True:
                        try:
                            await caller.call(uri).execute()
                            break
                        except ApplicationError:
                            await asyncio.sleep(0.01)

            await asyncio.wait_for(both_registered(), 5)

            setup.cancel()
            with pytest.raises(asyncio.CancelledError):
                await setup

            with pytest.raises(ApplicationError, match=r"wamp\.error\.no_such_procedure"):
                await caller.call("x.good").execute()

            assert not callee._callbacks
        finally:
            await caller.leave()
            await callee.leave()


async def test_async_setup_rolls_back_when_a_registration_is_cancelled_on_its_own(monkeypatch):
    async def cancelled_subscribe(session, uri, func):
        raise asyncio.CancelledError  # e.g. session teardown, not _setup itself being cancelled

    monkeypatch.setattr(async_, "subscribe_async", cancelled_subscribe)

    with Router() as router:
        router.listen()
        url = f"ws://{router.address}/ws"
        callee = await AsyncClient().connect(url, router.realm)
        caller = await AsyncClient().connect(url, router.realm)
        try:
            with pytest.raises(asyncio.CancelledError):
                await async_._setup(taken_app("async"), callee)

            with pytest.raises(ApplicationError, match=r"wamp\.error\.no_such_procedure"):
                await caller.call("x.good").execute()
        finally:
            await caller.leave()
            await callee.leave()
