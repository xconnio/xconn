import asyncio
import queue
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from router_fixture import AsyncRouter, Router
from xconn_core import Result
from xconn_core.types import Progress


def test_blocking_fluent_requests_route_and_await_does_not_execute_them():
    with Router("realm") as router, router.connect() as session:
        invoked = []

        def echo(inv):
            invoked.append(True)
            return Result.create(*(inv.args or []), **(inv.kwargs or {}))

        registration = session.register("fluent.echo", echo).invoke("single").execute()
        request = session.call("fluent.echo").arg("hello").kwarg("enabled", True)
        with pytest.raises(TypeError, match="AsyncSession"):
            request.__await__()
        assert not invoked
        result = request.execute()
        assert result.arg_str_or(0, "") == "hello"
        assert result.kwarg_bool_or("enabled", False)
        events = queue.Queue()
        subscription = session.subscribe("fluent.events", events.put).match("exact").execute()
        session.publish("fluent.events").arg(42).options(exclude_me=False, acknowledge=True).execute()
        assert events.get(timeout=3).args == [42]
        subscription.unsubscribe()
        registration.unregister()


def test_blocking_progress_input_failure_propagates_and_releases_the_call():
    release = threading.Event()
    with Router("realm") as router, router.connect() as callee, router.connect() as caller:

        def blocked(_):
            release.wait(5)
            return Result()

        callee.register("fluent.input", blocked).execute()
        count = 0

        def produce():
            nonlocal count
            count += 1
            if count == 1:
                return Progress(args=[1], options={"progress": True})
            raise ValueError("input failed")

        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(caller.call("fluent.input").on_input(produce).execute)
            try:
                with pytest.raises(ValueError, match="input failed"):
                    future.result(timeout=3)
            finally:
                release.set()
                caller.close()


def test_async_fluent_progress_callbacks_and_input_failure():
    async def scenario():
        async with AsyncRouter("realm") as router, await router.connect() as session:

            async def echo(inv):
                await inv.send_progress("step")
                return Result.create(*(inv.args or []), **(inv.kwargs or {}))

            registration = await session.register("fluent.echo", echo)
            updates = []

            class Receiver:
                async def __call__(self, result):
                    await asyncio.sleep(0)
                    updates.extend(result.args)

            result = await session.call("fluent.echo").arg("done").on_progress(Receiver())
            assert result.args == ["done"]
            assert updates == ["step"]
            event = asyncio.get_running_loop().create_future()
            subscription = await session.subscribe("fluent.events", event.set_result)
            await session.publish("fluent.events").arg(42).options(exclude_me=False, acknowledge=True)
            assert (await asyncio.wait_for(event, 3)).args == [42]
            await subscription.unsubscribe()
            await registration.unregister()

            release = asyncio.Event()

            async def blocked(_):
                await release.wait()
                return Result()

            await session.register("fluent.input", blocked).execute()
            count = 0

            async def produce():
                nonlocal count
                count += 1
                if count == 1:
                    return Progress(args=[1], options={"progress": True})
                raise ValueError("async input failed")

            try:
                with pytest.raises(ValueError, match="async input failed"):
                    await asyncio.wait_for(session.call("fluent.input").on_input(produce).execute(), 3)
            finally:
                release.set()

    asyncio.run(asyncio.wait_for(scenario(), 10))


def test_async_progress_input_task_is_finished_when_call_is_cancelled():
    async def scenario():
        async with AsyncRouter("realm") as router, await router.connect() as session:
            release = asyncio.Event()
            producing = asyncio.Event()
            stopped = asyncio.Event()

            async def blocked(_):
                await release.wait()
                return Result()

            await session.register("fluent.cancel", blocked).execute()
            count = 0

            async def produce():
                nonlocal count
                count += 1
                if count == 1:
                    return Progress(args=[1], options={"progress": True})
                producing.set()
                try:
                    await asyncio.Future()
                finally:
                    stopped.set()

            task = asyncio.create_task(session.call("fluent.cancel").on_input(produce).execute())
            try:
                await asyncio.wait_for(producing.wait(), 3)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                assert stopped.is_set()
            finally:
                release.set()

    asyncio.run(asyncio.wait_for(scenario(), 10))


def test_result_integer_defaults_handle_non_finite_values():
    result = Result([float("inf"), float("nan")], {"value": float("-inf")})
    assert result.arg_int_or(0, 7) == 7
    assert result.arg_int_or(1, 8) == 8
    assert result.kwarg_int_or("value", 9) == 9
