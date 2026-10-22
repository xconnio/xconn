import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest
from router_fixture import AsyncRouter, Router
from xconn_core import ApplicationError, Result


@pytest.mark.parametrize("serializer", ["json", "cbor", "msgpack"])
def test_blocking_stream_duplex_and_completion(serializer):
    with Router("realm") as router, router.connect(serializer) as session:
        total = 0

        def sum_input(inv):
            nonlocal total
            total += inv.args[0]
            inv.send_progress(total)
            return None if inv.details.get("progress") else Result.create(total)

        session.register("sum", sum_input).execute()
        with session.call("sum").arg(1).stream(progressive_input=True) as call:
            first = call.receive()
            assert first.progress and first.args == [1]
            with ThreadPoolExecutor() as executor:
                receiving = executor.submit(call.receive)
                call.send(2)
                assert receiving.result(timeout=2).args == [3]
            call.finish(3)
            results = list(call.results())
            assert [r.args for r in results] == [[6], [6]]
            assert [r.progress for r in results] == [True, False]
            assert call.receive() is None
            with pytest.raises(RuntimeError, match="finished"):
                call.send(4)
        with pytest.raises(TypeError, match="boolean"):
            session.call("sum").stream(progressive_input=1)
        with pytest.raises(ValueError, match="buffer"):
            session.call("sum").stream(buffer_size=0)


def test_async_stream_cancellation_errors_and_timeout():
    async def scenario():
        async with AsyncRouter("realm") as router, await router.connect() as session:

            async def progress(inv):
                await inv.send_progress("update")
                return Result.create("done")

            await session.register("progress", progress).execute()
            async with await session.call("progress").stream() as call:
                frames = [frame async for frame in call.results()]
                assert [r.args for r in frames] == [["update"], ["done"]]
                assert [r.progress for r in frames] == [True, False]
                assert await call.receive() is None

            async def fail(_):
                raise ApplicationError("example.failure", "detail", enabled=True)

            await session.register("fail", fail).execute()
            call = await session.call("fail").stream()
            with pytest.raises(ApplicationError) as error:
                await call.receive()
            assert error.value.uri == "example.failure"
            assert error.value.args == ("detail",)
            assert error.value.kwargs == {"enabled": True}
            assert await call.receive() is None

            started = asyncio.Event()

            async def wait(_):
                started.set()
                await asyncio.Event().wait()

            await session.register("wait", wait).execute()
            call = await session.call("wait").stream()
            await asyncio.wait_for(started.wait(), 2)
            receiving = asyncio.create_task(call.receive())
            await asyncio.sleep(0)
            with pytest.raises(RuntimeError, match="pending"):
                await call.receive()
            receiving.cancel()
            with pytest.raises(asyncio.CancelledError):
                await receiving
            assert await call.receive() is None
            call = await session.call("wait").stream(timeout=0.02)
            with pytest.raises(RuntimeError, match="timed out"):
                await call.receive()
            await call.close()
            assert (await session.call("progress").execute()).args == ["done"]

    asyncio.run(asyncio.wait_for(scenario(), 10))


@pytest.mark.parametrize("progressive_input", [False, True])
@pytest.mark.parametrize("progressive_results", [False, True])
@pytest.mark.parametrize("asynchronous", [False, True])
def test_input_and_result_progress_are_independent(progressive_input, progressive_results, asynchronous):
    def check_invocation(invocation):
        assert bool(invocation.details.get("receive_progress")) == progressive_results
        if progressive_results:
            invocation.send_progress("partial")
        return None if invocation.details.get("progress") else Result.create("final")

    if not asynchronous:
        with Router("realm") as router, router.connect() as session:
            session.register("modes", check_invocation).execute()
            with (
                session.call("modes")
                .option("receive_progress", not progressive_results)
                .stream(
                    progressive_input=progressive_input,
                    progressive_results=progressive_results,
                ) as call
            ):
                if progressive_input:
                    if progressive_results:
                        assert call.receive().progress
                    call.finish()
                if progressive_results:
                    assert call.receive().progress
                result = call.receive()
                assert not result.progress and result.args == ["final"]
                assert call.receive() is None
        return

    async def scenario():
        async with AsyncRouter("realm") as router, await router.connect() as session:

            async def handler(invocation):
                assert bool(invocation.details.get("receive_progress")) == progressive_results
                if progressive_results:
                    await invocation.send_progress("partial")
                return None if invocation.details.get("progress") else Result.create("final")

            await session.register("modes", handler).execute()
            async with (
                await session.call("modes")
                .option("receive_progress", not progressive_results)
                .stream(
                    progressive_input=progressive_input,
                    progressive_results=progressive_results,
                ) as call
            ):
                if progressive_input:
                    if progressive_results:
                        assert (await call.receive()).progress
                    await call.finish()
                if progressive_results:
                    assert (await call.receive()).progress
                result = await call.receive()
                assert not result.progress and result.args == ["final"]
                assert await call.receive() is None

    asyncio.run(asyncio.wait_for(scenario(), 5))
