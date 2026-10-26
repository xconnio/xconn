"""Streaming over real transports between separate Python and Go processes."""

import asyncio
import json
import os
import selectors
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
from xconn_core import ApplicationError, AsyncClient, Client, Result


@pytest.fixture
def go_peer():
    binary = os.environ.get("XCONN_STREAM_GO_TEST_BIN")
    if binary is None:
        pytest.skip("ci/test-bindings.sh builds the Go streaming peer")
    binary = str(Path(binary).resolve())
    process = subprocess.Popen(
        [binary, "-test.run=^TestStreamingInteropPeer$", "-test.timeout=45s"],
        env={**os.environ, "XCONN_STREAM_MODE": "serve"},
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        with selectors.DefaultSelector() as ready:
            ready.register(process.stdout, selectors.EVENT_READ)
            assert ready.select(10), "Go streaming peer did not announce its address"
        line = process.stdout.readline()
        assert line, "Go streaming peer exited before startup"
        yield binary, json.loads(line)
    finally:
        try:
            output, error = process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
            raise
        assert process.returncode == 0, output + error


@pytest.mark.parametrize("serializer", ["json", "cbor", "msgpack"])
@pytest.mark.parametrize("transport", ["ws", "rs"])
def test_python_blocking_caller_go_callee(go_peer, serializer, transport):
    _, addresses = go_peer
    with Client(serializer=serializer).connect(addresses[transport], "realm") as session:
        nested = {"nested": [True, None, "مرحبا 🦀"]}
        with (
            session.call("go.duplex")
            .args("first", nested)
            .kwarg("marker", "initial")
            .stream(progressive_input=True, timeout=5) as call
        ):
            first = call.receive()
            assert first.progress and first.args == ["first", nested]
            assert first.kwargs == {"marker": "initial"}
            with ThreadPoolExecutor() as pool:
                receiving = pool.submit(call.receive)
                call.send("second", marker="middle")
                middle = receiving.result(timeout=5)
                assert middle.progress and middle.args == ["second"]
                assert middle.kwargs == {"marker": "middle"}
            call.finish("last", marker="final")
            frames = list(call.results())
            assert [frame.args for frame in frames] == [["last"], ["done"]]
            assert [frame.progress for frame in frames] == [True, False]
            assert frames[0].kwargs == {"marker": "final"}
            assert frames[1].kwargs == {"complete": True}
            assert call.receive() is None
        with session.call("go.fail").stream(timeout=5) as call:
            assert call.receive().args == ["before-error"]
            with pytest.raises(ApplicationError) as error:
                call.receive()
            assert error.value.uri == "example.failure"
            assert error.value.args == ("detail",)
            assert error.value.kwargs == {"enabled": True}
            assert call.receive() is None
        for token in ("cancel", "deadline"):
            with session.call("go.wait").arg(token).stream(timeout=0.1 if token == "deadline" else 5) as call:
                assert call.receive().args == ["started"]
                if token == "cancel":
                    call.close()
                with pytest.raises(RuntimeError, match="timed out" if token == "deadline" else "cancelled"):
                    call.receive()
                assert call.receive() is None
            deadline = time.monotonic() + 3
            while not session.call("go.cancelled").arg(token).execute().args[0]:
                assert time.monotonic() < deadline, "Go handler did not observe cancellation"
                time.sleep(0.01)


@pytest.mark.parametrize("serializer", ["json", "cbor", "msgpack"])
@pytest.mark.parametrize("transport", ["ws", "rs"])
def test_go_caller_python_async_callee(go_peer, serializer, transport):
    binary, addresses = go_peer

    async def scenario():
        async with await AsyncClient(serializer=serializer).connect(addresses[transport], "realm") as session:
            cancelled = set()

            async def duplex(inv):
                await inv.send_progress(*(inv.args or []), **(inv.kwargs or {}))
                return None if inv.details.get("progress") else Result.create("done", complete=True)

            async def fail(inv):
                await inv.send_progress("before-error")
                raise ApplicationError("example.failure", "detail", enabled=True)

            async def wait(inv):
                token = inv.args[0]
                try:
                    await inv.send_progress("started")
                    await asyncio.Event().wait()
                finally:
                    cancelled.add(token)

            await session.register("python.duplex", duplex).execute()
            await session.register("python.fail", fail).execute()
            await session.register("python.wait", wait).execute()
            await session.register("python.cancelled", lambda inv: Result.create(inv.args[0] in cancelled)).execute()
            process = await asyncio.create_subprocess_exec(
                binary,
                "-test.run=^TestStreamingInteropPeer$",
                "-test.timeout=20s",
                env={
                    **os.environ,
                    "XCONN_STREAM_MODE": "verify",
                    "XCONN_STREAM_URI": addresses[transport],
                    "XCONN_STREAM_SERIALIZER": serializer,
                },
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                output, error = await asyncio.wait_for(process.communicate(), 25)
                assert process.returncode == 0, (output + error).decode()
                assert cancelled == {"cancel", "deadline"}
            finally:
                if process.returncode is None:
                    process.kill()
                    await process.wait()

    asyncio.run(asyncio.wait_for(scenario(), 30))


@pytest.mark.parametrize("serializer", ["json", "cbor", "msgpack"])
def test_python_async_caller_go_callee(go_peer, serializer):
    _, addresses = go_peer

    async def scenario():
        async with await AsyncClient(serializer=serializer).connect(addresses["ws"], "realm") as session:
            async with await session.call("go.duplex").arg("first").stream(progressive_input=True) as call:
                assert (await call.receive()).args == ["first"]
                receiving = asyncio.create_task(call.receive())
                await call.send("second", marker="middle")
                middle = await receiving
                assert middle.progress and middle.args == ["second"]
                assert middle.kwargs == {"marker": "middle"}
                await call.finish("last", marker="final")
                frames = [frame async for frame in call.results()]
                assert [frame.args for frame in frames] == [["last"], ["done"]]
                assert [frame.progress for frame in frames] == [True, False]
                assert frames[-1].kwargs == {"complete": True}
            async with await session.call("go.wait").arg("task-cancel").stream() as call:
                assert (await call.receive()).args == ["started"]
                receiving = asyncio.create_task(call.receive())
                await asyncio.sleep(0)
                receiving.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await receiving
                assert await call.receive() is None

            async def wait_for_cancel():
                while not (await session.call("go.cancelled").arg("task-cancel").execute()).args[0]:
                    await asyncio.sleep(0.01)

            await asyncio.wait_for(wait_for_cancel(), 3)
            # The session still carries ordinary traffic after cancelling a pending native receive.
            assert (await session.call("go.cancelled").arg("unknown").execute()).args == [False]

    asyncio.run(asyncio.wait_for(scenario(), 10))
