"""Owned RPC streams for blocking and asyncio sessions."""

from __future__ import annotations

import asyncio
import threading
import weakref

from . import _bridge as bridge


def _release(handle):
    bridge.execute("close", handle=handle)


class Call:
    def __init__(self, handle):
        self._handle = handle
        self._release = weakref.finalize(self, _release, handle)
        self._closed = False
        self._error = None
        self._receiving = threading.Lock()

    def receive(self):
        if not self._receiving.acquire(blocking=False):
            raise RuntimeError("another receive is already pending")

        try:
            if self._closed:
                error, self._error = self._error, None

                if error is not None:
                    raise error

                return None

            try:
                response = bridge.execute("stream_receive", handle=self._handle)
            except BaseException:
                self._error = None
                self._complete()
                raise

            if response is None:
                self._complete()
                return None

            result = bridge.result(response)

            if not result.progress:
                self._complete()

            return result
        finally:
            self._receiving.release()

    def send(self, *args, **kwargs):
        self._input(args, kwargs, False)

    def finish(self, *args, **kwargs):
        self._input(args, kwargs, True)

    def _input(self, args, kwargs, finish):
        if self._closed:
            raise RuntimeError("call is finished")

        bridge.execute("stream_input", handle=self._handle, args=list(args), kwargs=kwargs, finish=finish)

    def _complete(self):
        self._closed = True
        self._release()

    def cancel(self):
        if not self._closed:
            self._error = RuntimeError("call cancelled")
            self._complete()

    close = cancel

    def results(self):
        try:
            while (result := self.receive()) is not None:
                yield result
        finally:
            self.close()

    def __iter__(self):
        return self.results()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class AsyncCall:
    def __init__(self, handle):
        self._handle = handle
        self._release = weakref.finalize(self, _release, handle)
        self._closed = False
        self._error = None
        self._receiving = False

    async def receive(self):
        if self._receiving:
            raise RuntimeError("another receive is already pending")

        self._receiving = True

        try:
            if self._closed:
                error, self._error = self._error, None

                if error is not None:
                    raise error

                return None

            try:
                response = await bridge.execute_async("stream_receive", handle=self._handle)
            except BaseException:
                self._error = None
                self._complete()
                raise

            if response is None:
                self._complete()
                return None

            result = bridge.result(response)

            if not result.progress:
                self._complete()

            return result
        finally:
            self._receiving = False

    async def send(self, *args, **kwargs):
        await self._input(args, kwargs, False)

    async def finish(self, *args, **kwargs):
        await self._input(args, kwargs, True)

    async def _input(self, args, kwargs, finish):
        if self._closed:
            raise RuntimeError("call is finished")

        try:
            await bridge.execute_async(
                "stream_input", handle=self._handle, args=list(args), kwargs=kwargs, finish=finish
            )
        except asyncio.CancelledError:
            self._complete()
            raise

    def _complete(self):
        self._closed = True
        self._release()

    async def cancel(self):
        if not self._closed:
            self._error = RuntimeError("call cancelled")
            self._complete()

    async def close(self):
        await self.cancel()

    async def results(self):
        try:
            while (result := await self.receive()) is not None:
                yield result
        finally:
            await self.close()

    def __aiter__(self):
        return self.results()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.close()


async def open_async(session, fields):
    response = await session._request("stream_open", **fields)
    return AsyncCall(response["handle"])
