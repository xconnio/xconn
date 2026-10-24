"""Asyncio API: native futures wake the loop; handlers run on the Python side."""

import asyncio
import inspect
import itertools

from xconn_core import serializers

from . import _bridge as bridge
from .discovery import AsyncDiscovery
from .requests import CallRequest, PublishRequest, RegisterRequest, SubscribeRequest
from .types import Event, Invocation, SessionDetails, TransportConfig


class Registration:
    def __init__(self, registration_id, session, token=None):
        self.registration_id, self._session, self._token = (
            registration_id,
            session,
            token,
        )

    async def unregister(self):
        await self._session._request("unregister", id=self.registration_id)
        self._session._callbacks.pop(self._token, None)


class Subscription:
    def __init__(self, subscription_id, session, token=None):
        self.subscription_id, self._session, self._token = (
            subscription_id,
            session,
            token,
        )

    async def unsubscribe(self):
        await self._session._request("unsubscribe", id=self.subscription_id, token=self._token)
        self._session._callbacks.pop(self._token, None)


class AsyncSession:
    def discovery(self, prefix):
        return AsyncDiscovery(self, prefix)

    def __init__(self, base_session, config=None):
        self._base_session = None if isinstance(base_session, dict) else base_session
        callbacks = None

        if self._base_session is not None:
            response, callbacks = bridge.session_from_base(base_session, asynchronous=True)
        else:
            response = base_session

        self._config = TransportConfig() if config is None else config
        self._disconnect_callbacks = []
        self._host_callbacks = callbacks
        self._handle = response["handle"]
        self.details = SessionDetails(
            response["session_id"], response["authid"], response["authrole"], response.get("realm", "")
        )
        self._callbacks, self._tasks, self._invocations = {}, set(), {}
        self._tokens = itertools.count(1)
        self._reader = asyncio.create_task(self._events())

    async def _request(self, method, **fields):
        return await bridge.execute_async(method, handle=self._handle, **fields)

    async def _events(self):
        try:
            while True:
                event = await self._request("next_event")

                if event is None:
                    break

                if event["kind"] == "interrupt":
                    task = self._invocations.get(event["id"])

                    if task is not None:
                        task.cancel()

                    continue

                callback = self._callbacks.get(event["token"])

                if callback is None and event["kind"] == "invocation":
                    try:
                        await self._request("reply", id=event["id"], error={"uri": "wamp.error.canceled"})
                    except RuntimeError:
                        pass

                if callback is not None:
                    task = asyncio.create_task(self._dispatch(event, callback))
                    self._tasks.add(task)
                    task.add_done_callback(self._tasks.discard)

                    if event["kind"] == "invocation":
                        self._invocations[event["id"]] = task
                        task.add_done_callback(lambda done, key=event["id"]: self._invocations.pop(key, None))
        except RuntimeError:
            pass
        finally:
            for task in self._tasks:
                task.cancel()

            for callback in self._disconnect_callbacks:
                try:
                    result = callback()
                    if inspect.isawaitable(result):
                        await result
                except Exception as error:
                    asyncio.get_running_loop().call_exception_handler(
                        {"message": "XConn disconnect handler failed", "exception": error}
                    )

    async def _dispatch(self, event, callback):
        if event["kind"] == "invocation":
            value = Invocation(event["args"], event["kwargs"], event["details"])
            value._send_progress_fn = lambda a, kw: self._request(
                "invocation_progress", id=event["id"], args=a, kwargs=kw
            )
        else:
            value = Event(event["args"], event["kwargs"], event["details"])

        try:
            if inspect.iscoroutinefunction(callback):
                response = await callback(value)
            else:
                response = await asyncio.to_thread(callback, value)

            if event["kind"] == "event":
                return

            response = bridge.reply(response, progressive_input=bool(value.details.get("progress")))
        except Exception as error:  # User handler failures become WAMP ERROR replies.
            if event["kind"] == "event":
                asyncio.get_running_loop().call_exception_handler(
                    {"message": "XConn event handler failed", "exception": error}
                )
                return

            response = bridge.failure(error)
        finally:
            self._invocations.pop(event.get("id"), None)

        try:
            await self._request("reply", id=event["id"], **response)
        except RuntimeError:
            pass

    def call(self, procedure: str) -> CallRequest:
        return CallRequest(procedure, session=self)

    def register(self, procedure: str, invocation_handler) -> RegisterRequest:
        return RegisterRequest(procedure, invocation_handler, session=self)

    def publish(self, topic: str) -> PublishRequest:
        return PublishRequest(topic, session=self)

    def subscribe(self, topic: str, event_handler) -> SubscribeRequest:
        return SubscribeRequest(topic, event_handler, session=self)

    async def _execute_call(self, req: CallRequest):
        if req.progress_receiver is not None or req.progress_sender is not None:
            return await self._call_stream(
                req.procedure,
                args=req.arguments,
                kwargs=req.keyword_arguments,
                options=req.request_options,
                on_progress=req.progress_receiver,
                on_input=req.progress_sender,
            )

        return await self._call(
            req.procedure,
            args=req.arguments,
            kwargs=req.keyword_arguments,
            options=req.request_options,
        )

    async def _execute_register(self, req: RegisterRequest):
        return await self._register(req.procedure, req.handler, options=req.request_options)

    async def _execute_publish(self, req: PublishRequest):
        return await self._publish(
            req.topic,
            args=req.arguments,
            kwargs=req.keyword_arguments,
            options=req.request_options,
        )

    async def _execute_subscribe(self, req: SubscribeRequest):
        return await self._subscribe(req.topic, req.handler, options=req.request_options)

    async def _call_stream(
        self,
        procedure: str,
        args=None,
        kwargs=None,
        options=None,
        on_progress=None,
        on_input=None,
    ):
        first = None

        if on_input is not None:
            first = on_input()

            if inspect.isawaitable(first):
                first = await first

            if first is not None:
                args = first.args
                kwargs = first.kwargs
                options = {**(options or {}), **(first.options or {})}

        response = await self._request("call_open", uri=procedure, args=args, kwargs=kwargs, options=options)
        handle = response["handle"]

        async def send_inputs():
            more = first.options.get("progress") is True if (first and first.options) else False

            while more:
                next_in = on_input()

                if inspect.isawaitable(next_in):
                    next_in = await next_in

                more = next_in.options.get("progress") is True if (next_in and next_in.options) else False
                await bridge.execute_async(
                    "call_input",
                    handle=handle,
                    args=next_in.args if next_in else None,
                    kwargs=next_in.kwargs if next_in else None,
                    more=more,
                )

        input_task = None

        if on_input is not None and first and first.options and first.options.get("progress") is True:
            input_task = asyncio.create_task(send_inputs())

        pending_next = None
        watch_input = input_task is not None

        try:
            while True:
                if pending_next is None:
                    pending_next = asyncio.create_task(bridge.execute_async("call_next", handle=handle))

                if watch_input:
                    done, _ = await asyncio.wait((pending_next, input_task), return_when=asyncio.FIRST_COMPLETED)

                    if input_task in done:
                        input_task.result()
                        watch_input = False

                    if pending_next not in done:
                        continue

                res = bridge.result(await pending_next)
                pending_next = None

                if not (res.details and res.details.get("progress") is True):
                    return res

                if on_progress is not None:
                    result = on_progress(res)

                    if inspect.isawaitable(result):
                        await result
        finally:
            tasks = [task for task in (input_task, pending_next) if task is not None]

            for task in tasks:
                task.cancel()

            try:
                bridge.execute("close", handle=handle)
            finally:
                await asyncio.gather(*tasks, return_exceptions=True)

    async def _call(self, procedure, args=None, kwargs=None, options=None):
        return bridge.result(await self._request("call", uri=procedure, args=args, kwargs=kwargs, options=options))

    async def _register(self, procedure, invocation_handler, options=None):
        if not callable(invocation_handler):
            raise TypeError("invocation handler must be callable")

        token = next(self._tokens)
        self._callbacks[token] = invocation_handler

        try:
            response = await self._request("register", uri=procedure, token=token, options=options)
            return Registration(response["id"], self, token)
        except BaseException:
            self._callbacks.pop(token, None)
            raise

    async def _subscribe(self, topic, event_handler, options=None):
        if not callable(event_handler):
            raise TypeError("event handler must be callable")

        token = next(self._tokens)
        self._callbacks[token] = event_handler

        try:
            response = await self._request("subscribe", uri=topic, token=token, options=options)
            return Subscription(response["id"], self, token)
        except BaseException:
            self._callbacks.pop(token, None)
            raise

    async def _publish(self, topic, args=None, kwargs=None, options=None):
        await self._request("publish", uri=topic, args=args, kwargs=kwargs, options=options)

    async def leave(self):
        try:
            await self._request("leave")
        finally:
            await self.close()

    async def close(self):
        if self._host_callbacks is not None and self._base_session is not None and self._host_callbacks.claim_close():
            await self._base_session.close()

        await self._request("close")

        if self._reader is not asyncio.current_task():
            await asyncio.wait_for(asyncio.shield(self._reader), self._config.close_timeout)

        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

        if self._host_callbacks is not None:
            await asyncio.to_thread(self._host_callbacks.close)
            self._host_callbacks = None

    async def ping(self, timeout=10):
        if self._base_session is not None:
            return await self._base_session.transport.ping(timeout)

        return await self._request("ping", timeout=timeout)

    def _on_disconnect(self, callback):
        if callback is not None:
            self._disconnect_callbacks.append(callback)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()


class AsyncClient:
    def __init__(self, authenticator=None, serializer=None, ws_config=None):
        self.authenticator = authenticator
        self.serializer = serializers.JSONSerializer() if serializer is None else serializer
        self.ws_config = TransportConfig() if ws_config is None else ws_config

    async def connect(self, uri, realm, connect_callback=None, disconnect_callback=None):
        return await self._connect_native(
            "connect", {"uri": uri, "realm": realm}, connect_callback, disconnect_callback
        )

    async def _connect_native(
        self, method, fields, connect_callback=None, disconnect_callback=None, session_factory=AsyncSession
    ):
        if self.ws_config.tcp_nodelay:
            raise NotImplementedError("tcp_nodelay configuration is not exposed by the native binding yet")

        callbacks = None

        try:
            if self.authenticator is not None or bridge.custom_serializer(self.serializer):
                callbacks = bridge.AuthCallbacks(self.authenticator, self.serializer)

            response = await bridge.execute_async(
                method,
                **fields,
                auth=callbacks.fields() if callbacks else bridge.credentials(self.authenticator),
                serializer=bridge.serializer_fields(self.serializer, callbacks),
                open_timeout=self.ws_config.open_timeout,
                ping_interval=self.ws_config.ping_interval,
                ping_timeout=self.ws_config.ping_timeout,
            )
            session = session_factory(response, self.ws_config)

            if bridge.custom_serializer(self.serializer):
                session._host_callbacks = callbacks
                callbacks = None

            session._on_disconnect(disconnect_callback)

            if connect_callback is not None:
                try:
                    result = connect_callback()
                    if inspect.isawaitable(result):
                        await result
                except BaseException:
                    await session.close()
                    raise

            return session
        except BaseException as error:
            if callbacks is not None and callbacks.error is not None:
                raise callbacks.error from error

            raise
        finally:
            if callbacks is not None:
                await asyncio.to_thread(callbacks.close)


async def connect(
    uri, realm, authenticator=None, serializer=None, ws_config=None, connect_callback=None, disconnect_callback=None
):
    serializer = serializers.CBORSerializer() if serializer is None else serializer
    return await AsyncClient(authenticator, serializer, ws_config).connect(
        uri, realm, connect_callback, disconnect_callback
    )


async def connect_anonymous(uri, realm):
    return await AsyncClient().connect(uri, realm)
