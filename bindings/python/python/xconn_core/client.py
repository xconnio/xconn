"""Blocking Python API. Callbacks run in a Python worker pool, outside Rust."""

import itertools
import logging
import threading
from concurrent.futures import Future, ThreadPoolExecutor

from xconn_core import serializers

from . import _bridge as bridge
from .discovery import Discovery
from .requests import CallRequest, PublishRequest, RegisterRequest, SubscribeRequest
from .types import Event, Invocation, SessionDetails, TransportConfig


class Registration:
    def __init__(self, registration_id, session, token=None):
        self.registration_id, self._session, self._token = (
            registration_id,
            session,
            token,
        )

    def unregister(self):
        self._session._request("unregister", id=self.registration_id)
        self._session._callbacks.pop(self._token, None)


class Subscription:
    def __init__(self, subscription_id, session, token=None):
        self.subscription_id, self._session, self._token = (
            subscription_id,
            session,
            token,
        )

    def unsubscribe(self):
        self._session._request("unsubscribe", id=self.subscription_id, token=self._token)
        self._session._callbacks.pop(self._token, None)


class Session:
    def discovery(self, prefix):
        return Discovery(self, prefix)

    def __init__(self, base_session, config=None):
        self._base_session = None if isinstance(base_session, dict) else base_session
        callbacks = None

        if self._base_session is not None:
            response, callbacks = bridge.session_from_base(base_session, asynchronous=False)
        else:
            response = base_session

        self._config = TransportConfig() if config is None else config
        self._disconnect_callbacks = []
        self._host_callbacks = callbacks
        self._handle = response["handle"]
        self.details = SessionDetails(
            response["session_id"], response["authid"], response["authrole"], response.get("realm", "")
        )
        self._callbacks, self._invocations = {}, {}
        self._tokens = itertools.count(1)
        self._executor = ThreadPoolExecutor(thread_name_prefix="xconn-handler")
        self._stopped = threading.Event()
        self._reader = threading.Thread(target=self._events, daemon=True, name="xconn-events")
        self._reader.start()

    def _request(self, method, **fields):
        return bridge.execute(method, handle=self._handle, **fields)

    def _events(self):
        try:
            while True:
                event = self._request("next_event")

                if event is None:
                    break

                if event["kind"] == "interrupt":
                    invocation = self._invocations.get(event["id"])

                    if invocation is not None:
                        invocation._cancelled = True

                    continue

                callback = self._callbacks.get(event["token"])

                if callback is None and event["kind"] == "invocation":
                    try:
                        self._request("reply", id=event["id"], error={"uri": "wamp.error.canceled"})
                    except RuntimeError:
                        pass

                if callback is not None:
                    if event["kind"] == "invocation":
                        inv = Invocation(event["args"], event["kwargs"], event["details"])
                        inv._send_progress_fn = lambda a, kw, invocation_id=event["id"]: self._request(
                            "invocation_progress", id=invocation_id, args=a, kwargs=kw
                        )
                        self._invocations[event["id"]] = inv

                    self._executor.submit(self._dispatch, event, callback)
        except RuntimeError:
            pass
        finally:
            self._executor.shutdown(wait=False, cancel_futures=True)
            self._stopped.set()

            for callback in self._disconnect_callbacks:
                try:
                    callback()
                except Exception:
                    logging.getLogger(__name__).exception("XConn disconnect handler failed")

    def _dispatch(self, event, callback):
        if event["kind"] == "event":
            try:
                callback(Event(event["args"], event["kwargs"], event["details"]))
            except Exception:
                logging.getLogger(__name__).exception("XConn event handler failed")

            return

        invocation = self._invocations[event["id"]]

        if invocation.cancelled:
            self._invocations.pop(event["id"], None)
            return

        try:
            response = bridge.reply(callback(invocation), progressive_input=bool(invocation.details.get("progress")))
        except Exception as error:  # User handler failures become WAMP ERROR replies.
            response = bridge.failure(error)
        finally:
            self._invocations.pop(event["id"], None)

        try:
            self._request("reply", id=event["id"], **response)
        except RuntimeError:
            pass  # Cancellation or disconnect can finish the invocation first.

    def call(self, procedure: str) -> CallRequest:
        return CallRequest(procedure, session=self)

    def register(self, procedure: str, invocation_handler) -> RegisterRequest:
        return RegisterRequest(procedure, invocation_handler, session=self)

    def publish(self, topic: str) -> PublishRequest:
        return PublishRequest(topic, session=self)

    def subscribe(self, topic: str, event_handler) -> SubscribeRequest:
        return SubscribeRequest(topic, event_handler, session=self)

    def _execute_call(self, req: CallRequest):
        if req.progress_receiver is not None or req.progress_sender is not None:
            return self._call_stream(
                req.procedure,
                args=req.arguments,
                kwargs=req.keyword_arguments,
                options=req.request_options,
                on_progress=req.progress_receiver,
                on_input=req.progress_sender,
            )

        return self._call(
            req.procedure,
            args=req.arguments,
            kwargs=req.keyword_arguments,
            options=req.request_options,
        )

    def _execute_register(self, req: RegisterRequest):
        return self._register(req.procedure, req.handler, options=req.request_options)

    def _execute_publish(self, req: PublishRequest):
        return self._publish(
            req.topic,
            args=req.arguments,
            kwargs=req.keyword_arguments,
            options=req.request_options,
        )

    def _execute_subscribe(self, req: SubscribeRequest):
        return self._subscribe(req.topic, req.handler, options=req.request_options)

    def _call_stream(
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

            if first is not None:
                args = first.args
                kwargs = first.kwargs
                options = {**(options or {}), **(first.options or {})}

        response = self._request("call_open", uri=procedure, args=args, kwargs=kwargs, options=options)
        handle = response["handle"]

        stop_input = threading.Event()

        input_result = Future()

        def send_inputs():
            try:
                more = first.options.get("progress") is True if (first and first.options) else False

                while more and not stop_input.is_set():
                    next_in = on_input()

                    if stop_input.is_set():
                        break

                    more = next_in.options.get("progress") is True if (next_in and next_in.options) else False
                    bridge.execute(
                        "call_input",
                        handle=handle,
                        args=next_in.args if next_in else None,
                        kwargs=next_in.kwargs if next_in else None,
                        more=more,
                    )
            except BaseException as error:
                input_result.set_exception(error)
                bridge.execute("close", handle=handle)
            else:
                input_result.set_result(None)

        input_thread = None

        if on_input is not None and first and first.options and first.options.get("progress") is True:
            input_thread = threading.Thread(target=send_inputs, daemon=True)
            input_thread.start()

        try:
            while True:
                try:
                    res_val = bridge.execute("call_next", handle=handle)
                except BaseException:
                    if input_result.done():
                        input_result.result()

                    raise

                if input_result.done():
                    input_result.result()

                res = bridge.result(res_val)

                if not (res.details and res.details.get("progress") is True):
                    return res

                if on_progress is not None:
                    on_progress(res)
        finally:
            stop_input.set()

            try:
                bridge.execute("close", handle=handle)
            except RuntimeError:
                pass

    def _call(self, procedure, args=None, kwargs=None, options=None):
        return bridge.result(self._request("call", uri=procedure, args=args, kwargs=kwargs, options=options))

    def _register(self, procedure, invocation_handler, options=None):
        if not callable(invocation_handler):
            raise TypeError("invocation handler must be callable")

        token = next(self._tokens)
        self._callbacks[token] = invocation_handler

        try:
            response = self._request("register", uri=procedure, token=token, options=options)
            return Registration(response["id"], self, token)
        except BaseException:
            self._callbacks.pop(token, None)
            raise

    def _subscribe(self, topic, event_handler, options=None):
        if not callable(event_handler):
            raise TypeError("event handler must be callable")

        token = next(self._tokens)
        self._callbacks[token] = event_handler

        try:
            response = self._request("subscribe", uri=topic, token=token, options=options)
            return Subscription(response["id"], self, token)
        except BaseException:
            self._callbacks.pop(token, None)
            raise

    def _publish(self, topic, args=None, kwargs=None, options=None):
        self._request("publish", uri=topic, args=args, kwargs=kwargs, options=options)

    def leave(self):
        try:
            self._request("leave")
        finally:
            self.close()

    def close(self):
        if self._host_callbacks is not None and self._base_session is not None and self._host_callbacks.claim_close():
            self._base_session.close()

        self._request("close")
        self._stopped.wait(self._config.close_timeout)

        if self._host_callbacks is not None:
            self._host_callbacks.close()
            self._host_callbacks = None

    def ping(self, timeout=10):
        if self._base_session is not None:
            return self._base_session.transport.ping(timeout)

        return self._request("ping", timeout=timeout)

    def _on_disconnect(self, callback):
        if callback is not None:
            self._disconnect_callbacks.append(callback)

    def run_forever(self):
        try:
            self._stopped.wait()
        except KeyboardInterrupt:
            self.leave()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class Client:
    def __init__(self, authenticator=None, serializer=None, config=None):
        self.authenticator = authenticator
        self.serializer = serializers.JSONSerializer() if serializer is None else serializer
        self.config = TransportConfig() if config is None else config

    def connect(self, uri, realm, connect_callback=None, disconnect_callback=None):
        if self.config.tcp_nodelay:
            raise NotImplementedError("tcp_nodelay configuration is not exposed by the native binding yet")

        callbacks = None

        try:
            if self.authenticator is not None or bridge.custom_serializer(self.serializer):
                callbacks = bridge.AuthCallbacks(self.authenticator, self.serializer)

            response = bridge.execute(
                "connect",
                uri=uri,
                realm=realm,
                auth=callbacks.fields() if callbacks else bridge.credentials(self.authenticator),
                serializer=bridge.serializer_fields(self.serializer, callbacks),
                open_timeout=self.config.open_timeout,
                ping_interval=self.config.ping_interval,
                ping_timeout=self.config.ping_timeout,
            )
            session = Session(response, self.config)

            if bridge.custom_serializer(self.serializer):
                session._host_callbacks = callbacks
                callbacks = None

            session._on_disconnect(disconnect_callback)

            if connect_callback is not None:
                try:
                    connect_callback()
                except BaseException:
                    session.close()
                    raise

            return session
        except BaseException as error:
            if callbacks is not None and callbacks.error is not None:
                raise callbacks.error from error

            raise
        finally:
            if callbacks is not None:
                callbacks.close()


def connect_anonymous(uri, realm):
    return Client().connect(uri, realm)


def connect(
    uri, realm, authenticator=None, serializer=None, config=None, connect_callback=None, disconnect_callback=None
):
    serializer = serializers.CBORSerializer() if serializer is None else serializer
    return Client(authenticator, serializer, config).connect(uri, realm, connect_callback, disconnect_callback)
