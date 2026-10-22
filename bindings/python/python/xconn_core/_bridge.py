from . import _native
from .exception import ApplicationError
from .types import Result


def checked(response):
    if isinstance(response, dict) and response.get("error"):
        error = response["error"]
        raise ApplicationError(error["uri"], *(error["args"] or ()), **(error["kwargs"] or {}))

    return response


def execute(method, **fields):
    return checked(_native.execute({"method": method, **fields}))


async def execute_async(method, **fields):
    job = _native.submit({"method": method, **fields})

    try:
        await _native.wait_async(job)
        return checked(_native.take(job))
    finally:
        # Idempotent after take; otherwise cancels Rust work and unclaimed resources.
        _native.cancel(job)


def result(response):
    return Result(response["args"], response["kwargs"], response.get("details", {}))


def reply(value, *, progressive_input=False):
    if value is None:
        return {"error": {"uri": "io.xconn.no_result"}} if progressive_input else {}

    if not isinstance(value, Result):
        raise TypeError("invocation handlers must return xconn.types.Result or None")

    return {"args": value.args, "kwargs": value.kwargs}


def failure(error):
    if isinstance(error, ApplicationError):
        return {
            "error": {
                "uri": error.uri,
                "args": list(error.args),
                "kwargs": error.kwargs,
            }
        }

    return {"error": {"uri": "wamp.error.runtime_error", "args": [str(error)], "kwargs": {}}}


def credentials(authenticator):
    return {} if authenticator is None else authenticator._fields()


class AuthCallbacks:
    """Run the existing synchronous authenticator interface outside Rust."""

    def __init__(self, authenticator, serializer=None):
        import threading

        self.authenticator = authenticator
        self.serializer = serializer
        self.error = None
        self.handle = execute("callbacks_new")["handle"]
        self.worker = threading.Thread(target=self._run, daemon=True, name="xconn-auth")
        self.worker.start()

    def fields(self):
        if self.authenticator is None:
            return {}

        return {
            "method": self.authenticator.auth_method,
            "authid": self.authenticator.authid,
            "extra": self.authenticator.auth_extra,
            "callback": self.handle,
        }

    def _run(self):
        from xconn_core.messages import Challenge, ChallengeFields

        while True:
            try:
                event = execute("callbacks_next", handle=self.handle)

                if event is None:
                    return

                response = {"handle": self.handle, "id": event["id"]}

                try:
                    if event["method"] == "authenticate":
                        challenge = event["value"]
                        result = self.authenticator.authenticate(
                            Challenge(ChallengeFields(challenge["method"], challenge["extra"]))
                        )
                        response["value"] = {"signature": result.signature, "extra": result.extra}
                    elif event["method"] == "serialize":
                        from xconn_core.messages import to_message

                        response["value"] = self.serializer.serialize(to_message(event["value"]))
                    elif event["method"] == "deserialize":
                        payload = event["value"]

                        if serializer_name(self.serializer) == "json":
                            payload = payload.decode("utf-8")

                        response["value"] = self.serializer.deserialize(payload).marshal()
                    else:
                        raise RuntimeError(f"unknown callback {event['method']}")
                except BaseException as error:
                    self.error = error
                    response["error"] = str(error)

                execute("callbacks_reply", **response)
            except RuntimeError:
                return

    def close(self):
        execute("close", handle=self.handle)
        self.worker.join()


def serializer_name(serializer):
    from xconn_core.serializers import CBORSerializer, JSONSerializer, MsgPackSerializer

    if isinstance(serializer, str):
        return serializer

    for kind, name in [(JSONSerializer, "json"), (MsgPackSerializer, "msgpack"), (CBORSerializer, "cbor")]:
        if isinstance(serializer, kind):
            return name

    raise TypeError("serializer must implement a supported XConn serializer")


def custom_serializer(serializer):
    from xconn_core.serializers import CBORSerializer, JSONSerializer, MsgPackSerializer

    return not isinstance(serializer, str) and type(serializer) not in (
        JSONSerializer,
        MsgPackSerializer,
        CBORSerializer,
    )


def serializer_fields(serializer, callbacks):
    name = serializer_name(serializer)

    if custom_serializer(serializer):
        return {"name": name, "callback": callbacks.handle, "static": serializer.static()}

    return name


class PeerCallbacks:
    """Adapt user-supplied legacy transports without a Python protocol loop."""

    def __init__(self, base, asynchronous):
        import asyncio
        import threading
        from concurrent.futures import ThreadPoolExecutor

        self.base = base
        self._closed_base = False
        self._close_lock = threading.Lock()
        self.loop = asyncio.get_running_loop() if asynchronous else None
        self.executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="xconn-peer")
        self.handle = execute("callbacks_new")["handle"]
        self.worker = threading.Thread(target=self._run, daemon=True, name="xconn-peer-callbacks")
        self.worker.start()

    def claim_close(self):
        with self._close_lock:
            if self._closed_base:
                return False

            self._closed_base = True
            return True

    def _invoke(self, function, *args):
        import asyncio
        import inspect

        result = function(*args)

        if inspect.isawaitable(result):
            result = asyncio.run_coroutine_threadsafe(result, self.loop).result()

        return result

    def _respond(self, event):
        response = {"handle": self.handle, "id": event["id"]}

        try:
            method = event["method"]

            if method == "peer_read":
                value = self._invoke(self.base.receive)
                response["value"] = value.encode("utf-8") if isinstance(value, str) else value
            elif method == "peer_write":
                value = event["value"]

                if serializer_name(self.base.serializer) == "json":
                    value = value.decode("utf-8")

                self._invoke(self.base.send, value)
            elif method == "peer_close":
                if self.claim_close():
                    self._invoke(self.base.close)
            elif method == "serialize":
                from xconn_core.messages import to_message

                response["value"] = self.base.serializer.serialize(to_message(event["value"]))
            elif method == "deserialize":
                value = event["value"]

                if serializer_name(self.base.serializer) == "json":
                    value = value.decode("utf-8")

                response["value"] = self.base.serializer.deserialize(value).marshal()
            else:
                raise RuntimeError(f"unknown peer callback {method}")
        except Exception as error:
            response["error"] = str(error)

        try:
            execute("callbacks_reply", **response)
        except RuntimeError:
            pass  # Closing the owner also cancels outstanding callback replies.

    def _run(self):
        while True:
            try:
                event = execute("callbacks_next", handle=self.handle)
            except RuntimeError:
                return

            if event is None:
                return

            self.executor.submit(self._respond, event)

    def close(self):
        execute("close", handle=self.handle)
        self.worker.join()
        self.executor.shutdown(wait=False, cancel_futures=True)


def session_from_base(base, asynchronous=False):
    callbacks = PeerCallbacks(base, asynchronous)

    try:
        serializer = serializer_fields(base.serializer, callbacks)
        response = execute(
            "session_from_peer",
            session_id=base.id,
            realm=base.realm,
            authid=base.authid,
            authrole=base.authrole,
            authmethod="",
            authextra={},
            serializer=serializer,
            callback=callbacks.handle,
        )
        return response, callbacks
    except BaseException:
        callbacks.close()
        raise
