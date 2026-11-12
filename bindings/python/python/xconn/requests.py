from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any


def _await_request(request, method):
    if request._session is None:
        raise RuntimeError("Request is not attached to a session")

    if not inspect.iscoroutinefunction(getattr(request._session, method)):
        raise TypeError("Use execute() for a blocking session; await requires AsyncSession")

    return request.execute().__await__()


class CallRequest:
    def __init__(self, procedure: str, session=None):
        self.procedure = procedure
        self._session = session
        self._args: list[Any] | None = None
        self._kwargs: dict[str, Any] | None = None
        self._options: dict[str, Any] = {}
        self._progress_receiver: Callable | None = None
        self._progress_sender: Callable | None = None

    @property
    def arguments(self) -> list[Any] | None:
        return self._args

    @property
    def keyword_arguments(self) -> dict[str, Any] | None:
        return self._kwargs

    @property
    def request_options(self) -> dict[str, Any] | None:
        return self._options if self._options else None

    @property
    def progress_receiver(self) -> Callable | None:
        return self._progress_receiver

    @property
    def progress_sender(self) -> Callable | None:
        return self._progress_sender

    def arg(self, val: Any) -> CallRequest:
        if self._args is None:
            self._args = []

        self._args.append(val)
        return self

    def args(self, *vals: Any) -> CallRequest:
        if self._args is None:
            self._args = []

        self._args.extend(vals)
        return self

    def kwarg(self, key: str, val: Any) -> CallRequest:
        if self._kwargs is None:
            self._kwargs = {}

        self._kwargs[key] = val
        return self

    def kwargs(self, **kwargs: Any) -> CallRequest:
        if self._kwargs is None:
            self._kwargs = {}

        self._kwargs.update(kwargs)
        return self

    def option(self, key: str, val: Any) -> CallRequest:
        self._options[key] = val
        return self

    def options(self, **options: Any) -> CallRequest:
        self._options.update(options)
        return self

    def on_progress(self, receiver: Callable) -> CallRequest:
        self._progress_receiver = receiver
        return self

    def on_input(self, sender: Callable) -> CallRequest:
        self._progress_sender = sender
        return self

    def stream(self, *, progressive_input=False, progressive_results=True, buffer_size=32, timeout=None):
        """Open an RPC with independently configurable progressive input and results."""
        from .call import Call, open_async

        if self._session is None:
            raise RuntimeError("CallRequest is not attached to a session")

        if self._progress_receiver is not None or self._progress_sender is not None:
            raise ValueError("streaming calls use send/finish/receive instead of callbacks")

        if not isinstance(progressive_input, bool):
            raise TypeError("progressive_input must be a boolean")

        if not isinstance(progressive_results, bool):
            raise TypeError("progressive_results must be a boolean")

        if type(buffer_size) is not int or not 1 <= buffer_size <= 65536:
            raise ValueError("result buffer size must be between 1 and 65536")

        fields = dict(
            uri=self.procedure,
            args=self.arguments,
            kwargs=self.keyword_arguments,
            options=self.request_options,
            progressive_input=progressive_input,
            progressive_results=progressive_results,
            buffer_size=buffer_size,
            timeout=timeout,
        )

        if inspect.iscoroutinefunction(self._session._request):
            return open_async(self._session, fields)

        return Call(self._session._request("stream_open", **fields)["handle"])

    def execute(self):
        if self._session is None:
            raise RuntimeError("CallRequest is not attached to a session")

        return self._session._execute_call(self)

    def __await__(self):
        return _await_request(self, "_execute_call")


class RegisterRequest:
    def __init__(self, procedure: str, handler: Callable, session=None):
        self.procedure = procedure
        self.handler = handler
        self._session = session
        self._options: dict[str, Any] = {}

    @property
    def request_options(self) -> dict[str, Any] | None:
        return self._options if self._options else None

    def option(self, key: str, val: Any) -> RegisterRequest:
        self._options[key] = val
        return self

    def options(self, **options: Any) -> RegisterRequest:
        self._options.update(options)
        return self

    def match(self, val: str) -> RegisterRequest:
        return self.option("match", val)

    def invoke(self, val: str) -> RegisterRequest:
        return self.option("invoke", val)

    def execute(self):
        if self._session is None:
            raise RuntimeError("RegisterRequest is not attached to a session")

        return self._session._execute_register(self)

    def __await__(self):
        return _await_request(self, "_execute_register")


class PublishRequest:
    def __init__(self, topic: str, session=None):
        self.topic = topic
        self._session = session
        self._args: list[Any] | None = None
        self._kwargs: dict[str, Any] | None = None
        self._options: dict[str, Any] = {}

    @property
    def arguments(self) -> list[Any] | None:
        return self._args

    @property
    def keyword_arguments(self) -> dict[str, Any] | None:
        return self._kwargs

    @property
    def request_options(self) -> dict[str, Any] | None:
        return self._options if self._options else None

    def arg(self, val: Any) -> PublishRequest:
        if self._args is None:
            self._args = []

        self._args.append(val)
        return self

    def args(self, *vals: Any) -> PublishRequest:
        if self._args is None:
            self._args = []

        self._args.extend(vals)
        return self

    def kwarg(self, key: str, val: Any) -> PublishRequest:
        if self._kwargs is None:
            self._kwargs = {}

        self._kwargs[key] = val
        return self

    def kwargs(self, **kwargs: Any) -> PublishRequest:
        if self._kwargs is None:
            self._kwargs = {}

        self._kwargs.update(kwargs)
        return self

    def option(self, key: str, val: Any) -> PublishRequest:
        self._options[key] = val
        return self

    def options(self, **options: Any) -> PublishRequest:
        self._options.update(options)
        return self

    def execute(self):
        if self._session is None:
            raise RuntimeError("PublishRequest is not attached to a session")

        return self._session._execute_publish(self)

    def __await__(self):
        return _await_request(self, "_execute_publish")


class SubscribeRequest:
    def __init__(self, topic: str, handler: Callable, session=None):
        self.topic = topic
        self.handler = handler
        self._session = session
        self._options: dict[str, Any] = {}

    @property
    def request_options(self) -> dict[str, Any] | None:
        return self._options if self._options else None

    def option(self, key: str, val: Any) -> SubscribeRequest:
        self._options[key] = val
        return self

    def options(self, **options: Any) -> SubscribeRequest:
        self._options.update(options)
        return self

    def match(self, val: str) -> SubscribeRequest:
        return self.option("match", val)

    def execute(self):
        if self._session is None:
            raise RuntimeError("SubscribeRequest is not attached to a session")

        return self._session._execute_subscribe(self)

    def __await__(self):
        return _await_request(self, "_execute_subscribe")
