import contextlib
import inspect
import random
import time
from collections.abc import Generator
from typing import Any

from xconn_core import App
from xconn_core._client.helpers import (
    INITIAL_WAIT,
    MAX_WAIT,
    ProcedureMetadata,
    _handle_result,
    _sanitize_incoming_data,
    _validate_procedure_function,
    _validate_topic_function,
    assemble_call_details,
    assemble_event_details,
    collect_docs,
    ensure_caller_allowed,
    handle_model_validation,
    import_app,
    select_authenticator,
    validate_event_parameters,
    validate_invocation_parameters,
)
from xconn_core._client.types import ClientConfig
from xconn_core.client import Client, Session
from xconn_core.types import Event, Invocation, InvokeOptions, RegisterOptions, Result


def _setup(app: App, session: Session):
    app.set_session(session)

    for uri, func in app.procedures.items():
        register_sync(session, uri, func)

    for uri, func in app.topics.items():
        subscribe_sync(session, uri, func)


def _connect_sync(app: App, config: ClientConfig, directory: str = "."):

    auth = select_authenticator(config)
    client = Client(authenticator=auth, config=config.websocket_config)

    def wait_and_connect(previous_wait: float = INITIAL_WAIT):
        next_wait = min(random.uniform(INITIAL_WAIT, previous_wait * 3), MAX_WAIT)
        print(f"reconnecting in {next_wait:.1f} seconds...")
        time.sleep(next_wait)

        try:
            new_session = client.connect(config.url, config.realm, on_connect, on_disconnect)
        except Exception as e:
            print(e)

            wait_and_connect(next_wait)
            return

        _setup(app, new_session)

    def on_connect():
        print("connected", config.realm)

    def on_disconnect():
        print("disconnected", config.realm)
        wait_and_connect()

    session = client.connect(config.url, config.realm, on_connect, on_disconnect)
    _setup(app, session)

    if app.schema_procedure is not None and app.schema_procedure != "":
        docs = []

        for uri, func in app.procedures.items():
            docs.append(collect_docs(uri, func, "procedure"))

        for uri, func in app.topics.items():
            docs.append(collect_docs(uri, func, "topic"))

        def get_schema(_: Invocation) -> Result:
            return Result(args=docs)

        options = RegisterOptions(invoke=InvokeOptions.ROUNDROBIN)
        session.register(app.schema_procedure, get_schema).options(**options or {}).execute()
        print(f"serving schema at procedure {app.schema_procedure}")


def connect_sync(app: str, config: ClientConfig, directory: str = "."):
    imported_app = import_app(app, directory)
    _connect_sync(imported_app, config, directory)


@contextlib.contextmanager
def resolve_dependencies(meta: ProcedureMetadata) -> Generator[dict[Any, Any], Any, None]:
    result = {}

    for key, value in meta.dependencies.items():
        result[key] = value()

    with contextlib.ExitStack() as stack:
        for name, dependency in meta.ctx_dependencies.items():
            result[name] = stack.enter_context(dependency())

        yield result


def register_sync(session: Session, uri: str, func: callable):
    if inspect.iscoroutinefunction(func):
        raise RuntimeError(f"function {func.__name__} for procedure '{uri}' must not be a coroutine")

    meta = _validate_procedure_function(func, uri)

    def _handle_invocation(invocation: Invocation) -> Result:
        ensure_caller_allowed(invocation.details, meta.allowed_roles)
        validate_invocation_parameters(invocation, meta)
        details = assemble_call_details(uri, meta, invocation)

        if meta.dynamic_model:
            kwargs = _sanitize_incoming_data(invocation.args, invocation.kwargs, meta.request_args)
            handle_model_validation(meta.request_model, **kwargs)

            with resolve_dependencies(meta) as deps:
                result = func(**kwargs, **deps, **details)

            return _handle_result(result, meta.response_model, meta.response_args)
        elif meta.request_model is not None:
            kwargs = _sanitize_incoming_data(invocation.args, invocation.kwargs, meta.request_args)
            model = handle_model_validation(meta.request_model, **kwargs)

            with resolve_dependencies(meta) as deps:
                input_data = {meta.positional_field_name: model}
                result = func(**input_data, **deps, **details)

            return _handle_result(result, meta.response_model, meta.response_args)
        elif meta.no_args:
            with resolve_dependencies(meta) as deps:
                result = func(**deps, **details)

            return _handle_result(result, meta.response_model, meta.response_args)
        else:
            with resolve_dependencies(meta) as deps:
                input_data = {meta.positional_field_name: invocation}
                result = func(**input_data, **deps, **details)

            return _handle_result(result, meta.response_model, meta.response_args)

    session.register(uri, _handle_invocation).options(**meta.register_options or {}).execute()
    print(f"Registered procedure {uri}")


def subscribe_sync(session: Session, topic: str, func: callable):
    if inspect.iscoroutinefunction(func):
        raise RuntimeError(f"function {func.__name__} for topic '{topic}' must not be a coroutine")

    meta = _validate_topic_function(func, topic)

    def _handle_event(event: Event):
        details = assemble_event_details(topic, meta, event)
        validate_event_parameters(event, meta)

        if meta.dynamic_model:
            kwargs = _sanitize_incoming_data(event.args, event.kwargs, meta.request_args)
            handle_model_validation(meta.request_model, **kwargs)

            with resolve_dependencies(meta) as deps:
                func(**kwargs, **deps, **details)

        elif meta.request_model is not None:
            kwargs = _sanitize_incoming_data(event.args, event.kwargs, meta.request_args)
            model = handle_model_validation(meta.request_model, **kwargs)

            with resolve_dependencies(meta) as deps:
                input_data = {meta.positional_field_name: model}
                func(**input_data, **deps, **details)

        elif meta.no_args:
            with resolve_dependencies(meta) as deps:
                func(**deps, **details)
        else:
            with resolve_dependencies(meta) as deps:
                input_data = {meta.positional_field_name: event}
                func(**input_data, **deps, **details)

    session.subscribe(topic, _handle_event).options(**meta.subscribe_options or {}).execute()
    print(f"Subscribed topic {topic}")
