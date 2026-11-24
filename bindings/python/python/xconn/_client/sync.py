import contextlib
import inspect
import random
import time
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from xconn import App
from xconn._client.helpers import (
    INITIAL_WAIT,
    MAX_WAIT,
    SETUP_FAILED,
    EventMetadata,
    ProcedureMetadata,
    _handle_result,
    _sanitize_incoming_data,
    _validate_procedure_function,
    _validate_topic_function,
    assemble_call_details,
    assemble_event_details,
    check_app,
    collect_docs,
    ensure_caller_allowed,
    handle_model_validation,
    import_app,
    raise_setup_errors,
    report_undo,
    select_authenticator,
    setup_names,
    validate_event_parameters,
    validate_invocation_parameters,
)
from xconn._client.types import ClientConfig
from xconn.client import Client, Registration, Session, Subscription
from xconn.types import Event, Invocation, InvokeOptions, RegisterOptions, Result


def _setup(app: App, session: Session):
    app.set_session(session)
    check_app(app, _check_procedure, _check_topic)

    # Register concurrently; if the router rejects any, best-effort roll back the ones that succeeded.
    names = setup_names(app)
    with ThreadPoolExecutor(thread_name_prefix="xconn-setup") as pool:
        futures = [pool.submit(register_sync, session, uri, func) for uri, func in app.procedures.items()]
        futures += [pool.submit(subscribe_sync, session, uri, func) for uri, func in app.topics.items()]
        errors = [future.exception() for future in futures if future.exception() is not None]
        if not errors:
            return

        done = [(name, future.result()) for name, future in zip(names, futures, strict=True) if not future.exception()]
        undo = [
            pool.submit(handle.unregister if isinstance(handle, Registration) else handle.unsubscribe)
            for _, handle in done
        ]
        undo_failures = report_undo([name for name, _ in done], [future.exception() for future in undo])

    raise_setup_errors(errors, SETUP_FAILED, undo_failures)


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


def _check_procedure(uri: str, func: callable) -> ProcedureMetadata:
    if inspect.iscoroutinefunction(func):
        raise RuntimeError(f"function {func.__name__} for procedure '{uri}' must not be a coroutine")

    return _validate_procedure_function(func, uri)


def _check_topic(topic: str, func: callable) -> EventMetadata:
    if inspect.iscoroutinefunction(func):
        raise RuntimeError(f"function {func.__name__} for topic '{topic}' must not be a coroutine")

    return _validate_topic_function(func, topic)


def register_sync(session: Session, uri: str, func: callable) -> Registration:
    meta = _check_procedure(uri, func)

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

    registration = session.register(uri, _handle_invocation).options(**meta.register_options or {}).execute()
    print(f"Registered procedure {uri}")
    return registration


def subscribe_sync(session: Session, topic: str, func: callable) -> Subscription:
    meta = _check_topic(topic, func)

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

    subscription = session.subscribe(topic, _handle_event).options(**meta.subscribe_options or {}).execute()
    print(f"Subscribed topic {topic}")
    return subscription
