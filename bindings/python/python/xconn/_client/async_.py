import asyncio
import contextlib
import inspect
import random
from collections.abc import AsyncGenerator

from xconn import App, AsyncClient
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
from xconn.async_client import AsyncSession, Registration, Subscription
from xconn.types import Event, Invocation, InvokeOptions, RegisterOptions, Result
from xconn.utils import run


async def _setup(app: App, session: AsyncSession):
    app.set_session(session)
    check_app(app, _check_procedure, _check_topic)

    # Register concurrently; if the router rejects any, best-effort roll back the ones that succeeded.
    names = setup_names(app)
    tasks = [asyncio.ensure_future(register_async(session, uri, func)) for uri, func in app.procedures.items()]
    tasks += [asyncio.ensure_future(subscribe_async(session, uri, func)) for uri, func in app.topics.items()]
    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        await asyncio.gather(*tasks, return_exceptions=True)  # gather cancelled them; let them settle
        await _undo(names, tasks)
        raise

    errors = [result for result in results if isinstance(result, BaseException)]
    if errors:
        raise_setup_errors(errors, SETUP_FAILED, await _undo(names, tasks))


async def _undo(names: list[str], tasks: list[asyncio.Future]) -> list[str]:
    done = [
        (name, task.result())
        for name, task in zip(names, tasks, strict=True)
        if not task.cancelled() and task.exception() is None
    ]
    outcomes = await asyncio.gather(
        *(handle.unregister() if isinstance(handle, Registration) else handle.unsubscribe() for _, handle in done),
        return_exceptions=True,
    )
    return report_undo([name for name, _ in done], outcomes)


async def _connect_async(app: App, config: ClientConfig, directory: str = "."):
    auth = select_authenticator(config)
    client = AsyncClient(authenticator=auth, ws_config=config.websocket_config)

    async def wait_and_connect(previous_wait: float = INITIAL_WAIT):
        next_wait = min(random.uniform(INITIAL_WAIT, previous_wait * 3), MAX_WAIT)
        print(f"reconnecting in {next_wait:.1f} seconds...")
        await asyncio.sleep(next_wait)

        try:
            new_session = await client.connect(config.url, config.realm, on_connect, on_disconnect)
        except Exception as e:
            print(e)

            await wait_and_connect(next_wait)
            return

        await _setup(app, new_session)

    async def on_connect():
        print("connected", config.realm)

    async def on_disconnect():
        print("disconnected", config.realm)
        await wait_and_connect()

    session = await client.connect(config.url, config.realm, on_connect, on_disconnect)
    await _setup(app, session)

    if app.schema_procedure is not None and app.schema_procedure != "":
        docs = []

        for uri, func in app.procedures.items():
            docs.append(collect_docs(uri, func, "procedure"))

        for uri, func in app.topics.items():
            docs.append(collect_docs(uri, func, "topic"))

        async def get_schema(_: Invocation) -> Result:
            return Result(args=docs)

        options = RegisterOptions(invoke=InvokeOptions.ROUNDROBIN)
        await session.register(app.schema_procedure, get_schema).options(**options or {}).execute()
        print(f"serving schema at procedure {app.schema_procedure}")


def connect_async(app: str, config: ClientConfig, directory: str = "."):
    app = import_app(app, directory)
    # FIXME: also support running from an existing event loop instead of always starting a new one.
    run(_connect_async(app, config, directory=directory))


@contextlib.asynccontextmanager
async def resolve_dependencies(meta: ProcedureMetadata) -> AsyncGenerator:
    result = {}

    # Async handlers get plain and generator dependencies too. Sync handlers can't await, so registration refuses
    # async dependencies for them.
    for key, value in meta.dependencies.items():
        result[key] = value()

    for key, value in meta.async_dependencies.items():
        result[key] = await value()

    async with contextlib.AsyncExitStack() as stack:
        for name, dependency in meta.ctx_dependencies.items():
            result[name] = stack.enter_context(dependency())

        for name, dependency in meta.async_ctx_dependencies.items():
            result[name] = await stack.enter_async_context(dependency())

        yield result


def _check_procedure(uri: str, func: callable) -> ProcedureMetadata:
    if not inspect.iscoroutinefunction(func):
        raise RuntimeError(f"function {func.__name__} for procedure '{uri}' must be a coroutine")

    return _validate_procedure_function(func, uri)


def _check_topic(topic: str, func: callable) -> EventMetadata:
    if not inspect.iscoroutinefunction(func):
        raise RuntimeError(f"function {func.__name__} for topic '{topic}' must be a coroutine")

    return _validate_topic_function(func, topic)


async def register_async(session: AsyncSession, uri: str, func: callable) -> Registration:
    meta = _check_procedure(uri, func)

    async def _handle_invocation(invocation: Invocation) -> Result:
        ensure_caller_allowed(invocation.details, meta.allowed_roles)
        validate_invocation_parameters(invocation, meta)
        details = assemble_call_details(uri, meta, invocation)

        if meta.dynamic_model:
            kwargs = _sanitize_incoming_data(invocation.args, invocation.kwargs, meta.request_args)
            handle_model_validation(meta.request_model, **kwargs)

            async with resolve_dependencies(meta) as deps:
                result = await func(**kwargs, **deps, **details)

            return _handle_result(result, meta.response_model, meta.response_args)
        elif meta.request_model is not None:
            kwargs = _sanitize_incoming_data(invocation.args, invocation.kwargs, meta.request_args)
            model = handle_model_validation(meta.request_model, **kwargs, **details)
            input_data = {meta.positional_field_name: model}

            async with resolve_dependencies(meta) as deps:
                result = await func(**input_data, **deps, **details)

            return _handle_result(result, meta.response_model, meta.response_args)
        elif meta.no_args:
            async with resolve_dependencies(meta) as deps:
                result = await func(**deps, **details)

            return _handle_result(result, meta.response_model, meta.response_args)
        else:
            async with resolve_dependencies(meta) as deps:
                input_data = {meta.positional_field_name: invocation}
                result = await func(**input_data, **deps, **details)

            return _handle_result(result, meta.response_model, meta.response_args)

    registration = await session.register(uri, _handle_invocation).options(**meta.register_options or {}).execute()
    print(f"Registered procedure {uri}")
    return registration


async def subscribe_async(session: AsyncSession, topic: str, func: callable) -> Subscription:
    meta = _check_topic(topic, func)

    async def _handle_event(event: Event) -> None:
        details = assemble_event_details(topic, meta, event)
        validate_event_parameters(event, meta)

        if meta.dynamic_model:
            kwargs = _sanitize_incoming_data(event.args, event.kwargs, meta.request_args)
            handle_model_validation(meta.request_model, **kwargs)

            async with resolve_dependencies(meta) as deps:
                await func(**kwargs, **deps, **details)

        elif meta.request_model is not None:
            kwargs = _sanitize_incoming_data(event.args, event.kwargs, meta.request_args)
            model = handle_model_validation(meta.request_model, **kwargs)

            async with resolve_dependencies(meta) as deps:
                input_data = {meta.positional_field_name: model}
                await func(**input_data, **deps, **details)

        elif meta.no_args:
            async with resolve_dependencies(meta) as deps:
                await func(**deps, **details)
        else:
            async with resolve_dependencies(meta) as deps:
                input_data = {meta.positional_field_name: event}
                await func(**input_data, **deps, **details)

    subscription = await session.subscribe(topic, _handle_event).options(**meta.subscribe_options or {}).execute()
    print(f"Subscribed topic {topic}")
    return subscription
