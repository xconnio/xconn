"""Dependencies declared with Depends inside Annotated, as FastAPI allows."""

import functools
from typing import Annotated

import pytest
from pydantic import BaseModel, Field, ValidationError
from router_fixture import Router
from xconn import AsyncClient, Client
from xconn._client.async_ import register_async
from xconn._client.helpers import _validate_procedure_function, collect_docs
from xconn._client.sync import register_sync
from xconn.types import CallDetails, Depends


class Database:
    def __init__(self):
        self.closed = False


opened: list[Database] = []


def get_db():
    db = Database()
    opened.append(db)
    try:
        yield db
    finally:
        db.closed = True


async def get_async_db():
    db = Database()
    opened.append(db)
    try:
        yield db
    finally:
        db.closed = True


def get_setting() -> str:
    return "plain"


async def get_async_setting() -> str:
    return "async"


DB = Annotated[Database, Depends(get_db)]
AsyncDB = Annotated[Database, Depends(get_async_db)]
Setting = Annotated[str, Depends(get_setting)]
AsyncSetting = Annotated[str, Depends(get_async_setting)]


class Query(BaseModel):
    name: str
    limit: int = 10


def _deps(meta):
    return {
        "dependencies": list(meta.dependencies),
        "ctx_dependencies": list(meta.ctx_dependencies),
        "async_dependencies": list(meta.async_dependencies),
        "async_ctx_dependencies": list(meta.async_ctx_dependencies),
    }


def test_each_kind_of_dependency_on_an_async_handler():
    async def handler(
        item_id: int, details: CallDetails, db: DB, adb: AsyncDB, setting: Setting, asetting: AsyncSetting
    ): ...

    meta = _validate_procedure_function(handler, "x.kinds")

    assert _deps(meta) == {
        "dependencies": ["setting"],
        "ctx_dependencies": ["db"],
        "async_dependencies": ["asetting"],
        "async_ctx_dependencies": ["adb"],
    }
    assert meta.request_args == ["item_id"]
    assert meta.details_field == "details"


def test_annotated_dependency_with_a_pydantic_model():
    async def handler(rs: Query, db: DB): ...

    meta = _validate_procedure_function(handler, "x.model")

    assert meta.request_model is Query
    assert (meta.request_args, meta.request_kwargs) == (["name"], ["limit"])
    assert list(meta.ctx_dependencies) == ["db"]


def test_annotated_dependency_is_left_out_of_the_schema():
    async def handler(name: str, db: DB, limit: int = 10): ...

    schema = collect_docs("x.schema", handler, "procedure")["in_model"]

    assert set(schema["properties"]) == {"name", "limit"}


def test_other_annotated_metadata_is_not_a_dependency():
    async def handler(name: Annotated[str, "not a dependency"]): ...

    meta = _validate_procedure_function(handler, "x.plain")

    assert meta.request_args == ["name"]
    assert _deps(meta) == {k: [] for k in _deps(meta)}


def test_last_depends_in_annotated_wins():
    async def handler(setting: Annotated[str, Depends(get_async_setting), Depends(get_setting)]): ...

    meta = _validate_procedure_function(handler, "x.last")

    assert list(meta.dependencies) == ["setting"]
    assert not meta.async_dependencies


def test_optional_dependency_is_still_a_dependency():
    async def handler(name: str, db: DB | None, setting: Annotated[str, Depends(get_setting)] | None = None): ...

    meta = _validate_procedure_function(handler, "x.optional")

    assert (list(meta.ctx_dependencies), list(meta.dependencies)) == (["db"], ["setting"])
    assert (meta.request_args, meta.request_kwargs) == (["name"], [])


@pytest.mark.parametrize(
    "hint",
    [
        DB | AsyncDB,
        Annotated[Annotated[int, Depends(get_setting)] | Annotated[int, Depends(get_async_setting)], "metadata"],
        Annotated[Annotated[int, Depends(get_setting)] | None, Depends(get_async_setting)],
    ],
    ids=["union", "union-inside-annotated", "outer-depends-over-inner"],
)
def test_different_dependencies_are_refused(hint):
    async def handler(value: hint): ...

    with pytest.raises(RuntimeError, match="Parameter 'value' of '.*handler' declares different dependencies"):
        _validate_procedure_function(handler, "x.conflict")


def test_same_dependency_in_several_places_is_one_dependency():
    # Separate Depends objects for one async generator, which Depends wraps differently each time.
    hint = Annotated[Annotated[Database, Depends(get_async_db)] | None, "metadata", Depends(get_async_db)]

    async def handler(value: hint | Annotated[str, Depends(get_async_db)]): ...

    meta = _validate_procedure_function(handler, "x.same")

    assert list(meta.async_ctx_dependencies) == ["value"]


def test_plain_optional_argument():
    async def handler(name: str, limit: int | None = None): ...

    meta = _validate_procedure_function(handler, "x.plain_optional")

    assert (meta.request_args, meta.request_kwargs) == (["name"], ["limit"])


def test_constraints_inside_annotated_are_kept():
    async def handler(name: Annotated[str, Field(min_length=3)], db: DB): ...

    meta = _validate_procedure_function(handler, "x.constraints")

    assert meta.request_model(name="abc").name == "abc"
    with pytest.raises(ValidationError):
        meta.request_model(name="ab")


def test_partial_of_a_function_is_a_dependency():
    def get_named(prefix: str) -> str:
        return f"{prefix}-name"

    async def handler(name: Annotated[str, Depends(functools.partial(get_named, "x"))]): ...

    meta = _validate_procedure_function(handler, "x.partial")

    assert meta.dependencies["name"]() == "x-name"


class Settings:
    def __call__(self) -> str:
        return "instance"


@pytest.mark.parametrize(
    ("dependency", "kind"), [(Database, "type"), (Settings(), "Settings")], ids=["class", "callable-instance"]
)
def test_class_or_callable_instance_dependency_is_refused(dependency, kind):
    async def handler(value: Annotated[str, Depends(dependency)]): ...

    with pytest.raises(
        RuntimeError, match=rf"for parameter 'value' of '.*handler' must be a function or method, not {kind}"
    ):
        _validate_procedure_function(handler, "x.class")


def test_dependency_with_parameters_names_the_parameter_and_dependency():
    def get_user(user_id: int, session: str) -> str: ...

    async def handler(user: Annotated[str, Depends(get_user)]): ...

    with pytest.raises(
        RuntimeError,
        match=r"dependency '.*get_user' for parameter 'user' of '.*handler' must take no parameters, "
        r"but takes: user_id, session",
    ):
        _validate_procedure_function(handler, "x.params")


@pytest.mark.parametrize("dependency", [AsyncDB, AsyncSetting], ids=["async-generator", "async"])
def test_async_dependency_on_a_sync_handler_is_refused(dependency):
    def handler(value: dependency): ...

    with pytest.raises(RuntimeError, match=r"is async, so '.*handler' must be async too"):
        _validate_procedure_function(handler, "x.sync")


def test_errors_name_a_callable_instance_handler():
    class Handler:
        __annotations__ = {}

        async def __call__(self, db: Database = Depends(get_db)): ...  # noqa: B008

    with pytest.raises(RuntimeError, match="declares Depends as its default value"):
        _validate_procedure_function(Handler(), "x.instance")


@pytest.mark.parametrize("annotation", [Database, DB])
def test_depends_as_a_default_value_is_refused(annotation):
    async def handler(db: annotation = Depends(get_db)): ...  # noqa: B008

    with pytest.raises(RuntimeError, match=r"Declare it in the type instead: db: Annotated\[<type>, Depends\(...\)\]"):
        _validate_procedure_function(handler, "x.default")


def test_sync_procedure_gets_annotated_dependencies():
    opened.clear()

    def lookup(name: str, details: CallDetails, db: DB, setting: Setting) -> str:
        assert not db.closed
        assert isinstance(details, CallDetails)
        return f"{name}:{setting}"

    with Router() as router:
        router.listen()
        url = f"ws://{router.address}/ws"
        callee = Client().connect(url, router.realm)
        caller = Client().connect(url, router.realm)
        try:
            register_sync(callee, "x.sync.lookup", lookup)
            result = caller.call("x.sync.lookup").arg("drill").execute()
        finally:
            caller.leave()
            callee.leave()

    assert result.args == ["drill:plain"]
    assert len(opened) == 1 and opened[0].closed


async def test_async_procedure_gets_every_kind_of_dependency():
    opened.clear()

    async def lookup(rs: Query, db: DB, adb: AsyncDB, setting: Setting, asetting: AsyncSetting) -> str:
        assert not db.closed and not adb.closed
        return f"{rs.name}:{rs.limit}:{setting}:{asetting}"

    with Router() as router:
        router.listen()
        url = f"ws://{router.address}/ws"
        callee = await AsyncClient().connect(url, router.realm)
        caller = await AsyncClient().connect(url, router.realm)
        try:
            await register_async(callee, "x.async.lookup", lookup)
            result = await caller.call("x.async.lookup").arg("drill").kwarg("limit", 5).execute()
        finally:
            await caller.leave()
            await callee.leave()

    assert result.args == ["drill:5:plain:async"]
    assert len(opened) == 2 and all(db.closed for db in opened)
