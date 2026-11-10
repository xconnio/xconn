"""Dependencies declared with Depends inside Annotated, as FastAPI allows."""

from typing import Annotated

import pytest
from pydantic import BaseModel
from router_fixture import Router
from xconn_core import AsyncClient, Client
from xconn_core._client.async_ import register_async
from xconn_core._client.helpers import _validate_procedure_function, collect_docs
from xconn_core._client.sync import register_sync
from xconn_core.types import CallDetails, Depends


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


def test_each_kind_of_dependency():
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


async def test_async_procedure_gets_annotated_dependencies():
    opened.clear()

    async def lookup(rs: Query, db: AsyncDB, setting: AsyncSetting) -> str:
        assert not db.closed
        return f"{rs.name}:{rs.limit}:{setting}"

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

    assert result.args == ["drill:5:async"]
    assert len(opened) == 1 and opened[0].closed
