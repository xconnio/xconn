"""Run the original client behavior tests against an external Rust router."""

import pytest
from router_fixture import Router


@pytest.fixture(scope="module", autouse=True)
def legacy_router():
    with Router() as router:
        router.add_realm("realm1")
        router.listen("127.0.0.1:8079")
        yield router
