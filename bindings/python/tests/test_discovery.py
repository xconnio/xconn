"""Exercise the public client against an independent Rust discovery host."""

import asyncio
import os
import subprocess

import pytest
from xconn_core import Client, Endpoint
from xconn_core.async_client import AsyncClient


@pytest.fixture(scope="module")
def discovery_uri():
    binary = os.environ.get("XCONN_DISCOVERY_BIN")
    if binary is None:
        pytest.skip("Set XCONN_DISCOVERY_BIN to the Rust discovery example")

    process = subprocess.Popen([binary], stdout=subprocess.PIPE, text=True)
    try:
        uri = process.stdout.readline().strip()
        assert uri.startswith("ws://"), "discovery host did not start"
        yield uri
    finally:
        process.terminate()
        process.wait(timeout=5)


def test_blocking_discovery_advertises_updates_and_withdraws(discovery_uri):
    session = Client().connect(discovery_uri, "example")
    try:
        discovery = session.discovery("example.peers")
        endpoint = Endpoint("python-sync", ["quic://sensor:443"], ["telemetry"], {"version": 1})
        discovery.advertise(endpoint)
        assert discovery.get(endpoint.id) == endpoint
        endpoint.metadata["version"] = 2
        discovery.advertise(endpoint)
        assert endpoint in discovery.list()
        discovery.withdraw(endpoint.id)
        assert endpoint not in discovery.list()
    finally:
        session.close()


def test_async_discovery_uses_the_same_service_contract(discovery_uri):
    async def run():
        session = await AsyncClient().connect(discovery_uri, "example")
        try:
            discovery = session.discovery("example.peers")
            endpoint = Endpoint("python-async", ["https://sensor/wamp"], ["telemetry"])
            await discovery.advertise(endpoint)
            assert await discovery.get(endpoint.id) == endpoint
            assert endpoint in await discovery.list()
            await discovery.withdraw(endpoint.id)
        finally:
            await session.close()

    asyncio.run(run())
