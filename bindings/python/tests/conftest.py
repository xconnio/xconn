"""A real Rust router serving all transport adapters."""

import json
import os
import subprocess

import pytest


@pytest.fixture(scope="module")
def transport_peer():
    binary = os.environ.get("XCONN_TRANSPORT_PEER")
    if binary is None:
        pytest.skip("Set XCONN_TRANSPORT_PEER to the Rust transport_peer example")
    process = subprocess.Popen([binary], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        yield json.loads(process.stdout.readline())
    finally:
        process.stdin.close()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
