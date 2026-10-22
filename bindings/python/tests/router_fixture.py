"""External Rust router process used to exercise client APIs only."""

import asyncio
import json
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace


class Router:
    def __init__(self, realm=None):
        self.realm = realm or "realm"
        self._process = None
        self._directory = tempfile.TemporaryDirectory()
        self.address = None

    def add_realm(self, name):
        self.realm = name

    def listen(self, address="127.0.0.1:0", transport="websocket"):
        if self._process is not None:
            return SimpleNamespace(address=self.address)
        host, port = address.rsplit(":", 1)
        if int(port) == 0:
            with socket.socket() as listener:
                listener.bind((host, 0))
                port = listener.getsockname()[1]
        self.address = f"{host}:{port}"
        config = {
            "version": "1",
            "config": {"management": False},
            "realms": [
                {
                    "name": self.realm,
                    "roles": [
                        {
                            "name": "anonymous",
                            "permissions": [
                                {
                                    "uri": "",
                                    "match": "prefix",
                                    "allow_call": True,
                                    "allow_register": True,
                                    "allow_publish": True,
                                    "allow_subscribe": True,
                                }
                            ],
                        }
                    ],
                }
            ],
            "transports": [
                {
                    "type": transport,
                    "listener": "tcp",
                    "address": self.address,
                    "serializers": ["json", "cbor", "msgpack"],
                }
            ],
            "authenticators": {"anonymous": [{"authid": "anonymous", "realm": self.realm, "role": "anonymous"}]},
        }
        path = Path(self._directory.name) / "config.yaml"
        path.write_text(json.dumps(config))
        binary = Path(__file__).resolve().parents[3] / "target/debug/xconn-router"
        self._process = subprocess.Popen(
            [str(binary), "--config-file", str(path), "start"], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE
        )
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if self._process.poll() is not None:
                raise RuntimeError(self._process.stderr.read().decode())
            try:
                with socket.create_connection((host, int(port)), timeout=0.1):
                    return SimpleNamespace(address=self.address)
            except OSError:
                time.sleep(0.01)
        raise TimeoutError("Rust test router did not start")

    def connect(self, serializer="json"):
        from xconn_core import Client

        self.listen()
        return Client(serializer=serializer).connect(f"ws://{self.address}", self.realm)

    def close(self):
        if self._process is not None:
            self._process.terminate()
            self._process.wait(timeout=5)
            self._process.stderr.close()
        self._directory.cleanup()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class AsyncRouter(Router):
    async def connect(self, serializer="json"):
        from xconn_core import AsyncClient

        Router.listen(self)
        return await AsyncClient(serializer=serializer).connect(f"ws://{self.address}", self.realm)

    async def listen(self, address="127.0.0.1:0", transport="websocket"):
        return Router.listen(self, address, transport)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await asyncio.to_thread(self.close)
