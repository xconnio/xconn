import json
from pathlib import Path

import pytest
from xconn_core import crypto


def test_packets_match_the_independent_go_implementation():
    fixture = json.loads((Path(__file__).resolve().parents[3] / "tests/crypto/go-reference.json").read_text())

    def data(key):
        return bytes.fromhex(fixture[key])

    secret = crypto.shared_secret(data("private"), data("public"))
    assert secret == data("shared")
    key = crypto.derive_key(secret, data("info"))
    assert key == data("key")
    assert crypto.decrypt(data("packet"), key, data("aad")) == data("plaintext")
    packet = crypto.encrypt(data("plaintext"), key, data("aad"))
    assert crypto.decrypt(packet, key, data("aad")) == data("plaintext")
    with pytest.raises(RuntimeError):
        crypto.decrypt(packet, key, b"wrong context")
    public, private = crypto.generate_key_pair()
    assert len(public) == len(private) == 32
    with pytest.raises(RuntimeError):
        crypto.shared_secret(private, bytes(32))
