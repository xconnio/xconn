import json
import math
from pathlib import Path

import pytest
from xconn_core import Event, Invocation, PayloadError, Result

CASES = json.loads((Path(__file__).resolve().parents[3] / "tests/payload/cases.json").read_text())
TYPES = {
    "string": "str",
    "bool": "bool",
    "int": "int",
    "uint": "uint",
    "float": "float",
    "bytes": "bytes",
    "list": "list",
    "dict": "dict",
}
DEFAULTS = {
    "string": "fallback",
    "bool": False,
    "int": 7,
    "uint": 7,
    "float": 7.0,
    "bytes": b"fallback",
    "list": [],
    "dict": {},
}


def decode(case):
    tag, value = case["tag"], case["value"]
    if tag in ("int", "uint"):
        return int(value)
    if tag == "bytes":
        return bytes(value)
    if tag == "special":
        return {"nan": math.nan, "infinity": math.inf, "-infinity": -math.inf}[value]
    return value


@pytest.mark.parametrize("cls", [Invocation, Event, Result])
@pytest.mark.parametrize("case", CASES, ids=lambda case: case["name"])
def test_shared_payload_matrix(cls, case):
    value = decode(case)
    payload = cls([value], {"value": value}, {})
    for kind, suffix in TYPES.items():
        for scope, key in [("arg", 0), ("kwarg", "value")]:
            strict = getattr(payload, f"{scope}_{suffix}")
            defaulted = getattr(payload, f"{scope}_{suffix}_or")
            fallback = DEFAULTS[kind]
            if kind in case["accept"]:
                expected = float(value) if kind == "float" else int(value) if kind in ("int", "uint") else value
                assert strict(key) == expected
                assert defaulted(key, fallback) == expected
            else:
                with pytest.raises(PayloadError):
                    strict(key)
                assert defaulted(key, fallback) == fallback
            missing = -1 if scope == "arg" else "absent"
            with pytest.raises(PayloadError) as error:
                strict(missing)
            assert error.value.kind == "missing"
            assert defaulted(missing, fallback) == fallback
    assert payload.require_arg(0) is value
    assert payload.require_kwarg("value") is value


def test_payload_errors_and_raw_lookup_distinguish_null_from_absence():
    payload = Event([None, 1.5, True], {"null": None}, {})
    assert payload.require_arg(0) is None
    with pytest.raises(PayloadError, match="Missing"):
        payload.require_arg(3)
    for index, kind in [(0, "null"), (1, "type"), (2, "type")]:
        with pytest.raises(PayloadError) as error:
            payload.arg_int(index)
        assert error.value.kind == kind
    assert payload.arg_or(0, "fallback") == "fallback"
    assert payload.kwarg_or("absent", "fallback") == "fallback"
