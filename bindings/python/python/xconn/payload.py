"""Strict payload access shared by Invocation, Event and Result."""

from __future__ import annotations

import math
from typing import Any


class PayloadError(ValueError):
    """A missing, null, wrongly typed or out-of-range payload value."""

    def __init__(self, kind: str, message: str):
        super().__init__(message)
        self.kind = kind


def _convert(value: Any, kind: str):
    if value is None:
        raise PayloadError("null", "Payload value is null")

    if kind == "string" and type(value) is str:
        return value

    if kind == "bool" and type(value) is bool:
        return value

    if kind in ("int", "uint", "float") and type(value) in (int, float):
        if type(value) is float and not math.isfinite(value):
            raise PayloadError("range", "Numeric payload must be finite")

        if kind == "float":
            try:
                result = float(value)
            except OverflowError as error:
                raise PayloadError("range", "Payload overflows float64") from error

            if math.isfinite(result):
                return result

            raise PayloadError("range", "Payload overflows float64")

        if type(value) is float and abs(value) > (1 << 53) - 1:
            raise PayloadError("range", "Integer float is not safely representable")

        if type(value) is float and not value.is_integer():
            raise PayloadError("type", "Integer payload must not have a fractional part")

        lower, upper = (-(1 << 63), 1 << 63) if kind == "int" else (0, 1 << 64)

        if lower <= value < upper:
            return int(value)

        raise PayloadError("range", f"Payload overflows {kind}64")

    if kind == "bytes" and isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)

    if kind == "list" and isinstance(value, list):
        return value

    if kind == "dict" and isinstance(value, dict) and all(isinstance(key, str) for key in value):
        return value

    raise PayloadError("type", f"Expected {kind} payload, got {type(value).__name__}")


class Payload:
    args: list[Any] | None
    kwargs: dict[str, Any] | None

    def require_arg(self, index: int) -> Any:
        if type(index) is not int or index < 0 or self.args is None or index >= len(self.args):
            raise PayloadError("missing", f"Missing positional argument {index}")

        return self.args[index]

    def require_kwarg(self, key: str) -> Any:
        if self.kwargs is None or key not in self.kwargs:
            raise PayloadError("missing", f"Missing keyword argument {key}")

        return self.kwargs[key]

    def arg(self, index: int) -> Any:
        try:
            return self.require_arg(index)
        except PayloadError:
            return None

    def kwarg(self, key: str) -> Any:
        try:
            return self.require_kwarg(key)
        except PayloadError:
            return None

    def arg_or(self, index: int, default: Any) -> Any:
        value = self.arg(index)
        return default if value is None else value

    def kwarg_or(self, key: str, default: Any) -> Any:
        value = self.kwarg(key)
        return default if value is None else value

    def arg_str(self, index: int) -> str:
        return _convert(self.require_arg(index), "string")

    def arg_str_or(self, index: int, default: str) -> str:
        try:
            return self.arg_str(index)
        except PayloadError:
            return default

    def arg_bool(self, index: int) -> bool:
        return _convert(self.require_arg(index), "bool")

    def arg_bool_or(self, index: int, default: bool) -> bool:
        try:
            return self.arg_bool(index)
        except PayloadError:
            return default

    def arg_int(self, index: int) -> int:
        return _convert(self.require_arg(index), "int")

    def arg_int_or(self, index: int, default: int) -> int:
        try:
            return self.arg_int(index)
        except PayloadError:
            return default

    def arg_uint(self, index: int) -> int:
        return _convert(self.require_arg(index), "uint")

    def arg_uint_or(self, index: int, default: int) -> int:
        try:
            return self.arg_uint(index)
        except PayloadError:
            return default

    def arg_float(self, index: int) -> float:
        return _convert(self.require_arg(index), "float")

    def arg_float_or(self, index: int, default: float) -> float:
        try:
            return self.arg_float(index)
        except PayloadError:
            return default

    def arg_bytes(self, index: int) -> bytes:
        return _convert(self.require_arg(index), "bytes")

    def arg_bytes_or(self, index: int, default: bytes) -> bytes:
        try:
            return self.arg_bytes(index)
        except PayloadError:
            return default

    def arg_list(self, index: int) -> list[Any]:
        return _convert(self.require_arg(index), "list")

    def arg_list_or(self, index: int, default: list[Any]) -> list[Any]:
        try:
            return self.arg_list(index)
        except PayloadError:
            return default

    def arg_dict(self, index: int) -> dict[str, Any]:
        return _convert(self.require_arg(index), "dict")

    def arg_dict_or(self, index: int, default: dict[str, Any]) -> dict[str, Any]:
        try:
            return self.arg_dict(index)
        except PayloadError:
            return default

    def kwarg_str(self, key: str) -> str:
        return _convert(self.require_kwarg(key), "string")

    def kwarg_str_or(self, key: str, default: str) -> str:
        try:
            return self.kwarg_str(key)
        except PayloadError:
            return default

    def kwarg_bool(self, key: str) -> bool:
        return _convert(self.require_kwarg(key), "bool")

    def kwarg_bool_or(self, key: str, default: bool) -> bool:
        try:
            return self.kwarg_bool(key)
        except PayloadError:
            return default

    def kwarg_int(self, key: str) -> int:
        return _convert(self.require_kwarg(key), "int")

    def kwarg_int_or(self, key: str, default: int) -> int:
        try:
            return self.kwarg_int(key)
        except PayloadError:
            return default

    def kwarg_uint(self, key: str) -> int:
        return _convert(self.require_kwarg(key), "uint")

    def kwarg_uint_or(self, key: str, default: int) -> int:
        try:
            return self.kwarg_uint(key)
        except PayloadError:
            return default

    def kwarg_float(self, key: str) -> float:
        return _convert(self.require_kwarg(key), "float")

    def kwarg_float_or(self, key: str, default: float) -> float:
        try:
            return self.kwarg_float(key)
        except PayloadError:
            return default

    def kwarg_bytes(self, key: str) -> bytes:
        return _convert(self.require_kwarg(key), "bytes")

    def kwarg_bytes_or(self, key: str, default: bytes) -> bytes:
        try:
            return self.kwarg_bytes(key)
        except PayloadError:
            return default

    def kwarg_list(self, key: str) -> list[Any]:
        return _convert(self.require_kwarg(key), "list")

    def kwarg_list_or(self, key: str, default: list[Any]) -> list[Any]:
        try:
            return self.kwarg_list(key)
        except PayloadError:
            return default

    def kwarg_dict(self, key: str) -> dict[str, Any]:
        return _convert(self.require_kwarg(key), "dict")

    def kwarg_dict_or(self, key: str, default: dict[str, Any]) -> dict[str, Any]:
        try:
            return self.kwarg_dict(key)
        except PayloadError:
            return default
