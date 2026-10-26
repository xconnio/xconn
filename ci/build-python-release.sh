#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
output="$(pwd)/target/python-release"
uv build bindings/python --python "${PYTHON_BIN:-python3}" --out-dir "$output/build"
mkdir -p "$output/dist"
cp "$output/build/"*.tar.gz "$output/dist/"
if [[ -n "${MANYLINUX_PLATFORM:-}" ]]; then
  uvx --python "${PYTHON_BIN:-python3}" auditwheel repair \
    --plat "$MANYLINUX_PLATFORM" --wheel-dir "$output/dist" "$output/build/"*.whl
else
  cp "$output/build/"*.whl "$output/dist/"
fi
uvx --python "${PYTHON_BIN:-python3}" twine check "$output/dist/"*
uv venv --python "${PYTHON_BIN:-python3}" "$output/venv"
uv pip install --python "$output/venv/bin/python" "$output/dist/"*.whl pytest pytest-asyncio
# Exercise Rust-backed crypto from the installed wheel, with no Python crypto deps.
"$output/venv/bin/python" -m pytest bindings/python/tests/test_crypto.py -q
