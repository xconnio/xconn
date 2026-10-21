#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export XCONN_REFERENCE_GO_BIN="$(realpath "${1:?path to original Go reference binary}")"
export XCONN_GO_INTEROP_BIN="$XCONN_REFERENCE_GO_BIN"
cargo test --locked -p xconn-core --all-features --test independent -- --ignored
cargo test --locked -p xconn-core --all-features --test interop -- --ignored
cargo test --locked -p xconn-core --all-features --test router_binary -- --ignored
cargo test --locked -p xconn-webrtc --all-features --test go_interop -- --ignored
