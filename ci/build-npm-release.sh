#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
: "${CHROMIUM_BIN:?path to Chromium is required}"
version="$(node -p "require('./bindings/js/package.json').version")"
if [[ -n "${RELEASE_TAG:-}" && "$RELEASE_TAG" != "bindings/js/v$version" ]]; then
  echo "Release tag must match bindings/js/v$version" >&2
  exit 1
fi
npm --prefix bindings/js ci --ignore-scripts
npm --prefix bindings/js run lint
npm --prefix bindings/js run format:check
npm --prefix bindings/js run build
npm --prefix bindings/js run typecheck
cargo build --locked -p xconn-binding-runtime --all-features --example transport_peer
export XCONN_TRANSPORT_PEER="$PWD/target/debug/examples/transport_peer"
mkdir -p target/npm-release
(cd bindings/js && npm pack --ignore-scripts --pack-destination ../../target/npm-release)
consumer="$(mktemp -d)"
trap 'rm -rf -- "$consumer"' EXIT
npm install --prefix "$consumer" --ignore-scripts --no-audit --no-fund \
  "$PWD/target/npm-release/xconnio-core-$version.tgz"
node bindings/js/tests/run.mjs "$consumer/node_modules/@xconnio/core/dist" full
