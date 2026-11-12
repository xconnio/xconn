#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
: "${CHROMIUM_BIN:?path to Chromium is required}"
version="$(node -p "require('./bindings/js/package.json').version")"
if [[ -n "${RELEASE_TAG:-}" || "${PUBLISH_RELEASE:-false}" == true ]]; then
  python3 ci/release_version.py "${RELEASE_TAG:-}"
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
  "$PWD/target/npm-release/xconnio-xconn-$version.tgz"
node bindings/js/tests/run.mjs "$consumer/node_modules/@xconnio/xconn/dist" full
