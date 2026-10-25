#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
: "${1:?path to original Go reference binary}"
: "${CHROMIUM_BIN:?path to Chromium is required for browser CI}"
export XCONN_REFERENCE_GO_BIN="$(realpath "$1")"
if [[ "${XCONN_PREBUILT_HELPERS:-0}" != 1 ]]; then
  cargo build --locked -p xconn-core --features services --example discovery --example streaming_peer
  cargo build --locked -p xconn-binding-runtime --all-features --example transport_peer
fi
export XCONN_DISCOVERY_BIN="$PWD/target/debug/examples/discovery"
export XCONN_STREAM_RUST_BIN="$PWD/target/debug/examples/streaming_peer"
export XCONN_TRANSPORT_PEER="$PWD/target/debug/examples/transport_peer"
npm --prefix bindings/js ci --ignore-scripts
npm --prefix bindings/js run build
npm --prefix bindings/js run typecheck
if [[ "${XCONN_PREBUILT_HELPERS:-0}" != 1 ]]; then
  npm --prefix bindings/js run lint
  npm --prefix bindings/js run format:check
fi
npm --prefix bindings/js test
benchmarks/browser-wasm/build.sh
for variant in json codecs; do
  node bindings/js/tests/run.mjs "$PWD/target/browser-wasm/pkg/$variant" "$variant"
done
cmp bindings/js/dist/wasm/xconn_browser_bg.wasm target/browser-wasm/pkg/full/wasm/xconn_browser_bg.wasm
mkdir -p target/browser-package
package_archive="$(cd bindings/js && npm pack --json --ignore-scripts --pack-destination ../../target/browser-package | node -e 'let data=""; process.stdin.on("data", chunk => data += chunk); process.stdin.on("end", () => console.log(JSON.parse(data)[0].filename));')"
consumer="$(mktemp -d)"
trap 'rm -rf -- "$consumer"' EXIT
npm install --prefix "$consumer" --ignore-scripts --no-audit --no-fund "$PWD/target/browser-package/$package_archive"
printf '{"type":"module"}\n' > "$consumer/package.json"
sed 's|"../src/index.js"|"@xconnio/core"|' bindings/js/tests/compatibility.ts > "$consumer/compatibility.ts"
bindings/js/node_modules/.bin/tsc --noEmit --strict --target ES2022 --module NodeNext \
  --moduleResolution NodeNext --lib ES2022,DOM "$consumer/compatibility.ts"
node bindings/js/tests/run.mjs "$consumer/node_modules/@xconnio/core/dist" full
