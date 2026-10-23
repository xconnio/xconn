#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../.."
repo_root="$PWD"
variant="${1:-full}"
output="${2:-$repo_root/bindings/js/dist}"
mkdir -p "$output"
output="$(cd "$output" && pwd)"
features=()
case "$variant" in
  json) ;;
  codecs) features=(--features binary-codecs) ;;
  full) features=(--features binary-codecs,wampcra,cryptosign,packet-crypto) ;;
  *) echo 'Expected json, codecs, or full' >&2; exit 1 ;;
esac
bindgen="${WASM_BINDGEN_BIN:-$(command -v wasm-bindgen || true)}"
if [[ -z "$bindgen" ]]; then
  for candidate in "$HOME"/.cache/.wasm-pack/wasm-bindgen-*/wasm-bindgen; do
    if [[ -x "$candidate" ]] && [[ "$("$candidate" --version)" == "wasm-bindgen 0.2.129" ]]; then bindgen="$candidate"; break; fi
  done
fi
: "${bindgen:?Install wasm-bindgen-cli 0.2.129 or set WASM_BINDGEN_BIN}"
optimizer="${WASM_OPT_BIN:-$(command -v wasm-opt || true)}"
if [[ -z "$optimizer" ]]; then
  for candidate in "$HOME"/.cache/.wasm-pack/wasm-opt-*/bin/wasm-opt; do
    if [[ -x "$candidate" ]]; then optimizer="$candidate"; break; fi
  done
fi
: "${optimizer:?Install Binaryen wasm-opt or set WASM_OPT_BIN}"
cargo build --locked -p xconn-browser --target wasm32-unknown-unknown --profile browser --no-default-features "${features[@]}"
mkdir -p "$output/wasm" bindings/js/src/wasm
"$bindgen" "$repo_root/target/wasm32-unknown-unknown/browser/xconn_browser.wasm" \
  --target web --out-dir "$output/wasm" --out-name xconn_browser
"$optimizer" -Oz --enable-bulk-memory --enable-sign-ext --enable-nontrapping-float-to-int \
  "$output/wasm/xconn_browser_bg.wasm" -o "$output/wasm/optimized.wasm"
mv "$output/wasm/optimized.wasm" "$output/wasm/xconn_browser_bg.wasm"
# wasm-bindgen emits Symbol.dispose declarations; include their standard library
# so application projects can keep their existing ES2022/DOM library settings.
sed -i '1i/// <reference lib="esnext.disposable" />' "$output/wasm/xconn_browser.d.ts"
cp "$output/wasm/"*.d.ts bindings/js/src/wasm/
node bindings/js/scripts/constants.mjs
bindings/js/node_modules/.bin/tsc --project bindings/js/tsconfig.json --outDir "$output"
printf '{"type":"module"}\n' > "$output/package.json"
rustc --version > "$output/toolchain.txt"
"$bindgen" --version >> "$output/toolchain.txt"
"$optimizer" --version >> "$output/toolchain.txt"
