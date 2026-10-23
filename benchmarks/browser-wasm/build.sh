#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
for variant in json codecs full; do
  rm -rf -- "$PWD/target/browser-wasm/pkg/$variant"
  bindings/js/scripts/build.sh "$variant" "$PWD/target/browser-wasm/pkg/$variant"
done
node benchmarks/browser-wasm/measure.mjs "$PWD/target/browser-wasm/pkg"
