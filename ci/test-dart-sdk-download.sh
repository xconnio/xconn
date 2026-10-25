#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' EXIT

mkdir -p "$temporary/xconn_core" "$temporary/consumer/bin" "$temporary/tools"
cp -R bindings/dart/lib bindings/dart/hook "$temporary/xconn_core/"
cp bindings/dart/LICENSE "$temporary/xconn_core/"
# CI appends a native_library override to this manifest; test the default instead.
sed '/^hooks:/,$d' bindings/dart/pubspec.yaml > "$temporary/xconn_core/pubspec.yaml"
cat > "$temporary/consumer/pubspec.yaml" <<'YAML'
name: xconn_download_consumer
environment:
  sdk: '>=3.13.0 <4.0.0'
dependencies:
  xconn_core:
    path: ../xconn_core
YAML
cp bindings/dart/test/fixtures/native_asset_smoke.dart "$temporary/consumer/bin/main.dart"
for tool in cargo rustc; do
  cat > "$temporary/tools/$tool" <<'SH'
#!/bin/sh
echo 'SDK consumers must not invoke Rust tools' >&2
exit 99
SH
  chmod +x "$temporary/tools/$tool"
done
export PATH="$temporary/tools:$PATH"
cd "$temporary/consumer"
dart pub get
dart build cli --target bin/main.dart --output "$temporary/bundle"
"$temporary/bundle/bundle/bin/main"
