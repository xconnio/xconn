#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
version=$(sed -n 's/^version: //p' bindings/dart/pubspec.yaml)
output="$(pwd)/target/dart-release"
temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' EXIT
package="$temporary/bindings/dart"
mkdir -p "$package"
# Tests share fixtures with the other bindings.
ln -s "$(pwd)/tests" "$temporary/tests"
tar --exclude=.dart_tool --exclude=build --exclude=pubspec_overrides.yaml \
  -c -C bindings/dart . | tar -x -C "$package"
(
  cd "$package"
  dart pub get --enforce-lockfile
  dart format --line-length 120 lib/src/native_sdk.dart
  dart analyze
  dart format --output=none --set-exit-if-changed --line-length 120 .
  dart test
  dart pub publish --dry-run
)
ci/test-dart-sdk-download.sh
mkdir -p "$output"
tar --exclude=.dart_tool --exclude=build \
  -czf "$output/xconn-$version.tar.gz" -C "$package" .
