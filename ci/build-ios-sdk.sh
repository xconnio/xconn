#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ "$(uname -s)" != Darwin ]]; then
  echo 'Build iOS SDKs on macOS with Xcode installed' >&2
  exit 1
fi

targets=(aarch64-apple-ios aarch64-apple-ios-sim)
if [[ -n "${2:-}" ]]; then
  targets=("$2")
fi

export IPHONEOS_DEPLOYMENT_TARGET=13.0
for target in "${targets[@]}"; do
  case "$target" in
    aarch64-apple-ios) sdk=iphoneos ;;
    aarch64-apple-ios-sim) sdk=iphonesimulator ;;
    *) echo "Unsupported iOS SDK target: $target" >&2; exit 1 ;;
  esac
  SDKROOT=$(xcrun --sdk "$sdk" --show-sdk-path) \
    python3 scripts/package-native.py --profile native --target "$target" "${1:-target/native-dist}"
done
