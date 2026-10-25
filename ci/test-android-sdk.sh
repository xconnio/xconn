#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
: "${ANDROID_NDK_HOME:?set ANDROID_NDK_HOME to the Android NDK directory}"
archives=${1:-target/native-dist}
toolchain="$ANDROID_NDK_HOME/toolchains/llvm/prebuilt/linux-x86_64/bin"
temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' EXIT

cat > "$temporary/sdk-smoke.c" <<'C'
#include <xconn.h>
int main(void) {
  if (xc_abi_version() != XC_ABI_VERSION) return 1;
  XcResult *result = xc_process(5, 0, NULL, 0);
  if (!result) return 2;
  xc_result_free(result);
  return 0;
}
C

for archive in "$archives"/*-aarch64-linux-android.tar.gz; do
  tar -xzf "$archive" -C "$temporary"
  sdk="$temporary/$(basename "$archive" .tar.gz)"
  library="$sdk/lib/libxconn_ffi.so"
  "$toolchain/llvm-readelf" --file-header "$library" | grep -q 'Machine:.*AArch64'
  "$toolchain/llvm-readelf" --dyn-syms "$library" > "$temporary/symbols"
  for symbol in xc_abi_version xc_process xc_notify xc_result_free; do
    grep -Eq "GLOBAL +DEFAULT +[0-9]+ +$symbol$" "$temporary/symbols"
  done
  alignments=$("$toolchain/llvm-readelf" --program-headers "$library" | awk '$1 == "LOAD" {print $NF}')
  test -n "$alignments"
  for alignment in $alignments; do
    if (( alignment < 0x4000 )); then
      echo 'Android SDK must support 16 KB page sizes' >&2
      exit 1
    fi
  done
  "$toolchain/aarch64-linux-android24-clang" "$temporary/sdk-smoke.c" \
    -I"$sdk/include" -L"$sdk/lib" -lxconn_ffi -Wl,--no-allow-shlib-undefined \
    -o "$temporary/sdk-smoke"
done
