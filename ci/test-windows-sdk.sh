#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
archives=${1:-target/native-dist}
temporary=$(mktemp -d)
go_cache=${GOCACHE:?Go build cache directory required}
trap 'rm -rf "$temporary"' EXIT

cat > "$temporary/sdk-smoke.c" <<'C'
#include <xconn.h>
int main(void) { return xc_abi_version() != XC_ABI_VERSION; }
C

for archive in "$archives"/*-x86_64-pc-windows-gnu*.tar.gz; do
  (cd "$archives" && sha256sum --check "$(basename "$archive").sha256")
  tar -xzf "$archive" -C "$temporary"
  sdk="$temporary/$(basename "$archive" .tar.gz)"
  export PKG_CONFIG_PATH="$(cygpath -m "$sdk/lib/pkgconfig")"
  package=xconn
  test_tags=
  if [[ "$archive" == *-static.tar.gz ]]; then
    package=xconn-static
    test_tags=xconn_static
  fi
  read -r -a compiler_flags <<< "$(pkg-config --cflags --libs "$package")"
  gcc "$temporary/sdk-smoke.c" "${compiler_flags[@]}" -o "$temporary/sdk-smoke.exe"
  PATH="$sdk/lib:$PATH" "$temporary/sdk-smoke.exe"
  if [[ -n "$test_tags" ]] && objdump -p "$temporary/sdk-smoke.exe" | grep -qi 'DLL Name:.*xconn_ffi'; then
    echo 'Static SDK linked the XConn DLL' >&2
    exit 1
  fi
  # cgo caches pkg-config output; each SDK needs its own build cache.
  GOCACHE="$go_cache/$package" go -C bindings/go test -c -tags "$test_tags" \
    -o "$(cygpath -m "$temporary/go-sdk-smoke.exe")" .
  objdump -p "$temporary/go-sdk-smoke.exe" | grep -i 'DLL Name:'
  if [[ -n "$test_tags" ]] && objdump -p "$temporary/go-sdk-smoke.exe" | grep -qi 'DLL Name:.*xconn_ffi'; then
    echo 'Static Go consumer linked the XConn DLL' >&2
    exit 1
  fi
  PATH="$sdk/lib:$PATH" "$temporary/go-sdk-smoke.exe" -test.count=1 -test.timeout=90s \
    -test.run '^TestClientRouterMatrix$'
done
