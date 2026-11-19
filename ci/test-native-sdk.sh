#!/usr/bin/env bash
set -eo pipefail
cd "$(dirname "$0")/.."
archives=${1:-target/native-dist}
temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' EXIT

cat > "$temporary/sdk-smoke.c" <<'C'
#include <xconn.h>
int main(void) { return xc_abi_version() != XC_ABI_VERSION; }
C

for archive in "$archives"/*.tar.gz; do
  tar -xzf "$archive" -C "$temporary"
  sdk="$temporary/$(basename "$archive" .tar.gz)"
  export PKG_CONFIG_PATH="$sdk/lib/pkgconfig"
  package=xconn
  flags=()
  test_tags=
  if [[ "$archive" == *-static.tar.gz ]]; then
    package=xconn-static
    if [[ "$(uname -s)" == Linux ]]; then
      flags=(-static "-Wl,--gc-sections")
    fi
    test_tags=xconn_static
  elif [[ -n "${MAX_GLIBC_VERSION:-}" ]]; then
    required_glibc=$(LC_ALL=C readelf --version-info --wide "$sdk/lib/libxconn_ffi.so" |
      grep -oE 'Name: GLIBC_[^[:space:]]+' | cut -d_ -f2- | sort -V | tail -n 1)
    if [[ "$(printf '%s\n' "$required_glibc" "$MAX_GLIBC_VERSION" | sort -V | tail -n 1)" != "$MAX_GLIBC_VERSION" ]]; then
      echo "$archive requires GLIBC_$required_glibc (maximum GLIBC_$MAX_GLIBC_VERSION)" >&2
      exit 1
    fi
  fi
  # CI paths contain no spaces; pkg-config emits compiler arguments.
  read -r -a compiler_flags <<< "$(pkg-config --cflags --libs "$package")"
  cc "$temporary/sdk-smoke.c" "${compiler_flags[@]}" \
    "${flags[@]}" -o "$temporary/sdk-smoke"
  "$temporary/sdk-smoke"
  go -C bindings/go test -tags "$test_tags" -count=1 -timeout=90s \
    -run '^TestClientRouterMatrix$' .
  if [[ "$archive" == *-static.tar.gz ]]; then
    ci/test-go-sdk-install.sh "$archive"
  fi
done
