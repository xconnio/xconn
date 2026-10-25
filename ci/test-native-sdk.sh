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
    flags=(-static "-Wl,--gc-sections")
    test_tags=xconn_static
  fi
  # CI paths contain no spaces; pkg-config emits compiler arguments.
  read -r -a compiler_flags <<< "$(pkg-config --cflags --libs "$package")"
  cc "$temporary/sdk-smoke.c" "${compiler_flags[@]}" \
    "${flags[@]}" -o "$temporary/sdk-smoke"
  "$temporary/sdk-smoke"
  go -C bindings/go test -tags "$test_tags" -count=1 -timeout=90s \
    -run '^TestClientRouterMatrix$' .
done
