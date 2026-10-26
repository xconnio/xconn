#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
archive=$(realpath "${1:?static SDK archive required}")
name=$(basename "$archive" .tar.gz)
version=${name#xconn-native-}
version=${version%%-*}
temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' EXIT
# Redirect only the system prefix for an isolated install; serve the CI archive
# through a curl fixture so the SDK need not be published before testing it.
mkdir -p "$temporary/downloads" "$temporary/tools"
cp "$archive" "$archive.sha256" "$temporary/downloads/"
cat > "$temporary/tools/curl" <<'CURL'
#!/bin/sh
set -eu
output=
file=
while [ "$#" -gt 0 ]; do
  case "$1" in
    -o) shift; output=$1 ;;
    https://*) file=${1##*/} ;;
  esac
  shift
done
cp "$XCONN_TEST_DOWNLOADS/$file" "$output"
CURL
chmod +x "$temporary/tools/curl"
sed "s|prefix=/usr/local|prefix=$temporary/sdk|; s|prefix=/opt/homebrew|prefix=$temporary/sdk|" \
  scripts/install-xconn-sdk.sh > "$temporary/install.sh"
XCONN_TEST_DOWNLOADS="$temporary/downloads" PATH="$temporary/tools:$PATH" \
  sh "$temporary/install.sh" --version "$version"
# Reinstallation refreshes files; failed verification must not switch pkg-config.
installed_target=${name#xconn-native-"$version"-}
printf corrupt > "$temporary/sdk/lib/xconn/$version/$installed_target/lib/libxconn_ffi.a"
XCONN_TEST_DOWNLOADS="$temporary/downloads" PATH="$temporary/tools:$PATH" \
  sh "$temporary/install.sh" --version "$version"
cp "$temporary/sdk/lib/pkgconfig/xconn.pc" "$temporary/original.pc"
printf '%064d\n' 0 > "$temporary/downloads/$name.tar.gz.sha256"
if XCONN_TEST_DOWNLOADS="$temporary/downloads" PATH="$temporary/tools:$PATH" \
  sh "$temporary/install.sh" --version "$version" > "$temporary/rejected.log" 2>&1; then
  echo 'Installer accepted an invalid checksum' >&2; exit 1
fi
grep -q 'SHA-256 mismatch' "$temporary/rejected.log"
cmp "$temporary/original.pc" "$temporary/sdk/lib/pkgconfig/xconn.pc"
mkdir -p "$temporary/unsafe/$name"
ln -s /tmp "$temporary/unsafe/$name/link"
tar -czf "$temporary/downloads/$name.tar.gz" -C "$temporary/unsafe" "$name"
if command -v sha256sum >/dev/null; then
  sha256sum "$temporary/downloads/$name.tar.gz" > "$temporary/downloads/$name.tar.gz.sha256"
else
  shasum -a 256 "$temporary/downloads/$name.tar.gz" > "$temporary/downloads/$name.tar.gz.sha256"
fi
if XCONN_TEST_DOWNLOADS="$temporary/downloads" PATH="$temporary/tools:$PATH" \
  sh "$temporary/install.sh" --version "$version" > "$temporary/rejected.log" 2>&1; then
  echo 'Installer accepted a symlink archive' >&2; exit 1
fi
grep -q 'must not contain links' "$temporary/rejected.log"
cmp "$temporary/original.pc" "$temporary/sdk/lib/pkgconfig/xconn.pc"
export PKG_CONFIG_PATH="$temporary/sdk/lib/pkgconfig"
# Copy only the Go module: consumer builds must not need a Rust workspace.
cp -R bindings/go "$temporary/go"
mkdir "$temporary/consumer"
cat > "$temporary/consumer/go.mod" <<'MOD'
module sdk-consumer

go 1.26.0

require github.com/xconnio/xconn-core/bindings/go v0.0.0
replace github.com/xconnio/xconn-core/bindings/go => ../go
MOD
cat > "$temporary/consumer/main.go" <<'GO'
package main

import (
    "context"
    "fmt"
    xconn "github.com/xconnio/xconn-core/bindings/go"
)

func main() {
    router, err := xconn.NewRouter(nil)
    if err != nil {
        panic(err)
    }
    defer router.Close()

    if err := router.AddRealm("realm", nil); err != nil {
        panic(err)
    }
    session, err := xconn.ConnectInMemory(router, "realm")
    if err != nil {
        panic(err)
    }
    defer session.Close()

    registration := session.Register("echo", func(_ context.Context, inv *xconn.Invocation) *xconn.Result {
        return xconn.NewResult(inv.Args()...)
    }).Execute()
    if registration.Err != nil {
        panic(registration.Err)
    }
    result := session.Call("echo").Arg("installed SDK works").Execute()
    if result.Err != nil {
        panic(result.Err)
    }
    if result.ArgStringOr(0, "") != "installed SDK works" {
        panic("unexpected RPC result")
    }
    fmt.Println(result.ArgStringOr(0, ""))
}
GO
gofmt -w "$temporary/consumer/main.go"
go -C "$temporary/consumer" build -mod=mod -o "$temporary/consumer/app" .
"$temporary/consumer/app"
if [[ "$(uname -s)" == Darwin ]]; then
  dependencies=$(otool -L "$temporary/consumer/app")
else
  dependencies=$(ldd "$temporary/consumer/app")
fi
if [[ "$dependencies" == *libxconn* ]]; then
  echo 'Installed SDK must not introduce a runtime XConn shared library' >&2
  exit 1
fi
