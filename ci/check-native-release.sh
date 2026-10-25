#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
: "${RELEASE_TAG:?release tag required}"
: "${EXPECTED_TARGET:?expected target required}"

if [[ ! -f .github/workflows/native-release.yaml ]]; then
  echo 'Release tag must include the native release workflow' >&2
  exit 1
fi

package=$(cargo pkgid -p xconn-ffi)
version=${package##*@}
if [[ "$RELEASE_TAG" != "v$version" ]]; then
  echo 'Release tag must match the native package version' >&2
  exit 1
fi

host=$(rustc -vV | sed -n 's/^host: //p')
if [[ "$host" != "$EXPECTED_TARGET" ]]; then
  echo 'Runner architecture does not match the SDK target' >&2
  exit 1
fi
