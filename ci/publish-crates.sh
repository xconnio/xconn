#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
version=$(python3 ci/release_version.py "${RELEASE_TAG:?release tag required}")
for crate in wampproto xconn-framing xconn-crypto xconn; do
  status=$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' \
    --user-agent 'XConn release (https://github.com/xconnio/xconn)' \
    "https://crates.io/api/v1/crates/$crate/$version")
  case "$status" in
    200) echo "$crate $version is already published" ;;
    404) cargo publish --locked -p "$crate" ;;
    *) echo "Cannot check $crate $version: HTTP $status" >&2; exit 1 ;;
  esac
done
