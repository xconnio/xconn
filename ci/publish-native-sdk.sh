#!/usr/bin/env bash
set -euo pipefail
tag=${1:?release tag required}
directory=${2:?SDK archive directory required}
cd "$directory"

sha256sum --check ./*.sha256
cat ./*.sha256 > SHA256SUMS
gh release create "$tag" ./* --verify-tag \
  --title "$tag" --generate-notes --latest=false
