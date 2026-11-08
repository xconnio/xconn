#!/usr/bin/env bash
set -euo pipefail
release=${1:?release tag required}
[[ "$release" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo 'Invalid release tag' >&2; exit 1; }
[[ $(git rev-parse HEAD) == $(git rev-parse origin/main) ]] || {
  echo 'Release tags must point at main' >&2; exit 1;
}
if git ls-remote --exit-code --tags origin "refs/tags/$release" >/dev/null; then
  echo 'Release tag already exists; rerun its failed release jobs instead' >&2
  exit 1
else
  status=$?
  [[ "$status" == 2 ]] || exit "$status"
fi
git tag "$release" HEAD
git push origin "refs/tags/$release"
