#!/usr/bin/env bash
set -euo pipefail
release=${1:?release tag required}
[[ "$release" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo 'Invalid release tag' >&2; exit 1; }
tag="bindings/go/$release"
commit=$(git rev-parse "refs/tags/$release^{commit}")
[[ $(git rev-parse HEAD) == "$commit" ]] || { echo 'Checkout must match the release tag' >&2; exit 1; }

if git ls-remote --exit-code --tags origin "refs/tags/$tag" >/dev/null; then
  git fetch --no-tags origin "refs/tags/$tag"
  [[ $(git rev-parse 'FETCH_HEAD^{commit}') == "$commit" ]] || {
    echo 'Existing Go tag points at another commit' >&2; exit 1;
  }
  echo "$tag already points at $commit"
  exit 0
else
  status=$?
  [[ "$status" == 2 ]] || exit "$status"
fi

if git show-ref --verify --quiet "refs/tags/$tag"; then
  [[ $(git rev-parse "refs/tags/$tag^{commit}") == "$commit" ]] || {
    echo 'Existing local Go tag points at another commit' >&2; exit 1;
  }
else
  git tag "$tag" "$commit"
fi
git push origin "refs/tags/$tag"
