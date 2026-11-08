"""Reject a release tag unless every distributed binding uses that version."""

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def release_version(tag):
    if not re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+", tag):
        raise ValueError("Expected a release tag such as v0.1.5")
    return tag[1:]


def check_versions(tag, root=ROOT):
    version = release_version(tag)
    crates = ["wampproto", "xconn-framing", "xconn-crypto", "xconn-core"]
    manifests = [
        (f"crates/{crate}/Cargo.toml", r'^version = "([^"]+)"$') for crate in crates
    ]
    for filename, pattern in manifests + [
        ("bindings/ffi/Cargo.toml", r'^version = "([^"]+)"$'),
        ("bindings/python/pyproject.toml", r'^version = "([^"]+)"$'),
        ("bindings/dart/pubspec.yaml", r"^version: (\S+)$"),
        ("scripts/install-xconn-sdk.sh", r"^version=(\S+)$"),
    ]:
        found = re.findall(
            pattern, (root / filename).read_text(encoding="utf-8"), re.MULTILINE
        )
        if found != [version]:
            raise ValueError(f"{filename} must use release version {version}")
    for filename in ["bindings/js/package.json", "bindings/js/package-lock.json"]:
        package = json.loads((root / filename).read_text(encoding="utf-8"))
        if package["version"] != version or (
            "packages" in package and package["packages"][""]["version"] != version
        ):
            raise ValueError(f"{filename} must use release version {version}")
    return version


if __name__ == "__main__":
    try:
        print(check_versions(sys.argv[1]))
    except (IndexError, ValueError) as error:
        sys.exit(str(error))
