"""Prepare a shared release version without changing dependencies or SDK hashes."""

import json
import re
import sys

from release_version import ROOT, check_versions, release_version

CRATES = ("wampproto", "xconn-framing", "xconn-crypto", "xconn-core")


def prepare_release(version, root=ROOT):
    release_version(f"v{version}")
    package = json.loads((root / "bindings/js/package.json").read_text(encoding="utf-8"))
    previous = package["version"]
    check_versions(f"v{previous}", root)
    if tuple(map(int, version.split("."))) <= tuple(map(int, previous.split("."))):
        raise ValueError(f"Release version must be newer than {previous}")

    # Compute all edits first, so a malformed manifest leaves the tree unchanged.
    edits = {}
    own_manifests = [root / f"crates/{crate}/Cargo.toml" for crate in CRATES]
    own_manifests.append(root / "bindings/ffi/Cargo.toml")
    manifests = [
        root / "Cargo.toml",
        *root.glob("crates/*/Cargo.toml"),
        *root.glob("bindings/*/Cargo.toml"),
        root / "bindings/js/wasm/Cargo.toml",
    ]
    for path in manifests:
        source = path.read_text(encoding="utf-8")
        updated = source
        if path in own_manifests:
            updated, count = re.subn(r'^version = "[^"]+"$', f'version = "{version}"', updated, flags=re.MULTILINE)
            if count != 1:
                raise ValueError(f"Expected one package version in {path}")
        for crate in CRATES:
            updated = re.sub(
                rf'(^\s*{re.escape(crate)}\s*=\s*\{{[^\n]*\bversion\s*=\s*")[^"]+("[^\n]*\bpath\s*=)',
                rf"\g<1>{version}\2",
                updated,
                flags=re.MULTILINE,
            )
        if updated != source:
            edits[path] = updated

    path = root / "Cargo.lock"
    source = path.read_text(encoding="utf-8")
    for crate in (*CRATES, "xconn-ffi"):
        source, count = re.subn(
            rf'(\[\[package\]\]\nname = "{crate}"\nversion = ")[^"]+("\n)',
            rf"\g<1>{version}\2",
            source,
        )
        if count != 1:
            raise ValueError(f"Expected one lockfile entry for {crate}")
    edits[path] = source

    replacements = {
        "bindings/python/pyproject.toml": (r'^version = "[^"]+"$', f'version = "{version}"'),
        "bindings/dart/pubspec.yaml": (r"^version: \S+$", f"version: {version}"),
        "scripts/install-xconn-sdk.sh": (r"^version=\S+$", f"version={version}"),
    }
    for filename, (pattern, replacement) in replacements.items():
        path = root / filename
        edits[path], count = re.subn(pattern, replacement, path.read_text(encoding="utf-8"), flags=re.MULTILINE)
        if count != 1:
            raise ValueError(f"Expected one version in {filename}")
    for filename in ("bindings/js/package.json", "bindings/js/package-lock.json"):
        path = root / filename
        # Preserve npm's formatting and only change this package's version fields.
        source = path.read_text(encoding="utf-8")
        count = 2 if filename.endswith("package-lock.json") else 1
        edits[path], found = re.subn(
            r'("version": ")' + re.escape(previous) + '"', rf'\g<1>{version}"', source, count=count
        )
        if found != count:
            raise ValueError(f"Expected package version fields in {filename}")
    for language in ("js", "python", "dart"):
        path = root / f"bindings/{language}/CHANGELOG.md"
        source = path.read_text(encoding="utf-8")
        heading = f"## {version}\n\n"
        if f"## {version}\n" in source:
            raise ValueError(f"Release heading already exists in {path}")
        offset = source.index("## ")
        edits[path] = source[:offset] + heading + source[offset:]
    for path, source in edits.items():
        path.write_text(source, encoding="utf-8")
    check_versions(f"v{version}", root)


if __name__ == "__main__":
    try:
        prepare_release(sys.argv[1])
    except (IndexError, ValueError) as error:
        sys.exit(str(error))
