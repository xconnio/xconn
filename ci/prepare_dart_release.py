"""Pin the Dart package to the SDK archives produced for its release tag."""

import re
import sys
from pathlib import Path

from release_version import ROOT, check_versions, release_version

TARGETS = (
    "aarch64-apple-darwin",
    "aarch64-linux-android",
    "aarch64-unknown-linux-gnu",
    "x86_64-unknown-linux-gnu",
)


def sdk_metadata(tag, checksums, source):
    version = release_version(tag)
    expected = {f"xconn-native-{version}-{target}.tar.gz": target for target in TARGETS}
    hashes = {}
    for line in checksums.splitlines():
        if not line.strip():
            continue
        match = re.fullmatch(r"([a-f0-9]{64})\s+\*?(\S+)", line)
        if not match:
            raise ValueError("Invalid SDK checksum line")
        digest, filename = match.groups()
        if filename in expected:
            target = expected[filename]
            if target in hashes:
                raise ValueError(f"Duplicate SDK checksum for {target}")
            hashes[target] = digest
    if set(hashes) != set(TARGETS):
        raise ValueError("SDK checksums do not cover every supported Dart target")
    source, count = re.subn(
        r'const nativeSdkVersion = "[^"]+";',
        f'const nativeSdkVersion = "{version}";',
        source,
    )
    if count != 1:
        raise ValueError("Dart SDK version declaration not found")
    mapping = (
        "const nativeSdkChecksums = {\n"
        + "".join(f'  "{target}": "{hashes[target]}",\n' for target in TARGETS)
        + "};"
    )
    source, count = re.subn(
        r"const nativeSdkChecksums = \{.*?\n\};", mapping, source, flags=re.DOTALL
    )
    if count != 1:
        raise ValueError("Dart SDK checksum declaration not found")
    return source


if __name__ == "__main__":
    check_versions(sys.argv[1])
    path = ROOT / "bindings/dart/lib/src/native_sdk.dart"
    path.write_text(
        sdk_metadata(
            sys.argv[1],
            Path(sys.argv[2]).read_text(encoding="utf-8"),
            path.read_text(encoding="utf-8"),
        )
    )
