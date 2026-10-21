"""Native SDK updates must preserve libraries already mapped by running processes."""

import importlib.util
import json
import mmap
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location(
    "package_native", Path(__file__).resolve().parents[2] / "scripts/package-native.py"
)
PACKAGE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PACKAGE)


class NativeStagingTests(unittest.TestCase):
    def test_replacement_preserves_existing_mapping(self):
        with tempfile.TemporaryDirectory() as directory:
            source, installed = (
                Path(directory) / "new.so",
                Path(directory) / "installed.so",
            )
            installed.write_bytes(b"original")
            source.write_bytes(b"updated!")
            with (
                installed.open("rb") as old,
                mmap.mmap(old.fileno(), 0, access=mmap.ACCESS_READ) as mapping,
            ):
                PACKAGE.install_library(source, installed, "Linux")
                self.assertEqual(mapping[:], b"original")
                self.assertEqual(installed.read_bytes(), b"updated!")

    def test_static_sdk_selects_archive_without_openssl(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            release = root / "target/native"
            release.mkdir(parents=True)
            (release / "libxconn_ffi.a").write_bytes(b"static archive")
            metadata = {
                "target_directory": str(root / "target"),
                "packages": [{"name": "xconn-ffi", "version": "0.1.0", "features": {"default": []}}],
            }

            def output(*command):
                if command[0] == "cargo":
                    return json.dumps(metadata)
                if command == ("rustc", "-vV"):
                    return "host: x86_64-unknown-linux-gnu"
                if command[:2] == ("git", "status"):
                    return ""
                if command[0] == "ldd":
                    self.fail("Static archives must not be inspected with ldd")
                return "fixture"

            destination = root / "sdk"
            with (
                patch.object(PACKAGE, "output", side_effect=output),
                patch.object(PACKAGE.platform, "system", return_value="Linux"),
            ):
                name = PACKAGE.stage(destination, static=True, profile="native")

            flags = (destination / "lib/pkgconfig/xconn-static.pc").read_text()
            self.assertIn('"${libdir}/libxconn_ffi.a"', flags)
            self.assertNotIn("-lssl", flags)
            self.assertNotIn("-lcrypto", flags)
            self.assertNotIn("-lxconn_ffi", flags)
            self.assertTrue(name.endswith("-static"))
            manifest = json.loads((destination / "manifest.json").read_text())
            self.assertEqual(manifest["profile"], "native")
            self.assertEqual(manifest["linkage"], "static")
            self.assertEqual(manifest["runtime_dependencies"], "")

    def test_signing_failure_preserves_installed_library(self):
        with tempfile.TemporaryDirectory() as directory:
            source, installed = (
                Path(directory) / "new.dylib",
                Path(directory) / "installed.dylib",
            )
            installed.write_bytes(b"original")
            source.write_bytes(b"updated!")
            failure = subprocess.CalledProcessError(1, "codesign")
            with (
                patch.object(PACKAGE.subprocess, "run", side_effect=[None, failure]),
                self.assertRaises(subprocess.CalledProcessError),
            ):
                PACKAGE.install_library(source, installed, "Darwin")
            self.assertEqual(installed.read_bytes(), b"original")
            self.assertFalse(list(Path(directory).glob(".xconn-stage-*")))


if __name__ == "__main__":
    unittest.main()
