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
            self.assertEqual(
                set(manifest["files"]),
                {"lib/libxconn_ffi.a", "include/xconn.h", "lib/pkgconfig/xconn-static.pc", "README.md", "LICENSE"},
            )
            for relative, checksum in manifest["files"].items():
                self.assertEqual(PACKAGE.digest(destination / relative), checksum)
            self.assertEqual((destination / "LICENSE").read_bytes(), (PACKAGE.ROOT / "LICENSE").read_bytes())

    def test_android_sdk_uses_cross_target_library_and_ndk_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            host_library = root / "target/native/libxconn_ffi.so"
            android_library = root / "target/aarch64-linux-android/native/libxconn_ffi.so"
            for library, content in [(host_library, b"host library"), (android_library, b"Android library")]:
                library.parent.mkdir(parents=True)
                library.write_bytes(content)
            ndk = root / "ndk"
            readelf = ndk / "toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-readelf"
            readelf.parent.mkdir(parents=True)
            readelf.touch()
            (ndk / "source.properties").write_text("Pkg.Revision = 28.2.13676358\n")
            metadata = {
                "target_directory": str(root / "target"),
                "packages": [{"name": "xconn-ffi", "version": "0.1.1", "features": {"default": []}}],
            }

            def output(*command):
                if command[0] == "cargo":
                    return json.dumps(metadata)
                if command == ("rustc", "-vV"):
                    return "host: x86_64-unknown-linux-gnu"
                if command[0] in ("ldd", "otool"):
                    self.fail("Foreign Android libraries must not be inspected with host loader tools")
                if command[0] == str(readelf):
                    return "NEEDED libc.so"
                if command[:2] == ("git", "status"):
                    return ""
                return "fixture"

            destination = root / "sdk"
            with (
                patch.object(PACKAGE, "output", side_effect=output),
                patch.object(PACKAGE.platform, "system", return_value="Linux"),
                patch.dict(PACKAGE.os.environ, {"ANDROID_NDK_HOME": str(ndk)}),
            ):
                name = PACKAGE.stage(destination, profile="native", target="aarch64-linux-android")
                with self.assertRaisesRegex(SystemExit, "native Linux"):
                    PACKAGE.stage(destination, static=True, target="aarch64-linux-android")
                with self.assertRaisesRegex(SystemExit, "API level 24"):
                    PACKAGE.stage(destination, target="aarch64-linux-android", android_api_level=23)

            self.assertEqual(name, "xconn-native-0.1.1-aarch64-linux-android")
            self.assertEqual((destination / "lib/libxconn_ffi.so").read_bytes(), b"Android library")
            self.assertNotIn("-rpath", (destination / "lib/pkgconfig/xconn.pc").read_text())
            manifest = json.loads((destination / "manifest.json").read_text())
            self.assertEqual(manifest["target"], "aarch64-linux-android")
            self.assertEqual(manifest["build_host"], "x86_64-unknown-linux-gnu")
            self.assertEqual(manifest["android_api_level"], 24)
            self.assertEqual(manifest["android_ndk_version"], "28.2.13676358")
            self.assertEqual(manifest["runtime_dependencies"], "NEEDED libc.so")

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
