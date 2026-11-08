"""Release metadata and tags must not publish inconsistent or replacement builds."""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
# CI helpers are standalone scripts rather than an installed package.
sys.path.insert(0, str(ROOT / "ci"))
from prepare_dart_release import TARGETS, sdk_metadata  # noqa: E402
from release_version import check_versions, release_version  # noqa: E402


class ReleaseVersionTests(unittest.TestCase):
    def test_only_root_release_tags_are_accepted(self):
        self.assertEqual(release_version("v0.1.5"), "0.1.5")
        for tag in ["main", "0.1.5", "bindings/js/v0.1.5", "v0.1.5/extra"]:
            with self.subTest(tag=tag), self.assertRaises(ValueError):
                release_version(tag)

    def test_every_binding_must_match_before_publication(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            files = {
                "bindings/ffi/Cargo.toml": 'version = "0.1.5"\n',
                "bindings/python/pyproject.toml": 'version = "0.1.5"\n',
                "bindings/dart/pubspec.yaml": "version: 0.1.5\n",
                "scripts/install-xconn-sdk.sh": "version=0.1.5\n",
                "bindings/js/package.json": json.dumps({"version": "0.1.5"}),
                "bindings/js/package-lock.json": json.dumps(
                    {"version": "0.1.5", "packages": {"": {"version": "0.1.5"}}}
                ),
            }
            for crate in ["wampproto", "xconn-framing", "xconn-crypto", "xconn-core"]:
                files[f"crates/{crate}/Cargo.toml"] = 'version = "0.1.5"\n'
            for filename, text in files.items():
                path = root / filename
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(text)
            self.assertEqual(check_versions("v0.1.5", root), "0.1.5")
            for filename, text in files.items():
                path = root / filename
                path.write_text(text.replace("0.1.5", "0.1.4"))
                with self.subTest(filename=filename), self.assertRaises(ValueError):
                    check_versions("v0.1.5", root)
                path.write_text(text)


class DartMetadataTests(unittest.TestCase):
    def setUp(self):
        self.source = (ROOT / "bindings/dart/lib/src/native_sdk.dart").read_text()
        self.lines = [f"{index + 1:064x}  xconn-native-0.1.5-{target}.tar.gz" for index, target in enumerate(TARGETS)]

    def test_sdk_hashes_are_taken_from_the_matching_shared_archives(self):
        checksums = "\n".join(self.lines + ["f" * 64 + "  xconn-native-0.1.5-x86_64-unknown-linux-gnu-static.tar.gz"])
        result = sdk_metadata("v0.1.5", checksums, self.source)
        self.assertIn('const nativeSdkVersion = "0.1.5";', result)
        for index, target in enumerate(TARGETS):
            self.assertIn(f'"{target}": "{index + 1:064x}"', result)
        self.assertNotIn('"' + "f" * 64 + '"', result)

    def test_missing_duplicate_and_malformed_hashes_are_rejected(self):
        for lines in [self.lines[:-1], self.lines + self.lines[:1], ["invalid"]]:
            with self.subTest(lines=lines), self.assertRaises(ValueError):
                sdk_metadata("v0.1.5", "\n".join(lines), self.source)


class GoTagTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        root = Path(self.temporary.name)
        self.repo = root / "source"
        self.repo.mkdir()
        self.remote = root / "remote.git"
        subprocess.run(["git", "init", "--bare", str(self.remote)], check=True, capture_output=True)
        self.git("init")
        self.git("config", "user.name", "Release Test")
        self.git("config", "user.email", "release@example.test")
        self.git("commit", "--allow-empty", "-m", "first")
        self.first = self.git("rev-parse", "HEAD")
        self.git("commit", "--allow-empty", "-m", "release")
        self.commit = self.git("rev-parse", "HEAD")
        self.git("tag", "v0.1.5")
        self.git("remote", "add", "origin", str(self.remote))
        self.git("push", "origin", "HEAD:refs/heads/main", "refs/tags/v0.1.5")

    def test_release_tag_is_created_once_without_moving_existing_tags(self):
        script = str(ROOT / "ci/create-release-tag.sh")
        result = subprocess.run(
            ["bash", script, "v0.1.6"],
            cwd=self.repo,
            check=False,
            capture_output=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.git("rev-parse", "refs/tags/v0.1.6"), self.commit)
        result = subprocess.run(
            ["bash", script, "v0.1.6"],
            cwd=self.repo,
            check=False,
            capture_output=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b"already exists", result.stderr)

    def test_release_tag_rejects_invalid_versions_and_commits_off_main(self):
        script = str(ROOT / "ci/create-release-tag.sh")
        for tag in ["main", "0.1.6", "v0.1.6/extra"]:
            result = subprocess.run(
                ["bash", script, tag],
                cwd=self.repo,
                check=False,
                capture_output=True,
            )
            self.assertNotEqual(result.returncode, 0)
        self.git("commit", "--allow-empty", "-m", "not on main")
        result = subprocess.run(
            ["bash", script, "v0.1.6"],
            cwd=self.repo,
            check=False,
            capture_output=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b"must point at main", result.stderr)
        self.assertEqual(self.git("ls-remote", "--tags", "origin", "refs/tags/v0.1.6"), "")

    def git(self, *args):
        return subprocess.check_output(["git", *args], cwd=self.repo, text=True, stderr=subprocess.DEVNULL).strip()

    def tag(self):
        return subprocess.run(
            ["bash", str(ROOT / "ci/tag-go-release.sh"), "v0.1.5"],
            cwd=self.repo,
            check=False,
            text=True,
            capture_output=True,
        )

    def test_go_tag_is_created_once_at_the_sdk_commit(self):
        self.assertEqual(self.tag().returncode, 0)
        self.assertEqual(self.tag().returncode, 0)
        value = self.git("ls-remote", "origin", "refs/tags/bindings/go/v0.1.5").split()[0]
        self.assertEqual(value, self.commit)

    def test_existing_go_tag_is_never_moved(self):
        self.git("push", "origin", f"{self.first}:refs/tags/bindings/go/v0.1.5")
        self.assertNotEqual(self.tag().returncode, 0)
        value = self.git("ls-remote", "origin", "refs/tags/bindings/go/v0.1.5").split()[0]
        self.assertEqual(value, self.first)


class CratePublicationTests(unittest.TestCase):
    def test_published_dependencies_are_skipped_and_errors_stop_publication(self):
        with tempfile.TemporaryDirectory() as directory:
            tools = Path(directory)
            log = tools / "cargo.log"
            cargo = tools / "cargo"
            cargo.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$PUBLISH_TEST_LOG"\n')
            cargo.chmod(0o755)
            curl = tools / "curl"
            curl.write_text('#!/bin/sh\ncase "$*" in *wampproto*) echo 200;; *) echo "$PUBLISH_TEST_STATUS";; esac\n')
            curl.chmod(0o755)
            version = json.loads((ROOT / "bindings/js/package.json").read_text())["version"]
            env = dict(
                os.environ,
                PATH=str(tools) + os.pathsep + os.environ["PATH"],
                RELEASE_TAG=f"v{version}",
                PUBLISH_TEST_LOG=str(log),
                PUBLISH_TEST_STATUS="404",
            )
            script = ["bash", str(ROOT / "ci/publish-crates.sh")]
            result = subprocess.run(script, check=False, env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(
                log.read_text().splitlines(),
                [
                    "publish --locked -p xconn-framing",
                    "publish --locked -p xconn-crypto",
                    "publish --locked -p xconn-core",
                ],
            )
            log.unlink()
            env["PUBLISH_TEST_STATUS"] = "503"
            self.assertNotEqual(
                subprocess.run(script, check=False, env=env, capture_output=True).returncode,
                0,
            )
            self.assertFalse(log.exists())
