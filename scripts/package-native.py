#!/usr/bin/env python3
"""Stage or archive the host's native SDK. Does not install or publish it."""

import argparse
import hashlib
import json
import os
import platform
import re
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def output(*command):
    return subprocess.check_output(command, cwd=ROOT, text=True).strip()


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def install_library(source, destination, system):
    # A loaded library must retain its inode and bytes until its process exits.
    # Prepare and sign a new file, then atomically replace the directory entry.
    with tempfile.TemporaryDirectory(prefix=".xconn-stage-", dir=destination.parent) as temporary:
        staged = Path(temporary) / destination.name
        shutil.copy2(source, staged)
        if system == "Darwin":
            subprocess.run(
                ["install_name_tool", "-id", f"@rpath/{destination.name}", str(staged)],
                check=True,
            )
            subprocess.run(["codesign", "--force", "--sign", "-", str(staged)], check=True)
        staged.replace(destination)


def stage(destination, static=False, profile="release", target=None, android_api_level=24):
    metadata = json.loads(output("cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"))
    package = next(package for package in metadata["packages"] if package["name"] == "xconn-ffi")
    host = output("rustc", "-vV").split("host: ")[1].splitlines()[0]
    build_target = target
    target = target or host
    android = target == "aarch64-linux-android"
    system = platform.system()
    if system not in ("Linux", "Darwin"):
        raise SystemExit("native SDK packaging supports Linux and macOS")
    if android:
        if host != "x86_64-unknown-linux-gnu":
            raise SystemExit("Android SDK packaging currently requires a Linux x86-64 host")
        if android_api_level < 24:
            raise SystemExit("Android SDKs require API level 24 or newer")
        ndk = Path(os.environ.get("ANDROID_NDK_HOME", ""))
        readelf = ndk / "toolchains/llvm/prebuilt/linux-x86_64/bin/llvm-readelf"
        if not readelf.is_file():
            raise SystemExit("Set ANDROID_NDK_HOME to the Android NDK directory")
    if static and (system != "Linux" or android):
        raise SystemExit("static SDK packaging currently supports native Linux targets only")
    filename = "libxconn_ffi.a" if static else ("libxconn_ffi.dylib" if system == "Darwin" else "libxconn_ffi.so")
    build_directory = Path(metadata["target_directory"])
    if build_target:
        build_directory /= build_target
    library = build_directory / profile / filename
    if not library.is_file():
        raise SystemExit(f"build first: cargo build --locked --profile {profile} -p xconn-ffi")
    destination.mkdir(parents=True, exist_ok=True)
    for directory in ("lib/pkgconfig", "include"):
        (destination / directory).mkdir(parents=True, exist_ok=True)
    installed = destination / "lib" / filename
    install_library(library, installed, system)
    header = destination / "include/xconn.h"
    shutil.copy2(ROOT / "bindings/ffi/include/xconn.h", header)
    abi = int(re.search(r"#define XC_ABI_VERSION (\d+)", header.read_text())[1])
    version = package["version"]
    name = "xconn-static" if static else "xconn"
    if static:
        libraries = '"${libdir}/libxconn_ffi.a" -lgcc_eh -lutil -lrt -lpthread -lm -ldl -lc'
    elif android:
        libraries = '-L"${libdir}" -lxconn_ffi'
    else:
        libraries = '-L"${libdir}" -lxconn_ffi -Wl,-rpath,"${libdir}"'

    pkg_config = destination / f"lib/pkgconfig/{name}.pc"
    pkg_config.write_text(
        "prefix=${pcfiledir}/../..\nlibdir=${prefix}/lib\nincludedir=${prefix}/include\n\n"
        f"Name: xconn\nDescription: XConn native runtime (C ABI {abi})\nVersion: {version}\n"
        f'Libs: {libraries}\nCflags: -I"${{includedir}}"\n'
    )
    readme = destination / "README.md"
    license_file = destination / "LICENSE"
    shutil.copy2(ROOT / "docs/native-install.md", readme)
    shutil.copy2(ROOT / "LICENSE", license_file)
    dependencies = ""
    if android:
        dependencies = output(str(readelf), "--dynamic", str(installed))
    elif not static:
        dependencies = output("otool", "-L", str(installed)) if system == "Darwin" else output("ldd", str(installed))
    manifest = {
        "version": version,
        "abi_version": abi,
        "target": target,
        "build_host": host,
        "platform": platform.platform(),
        "rustc": output("rustc", "--version"),
        "revision": output("git", "rev-parse", "HEAD"),
        "working_tree_dirty": bool(output("git", "status", "--porcelain")),
        "cargo_lock_sha256": digest(ROOT / "Cargo.lock"),
        "features": package["features"]["default"],
        "profile": profile,
        "linkage": "static" if static else "dynamic",
        "link_libraries": libraries if static else "",
        "runtime_dependencies": dependencies,
        "files": {
            str(path.relative_to(destination)): digest(path)
            for path in (installed, header, pkg_config, readme, license_file)
        },
    }
    if android:
        revision = re.search(r"^Pkg.Revision\s*=\s*(.+)$", (ndk / "source.properties").read_text(), re.MULTILINE)
        if revision is None:
            raise SystemExit("Android NDK source.properties is missing Pkg.Revision")
        manifest.update(android_api_level=android_api_level, android_ndk_version=revision.group(1))
    (destination / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return f"xconn-native-{version}-{target}" + ("-static" if static else "")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stage-only",
        action="store_true",
        help="create a relocatable SDK directory instead of an archive",
    )
    parser.add_argument("--static", action="store_true", help="stage the Linux static archive and linker dependencies")
    parser.add_argument("--profile", choices=("release", "native"), default="release", help="Cargo build profile")
    parser.add_argument("--target", choices=("aarch64-linux-android",), help="cross-compile the Android ARM64 SDK")
    parser.add_argument("--android-api-level", type=int, default=24, help="minimum Android API level (default: 24)")
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    if args.target and args.static:
        parser.error("Android SDKs are shared libraries; --static is only supported for native Linux")
    destination = args.destination.resolve()
    if args.stage_only:
        stage(
            destination,
            static=args.static,
            profile=args.profile,
            target=args.target,
            android_api_level=args.android_api_level,
        )
        print(destination)
        return
    subprocess.run(
        [
            "cargo",
            "build",
            "--locked",
            "--profile",
            args.profile,
            "-p",
            "xconn-ffi",
            *(["--target", args.target] if args.target else []),
        ],
        cwd=ROOT,
        check=True,
    )
    destination.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="xconn-native-") as temporary:
        staging = Path(temporary)
        name = stage(
            staging,
            static=args.static,
            profile=args.profile,
            target=args.target,
            android_api_level=args.android_api_level,
        )
        archive = destination / f"{name}.tar.gz"
        # Exclusive creation prevents accidentally replacing an existing release artifact.
        with (
            archive.open("xb") as handle,
            tarfile.open(fileobj=handle, mode="w:gz") as bundle,
        ):
            for path in sorted(staging.iterdir()):
                bundle.add(path, arcname=f"{name}/{path.name}")
    archive.with_suffix(archive.suffix + ".sha256").write_text(f"{digest(archive)}  {archive.name}\n")
    print(archive)


if __name__ == "__main__":
    main()
