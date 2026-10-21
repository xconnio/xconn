#!/usr/bin/env python3
"""Stage or archive the host's native SDK. Does not install or publish it."""

import argparse
import hashlib
import json
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


def stage(destination, static=False, profile="release"):
    metadata = json.loads(output("cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"))
    package = next(package for package in metadata["packages"] if package["name"] == "xconn-ffi")
    target = output("rustc", "-vV").split("host: ")[1].splitlines()[0]
    system = platform.system()
    if system not in ("Linux", "Darwin"):
        raise SystemExit("native SDK packaging supports Linux and macOS")
    if static and system != "Linux":
        raise SystemExit("static SDK packaging currently supports Linux only")
    filename = "libxconn_ffi.a" if static else ("libxconn_ffi.dylib" if system == "Darwin" else "libxconn_ffi.so")
    library = Path(metadata["target_directory"]) / profile / filename
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
    else:
        libraries = '-L"${libdir}" -lxconn_ffi -Wl,-rpath,"${libdir}"'

    (destination / f"lib/pkgconfig/{name}.pc").write_text(
        "prefix=${pcfiledir}/../..\nlibdir=${prefix}/lib\nincludedir=${prefix}/include\n\n"
        f"Name: xconn\nDescription: XConn native runtime (C ABI {abi})\nVersion: {version}\n"
        f'Libs: {libraries}\nCflags: -I"${{includedir}}"\n'
    )
    dependencies = ""
    if not static:
        dependencies = output("otool", "-L", str(installed)) if system == "Darwin" else output("ldd", str(installed))
    manifest = {
        "version": version,
        "abi_version": abi,
        "target": target,
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
        "files": {str(path.relative_to(destination)): digest(path) for path in (installed, header)},
    }
    (destination / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    shutil.copy2(ROOT / "docs/native-install.md", destination / "README.md")
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
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    destination = args.destination.resolve()
    if args.stage_only:
        stage(destination, static=args.static, profile=args.profile)
        print(destination)
        return
    subprocess.run(
        ["cargo", "build", "--locked", "--profile", args.profile, "-p", "xconn-ffi"],
        cwd=ROOT,
        check=True,
    )
    destination.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="xconn-native-") as temporary:
        staging = Path(temporary)
        name = stage(staging, static=args.static, profile=args.profile)
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
