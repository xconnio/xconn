#!/usr/bin/env python3
"""Build a pinned, pure-Go reference without editing any source checkout."""

import argparse
import hashlib
import io
import json
import os
import subprocess
import tarfile
from pathlib import Path

SOURCES = {
    "xconn-go": "191a086e99b94bcfb50e2c8ee3667c9cd23015b4",
    "xconn-webrtc-go": "68c1489ee71c",
    "wampproto-go": "ecb54c6c2318d7d3184f77ca95cf5f49c779edae",
}
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("destination", type=Path, help="new directory for immutable reference snapshots and fixture")
parser.add_argument("--sources", type=Path, default=Path(__file__).resolve().parents[3])
args = parser.parse_args()
dest = args.destination.resolve()
dest.mkdir(parents=True, exist_ok=False)
revisions = {}
for name, revision in SOURCES.items():
    source = args.sources.resolve() / name
    resolved = subprocess.check_output(
        ["git", "-C", str(source), "rev-parse", revision + "^{commit}"], text=True
    ).strip()
    archive = subprocess.check_output(["git", "-C", str(source), "archive", resolved])
    target = dest / name
    target.mkdir()
    with tarfile.open(fileobj=io.BytesIO(archive)) as files:
        files.extractall(target, filter="data")
    revisions[name] = resolved
fixture = dest / "fixture"
fixture.mkdir()
for path in Path(__file__).parent.glob("*.go"):
    (fixture / path.name).write_text(path.read_text())
(fixture / "go.mod").write_text("""module xconn-independent-go-reference

go 1.26.0

require (
    github.com/xconnio/xconn-go v0.0.0
    github.com/xconnio/xconn-webrtc-go v0.0.0
    github.com/xconnio/wampproto-go v0.0.0
)
replace github.com/xconnio/xconn-go => ../xconn-go
replace github.com/xconnio/xconn-webrtc-go => ../xconn-webrtc-go
replace github.com/xconnio/wampproto-go => ../wampproto-go
""")
env = dict(os.environ, CGO_ENABLED="0", GOWORK="off")
subprocess.run(["go", "mod", "tidy"], cwd=fixture, env=env, check=True)
dependencies = subprocess.check_output(
    ["go", "list", "-buildvcs=false", "-deps", "."], cwd=fixture, env=env, text=True
).splitlines()
assert any(p == "github.com/xconnio/wampproto-go" for p in dependencies), "reference engine missing"
assert not any(
    p.startswith(
        (
            "github.com/xconnio/wampproto/",
            "github.com/xconnio/xconn-core/bindings/",
            "github.com/xconnio/xconn/bindings/",
        )
    )
    for p in dependencies
), "Rust binding leaked into reference"
manifest = {
    "kind": "independent-pure-go",
    "revisions": revisions,
    "cgo_enabled": False,
    "go_version": subprocess.check_output(["go", "version"], text=True).strip(),
}
# Embed exact source provenance in the fixture; tests can verify the executable itself.
(fixture / "reference_provenance.go").write_text(
    "package main\nfunc init() { fixtureProvenance = `" + json.dumps(manifest, sort_keys=True, indent=2) + "` }\n"
)
subprocess.run(["gofmt", "-w", *map(str, fixture.glob("*.go"))], check=True)
binary = dest / "xconn-go-reference"
subprocess.run(
    ["go", "build", "-buildvcs=false", "-trimpath", "-o", str(binary), "."], cwd=fixture, env=env, check=True
)
manifest["binary_sha256"] = hashlib.sha256(binary.read_bytes()).hexdigest()
manifest["dependencies"] = dependencies
(dest / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
print(json.dumps({"binary": str(binary), "manifest": str(dest / "manifest.json"), "revisions": revisions}, indent=2))
