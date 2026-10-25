# Binding distribution

Source, CI, and local packaging live in this repository. Publishing registry
packages and platform artifacts is a separate release step; no public release
is implied by local tests.

| Consumer | Distribution | Consumer requirement |
| --- | --- | --- |
| Rust | crates.io packages | Rust toolchain |
| Python | `xconn-core` ABI3 wheels on PyPI | Matching supported wheel |
| Go | `github.com/xconnio/xconn-core/bindings/go` plus native SDK | Cgo toolchain and matching SDK |
| Dart / Flutter | `xconn_core` plus platform native library | Native asset integration |
| Browser JS / TS | `@xconnio/core` on npm with WASM | Browser WebSocket and WASM support |

## Native SDK

[`scripts/package-native.py`](../scripts/package-native.py) stages or archives
headers, the shared or static Rust library, pkg-config metadata, checksums, and
build provenance. Archives support the host Linux/macOS platform; Linux static
SDK staging is also available. See [native installation](native-install.md).
Build outputs belong in release artifacts, not Git source history.

The `Native SDK release` workflow builds and tests five archives across Linux and
macOS, then creates the GitHub Release after every matrix job passes. It runs
on tags named `vVERSION`; the tag must match `bindings/ffi/Cargo.toml`. It can
also be dispatched manually with an existing tag. Both paths check out the tag,
so the manifest's source revision identifies the released code.

Merge the release workflow and intended source changes before creating a tag.
For the first release, create `v0.1.0` on that reviewed commit and push the tag.
No extra publishing token is needed: only the final job receives the repository
`GITHUB_TOKEN` with release-write permission. Existing releases/assets are not
overwritten; a failed upload may require removing an incomplete draft before
retrying. The archives include WebRTC with the workspace's pinned SCTP fix.
The workflow calls `ci/check-native-release.sh` to check the tag/host,
`ci/test-native-sdk.sh` to exercise extracted SDKs, and
`ci/publish-native-sdk.sh` to verify checksums and create the release. The smoke
test can also run locally with an archive directory as its argument.
See [native installation](native-install.md) for the platform matrix and limits.

Go modules in this repository use tags such as `bindings/go/v0.1.0`. Installing
the Go module alone does not install the native SDK. Static linking is explicit.

## Python

Maturin builds the PyO3 extension and Python package together. Wheels target the
CPython 3.10+ stable ABI. Platform wheels must be built and validated on the
supported target or repaired using the appropriate platform tooling before
publication. Consumers of a compatible wheel do not need Rust or a separate SDK.
The package imports as `xconn_core` and has no required Python runtime dependency.

## Dart and Flutter

Dart uses the shared C ABI through FFI. Local tests can set `XCONN_LIBRARY` to an
explicit native library. Flutter platform packaging needs to deliver and load
that library for each supported target; this repository does not yet automate
all platform distributions. Python and Dart expose clients only.

## Browser package

The npm package includes compiled JS, TypeScript declarations, and WASM. Its
browser networking uses native WebSocket APIs; it does not load the native C ABI.
Build and test the full package and the reduced WASM variants before release.
