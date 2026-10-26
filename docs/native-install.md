# Native SDK installation

## Go quick start

Prerequisites are Go, a C compiler, `pkg-config`, `curl`, `tar`, and standard
Unix utilities. The installer uses `sha256sum` on Linux or `shasum` on macOS.
It installs the SDK only; it does not install prerequisite packages.

```sh
curl -fL https://raw.githubusercontent.com/xconnio/xconn-core/main/scripts/install-xconn-sdk.sh -o install-xconn-sdk.sh
sh install-xconn-sdk.sh
go get github.com/xconnio/xconn-core/bindings/go
go build ./...
```

Supported hosts are Linux amd64/arm64 with a compatible glibc (SDKs are built
on Ubuntu 22.04), and macOS arm64. No Windows, Intel macOS, musl or
cross-installation is provided.

The script downloads the archive and SHA-256 sidecar from the same versioned
GitHub Release over HTTPS. It checks the archive hash before extracting, rejects
paths outside the SDK root and links/special files, and checks the ABI header and
pkg-config version. Reinstalling copies the verified SDK files again.

Files live under `PREFIX/lib/xconn/VERSION/TARGET-static`. Both `xconn.pc` and
`xconn-static.pc` in `PREFIX/lib/pkgconfig` select the archive. The prefix is
`/usr/local`, or `/opt/homebrew` on Apple Silicon when Homebrew is installed.
The script requests sudo when needed, without changing shell profiles or global
Go settings. These standard locations require no environment variables when in
pkg-config's search path. Custom prefixes and offline installs use the manual
instructions below.

`pkg-config --modversion xconn` reports the selected SDK. Rerunning the installer
with another version switches the pkg-config entries while retaining old SDKs.
Relink existing applications with `go build -a` after changing SDK versions.
Static XConn linking does not imply a fully static executable: system libraries,
libc compatibility and certificate roots remain requirements. Cross-compiling
requires a matching SDK and C cross-compiler. The explicit `xconn_static` tag and
static-libc linker flags below are only needed for fully static Linux executables.

## Manual SDK installation

To select a specific SDK release, run `sh install-xconn-sdk.sh --version VERSION`.

Extract the versioned `xconn-native-VERSION-TARGET.tar.gz` archive to a permanent
location. Verify its companion SHA-256 file before extracting. The package has
`lib/`, `include/`, a relocatable `lib/pkgconfig/xconn.pc`, and `manifest.json`.
The manifest records the target, source revision, ABI version and dependencies.
It also hashes the library, header, pkg-config metadata, README, and MIT license.

Go requires Go, a C compiler and pkg-config; Rust is needed only to build the SDK,
not to consume it. Point pkg-config to the extracted SDK:

```sh
export PKG_CONFIG_PATH="/absolute/path/to/sdk/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
pkg-config --modversion xconn
go build ./...
```

The executable embeds the SDK's library directory as a runtime search path. Keep
that directory installed on the deployment machine, or install the library into
a standard dynamic-loader location. To move the library independently, configure
`LD_LIBRARY_PATH` on Linux or the appropriate macOS loader search path. The Go
module contains no workspace-relative include or library paths. It does not
automatically download or install native code during a build.

## GitHub Releases

Download SDK archives from the [Core releases](https://github.com/xconnio/xconn-core/releases).
Assets inherit repository visibility: public downloads require a public
repository; private releases require authenticated repository access.
The release workflow builds these targets:

| Target | Build host | Archives |
| --- | --- | --- |
| `x86_64-unknown-linux-gnu` | Ubuntu 22.04 | Shared and static |
| `aarch64-unknown-linux-gnu` | Ubuntu 22.04 ARM64 | Shared and static |
| `aarch64-apple-darwin` | macOS 15 ARM64 | Shared; static from 0.1.2 |
| `aarch64-linux-android` | Ubuntu 22.04 + NDK r28c | Shared, Android API 24+ |

Each archive has its own `.sha256` file; `SHA256SUMS` covers all archives.
For example, verify a Linux SDK before extracting it:

```sh
sha256sum --check xconn-native-0.1.0-x86_64-unknown-linux-gnu.tar.gz.sha256
tar -xzf xconn-native-0.1.0-x86_64-unknown-linux-gnu.tar.gz
```

Linux/macOS SDKs are host builds, not manylinux packages. The default shared SDK
requires the system libraries recorded in `manifest.json`; use a compatible
distribution. TLS uses rustls with native certificate roots. Static Linux linking
is available separately below. The archive target describes architecture/OS;
the manifest describes the build host.
Intel macOS, Windows and iOS SDK packaging are not included in this release path.

For source development from the repository root:

```sh
cargo build --locked --release -p xconn-ffi
python3 scripts/package-native.py --stage-only target/native
export PKG_CONFIG_PATH="$PWD/target/native/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
go -C bindings/go test -race ./...
```

Build the default native library and create a versioned archive:

```sh
python3 scripts/package-native.py target/binding-dist/native
```

Existing archives are never overwritten. These commands do not publish artifacts
or change system library paths. See [binding distribution](releases.md) for
maintainer release instructions.

## Static Linux linking

The native crate also produces `libxconn_ffi.a`. To stage a static SDK:

```sh
cargo build --locked --profile native -p xconn-ffi
python3 scripts/package-native.py --stage-only --static --profile native target/native
export PKG_CONFIG_PATH="$PWD/target/native/lib/pkgconfig"
CGO_ENABLED=1 go build -tags xconn_static,netgo,osusergo \
  -ldflags "-linkmode external -extldflags '-static -Wl,--gc-sections'" ./path/to/application
```

Run these commands from the XConn checkout and replace the application path.
The `xconn_static` tag selects `xconn-static.pc`, which explicitly links the Rust
archive instead of a neighboring shared library. The native profile enables thin LTO, one codegen unit, and size optimization
while retaining panic unwinding. The host needs a C toolchain and static libc.
TLS uses rustls/ring; no OpenSSL development libraries are required.
Relink Go applications after rebuilding the Rust archive: Go does not track
changes to external native archives in its build cache (use `go build -a`). Static packaging supports
Linux and macOS on the build host architecture. Only Linux supports the explicit
fully static libc link command above. Certificates and any glibc runtime modules
remain system resources; this is not a hermetic cross-distribution build.

## Android ARM64

The Android archive contains `lib/libxconn_ffi.so` for `arm64-v8a`, targeting
Android API 24 or newer. It uses NDK r28c (`28.2.13676358`) and supports 16 KB
page alignment. The manifest records the Android API level, NDK revision,
build host, and runtime dependencies. The Android library requires Bionic;
it cannot run on a Linux desktop despite both platforms using ELF files.

To build and validate the archive on a Linux x86-64 host with that NDK installed:

```sh
rustup target add aarch64-linux-android
export ANDROID_NDK_HOME="$ANDROID_HOME/ndk/28.2.13676358"
ci/build-android-sdk.sh target/android-native-dist
ci/test-android-sdk.sh target/android-native-dist
```

The check validates ARM64 exports, 16 KB load-segment alignment, and linking an
Android C consumer. It does not execute the library on an Android device.
Android SDK packaging is separate from teaching the Dart build hook to download
and bundle these release assets; the hook currently still builds from source.
