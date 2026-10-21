# Native SDK installation

Extract the versioned `xconn-native-VERSION-TARGET.tar.gz` archive to a permanent
location. Verify its companion SHA-256 file before extracting. The package has
`lib/`, `include/`, a relocatable `lib/pkgconfig/xconn.pc`, and `manifest.json`.
The manifest records the target, source revision, ABI version and dependencies.

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

Native SDKs are host builds, not manylinux packages. The default shared SDK
requires the system libraries recorded in `manifest.json`; use a compatible
distribution. TLS uses rustls with native certificate roots. Static Linux linking
is available separately below. The archive target describes architecture/OS;
the manifest describes the build host.
Windows, Android and iOS SDK packaging are not included in this first release path.

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
or change system library paths. CI produces workflow artifacts; public releases
and Go module tags remain separate actions. The Go module's eventual release tag
must use its subdirectory prefix, for example `bindings/go/v0.1.0`.

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
changes to external native archives in its build cache (use `go build -a`). Static packaging currently supports
Linux on the build host architecture. Certificates and any glibc runtime modules
remain system resources; this is not a hermetic cross-distribution build.
