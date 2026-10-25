# Browser client package size

This benchmark builds and measures the actual [browser client](../../bindings/js/README.md).
The WASM adapter lives in `bindings/js/wasm`, and the public TypeScript API lives in
`bindings/js/src`. There is no separate protocol-only benchmark implementation.

Rust owns joining/authentication, message layouts and validation, request IDs and
codecs. JavaScript owns browser WebSocket IO, Promise correlation, callbacks,
cancellation and session lifecycle. The native Tokio runtime, C FFI, router and
native WebRTC stack are excluded. Browser WebRTC uses browser networking and
the shared Rust chunk assembler; WebTransport uses the browser HTTP/3 API.

## Measured sizes

Sizes are KiB (1,024 bytes). Totals include the WASM binary, generated JavaScript
glue, and **all compiled high-level client modules**. Each runtime file is
compressed independently with gzip level 9 or Brotli quality 11. JavaScript is
unbundled and unminified. Declarations, docs and build metadata are excluded.

| Variant | WASM raw | JS raw | Total raw | Total gzip | Total Brotli |
| --- | ---: | ---: | ---: | ---: | ---: |
| JSON; anonymous + ticket | 189.2 | 140.9 | 330.0 | 106.6 | 91.1 |
| JSON + CBOR + MessagePack; anonymous + ticket | 254.8 | 140.9 | 395.7 | 126.0 | 106.5 |
| All codecs and authentication; packet encryption | 340.4 | 150.3 | 490.7 | 179.9 | 154.3 |

Exact bytes, per-file SHA-256 hashes and tool versions are recorded in
[measurements.json](measurements.json). Compression assumes the web server serves
the corresponding gzip/Brotli content; these are transfer sizes, not memory usage.

## Reproduce

Install the JS package's dev dependencies and its documented Rust/WASM tools:

```sh
npm --prefix bindings/js ci
benchmarks/browser-wasm/build.sh
```

Builds use the workspace lockfile and `browser` profile: size optimization, LTO,
one codegen unit, stripped symbols, aborting panics, then `wasm-opt -Oz`.
`WASM_BINDGEN_BIN` and `WASM_OPT_BIN` override tool discovery.

Artifacts are under `target/browser-wasm/pkg/{json,codecs,full}/`, each containing
the actual compiled package, a `wasm/` directory, and `.gz`/`.br` runtime copies.
The report is `target/browser-wasm/pkg/sizes.json`. Generated artifacts are ignored.

## Validate

```sh
XCONN_REFERENCE_GO_BIN=/path/to/original-go-reference \
CHROMIUM_BIN=/path/to/chromium \
node bindings/js/tests/run.mjs target/browser-wasm/pkg/full full
```

Use `json` or `codecs` for the corresponding directory and final argument. Node 24
and real Chromium run the same public API tests against the pinned original Go
router. Tests cover authentication across codecs, concurrent/reentrant RPC,
PubSub, progressive results, cancellation, malformed frames, custom adapters,
maximum IDs, lossless binary/64-bit payloads and disconnect cleanup.

`ci/test-browser.sh` builds, checks and tests all variants and packs the full npm
artifact. It requires the reference router and Chromium. The full packed artifact
has also been installed outside the checkout and exercised in Node and Chromium,
including compilation of the exported TypeScript declarations.
