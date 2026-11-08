# Releases

Use one version and one root Git tag for the native SDK, public Rust crates and
language bindings. For example, `v0.1.6` releases version `0.1.6` everywhere.
The only additional tag is `bindings/go/v0.1.6`, required by Go for the module in
`bindings/go`; CI creates it automatically after the SDK is published.

| Distribution | Workflow | Starts after |
| --- | --- | --- |
| Native SDK archives on GitHub | `native-release.yaml` | Root tag push |
| `wampproto`, `xconn-framing`, `xconn-crypto`, `xconn-core` on crates.io | `crates-release.yaml` | Root tag push |
| `@xconnio/core` on npm | `npm-release.yaml` | Root tag push |
| `xconn-core` ABI3 wheels/source on PyPI | `python-release.yaml` | Root tag push |
| `xconn_core` on pub.dev | `dart-release.yaml`, called by the SDK workflow | SDK publication |
| Go submodule tag | SDK workflow | SDK publication |

`xconn-webrtc` is not added to crates.io by this flow. Its native builds include
the workspace's pinned SCTP patch; see [SCTP provenance](sctp-patch.md).

## Prepare and release

1. Update the public crate versions and internal dependency requirements, the
   FFI package version, Python's project version, both npm manifests, Dart's
   `pubspec.yaml`, the installer default, Cargo.lock and the changelogs.
2. Run `python3 ci/release_version.py vVERSION`. Every distributed package must
   match the intended tag. No language-specific tags are needed for npm or Dart.
3. Land the release preparation on main and wait for its checks.
4. Open **Actions → Create release tag → Run workflow**, select `main`, and
   enter the version without `v` (for example `0.1.6`). The workflow checks all
   package versions and requires the latest main CI run for that commit to
   have succeeded. It creates a new tag and starts the release workflows.

   Alternatively, tag the reviewed main commit locally and push it:

   ```sh
   git tag -a v0.1.6 -m 'XConn 0.1.6'
   git push origin v0.1.6
   ```

Approve publishing environments if they require review. Builds and registry
publications run independently; a failure does not roll back an already-published
package. Retry the failed jobs from their original run. Never move a published
tag or replace a release asset to retry a release.

The existing `v0.1.5` tag predates this automation and is left untouched. These
workflows apply to a fresh tag created after they land.

## One-time publisher configuration

Create GitHub environments named `npm`, `pypi`, `pub.dev` and `crates.io`. They can
require reviewer approval. Registry authentication uses short-lived OIDC tokens;
no registry token is stored in this repository.

- npm: retain the trusted publisher for owner `xconnio`, repository `xconn`,
  workflow `npm-release.yaml`, environment `npm`.
- PyPI: configure the existing `xconn-core` project's GitHub trusted publisher
  for owner `xconnio`, repository `xconn`, workflow `python-release.yaml`,
  environment `pypi`.
- pub.dev: enable GitHub Actions publishing for `xconn_core` from `xconnio/xconn`,
  with tag pattern `v{{version}}` and required environment `pub.dev`.
- crates.io: configure each of the four existing public crates for repository
  `xconnio/xconn`, workflow `crates-release.yaml`, environment `crates.io`.
  The authentication action obtains and revokes a temporary publishing token.

For the release button, create a GitHub App with **Contents: read and write**
and install it only on `xconnio/xconn`. Set the repository Actions variable
`RELEASE_APP_ID` to its App ID and Actions secret
`RELEASE_APP_PRIVATE_KEY` to its generated private key. The workflow requests a
short-lived token scoped to this repository and revokes it afterward. Existing
tags are rejected and never moved. No commits or version bumps are created.

The App token is needed because tags pushed with the built-in `GITHUB_TOKEN`
do not start other workflows. See [GitHub's workflow triggering documentation](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/trigger-a-workflow).

The Go-tag job uses GitHub's workflow token with repository write permission.
It only creates the expected submodule tag at the SDK commit; an existing tag
pointing elsewhere is an error, never overwritten.

See the official [PyPI trusted publishing guide](https://docs.pypi.org/trusted-publishers/using-a-publisher/),
[pub.dev automation guide](https://dart.dev/tools/pub/automated-publishing), and
[crates.io authentication action](https://github.com/rust-lang/crates-io-auth-action)
for registry setup.

## What is verified

SDK builds retain the Linux x86-64/ARM64, macOS ARM64, Windows amd64 GNU and
Android ARM64 matrix. Shared/static archives include headers, pkg-config files,
checksums and build provenance. C and Go consumers verify host linking; Android
checks verify ELF/API compatibility. See [native installation](native-install.md).

The Rust job verifies each crate as it publishes dependencies before `xconn-core`.
Manual runs verify all four crates without uploading. On retry, already-published
versions are skipped without replacement.
Cargo waits for newly uploaded dependencies to appear in the registry index.

The npm workflow tests the packed package, including browser WebRTC and
WebTransport. The Python matrix builds Linux x86-64/ARM64 manylinux 2.28 wheels
and a macOS ARM64 wheel, checks repaired metadata and installed native crypto, and
publishes one source archive plus the wheels. Python/Go consumers do not need to
build Rust when using the supported published artifacts.

Dart packaging reads `SHA256SUMS` from that tag's completed SDK release and
replaces only `nativeSdkVersion` and `nativeSdkChecksums` in its staged source.
Those generated values are included in the tested package artifact. This avoids
a second source commit after the SDK build while keeping every downloaded
archive pinned by its exact checksum. Publication installs and publishes that
same staged package. Its consumer smoke test verifies automatic native-library
download and loading without a Rust toolchain.

The npm, Python and crates.io workflows can also run manually to build/test
without publishing. Manual runs never publish. To retry publication, rerun the failed jobs from
the original tag-push run. This also preserves the tag-push identity required
by pub.dev.

No package version or tag is changed automatically on main. Source changes,
version updates, the root tag, and any environment approvals remain maintainer
choices.
